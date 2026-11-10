package org.rmow.bench.arms;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.rmow.bench.bench.ThreadCounters;
import org.rmow.bench.io.Held;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.Spans;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.ParquetBatches;
import org.rmow.bench.rmow.SplitFile;
import org.rmow.bench.rmow.VersionedInputFile;
import org.rmow.bench.workload.Rows;

/**
 * The one file-scan path every arm uses, as Spark's Iceberg reader opens a scan task: a {@code
 * FileScanTask} for the data file and its delete files, Iceberg's delete filter (opening delete
 * files with their lengths) for the file's position deletes, the {@link VersionedInputFile} view
 * (the file's length, the optional cached tail and footer prefetch), Iceberg's vectorized Parquet
 * reader ({@link ParquetBatches}), and, on each batch, the deletes by position and the filter as a
 * residual ({@link RowBatch}).
 */
final class Scans {

  private static final String SCHEMA_JSON = SchemaParser.toJson(Rows.SCHEMA);
  private static final ThreadMXBean MX = ManagementFactory.getThreadMXBean();
  private static final String SPEC_JSON = PartitionSpecParser.toJson(DataFileWriter.SPEC);

  private Scans() {}

  /** A data file description of one version of an object: its path and that version's length. */
  static DataFile version(String location, long length, long rows) {
    return version(location, length, rows, null);
  }

  /** The same, with the version's split offsets (null: none). */
  static DataFile version(String location, long length, long rows, List<Long> splitOffsets) {
    return DataFiles.builder(DataFileWriter.SPEC)
        .withPath(location)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(length)
        .withRecordCount(rows)
        .withSplitOffsets(splitOffsets)
        .build();
  }

  /**
   * Reads {@code df} with {@code deletes} applied and {@code filter} as a residual, as batches of
   * the live rows of {@code projection} (which may hold {@code _pos}). The scan is a {@link
   * Spans.Kind#DECODE} span: its requests and its deletes' loading are spans of their own.
   *
   * @param tail the file's cached tail, or null to read its footer from storage
   * @param start the split's start, or -1 for the whole file
   */
  static void scan(
      ArmContext ctx,
      Reader reader,
      DataFile df,
      DeleteFile[] deletes,
      Schema projection,
      Expression filter,
      byte[] tail,
      long start,
      long length,
      Consumer<RowBatch> rows)
      throws IOException {
    scan(ctx, reader, df, deletes, projection, filter, tail, start, length, null, rows);
  }

  /** @param deleteCache the read's loaded deletes ({@link Reader#deleteCache}); null: none */
  static void scan(
      ArmContext ctx,
      Reader reader,
      DataFile df,
      DeleteFile[] deletes,
      Schema projection,
      Expression filter,
      byte[] tail,
      long start,
      long length,
      DeleteCache deleteCache,
      Consumer<RowBatch> rows)
      throws IOException {
    // storage holds what is below a cached tail (for a split file's object, the tail is not in
    // storage at all, and the virtual file may be longer than the object)
    long stored = df.fileSizeInBytes() - (tail == null ? 0 : tail.length);
    scan(ctx, reader, df, deletes, projection, filter, ctx.io().newInputFile(df.location(), stored),
        tail, start, length, deleteCache, rows);
  }

  /**
   * @param stored the file's bytes below {@code tail}, as the scan opens them. What the scan holds
   *     ({@link ChunksHeld}, and its deletes without a cache) is released when it ends.
   */
  static void scan(
      ArmContext ctx,
      Reader reader,
      DataFile df,
      DeleteFile[] deletes,
      Schema projection,
      Expression filter,
      InputFile stored,
      byte[] tail,
      long start,
      long length,
      DeleteCache deleteCache,
      Consumer<RowBatch> rows)
      throws IOException {
    FileScanTask task =
        new BaseFileScanTask(
            df, deletes, SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
    try (ChunksHeld chunks = new ChunksHeld(df);
        Held.Scope scanDeletes = Held.scope("scan's deletes released")) {
      LengthAwareDeleteFilter deleteFilter =
          new LengthAwareDeleteFilter(
              ctx.io(), task, Rows.SCHEMA, projection, reader.footerPrefetchBytes(), deleteCache,
              scanDeletes);
      VersionedInputFile in =
          new VersionedInputFile(
              stored, df.fileSizeInBytes(), tail, reader.footerPrefetchBytes(), chunks);
      decode(deleteFilter, in, projection, filter, start, length, rows);
    }
  }

  private static void decode(
      LengthAwareDeleteFilter deleteFilter,
      VersionedInputFile in,
      Schema projection,
      Expression filter,
      long start,
      long length,
      Consumer<RowBatch> rows)
      throws IOException {
    RowBatch.Columns columns = new RowBatch.Columns(projection);
    RowBatch.Residual residual = RowBatch.Residual.of(filter, columns);
    try (Spans.Span decode = Spans.open(Spans.Kind.DECODE);
        CloseableIterable<ParquetBatches.Batch> batches =
            ParquetBatches.open(in, columns.data, filter, start, length)) {
      PositionDeleteIndex deleted = positionDeletes(deleteFilter);
      for (ParquetBatches.Batch b : batches) {
        RowBatch live = RowBatch.select(b, columns, deleted, residual);
        if (live.size() > 0) {
          rows.accept(live);
        }
      }
    }
  }

  /** The file's position deletes, loaded once (a deletes span); null if it has none. */
  private static PositionDeleteIndex positionDeletes(LengthAwareDeleteFilter deleteFilter) {
    if (deleteFilter.hasEqDeletes()) {
      throw new IllegalStateException("No arm writes equality deletes");
    }
    return deleteFilter.hasPosDeletes() ? deleteFilter.deletedRowPositions() : null;
  }

  /** Adds a batch's live rows to {@code h}, checking each one's payload when {@code verify}. */
  static void hash(TableHash h, RowBatch b, boolean verify) {
    for (int k = 0; k < b.size(); k++) {
      if (verify) {
        Rows.verify(b.record(k));
      }
      h.add(b.id(k), b.version(k));
    }
  }


  /** Iceberg fetches Parquet files up to this size whole ({@code Parquet.ReadBuilder}). */
  static final long EAGER_BYTES = 1L << 20;

  /**
   * A file's tail, read the way parquet-java reads a footer: the last 8 bytes, then the footer
   * alone (one request, if the footer-prefetch knob covers it).
   */
  static byte[] readTail(ArmContext ctx, Reader reader, String location, long length)
      throws IOException {
    try (SeekableInputStream in =
        new VersionedInputFile(
                ctx.io().newInputFile(location, length),
                length,
                null,
                reader.footerPrefetchBytes())
            .newStream()) {
      byte[] probe = new byte[8];
      in.seek(length - 8);
      IOUtil.readFully(in, probe, 0, 8);
      int footerLength = ByteBuffer.wrap(probe, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      byte[] tail = new byte[footerLength + 8];
      in.seek(length - tail.length);
      IOUtil.readFully(in, tail, 0, footerLength);
      System.arraycopy(probe, 0, tail, footerLength, 8);
      return tail;
    }
  }

  /** A file a read opens, with the delete files that apply to it. */
  record ReadFile(DataFile file, DeleteFile[] deletes) {}

  /**
   * Reads the files as an engine would: each is planned into split tasks as Iceberg plans them
   * ({@code FileScanTask.split}: one per row group, on the file's split offsets), and the tasks
   * run on the read pool, or one after another without one. A {@code footerCache} reader reads
   * each file version's one footer once and shares it with the file's splits; a stock reader's
   * splits each open the file and read the footer, as Iceberg's readers do today.
   */
  static TableHash readAll(ArmContext ctx, Reader reader, List<ReadFile> files, Expression filter)
      throws IOException {
    DeleteCache deletes = reader.deleteCache() ? new DeleteCache() : null;
    try {
      return readAll(ctx, reader, files, filter, deletes);
    } finally {
      if (deletes != null) {
        deletes.close();
      }
    }
  }

  private static TableHash readAll(
      ArmContext ctx, Reader reader, List<ReadFile> files, Expression filter, DeleteCache deletes)
      throws IOException {
    List<Callable<TableHash>> tasks = new ArrayList<>();
    for (ReadFile f : files) {
      if (reader.skipDeadRowGroups() && deadWhole(f)) {
        continue; // its DV covers every row
      }
      FileScanTask task =
          new BaseFileScanTask(
              f.file(), f.deletes(), SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
      List<FileScanTask> splits = new ArrayList<>();
      byte[] tail = null;
      if (reader.footerCache() && f.file().fileSizeInBytes() <= EAGER_BYTES) {
        // one fetch of the whole version (Iceberg reads files up to 1 MiB whole), not one per
        // split
        splits.add(task);
      } else {
        task.split(reader.splitTargetBytes()).forEach(splits::add);
        if (reader.footerCache() && splits.size() > 1) {
          // the version's one footer, read once and shared by every split of the file
          tail = readTail(ctx, reader, f.file().location(), f.file().fileSizeInBytes());
          if (reader.skipDeadRowGroups() && f.deletes().length > 0) {
            splits = liveSplits(ctx, reader, task, splits, tail, deletes);
          }
        }
      }
      byte[] footer = tail;
      for (FileScanTask split : splits) {
        long start = split.start();
        long length = split.length();
        tasks.add(
            () -> {
              TableHash h = new TableHash();
              scan(ctx, reader, f.file(), f.deletes(), Rows.SCHEMA, filter, footer, start, length,
                  deletes, b -> hash(h, b, ctx.verifyRows()));
              return h;
            });
      }
    }
    return run(ctx, reader, tasks);
  }

  /** Whether a file's DV covers every row, as its record count says without a read. */
  private static boolean deadWhole(ReadFile f) {
    return deadWhole(f.file(), f.deletes());
  }

  /** Whether {@code df}'s DV covers every row, as its record count says without a read. */
  static boolean deadWhole(DataFile df, DeleteFile[] deletes) {
    for (DeleteFile d : deletes) {
      if (d.format() == FileFormat.PUFFIN && d.recordCount() >= df.recordCount()) {
        return true;
      }
    }
    return false;
  }

  /** A version's row groups, from its tail: each {first row, rows, start offset, size}. */
  static List<long[]> rowGroups(byte[] tail) {
    List<RowGroup> rgs = FooterCodec.read(tail, 0, tail.length - 8).getRow_groups();
    List<long[]> groups = new ArrayList<>(rgs.size());
    long first = 0;
    for (RowGroup rg : rgs) {
      groups.add(
          new long[] {
            first, rg.getNum_rows(), FooterCodec.startOffset(rg), FooterCodec.compressedSize(rg)
          });
      first += rg.getNum_rows();
    }
    return groups;
  }

  private static final int ID_COLUMN = Rows.SCHEMA.columns().indexOf(Rows.SCHEMA.findField("id"));

  /**
   * For each of a version's row groups, in its footer's order: whether its {@code id} statistics
   * may hold one of the sorted {@code keys} (true without statistics).
   */
  static boolean[] rowGroupsMayHold(byte[] tail, long[] keys) {
    List<RowGroup> rgs = FooterCodec.read(tail, 0, tail.length - 8).getRow_groups();
    boolean[] may = new boolean[rgs.size()];
    for (int g = 0; g < rgs.size(); g++) {
      var meta = rgs.get(g).getColumns().get(ID_COLUMN).getMeta_data();
      if (!meta.isSetStatistics() || !meta.getStatistics().isSetMin_value()
          || !meta.getStatistics().isSetMax_value()) {
        may[g] = true;
        continue;
      }
      long lo = littleEndianLong(meta.getStatistics().getMin_value());
      long hi = littleEndianLong(meta.getStatistics().getMax_value());
      int i = Arrays.binarySearch(keys, lo);
      int first = i >= 0 ? i : -i - 1;
      may[g] = first < keys.length && keys[first] <= hi;
    }
    return may;
  }

  private static long littleEndianLong(byte[] b) {
    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  /**
   * The start offsets of {@code df}'s row groups that its deletes cover whole: the deletes loaded
   * once (through {@code deleteCache}, if not null, so later scans reuse them), checked against the
   * row groups of {@code tail}.
   */
  static Set<Long> deadRowGroups(
      ArmContext ctx,
      int footerPrefetch,
      DataFile df,
      DeleteFile[] deletes,
      byte[] tail,
      DeleteCache deleteCache) {
    // without the read's cache, what planning loads is held while it plans
    try (DeleteCache planning = deleteCache == null ? new DeleteCache() : null) {
      return deadRowGroups(
          loadDeletes(ctx, footerPrefetch, df, deletes, deleteCache == null ? planning : deleteCache),
          tail);
    }
  }

  /**
   * {@code df}'s deleted positions: its {@code deletes} loaded from storage as a read loads them
   * (each load a {@link Spans.Kind#DELETES} span), through {@code cache}, which holds them until it
   * closes.
   */
  static PositionDeleteIndex loadDeletes(
      ArmContext ctx, int footerPrefetch, DataFile df, DeleteFile[] deletes, DeleteCache cache) {
    FileScanTask task =
        new BaseFileScanTask(
            df,
            deletes,
            SCHEMA_JSON,
            SPEC_JSON,
            ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
    LengthAwareDeleteFilter filter =
        new LengthAwareDeleteFilter(
            ctx.io(), task, Rows.SCHEMA, Rows.SCHEMA, footerPrefetch, cache, null);
    return filter.deletedRowPositions();
  }

  private static Set<Long> deadRowGroups(PositionDeleteIndex deleted, byte[] tail) {
    if (deleted == null || deleted.isEmpty()) {
      return Set.of();
    }
    DeletedRanges ranges = new DeletedRanges(deleted);
    Set<Long> dead = new HashSet<>();
    for (long[] g : rowGroups(tail)) {
      if (ranges.coversAll(g[0], g[1])) {
        dead.add(g[2]);
      }
    }
    return dead;
  }

  /**
   * The splits of a file whose row groups its deletes do not cover whole: the deletes loaded once
   * to plan (through the read's delete cache, if it has one, so its tasks reuse them), checked
   * against the row groups of the footer the read holds.
   */
  private static List<FileScanTask> liveSplits(
      ArmContext ctx,
      Reader reader,
      FileScanTask task,
      List<FileScanTask> splits,
      byte[] tail,
      DeleteCache deleteCache) {
    Set<Long> dead =
        deadRowGroups(
            ctx, reader.footerPrefetchBytes(), task.file(), task.deletes().toArray(new DeleteFile[0]),
            tail, deleteCache);
    if (dead.isEmpty()) {
      return splits;
    }
    List<FileScanTask> live = new ArrayList<>(splits.size());
    for (FileScanTask s : splits) {
      if (!dead.contains(s.start())) {
        live.add(s);
      }
    }
    return live;
  }

  /**
   * Reads the files in event-time order ({@code ts}, then {@code id}), as a merge of sorted runs,
   * one per row group, on one thread. The read fetches each file's footer once and opens each row
   * group only when the merge reaches its smallest {@code ts} (its statistics), so it holds at once
   * only the row groups whose event times overlap: one per run in flight ({@link ChunksHeld}). A
   * file of at most 1 MiB, which Iceberg fetches whole, is decoded and sorted in memory as one run,
   * its rows held until merged. Every run must come out sorted, so every write must be
   * ({@code sortWrites}) and the initial load's event times in id order ({@code
   * initialJitterRows} 0); a row out of order fails the read. Needs {@code footerCache}; with
   * {@code deleteCache}, each file's deletes load once. With {@code skipDeadRowGroups} it leaves
   * out, as the full read does, each file whose deletes cover every row and each row group they
   * cover whole (the deletes loaded once to plan, through the read's delete cache if it has one).
   */
  static TableHash readSorted(ArmContext ctx, Reader reader, List<ReadFile> files)
      throws IOException {
    if (!reader.footerCache()) {
      throw new IllegalArgumentException(
          "Reader " + reader.name() + ": a sorted read plans from footers, so needs footerCache");
    }
    try (DeleteCache deletes = reader.deleteCache() ? new DeleteCache() : null) {
      List<Run> runs = new ArrayList<>();
      for (ReadFile f : files) {
        if (reader.skipDeadRowGroups() && deadWhole(f)) {
          continue; // its DV covers every row
        }
        long size = f.file().fileSizeInBytes();
        if (size <= EAGER_BYTES) {
          runs.add(new Run(f, null, -1, -1, Long.MIN_VALUE)); // fetched whole: opened first
          continue;
        }
        byte[] tail = readTail(ctx, reader, f.file().location(), size);
        Set<Long> dead =
            reader.skipDeadRowGroups() && f.deletes().length > 0
                ? deadRowGroups(ctx, reader.footerPrefetchBytes(), f.file(), f.deletes(), tail, deletes)
                : Set.of();
        for (RowGroup rg : FooterCodec.read(tail, 0, tail.length - 8).getRow_groups()) {
          if (dead.contains(FooterCodec.startOffset(rg))) {
            continue; // its deletes cover every row
          }
          runs.add(
              new Run(f, tail, FooterCodec.startOffset(rg), FooterCodec.compressedSize(rg),
                  minTs(rg)));
        }
      }
      runs.sort(Comparator.comparingLong(Run::minTs));
      return merge(ctx, reader, runs, deletes);
    }
  }

  /** A row group of a merge, a sorted run: all of a small file ({@code start} -1), or one. */
  private record Run(ReadFile file, byte[] tail, long start, long length, long minTs) {}

  private static final int TS_COLUMN = Rows.SCHEMA.columns().indexOf(Rows.SCHEMA.findField("ts"));

  /** A row group's smallest {@code ts}, from its statistics; the least long without them. */
  private static long minTs(RowGroup rg) {
    var meta = rg.getColumns().get(TS_COLUMN).getMeta_data();
    if (!meta.isSetStatistics() || !meta.getStatistics().isSetMin_value()) {
      return Long.MIN_VALUE;
    }
    byte[] b = meta.getStatistics().getMin_value();
    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  private static TableHash merge(
      ArmContext ctx, Reader reader, List<Run> runs, DeleteCache deletes) throws IOException {
    TableHash h = new TableHash();
    PriorityQueue<OpenRun> open =
        new PriorityQueue<>(
            Comparator.comparingLong((OpenRun o) -> o.ts).thenComparingLong(o -> o.id));
    int next = 0;
    long lastTs = Long.MIN_VALUE;
    long lastId = Long.MIN_VALUE;
    try (Spans.Span decode = Spans.open(Spans.Kind.DECODE)) {
      try {
        while (true) {
          // every run that may hold a row at or before the smallest open one is opened first
          while (next < runs.size() && (open.isEmpty() || runs.get(next).minTs() <= open.peek().ts)) {
            OpenRun o = OpenRun.open(ctx, reader, runs.get(next++), deletes);
            if (o.advance()) {
              open.add(o);
            } else {
              o.close();
            }
          }
          OpenRun o = open.poll();
          if (o == null) {
            return h;
          }
          if (o.ts < lastTs || (o.ts == lastTs && o.id < lastId)) {
            o.close();
            throw new IllegalStateException(
                "Sorted read out of order: row " + o.id + " at ts " + o.ts + " after row " + lastId
                    + " at ts " + lastTs + ", in " + o.what);
          }
          lastTs = o.ts;
          lastId = o.id;
          o.addTo(h, ctx.verifyRows());
          if (o.advance()) {
            open.add(o);
          } else {
            o.close();
          }
        }
      } finally {
        for (OpenRun o : open) {
          o.close();
        }
      }
    }
  }

  /** An open run of a merge: its rows, and the one it gives next ({@code id}, {@code ts}). */
  private abstract static class OpenRun implements AutoCloseable {
    final String what;
    long ts;
    long id;

    OpenRun(String what) {
      this.what = what;
    }

    /** Moves to the run's next row; false at its end. */
    abstract boolean advance();

    /** Adds the current row to {@code h}, checking its payload when {@code verify}. */
    abstract void addTo(TableHash h, boolean verify);

    /**
     * A row group's scan, its batches decoded as the merge reaches them; or a small file, read
     * whole and sorted in memory, its rows held until merged.
     */
    static OpenRun open(ArmContext ctx, Reader reader, Run run, DeleteCache deletes)
        throws IOException {
      DataFile df = run.file().file();
      String what = df.location() + (run.start() < 0 ? "" : " at " + run.start());
      if (run.start() < 0) {
        return SortedRows.read(ctx, reader, run, deletes, what);
      }
      FileScanTask task =
          new BaseFileScanTask(
              df, run.file().deletes(), SCHEMA_JSON, SPEC_JSON,
              ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
      ChunksHeld chunks = new ChunksHeld(df);
      Held.Scope scanDeletes = Held.scope("scan's deletes released");
      LengthAwareDeleteFilter filter =
          new LengthAwareDeleteFilter(
              ctx.io(), task, Rows.SCHEMA, Rows.SCHEMA, reader.footerPrefetchBytes(), deletes,
              scanDeletes);
      VersionedInputFile in =
          new VersionedInputFile(
              ctx.io().newInputFile(df.location(), df.fileSizeInBytes() - run.tail().length),
              df.fileSizeInBytes(), run.tail(), reader.footerPrefetchBytes(), chunks);
      RowBatch.Columns columns = new RowBatch.Columns(Rows.SCHEMA);
      PositionDeleteIndex deleted = positionDeletes(filter);
      CloseableIterable<ParquetBatches.Batch> batches =
          ParquetBatches.open(in, columns.data, Expressions.alwaysTrue(), run.start(), run.length());
      return new BatchRows(
          what,
          batches,
          columns,
          deleted,
          () -> {
            try {
              batches.close();
            } finally {
              scanDeletes.close();
              chunks.close();
            }
          });
    }

    @Override
    public abstract void close() throws IOException;
  }

  /** A row group's run: its live rows, a batch at a time. */
  private static final class BatchRows extends OpenRun {
    private final Iterator<ParquetBatches.Batch> batches;
    private final RowBatch.Columns columns;
    private final PositionDeleteIndex deleted;
    private final AutoCloseable resources;
    private RowBatch rows = null;
    private int next = 0; // the batch's next live row
    private int at = -1; // the current one

    BatchRows(
        String what,
        CloseableIterable<ParquetBatches.Batch> batches,
        RowBatch.Columns columns,
        PositionDeleteIndex deleted,
        AutoCloseable resources) {
      super(what);
      this.batches = batches.iterator();
      this.columns = columns;
      this.deleted = deleted;
      this.resources = resources;
    }

    @Override
    boolean advance() {
      while (rows == null || next >= rows.size()) {
        if (!batches.hasNext()) {
          return false;
        }
        rows = RowBatch.select(batches.next(), columns, deleted, null);
        next = 0;
      }
      at = next++;
      id = rows.id(at);
      ts = rows.ts(at);
      return true;
    }

    @Override
    void addTo(TableHash h, boolean verify) {
      if (verify) {
        Rows.verify(rows.record(at));
      }
      h.add(id, rows.version(at));
    }

    @Override
    public void close() throws IOException {
      try {
        resources.close();
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /** A small file's run: its live rows read whole, sorted by {@code ts} then {@code id}, held. */
  private static final class SortedRows extends OpenRun {
    private final long[][] rows; // {ts, id, version}, sorted
    private final List<Record> records; // with verifyRows, in rows' order
    private final Held.Scope held;
    private int at = -1;

    private SortedRows(
        String what, long[][] rows, List<Record> records, Held.Scope held) {
      super(what);
      this.rows = rows;
      this.records = records;
      this.held = held;
    }

    static SortedRows read(ArmContext ctx, Reader reader, Run run, DeleteCache deletes, String what)
        throws IOException {
      DataFile df = run.file().file();
      List<long[]> rows = new ArrayList<>();
      Map<Long, Record> records = ctx.verifyRows() ? new HashMap<>() : null;
      scan(ctx, reader, df, run.file().deletes(), Rows.SCHEMA, Expressions.alwaysTrue(), null,
          -1, -1, deletes,
          b -> {
            for (int k = 0; k < b.size(); k++) {
              rows.add(new long[] {b.ts(k), b.id(k), b.version(k)});
              if (records != null) {
                records.put(b.id(k), b.record(k));
              }
            }
          });
      long[][] sorted = rows.toArray(new long[0][]);
      Arrays.sort(sorted, Comparator.comparingLong((long[] r) -> r[0]).thenComparingLong(r -> r[1]));
      List<Record> inOrder = null;
      if (records != null) {
        inOrder = new ArrayList<>(sorted.length);
        for (long[] r : sorted) {
          inOrder.add(records.get(r[1]));
        }
      }
      Held.Scope held = Held.scope("merged " + what);
      held.hold(Held.Kind.ROWS, sorted.length, "read " + what + " whole to sort");
      return new SortedRows(what, sorted, inOrder, held);
    }

    @Override
    boolean advance() {
      if (at + 1 >= rows.length) {
        return false;
      }
      at++;
      ts = rows[at][0];
      id = rows[at][1];
      return true;
    }

    @Override
    void addTo(TableHash h, boolean verify) {
      if (verify) {
        Rows.verify(records.get(at));
      }
      h.add(id, (int) rows[at][2]);
    }

    @Override
    public void close() {
      held.close();
    }
  }

  /**
   * Reads footer-only versions ({@link SplitFile}): each version's one footer, then every object
   * it names as a virtual Parquet file whose footer comes from memory ({@link SplitFile#views}),
   * so an object costs only its row groups' reads. Each object is planned as {@link #readAll}
   * plans an appendable file's version: into split tasks on its own split offsets, one per row
   * group, except that a footer-cache reader reads an object of at most 1 MiB in one task (Iceberg
   * fetches it whole, so each split would fetch it again). A footer-cache reader reads the
   * version's footer once; a stock one reads it again for every split, as stock readers re-read a
   * file's footer for every split. No stock reader reads these files at all: this is the reader
   * an engine would need.
   */
  static TableHash readSplit(
      ArmContext ctx, Reader reader, List<ReadFile> versions, Expression filter)
      throws IOException {
    List<Callable<TableHash>> tasks = new ArrayList<>();
    for (ReadFile f : versions) {
      String location = f.file().location();
      long length = f.file().fileSizeInBytes();
      byte[] tail = readFooterObject(ctx, reader, location, length);
      FileMetaData footer = FooterCodec.read(tail, 0, tail.length - 8);
      for (SplitFile.ObjectView v : SplitFile.views(location, footer)) {
        DataFile object = version(v.location(), v.length(), v.rows(), v.splitOffsets());
        FileScanTask whole =
            new BaseFileScanTask(
                object, f.deletes(), SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
        Iterable<FileScanTask> splits =
            reader.footerCache() && v.length() <= EAGER_BYTES
                ? List.of(whole)
                : whole.split(reader.splitTargetBytes());
        for (FileScanTask split : splits) {
          long start = split.start();
          long splitLength = split.length();
          tasks.add(
              () -> {
                if (!reader.footerCache()) {
                  readFooterObject(ctx, reader, location, length);
                }
                TableHash h = new TableHash();
                scan(ctx, reader, object, f.deletes(), Rows.SCHEMA, filter, v.tail(), start,
                    splitLength, b -> hash(h, b, ctx.verifyRows()));
                return h;
              });
        }
      }
    }
    return run(ctx, reader, tasks);
  }

  /**
   * A file version's tail (footer, length, magic), read as an open reads it: fetched whole when it
   * is at most 1 MiB, as Iceberg fetches small files (one request); otherwise read as
   * parquet-java reads a footer. Footer-only objects ({@link SplitFile}) are read this way too.
   */
  static byte[] readFooterObject(ArmContext ctx, Reader reader, String location, long length)
      throws IOException {
    if (length > EAGER_BYTES) {
      return readTail(ctx, reader, location, length);
    }
    return tailOf(readWhole(ctx, location, length));
  }

  /** A file of at most 1 MiB, fetched whole in one request, as Iceberg fetches it. */
  static byte[] readWhole(ArmContext ctx, String location, long length) throws IOException {
    byte[] all = new byte[(int) length];
    try (SeekableInputStream in =
        new VersionedInputFile(ctx.io().newInputFile(location, length), length).newStream()) {
      ((RangeReadable) in).readFully(0, all, 0, all.length);
    }
    return all;
  }

  /** A whole Parquet file's tail: its footer, the footer's length and the magic. */
  static byte[] tailOf(byte[] file) {
    int footer = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    return Arrays.copyOfRange(file, file.length - footer - 8, file.length);
  }

  /**
   * Runs a read's tasks on the reader's pool, or one after another without one. A task on the
   * pool adds its thread CPU, and its wall time and spans ({@link Spans}), to the trace's pool
   * totals (the arm's thread counts the others itself). When the trace logs requests, each task's
   * requests carry its number, and its end its times.
   */
  private static TableHash run(ArmContext ctx, Reader reader, List<Callable<TableHash>> tasks)
      throws IOException {
    IoTrace trace = ctx.io().trace();
    boolean inline = reader.pool() == null;
    boolean logging = trace.logging();
    if (logging || !inline) {
      int group = logging ? trace.nextTasks(tasks.size()) : 0;
      List<Callable<TableHash>> tagged = new ArrayList<>(tasks.size());
      for (int i = 0; i < tasks.size(); i++) {
        int id = group + i;
        Callable<TableHash> t = tasks.get(i);
        tagged.add(
            () -> {
              if (logging) {
                IoTrace.enterTask(id);
              }
              Spans.measure(); // a pool's thread is measured only inside its tasks
              Spans.Totals spans0 = Spans.totals();
              long start = System.nanoTime();
              long cpu0 = MX.getCurrentThreadCpuTime();
              long user0 = MX.getCurrentThreadUserTime();
              ThreadCounters.Sample counts0 = ThreadCounters.now();
              try {
                return t.call();
              } finally {
                long cpu = MX.getCurrentThreadCpuTime() - cpu0;
                long wall = System.nanoTime() - start;
                ThreadCounters.Sample d =
                    counts0 == null ? null : ThreadCounters.now().minus(counts0);
                Spans.unmeasure();
                if (!inline) {
                  trace.addPoolCpu(cpu, MX.getCurrentThreadUserTime() - user0);
                  trace.addPoolSpans(wall, Spans.totals().minus(spans0));
                  if (d != null) {
                    trace.addPoolCounts(d.instructions(), d.cycles());
                  }
                }
                if (logging) {
                  trace.taskDone(id, group, start, wall, cpu, d == null ? -1 : d.instructions(),
                      d == null ? -1 : d.cycles(), inline);
                  IoTrace.exitTask();
                }
              }
            });
      }
      tasks = tagged;
    }
    TableHash hash = new TableHash();
    try {
      if (reader.pool() == null) {
        for (Callable<TableHash> t : tasks) {
          hash.addAll(t.call());
        }
      } else {
        for (Future<TableHash> f : reader.pool().invokeAll(tasks)) {
          hash.addAll(f.get());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    return hash;
  }
}
