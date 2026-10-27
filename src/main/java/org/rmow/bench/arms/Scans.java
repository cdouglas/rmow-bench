package org.rmow.bench.arms;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Evaluator;
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
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.Spans;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.ParquetReads;
import org.rmow.bench.rmow.SplitFile;
import org.rmow.bench.rmow.VersionedInputFile;
import org.rmow.bench.workload.Rows;

/**
 * The one file-scan path every arm uses, as Iceberg's generic reader opens a scan task: a {@code
 * FileScanTask} for the data file and its delete files, Iceberg's delete filter (opening delete
 * files with their lengths), the {@link VersionedInputFile} view (the file's length, the optional
 * cached tail and footer prefetch), the stock Parquet reader, and the filter as a residual.
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
   * Reads {@code df} with {@code deletes} applied and {@code filter} as a residual; rows have the
   * delete filter's required schema ({@code projection}, plus {@code _pos} if there are deletes).
   * The scan is a {@link Spans.Kind#DECODE} span: its requests and its deletes' loading are spans
   * of their own.
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
      Consumer<Record> rows)
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
      Map<String, PositionDeleteIndex> deleteCache,
      Consumer<Record> rows)
      throws IOException {
    // storage holds what is below a cached tail (for a split file's object, the tail is not in
    // storage at all, and the virtual file may be longer than the object)
    long stored = df.fileSizeInBytes() - (tail == null ? 0 : tail.length);
    scan(ctx, reader, df, deletes, projection, filter, ctx.io().newInputFile(df.location(), stored),
        tail, start, length, deleteCache, rows);
  }

  /** @param stored the file's bytes below {@code tail}, as the scan opens them */
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
      Map<String, PositionDeleteIndex> deleteCache,
      Consumer<Record> rows)
      throws IOException {
    FileScanTask task =
        new BaseFileScanTask(
            df, deletes, SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
    LengthAwareDeleteFilter deleteFilter =
        new LengthAwareDeleteFilter(
            ctx.io(), task, Rows.SCHEMA, projection, reader.footerPrefetchBytes(), deleteCache);
    Schema required = deleteFilter.requiredSchema();
    Evaluator residual =
        filter == Expressions.alwaysTrue() ? null : new Evaluator(required.asStruct(), filter);
    InternalRecordWrapper wrapper = new InternalRecordWrapper(required.asStruct());
    VersionedInputFile in =
        new VersionedInputFile(stored, df.fileSizeInBytes(), tail, reader.footerPrefetchBytes());
    try (Spans.Span decode = Spans.open(Spans.Kind.DECODE);
        CloseableIterable<Record> rs =
            deleteFilter.filter(ParquetReads.open(in, required, filter, start, length))) {
      for (Record r : rs) {
        if (residual == null || residual.eval(wrapper.wrap(r))) {
          rows.accept(r);
        }
      }
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
    List<Callable<TableHash>> tasks = new ArrayList<>();
    Map<String, PositionDeleteIndex> deletes = reader.deleteCache() ? new ConcurrentHashMap<>() : null;
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
                  deletes, r -> h.add(r, ctx.verifyRows()));
              return h;
            });
      }
    }
    return run(ctx, reader, tasks);
  }

  /** Whether a file's DV covers every row, as its record count says without a read. */
  private static boolean deadWhole(ReadFile f) {
    for (DeleteFile d : f.deletes()) {
      if (d.format() == FileFormat.PUFFIN && d.recordCount() >= f.file().recordCount()) {
        return true;
      }
    }
    return false;
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
      Map<String, PositionDeleteIndex> deleteCache) {
    PositionDeleteIndex deleted =
        new LengthAwareDeleteFilter(
                ctx.io(), task, Rows.SCHEMA, Rows.SCHEMA, reader.footerPrefetchBytes(), deleteCache)
            .deletedRowPositions();
    if (deleted == null || deleted.isEmpty()) {
      return splits;
    }
    Map<Long, long[]> rowsAt = new HashMap<>(); // row group start -> {first row, rows}
    long first = 0;
    for (RowGroup rg : FooterCodec.read(tail, 0, tail.length - 8).getRow_groups()) {
      rowsAt.put(FooterCodec.startOffset(rg), new long[] {first, rg.getNum_rows()});
      first += rg.getNum_rows();
    }
    List<FileScanTask> live = new ArrayList<>(splits.size());
    for (FileScanTask s : splits) {
      long[] g = rowsAt.get(s.start());
      if (g == null || !coversAll(deleted, g[0], g[1])) {
        live.add(s);
      }
    }
    return live;
  }

  private static boolean coversAll(PositionDeleteIndex deleted, long first, long rows) {
    for (long p = first; p < first + rows; p++) {
      if (!deleted.isDeleted(p)) {
        return false;
      }
    }
    return true;
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
                    splitLength, r -> h.add(r, ctx.verifyRows()));
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
