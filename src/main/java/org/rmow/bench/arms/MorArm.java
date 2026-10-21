package org.rmow.bench.arms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * Merge-on-read: each batch writes one data file (inserts and the new versions of updated rows),
 * and the subclass marks the replaced and deleted rows in delete files.
 *
 * <p>Table state (data files, delete files, sequence numbers, and their history for time travel)
 * is kept in memory: level L1 measures data-plane I/O only, so no Iceberg metadata is written.
 * Reads plan as Iceberg does: files are pruned by their column metrics, and each data file gets
 * the delete files Iceberg's {@code DeleteFileIndex} would give it (see {@link #deletesFor}).
 */
public abstract class MorArm implements Arm {

  /** A live delete file and the sequence number of the batch that wrote it. */
  protected record Delete(DeleteFile file, long seq) {}

  private record State(List<DataFile> files, List<Long> fileSeqs, List<Delete> deletes) {}

  protected final ArmContext ctx;
  /** Every data file written, by index; {@link #live} says which are in the table. */
  protected final List<DataFile> files = new ArrayList<>();
  private final List<Long> fileSeqs = new ArrayList<>();
  protected final BitSet live = new BitSet();
  /** Live delete files; subclasses add and replace them. */
  protected final List<Delete> deletes = new ArrayList<>();
  private final List<State> history = new ArrayList<>();
  /** Every delete object written and still stored, with its size (for space accounting). */
  protected final Map<String, Long> deleteObjects = new HashMap<>();
  private static final int ID_FIELD = Rows.SCHEMA.findField("id").fieldId();

  protected final int[] fileOfId; // -1: absent or deleted
  protected final int[] posOfId;
  private final List<Integer> removedRows = new ArrayList<>(); // per file: rows deleted or moved
  private int lastBatch = -1;

  protected MorArm(ArmContext ctx) {
    this.ctx = ctx;
    this.fileOfId = new int[(int) ctx.log().totalIds()];
    this.posOfId = new int[(int) ctx.log().totalIds()];
    Arrays.fill(fileOfId, -1);
  }

  @Override
  public String name() {
    return ctx.name();
  }

  @Override
  public void load(List<InitialLoad.InitialFile> initial) {
    for (InitialLoad.InitialFile f : initial) {
      int k = files.size();
      files.add(f.file());
      fileSeqs.add(0L);
      removedRows.add(0);
      live.set(k);
      for (long i = 0; i < f.rows(); i++) {
        fileOfId[(int) (f.firstId() + i)] = k;
        posOfId[(int) (f.firstId() + i)] = (int) i;
      }
    }
    history.add(snapshot());
  }

  private State snapshot() {
    List<DataFile> liveFiles = new ArrayList<>();
    List<Long> liveSeqs = new ArrayList<>();
    for (int k = live.nextSetBit(0); k >= 0; k = live.nextSetBit(k + 1)) {
      liveFiles.add(files.get(k));
      liveSeqs.add(fileSeqs.get(k));
    }
    return new State(liveFiles, liveSeqs, List.copyOf(deletes));
  }

  /** Removes a data file from the table (a merge replaced it), with its deletes. */
  protected void retire(int k) {
    live.clear(k);
    String location = files.get(k).location();
    deletes.removeIf(d -> location.equals(ContentFileUtil.referencedDataFileLocation(d.file())));
  }

  /** Records the table state after a batch; subclasses that override apply must call it. */
  protected void endBatch() {
    history.add(snapshot());
  }

  /**
   * The data sequence number of a batch's files. Batches take even numbers and their compactions
   * the odd number after, so deletes committed before a compaction never apply to its files.
   */
  protected long batchSeq(OpLog.Batch b) {
    lastBatch = b.index();
    return 2L * (b.index() + 1);
  }

  /** Runs the locate phase when the layout asks writers to find their rows by scanning. */
  protected void locate(OpLog.Batch b) throws IOException {
    if ("scan".equals(ctx.layout().locate)) {
      ctx.phases().run("locate", () -> locateByScan(b));
    }
  }

  /**
   * Finds the batch's updated and deleted rows as a MERGE would: reads {@code id} and {@code _pos}
   * (deletes applied) of every live data file whose {@code id} bounds may hold one of them, with
   * row groups pruned to the targets' id range. Checks every position found against the index.
   */
  private void locateByScan(OpLog.Batch b) throws IOException {
    long[] targets = new long[b.updateIds().length + b.deleteIds().length];
    System.arraycopy(b.updateIds(), 0, targets, 0, b.updateIds().length);
    System.arraycopy(b.deleteIds(), 0, targets, b.updateIds().length, b.deleteIds().length);
    if (targets.length == 0) {
      return;
    }
    Arrays.sort(targets);
    Schema projection = new Schema(Rows.SCHEMA.findField("id"), MetadataColumns.ROW_POSITION);
    Expression range =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", targets[0]),
            Expressions.lessThanOrEqual("id", targets[targets.length - 1]));
    int[] found = {0};
    for (int k = live.nextSetBit(0); k >= 0; k = live.nextSetBit(k + 1)) {
      DataFile df = files.get(k);
      if (!mayHoldAny(df, targets)) {
        continue;
      }
      int file = k;
      scan(
          df,
          fileSeqs.get(k),
          deletes,
          projection,
          range,
          r -> {
            long id = (Long) r.get(0);
            if (Arrays.binarySearch(targets, id) >= 0) {
              // locate runs before the batch moves anything, so the index is still exact
              if (fileOfId[(int) id] != file || posOfId[(int) id] != (Long) r.get(1)) {
                throw new IllegalStateException(
                    "Located row " + id + " at file " + file + " pos " + r.get(1)
                        + "; the index says file " + fileOfId[(int) id] + " pos " + posOfId[(int) id]);
              }
              found[0]++;
            }
          });
    }
    if (found[0] != targets.length) {
      throw new IllegalStateException(
          "Located " + found[0] + " of the batch's " + targets.length + " rows");
    }
  }

  /** Whether a file's {@code id} bounds contain any of the sorted {@code targets}. */
  private static boolean mayHoldAny(DataFile df, long[] targets) {
    ByteBuffer lo = df.lowerBounds() == null ? null : df.lowerBounds().get(ID_FIELD);
    ByteBuffer hi = df.upperBounds() == null ? null : df.upperBounds().get(ID_FIELD);
    if (lo == null || hi == null) {
      return true;
    }
    long lower = Conversions.<Long>fromByteBuffer(Types.LongType.get(), lo);
    long upper = Conversions.<Long>fromByteBuffer(Types.LongType.get(), hi);
    int i = Arrays.binarySearch(targets, lower);
    int first = i >= 0 ? i : -i - 1;
    return first < targets.length && targets[first] <= upper;
  }

  /** Notes that a row of data file {@code k} was deleted or moved (for the delete ratio). */
  protected void removed(int k) {
    removedRows.set(k, removedRows.get(k) + 1);
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    long seq = batchSeq(b);
    locate(b);
    // the old locations of updated and deleted rows, before the updates move them
    Map<Integer, List<Integer>> deleted = new HashMap<>();
    for (long id : b.updateIds()) {
      deleted.computeIfAbsent(fileOfId[(int) id], k -> new ArrayList<>()).add(posOfId[(int) id]);
      removed(fileOfId[(int) id]);
    }
    for (long id : b.deleteIds()) {
      deleted.computeIfAbsent(fileOfId[(int) id], k -> new ArrayList<>()).add(posOfId[(int) id]);
      removed(fileOfId[(int) id]);
      fileOfId[(int) id] = -1;
    }

    ctx.phases().run("write", () -> writeDataFile(batchName(b), batchRows(b), seq));
    if (!deleted.isEmpty()) {
      ctx.phases().run("delete-write", () -> writeDeletes(b.index(), seq, deleted));
    }
    endBatch();
  }

  protected static String batchName(OpLog.Batch b) {
    return String.format("batch-%05d", b.index());
  }

  /** A batch's rows to write, as {id, version}: the new versions of updated rows and inserts. */
  protected static List<long[]> batchRows(OpLog.Batch b) {
    List<long[]> rows = new ArrayList<>(b.insertIds().length + b.updateIds().length);
    for (int i = 0; i < b.updateIds().length; i++) {
      rows.add(new long[] {b.updateIds()[i], b.updateVersions()[i]});
    }
    for (long id : b.insertIds()) {
      rows.add(new long[] {id, 0});
    }
    return rows;
  }

  /**
   * Writes this batch's deletes: {@code deleted} maps data file indexes (into {@link #files}) to
   * the positions to delete; the subclass updates {@link #deletes} and {@link #deleteObjects}.
   */
  protected abstract void writeDeletes(int batch, long seq, Map<Integer, List<Integer>> deleted)
      throws IOException;

  /**
   * Writes {@code rows} ({id, version}) as a new live data file {@code data/<name>.parquet};
   * returns its index, or -1 if there are no rows.
   */
  protected int writeDataFile(String name, List<long[]> rows, long seq) throws IOException {
    if (rows.isEmpty()) {
      return -1;
    }
    if (ctx.layout().sortWrites) {
      rows.sort(
          Comparator.comparingLong((long[] r) -> ctx.log().ts(r[0])).thenComparingLong(r -> r[0]));
    }
    String location = String.format("%s/data/%s.parquet", ctx.root(), name);
    List<Record> records = new ArrayList<>(rows.size());
    for (long[] r : rows) {
      records.add(Rows.record(r[0], ctx.log().ts(r[0]), (int) r[1]));
    }
    DataFile df = DataFileWriter.write(ctx.io(), location, records, ctx.layout().writerProperties());
    int k = files.size();
    files.add(df);
    fileSeqs.add(seq);
    removedRows.add(0);
    live.set(k);
    for (int pos = 0; pos < rows.size(); pos++) {
      int id = (int) rows.get(pos)[0];
      fileOfId[id] = k;
      posOfId[id] = pos;
    }
    return k;
  }

  /**
   * Writes deletion vectors for {@code deleted} (data file index to positions) in one Puffin file;
   * a data file's existing DV is read from storage and merged into its new one, which replaces it.
   */
  protected void writeDVs(int batch, long seq, Map<Integer, List<Integer>> deleted)
      throws IOException {
    String location = String.format("%s/deletes/dv-%05d.puffin", ctx.root(), batch);
    Map<String, List<Long>> positions = new HashMap<>();
    for (Map.Entry<Integer, List<Integer>> e : deleted.entrySet()) {
      List<Long> ps = new ArrayList<>(e.getValue().size());
      e.getValue().forEach(p -> ps.add((long) p));
      positions.put(files.get(e.getKey()).location(), ps);
    }
    DeleteWriteResult result =
        Dvs.write(
            ctx,
            location,
            positions,
            path -> {
              Delete d = dvOf(path);
              return d == null ? null : d.file();
            });
    for (DeleteFile dv : result.deleteFiles()) {
      Delete previous = dvOf(dv.referencedDataFile());
      if (previous != null) {
        deletes.remove(previous);
      }
      deletes.add(new Delete(dv, seq));
    }
    if (!result.deleteFiles().isEmpty()) {
      deleteObjects.put(location, result.deleteFiles().get(0).fileSizeInBytes());
    }
  }

  private Delete dvOf(String dataFile) {
    for (Delete d : deletes) {
      if (dataFile.equals(d.file().referencedDataFile())) {
        return d;
      }
    }
    return null;
  }

  /** The live rows of data file {@code k} (its deletes applied), read from storage. */
  protected List<Record> readLive(int k) throws IOException {
    List<Record> out = new ArrayList<>();
    DataFile df = files.get(k);
    scan(df, fileSeqs.get(k), deletes, Expressions.alwaysTrue(), out::add);
    return out;
  }

  /** Whether compaction may rewrite data file {@code k} (TailCoW keeps its open tail files). */
  protected boolean compactable(int k) {
    return true;
  }

  private double deleteRatio(int k) {
    return removedRows.get(k) / (double) files.get(k).recordCount();
  }

  /**
   * Bin-packing compaction, as Iceberg's {@code RewriteDataFiles} with its defaults: files smaller
   * than {@code compactSmallFileFraction} of the target size, or with at least {@code
   * compactDeleteRatio} of their rows deleted, are packed in file order into groups of about the
   * target size; each group is rewritten (deletes applied, rows sorted if {@code sortWrites}) as
   * one file. A group of one file is rewritten only for its deletes.
   */
  @Override
  public void compact() throws IOException {
    if (lastBatch < 0) {
      return;
    }
    LayoutSpec layout = ctx.layout();
    long seq = 2L * (lastBatch + 1) + 1;
    List<List<Integer>> groups = new ArrayList<>();
    List<Integer> group = new ArrayList<>();
    double groupBytes = 0;
    for (int k = live.nextSetBit(0); k >= 0; k = live.nextSetBit(k + 1)) {
      long size = files.get(k).fileSizeInBytes();
      boolean small = size < layout.compactSmallFileFraction * layout.targetFileBytes;
      if (!compactable(k) || !(small || deleteRatio(k) >= layout.compactDeleteRatio)) {
        continue;
      }
      double liveSize = size * (1 - deleteRatio(k));
      if (!group.isEmpty() && groupBytes + liveSize > layout.targetFileBytes) {
        groups.add(group);
        group = new ArrayList<>();
        groupBytes = 0;
      }
      group.add(k);
      groupBytes += liveSize;
    }
    if (!group.isEmpty()) {
      groups.add(group);
    }
    int n = 0;
    for (List<Integer> g : groups) {
      if (g.size() == 1 && deleteRatio(g.get(0)) < layout.compactDeleteRatio) {
        continue;
      }
      List<long[]> rows = new ArrayList<>();
      for (int k : g) {
        for (Record r : readLive(k)) {
          rows.add(new long[] {(Long) r.get(Rows.ID), (Integer) r.get(Rows.VERSION)});
        }
      }
      writeDataFile(String.format("compact-%05d-%03d", lastBatch, n++), rows, seq);
      for (int k : g) {
        retire(k);
      }
    }
  }

  @Override
  public TableHash read(Expression filter, Reader reader) throws IOException {
    return read(snapshot(), filter, reader);
  }

  @Override
  public TableHash readAfter(int steps, Reader reader) throws IOException {
    return read(history.get(steps), Expressions.alwaysTrue(), reader);
  }

  /**
   * The delete files Iceberg 1.12's {@code DeleteFileIndex} gives a data file: DVs and
   * file-scoped position deletes that reference it, and every partition-scoped position delete
   * file of its partition (this table has one) with a sequence number at least its own. Partition
   * deletes are matched on sequence number only; their {@code file_path} bounds prune row groups
   * when the delete file is read, not delete files when planning.
   */
  private static DeleteFile[] deletesFor(DataFile df, long dataSeq, List<Delete> deletes) {
    List<DeleteFile> out = new ArrayList<>();
    for (Delete d : deletes) {
      if (d.seq() < dataSeq) {
        continue;
      }
      String ref = ContentFileUtil.referencedDataFileLocation(d.file());
      if (ref == null || ref.equals(df.location())) {
        out.add(d.file());
      }
    }
    return out.toArray(new DeleteFile[0]);
  }

  private TableHash read(State state, Expression filter, Reader reader) throws IOException {
    InclusiveMetricsEvaluator prune = new InclusiveMetricsEvaluator(Rows.SCHEMA, filter);
    List<Scans.ReadFile> toRead = new ArrayList<>();
    for (int i = 0; i < state.files().size(); i++) {
      DataFile df = state.files().get(i);
      if (prune.eval(df)) {
        toRead.add(
            new Scans.ReadFile(df, deletesFor(df, state.fileSeqs().get(i), state.deletes())));
      }
    }
    return Scans.readAll(ctx, reader, toRead, filter);
  }

  /** Reads one data file as Iceberg's generic reader does: its deletes and the filter applied. */
  private void scan(
      DataFile df, long seq, List<Delete> liveDeletes, Expression filter, Consumer<Record> rows)
      throws IOException {
    scan(df, seq, liveDeletes, Rows.SCHEMA, filter, rows);
  }

  private void scan(
      DataFile df,
      long seq,
      List<Delete> liveDeletes,
      Schema projection,
      Expression filter,
      Consumer<Record> rows)
      throws IOException {
    Scans.scan(
        ctx,
        Reader.writer(ctx.layout()),
        df,
        deletesFor(df, seq, liveDeletes),
        projection,
        filter,
        null,
        -1,
        -1,
        rows);
  }

  /** The live bytes of one delete file (a DV counts its blob, not its whole Puffin file). */
  protected abstract long liveBytes(DeleteFile d);

  @Override
  public ArmStats stats() {
    long dataBytes = 0;
    long liveDataBytes = 0;
    long rowGroups = 0;
    for (int k = 0; k < files.size(); k++) {
      DataFile df = files.get(k);
      dataBytes += df.fileSizeInBytes();
      if (live.get(k)) {
        liveDataBytes += df.fileSizeInBytes();
        rowGroups += df.splitOffsets() == null ? 1 : df.splitOffsets().size();
      }
    }
    long liveDeleteBytes = 0;
    Map<String, Long> liveObjects = new HashMap<>();
    for (Delete d : deletes) {
      liveDeleteBytes += liveBytes(d.file());
      liveObjects.put(d.file().location(), d.file().fileSizeInBytes());
    }
    long storedDeleteBytes = deleteObjects.values().stream().mapToLong(Long::longValue).sum();
    long liveObjectBytes = liveObjects.values().stream().mapToLong(Long::longValue).sum();
    return new ArmStats(
        files.size() + deleteObjects.size(),
        dataBytes + storedDeleteBytes,
        liveDataBytes + liveDeleteBytes,
        (dataBytes - liveDataBytes) + (storedDeleteBytes - liveObjectBytes),
        rowGroups,
        live.cardinality(),
        deletes.size(),
        -1, // data-file footers are not tracked yet
        -1);
  }
}
