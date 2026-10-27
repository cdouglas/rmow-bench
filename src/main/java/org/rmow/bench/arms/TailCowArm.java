package org.rmow.bench.arms;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * Plain Iceberg (v3) with tail merging: the baseline that separates what merging the tail buys
 * from what a single appendable object buys. The same {@link TailPolicy} as RMoW picks the open
 * tail units to merge, but here a unit is a small data file: the batch reads the picked files back
 * (their DVs applied) and writes them, with its inserts and updated rows, as one new data file
 * that replaces them. Updates and deletes of rows in any other file get DVs, as in {@link
 * MorV3Arm}.
 *
 * <p>With a rewrite share θ of at most 1 ({@code layout.rmorRewriteShare}) it is regional
 * merge-on-read, RMoR-C ({@code rmor-c}): a region is the unit of a write, as in RMoW-C, but a
 * rewritten region goes to a new data file and a DV hides the old one, so the format is Iceberg's
 * and no store needs to append. A batch rewrites a region it changes, outside the files it merges,
 * when it updates or deletes at least θ of the region's live rows:
 *
 * <ul>
 *   <li>a sealed file's row group: its rows, with the batch's changes, go into the batch's new data
 *       file with its inserts, which the tail policy merges as it merges any, and a DV covers the
 *       old row group. A file with no live row left leaves the table instead of taking a DV, as a
 *       copy-on-write commit removes a file it rewrites;
 *   <li>an open tail file, the tail policy's unit: rewritten whole as a new data file, which takes
 *       its place in the tail, as RMoW-C's open row groups do.
 * </ul>
 *
 * Below θ, the rows it changes get DVs. θ = 0 rewrites every region a batch changes; above 1 the
 * arm is TailCoW-C, request for request. To plan, the writer reads the footer of each sealed file
 * of several row groups it changes, once per batch; a file of at most 1 MiB, which every open
 * fetches whole, is fetched whole for it once, and its row groups read from what was fetched.
 */
public final class TailCowArm extends MorArm {

  /**
   * A region a batch rewrites: rows {@code [first, first + rows)} of data file {@code k}, the whole
   * file ({@code whole}, an open tail file) or one row group, of which the batch changes {@code
   * changed}; and its live rows, read back.
   */
  private record Rewrite(
      int k, boolean whole, long first, long rows, int changed, List<Record> live) {}

  private final TailPolicy policy;
  private final double rewriteShare; // above 1: regions are never rewritten (TailCoW)
  // under RMoR, each data file's positions its DVs cover
  private final Map<Integer, BitSet> dead = new HashMap<>();
  // the files of at most 1 MiB fetched whole to plan this batch, by file
  private final Map<Integer, byte[]> planned = new HashMap<>();
  private double bytesPerRow = 40; // running estimate, for planning merges

  public TailCowArm(ArmContext ctx, TailPolicy.Kind kind) {
    this(ctx, kind, Double.POSITIVE_INFINITY);
  }

  /** @param rewriteShare θ: at most 1 for RMoR, above 1 for TailCoW */
  public TailCowArm(ArmContext ctx, TailPolicy.Kind kind, double rewriteShare) {
    super(ctx);
    this.policy = new TailPolicy(kind, ctx.layout().tailSealBytes, ctx.layout().tierFanout);
    this.rewriteShare = rewriteShare;
  }

  private boolean regional() {
    return rewriteShare <= 1;
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    footers.begin();
    planned.clear();
    long seq = batchSeq(b);
    locate(b);
    TailPolicy.Plan plan =
        b.insertIds().length > 0
            ? policy.plan((long) (b.insertIds().length * bytesPerRow))
            : new TailPolicy.Plan(List.of(), 0);
    Set<Integer> merge = new HashSet<>();
    for (TailPolicy.Unit u : plan.merge()) {
      merge.add((int) u.id());
    }

    // rows of merged files are dropped by the merge; rows elsewhere get DVs, or, under RMoR, their
    // regions rewritten
    Set<Long> replaced = new HashSet<>();
    Map<Integer, List<Integer>> dvPositions = new HashMap<>();
    Map<Long, Integer> versions = new HashMap<>(); // each changed row's new version; -1: deleted
    for (int i = 0; i < b.updateIds().length; i++) {
      versions.put(b.updateIds()[i], b.updateVersions()[i]);
      mark(b.updateIds()[i], merge, replaced, dvPositions);
    }
    for (long id : b.deleteIds()) {
      versions.put(id, -1);
      mark(id, merge, replaced, dvPositions);
      fileOfId[(int) id] = -1;
    }

    Map<Integer, List<Record>> mergedRows = new HashMap<>();
    List<Rewrite> rewrites = new ArrayList<>();
    boolean planRegions = regional() && !dvPositions.isEmpty();
    if (!merge.isEmpty() || planRegions) {
      ctx.phases()
          .run(
              "merge-read",
              () -> {
                for (int k : merge) {
                  mergedRows.put(k, readLive(k));
                }
                if (planRegions) {
                  rewrites.addAll(rewrites(dvPositions));
                }
              });
    }
    Set<Long> rewritten = new HashSet<>(); // the changed rows whose regions are rewritten
    for (Rewrite r : rewrites) {
      for (Record rec : r.live()) {
        long id = (Long) rec.get(Rows.ID);
        if (versions.containsKey(id)) {
          rewritten.add(id);
        }
      }
    }
    ctx.phases()
        .run(
            "write",
            () -> {
              List<long[]> rows = batchRows(b);
              if (!rewritten.isEmpty()) {
                rows.removeIf(r -> rewritten.contains(r[0])); // their regions hold them
              }
              for (List<Record> rs : mergedRows.values()) {
                for (Record r : rs) {
                  long id = (Long) r.get(Rows.ID);
                  if (!replaced.contains(id)) {
                    rows.add(new long[] {id, (Integer) r.get(Rows.VERSION)});
                  }
                }
              }
              for (Rewrite r : rewrites) {
                if (!r.whole()) {
                  rows.addAll(changed(r.live(), versions)); // a sealed file's row group
                }
              }
              int k = writeDataFile(batchName(b), rows, seq);
              for (int m : merge) {
                retire(m);
              }
              if (k >= 0 && b.insertIds().length > 0) {
                long bytes = files.get(k).fileSizeInBytes();
                bytesPerRow = bytes / (double) rows.size();
                policy.commit(plan, k, bytes);
              }
              rewriteOpen(b, rewrites, versions, seq);
            });
    if (regional()) {
      deadAfter(rewrites, dvPositions);
    }
    if (!dvPositions.isEmpty()) {
      ctx.phases().run("delete-write", () -> writeDVs(b.index(), seq, dvPositions));
    }
    endBatch();
  }

  /**
   * The regions the batch rewrites, of the files outside its merge whose rows at {@code changed}
   * it updates or deletes, with their live rows read back: each open tail file whole, and each
   * sealed file's row groups, where the batch changes at least θ of the region's live rows.
   */
  private List<Rewrite> rewrites(Map<Integer, List<Integer>> changed) throws IOException {
    List<Rewrite> out = new ArrayList<>();
    for (int k : new TreeSet<>(changed.keySet())) {
      List<Integer> positions = changed.get(k);
      BitSet gone = dead.getOrDefault(k, new BitSet());
      DataFile df = files.get(k);
      if (policy.isOpen(k)) {
        long liveRows = df.recordCount() - gone.cardinality();
        if (positions.size() >= rewriteShare * liveRows) {
          out.add(new Rewrite(k, true, 0, df.recordCount(), positions.size(), readLive(k)));
        }
        continue;
      }
      for (long[] g : rowGroups(k)) {
        long first = g[0];
        long rows = g[1];
        int touched = 0;
        for (int p : positions) {
          if (p >= first && p < first + rows) {
            touched++;
          }
        }
        long liveRows = rows - gone.get((int) first, (int) (first + rows)).cardinality();
        if (touched == 0 || touched < rewriteShare * liveRows) {
          continue;
        }
        List<Record> live =
            g[2] < 0 ? readLive(k) : readLive(k, held(k), tailOf(k), g[2], g[3]);
        out.add(new Rewrite(k, false, first, rows, touched, live));
      }
    }
    return out;
  }

  /**
   * Data file {@code k}'s row groups, each {first row, rows, start, length}; one covering the whole
   * file (start -1) if it has one row group. The footer of a file of several is read once per
   * batch.
   */
  private List<long[]> rowGroups(int k) throws IOException {
    DataFile df = files.get(k);
    if (df.splitOffsets() == null || df.splitOffsets().size() <= 1) {
      return List.of(new long[] {0, df.recordCount(), -1, -1});
    }
    byte[] tail = tailOf(k);
    FileMetaData footer = FooterCodec.read(tail, 0, tail.length - 8);
    List<long[]> groups = new ArrayList<>(footer.getRow_groupsSize());
    long first = 0;
    for (RowGroup rg : footer.getRow_groups()) {
      groups.add(
          new long[] {
            first, rg.getNum_rows(), FooterCodec.startOffset(rg), FooterCodec.compressedSize(rg)
          });
      first += rg.getNum_rows();
    }
    return groups;
  }

  /**
   * Data file {@code k}'s tail, as the writer holds it this batch: read once ({@link
   * WriterFooters}), or, for a file of at most 1 MiB, which every open fetches whole, from the file
   * fetched whole once to plan.
   */
  private byte[] tailOf(int k) throws IOException {
    DataFile df = files.get(k);
    byte[] tail = footers.tail(df.location(), df.fileSizeInBytes());
    if (tail != null) {
      return tail;
    }
    byte[] whole = planned.get(k);
    if (whole == null) {
      whole = Scans.readWhole(ctx, df.location(), df.fileSizeInBytes());
      planned.put(k, whole);
    }
    return Scans.tailOf(whole);
  }

  /** Data file {@code k} as fetched whole to plan this batch, in memory; null if it was not. */
  private InputFile held(int k) throws IOException {
    byte[] whole = planned.get(k);
    if (whole == null) {
      return null;
    }
    MemFileIO mem = new MemFileIO();
    String location = files.get(k).location();
    try (OutputStream out = mem.newOutputFile(location).create()) {
      out.write(whole);
    }
    return mem.newInputFile(location, whole.length);
  }

  /**
   * A region's live rows with the batch's changes, as {id, version}: new versions, deleted rows
   * dropped.
   */
  private static List<long[]> changed(List<Record> live, Map<Long, Integer> versions) {
    List<long[]> out = new ArrayList<>(live.size());
    for (Record r : live) {
      long id = (Long) r.get(Rows.ID);
      Integer v = versions.get(id);
      int version = v == null ? (Integer) r.get(Rows.VERSION) : v;
      if (version >= 0) {
        out.add(new long[] {id, version});
      }
    }
    return out;
  }

  /** Writes each rewritten open tail file as a new data file, which takes its place in the tail. */
  private void rewriteOpen(
      OpLog.Batch b, List<Rewrite> rewrites, Map<Long, Integer> versions, long seq)
      throws IOException {
    int n = 0;
    for (Rewrite r : rewrites) {
      if (r.whole()) {
        int k = writeDataFile(
            String.format("%s-tail-%03d", batchName(b), n++), changed(r.live(), versions), seq);
        policy.replace(r.k(), k, k < 0 ? 0 : files.get(k).fileSizeInBytes());
        retire(r.k());
      }
    }
  }

  /**
   * The DVs the batch writes once its regions are rewritten: a rewritten row group's covers all of
   * it, and a rewritten open tail file left the table. A file whose every row is then dead leaves
   * the table rather than take a DV that covers it.
   */
  private void deadAfter(List<Rewrite> rewrites, Map<Integer, List<Integer>> dvPositions) {
    for (Rewrite r : rewrites) {
      if (r.whole()) {
        dvPositions.remove(r.k());
        continue;
      }
      List<Integer> ps = dvPositions.get(r.k());
      for (long p = r.first(); p < r.first() + r.rows(); p++) {
        ps.add((int) p);
      }
      // the row group's live rows the batch did not change are gone from it too
      for (int i = r.changed(); i < r.live().size(); i++) {
        removed(r.k());
      }
    }
    for (var it = dvPositions.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<Integer, List<Integer>> e = it.next();
      BitSet gone = dead.computeIfAbsent(e.getKey(), k -> new BitSet());
      for (int p : e.getValue()) {
        gone.set(p);
      }
      if (gone.cardinality() == files.get(e.getKey()).recordCount()) {
        retire(e.getKey());
        it.remove();
      }
    }
  }

  private void mark(
      long id, Set<Integer> merge, Set<Long> replaced, Map<Integer, List<Integer>> dvPositions) {
    int k = fileOfId[(int) id];
    removed(k);
    if (merge.contains(k)) {
      replaced.add(id);
    } else {
      dvPositions.computeIfAbsent(k, x -> new ArrayList<>()).add(posOfId[(int) id]);
    }
  }

  /** The open tail files are the policy's to merge; compaction leaves them alone. */
  @Override
  protected boolean compactable(int k) {
    return !policy.isOpen(k);
  }

  @Override
  protected void writeDeletes(int batch, long seq, Map<Integer, List<Integer>> deleted) {
    throw new UnsupportedOperationException("TailCoW writes its deletes in apply");
  }

  @Override
  protected long liveBytes(DeleteFile d) {
    return d.contentSizeInBytes();
  }
}
