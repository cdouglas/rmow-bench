package org.rmow.bench.arms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.rmow.bench.io.Held;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.RegionWriter;
import org.rmow.bench.rmow.RmowFile;
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
 *
 * <p>With {@code elide} it is RMoR-C with footers ({@code rmor-c-footer}): a rewritten row group's
 * old copy is hidden by a new footer appended to its file, listing the file's other row groups,
 * instead of a DV. Only that differs from RMoR-C; against RMoW-C, only where the rewritten region
 * goes (the batch's new data file, not its own object). Every data file is created as an object
 * the store can extend, and each append makes a new version of the file, with its length, split
 * offsets and metrics, as RMoW's do. Rows after an elided row group move up in their file, so a DV
 * on its other rows would go stale; θ must be 0, which rewrites every region a batch changes and
 * so leaves no DV.
 *
 * <p>The batch's new data file mixes its rows: the rewritten row groups' rows and the inserts are
 * sorted together by event time and cut into new row groups, so a row group's rows do not stay
 * together, and late inserts land among old regions' rows. With {@code keep} ({@code rmor-c-kept},
 * {@code rmor-c-footer-kept}; θ must be 0) the file is laid out as RMoW-C lays out its regions:
 * each rewritten row group is one row group of the same rows with the batch's changes, sorted by
 * event time within itself, and the inserts (with the open tail files the policy merges) follow
 * as a row group of their own. It is written a row group at a time, as parts of the batch's
 * merge-read and write: a row group is read, rewritten, encoded and its rows dropped before the
 * next is read. RMoR-C's file streams to the store as it is encoded; with footers, the file is
 * held to its single create, as before.
 */
public final class TailCowArm extends MorArm {

  /**
   * A region a batch rewrites: rows {@code [first, first + rows)} of data file {@code k}, the whole
   * file ({@code whole}, an open tail file) or one row group at {@code [start, start + length)}
   * (start -1: the file's one row group), of which the batch changes {@code changed}; its live rows
   * ({@code liveRows}; -1 until read) and, while held, the rows themselves, as {id, version}.
   */
  private record Rewrite(
      int k,
      boolean whole,
      long first,
      long rows,
      int changed,
      long start,
      long length,
      int liveRows,
      List<long[]> live) {
    Rewrite read(List<long[]> rows) {
      return new Rewrite(k, whole, first, this.rows, changed, start, length, rows.size(), rows);
    }

    /** The same rewrite once its rows are written and dropped. */
    Rewrite dropped() {
      return new Rewrite(k, whole, first, rows, changed, start, length, liveRows, null);
    }
  }

  private final TailPolicy policy;
  private final double rewriteShare; // above 1: regions are never rewritten (TailCoW)
  private final boolean elide; // hide a rewritten row group with a new footer, not a DV
  private final boolean keep; // a rewritten row group stays one row group; inserts apart
  private final RegionWriter regions; // with keep: encodes one row group at a time
  // with elide: each data file's ids by position, and the last footer appended to it, serialized
  private final Map<Integer, long[]> idsOf = new HashMap<>();
  private final Map<Integer, FileMetaData> appended = new HashMap<>();
  private final Map<Integer, Integer> appendedBytes = new HashMap<>();
  // under RMoR, each data file's positions its DVs cover
  private final Map<Integer, BitSet> dead = new HashMap<>();
  private double bytesPerRow = 40; // running estimate, for planning merges
  // with a cleaner: its settings, each file's batch of birth (-1: the initial load), and each row
  // group's last change (by its first row), the history the cleaner judges age by
  private final LayoutSpec.Clean clean;
  private final Map<Integer, Integer> born = new HashMap<>();
  private final Map<Integer, Map<Long, Integer>> changedAt = new HashMap<>();
  private int batch = -1; // the batch being applied
  // with deferred merges: their settings; each file's delta row groups (by first row); the ids
  // whose live version waits in one, and the region each waits for; each region's row group (by
  // file and first row: a region is numbered when first needed, and keeps its number through its
  // merges), and the ids that entered its deltas since its last merge; this batch's delta row
  // groups read to fold (by file and first row), and the ids it folds into the regions it merges
  private final LayoutSpec.Defer defer;
  private final Map<Integer, Set<Long>> deltaGroups = new HashMap<>();
  private final BitSet inDelta = new BitSet();
  private final int[] regionOf;
  private final Map<Long, Integer> regionAt = new HashMap<>();
  private final Map<Integer, List<Long>> waitingFor = new HashMap<>();
  private int nextRegion = 0;
  private final Map<Long, List<long[]>> deltaReads = new HashMap<>();
  private final Set<Long> folded = new HashSet<>();

  public TailCowArm(ArmContext ctx, TailPolicy.Kind kind) {
    this(ctx, kind, Double.POSITIVE_INFINITY);
  }

  /** @param rewriteShare θ: at most 1 for RMoR, above 1 for TailCoW */
  public TailCowArm(ArmContext ctx, TailPolicy.Kind kind, double rewriteShare) {
    this(ctx, kind, rewriteShare, false);
  }

  /** @param elide hide a rewritten row group with a footer appended to its file (θ must be 0) */
  public TailCowArm(ArmContext ctx, TailPolicy.Kind kind, double rewriteShare, boolean elide) {
    this(ctx, kind, rewriteShare, elide, false);
  }

  /**
   * @param keep write each rewritten row group as one row group of its own, and the inserts apart
   *     (θ must be 0)
   */
  public TailCowArm(
      ArmContext ctx, TailPolicy.Kind kind, double rewriteShare, boolean elide, boolean keep) {
    this(ctx, kind, rewriteShare, elide, keep, null);
  }

  /**
   * @param clean with row groups kept, a cleaner with these settings ({@code rmor-c-clean}): a
   *     write rewrites a row group only when it changes at least {@code clean.rewriteShare} of its
   *     live rows, and carries sparse, cold row groups forward (null: no cleaner, and θ must be 0)
   */
  public TailCowArm(
      ArmContext ctx,
      TailPolicy.Kind kind,
      double rewriteShare,
      boolean elide,
      boolean keep,
      LayoutSpec.Clean clean) {
    this(ctx, kind, rewriteShare, elide, keep, clean, null);
  }

  /**
   * @param defer with row groups kept, deferred merges with these settings ({@code rmor-density}):
   *     a write merges a row group it changes once its dead rows and the write's changes reach
   *     {@code rewriteShare} of its rows, folding in the rows that wait for it in delta row groups
   *     (null: none)
   */
  public TailCowArm(
      ArmContext ctx,
      TailPolicy.Kind kind,
      double rewriteShare,
      boolean elide,
      boolean keep,
      LayoutSpec.Clean clean,
      LayoutSpec.Defer defer) {
    super(ctx);
    if (elide && rewriteShare != 0) {
      throw new IllegalArgumentException(
          "RMoR-C with footers rewrites every region it changes (rmorRewriteShare 0), not "
              + rewriteShare);
    }
    if (keep && clean == null && defer == null && rewriteShare != 0) {
      throw new IllegalArgumentException(
          "RMoR-C with its row groups kept rewrites every region it changes (rmorRewriteShare 0),"
              + " not " + rewriteShare);
    }
    if (clean != null && (!keep || elide)) {
      throw new IllegalArgumentException("The cleaner needs row groups kept, and DVs");
    }
    if (defer != null && (!keep || elide || clean != null)) {
      throw new IllegalArgumentException(
          "Deferred merges need row groups kept, and DVs, without the cleaner");
    }
    if (defer != null && !List.of("region", "write").contains(defer.deltas)) {
      throw new IllegalArgumentException("Unknown defer.deltas: " + defer.deltas);
    }
    this.policy = new TailPolicy(kind, ctx.layout().tailSealBytes, ctx.layout().tierFanout);
    this.rewriteShare = rewriteShare;
    this.elide = elide;
    this.keep = keep;
    this.regions = keep ? new RegionWriter(Rows.SCHEMA, ctx.layout().writerProperties()) : null;
    this.clean = clean;
    this.defer = defer;
    this.regionOf = defer == null ? null : new int[(int) ctx.log().totalIds()];
  }

  @Override
  protected boolean appendable() {
    return elide;
  }

  @Override
  public void load(List<InitialLoad.InitialFile> initial) {
    super.load(initial);
    if (elide) {
      for (int k = 0; k < initial.size(); k++) {
        InitialLoad.InitialFile f = initial.get(k);
        long[] ids = new long[(int) f.rows()];
        for (int i = 0; i < ids.length; i++) {
          ids[i] = f.firstId() + i;
        }
        idsOf.put(k, ids);
      }
    }
  }

  @Override
  protected int addFile(DataFile df, long seq, long[] ids) {
    int k = super.addFile(df, seq, ids);
    if (elide) {
      idsOf.put(k, ids);
    }
    if (defer != null) {
      for (long id : ids) {
        inDelta.clear((int) id); // a row written in place; a file's deltas are marked after
      }
    }
    born.put(k, batch);
    return k;
  }

  /** A file that leaves the table takes what this arm keeps of it along. */
  @Override
  protected void retire(int k) {
    super.retire(k);
    born.remove(k);
    changedAt.remove(k);
    idsOf.remove(k);
    dead.remove(k);
    deltaGroups.remove(k);
  }

  private boolean regional() {
    return rewriteShare <= 1;
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    batch = b.index();
    footers.begin();
    beginPlanning();
    deltaReads.clear();
    folded.clear();
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
      inDelta.clear((int) id);
    }

    rowsHeld = Held.scope("the batch's files written");
    try {
      applyHeld(b, seq, plan, merge, replaced, dvPositions, versions);
    } finally {
      rowsHeld.close();
      rowsHeld = null;
    }
  }

  private void applyHeld(
      OpLog.Batch b,
      long seq,
      TailPolicy.Plan plan,
      Set<Integer> merge,
      Set<Long> replaced,
      Map<Integer, List<Integer>> dvPositions,
      Map<Long, Integer> versions)
      throws IOException {
    List<Rewrite> rewrites =
        keep
            ? writeKept(b, seq, plan, merge, replaced, dvPositions, versions)
            : writeMixed(b, seq, plan, merge, replaced, dvPositions, versions);
    if (elide) {
      Map<Integer, List<Rewrite>> sealed = sealedRewrites(rewrites, dvPositions);
      if (!sealed.isEmpty()) {
        ctx.phases().run("delete-write", () -> elide(sealed));
      }
      endBatch();
      return;
    }
    // Kept row groups take DVs over the regions they rewrite, at any share: with a share above 1
    // the cleaner's carries are still rewrites (found by the fuzzer, seed 46, 2026-11-08)
    if (keep || regional()) {
      deadAfter(rewrites, dvPositions);
    }
    if (!dvPositions.isEmpty()) {
      ctx.phases().run("delete-write", () -> writeDVs(b.index(), seq, dvPositions));
    }
    endBatch();
  }

  /**
   * The batch's new data file with its rows mixed: the inserts, the merged tail files' rows and the
   * rewritten row groups' rows, sorted together and cut into row groups by the Parquet writer.
   * Returns the regions it rewrote.
   */
  private List<Rewrite> writeMixed(
      OpLog.Batch b,
      long seq,
      TailPolicy.Plan plan,
      Set<Integer> merge,
      Set<Long> replaced,
      Map<Integer, List<Integer>> dvPositions,
      Map<Long, Integer> versions)
      throws IOException {
    Map<Integer, List<long[]>> mergedRows = new HashMap<>();
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
                  rewrites.addAll(rewrites(dvPositions, true));
                }
              });
    }
    if (readsUpdatedRows()) {
      readUpdated(b, readWhole(merge, rewrites));
    }
    Set<Long> rewritten = new HashSet<>(); // the changed rows whose regions are rewritten
    for (Rewrite r : rewrites) {
      for (long[] row : r.live()) {
        if (versions.containsKey(row[0])) {
          rewritten.add(row[0]);
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
              rowsHeld.hold(Held.Kind.ROWS, rows.size(), "the batch's rows to sort");
              for (List<long[]> rs : mergedRows.values()) {
                for (long[] r : rs) {
                  if (!replaced.contains(r[0])) {
                    rows.add(r);
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
              rowsHeld.close(); // the batch's files are written
            });
    return rewrites;
  }

  /**
   * With keep: the batch's new data file, a row group at a time, as RMoW-C writes its regions. The
   * regions to rewrite are planned from footers, with the merged tail files and the open files
   * rewritten whole read back; then each rewritten row group is read, rewritten as one row group of
   * the same rows and encoded, and its rows dropped, before the next is read (parts of merge-read
   * and write). The inserts, with the merged files' rows, follow as a row group of their own.
   * Returns the regions it rewrote, their rows dropped.
   */
  private List<Rewrite> writeKept(
      OpLog.Batch b,
      long seq,
      TailPolicy.Plan plan,
      Set<Integer> merge,
      Set<Long> replaced,
      Map<Integer, List<Integer>> dvPositions,
      Map<Long, Integer> versions)
      throws IOException {
    Map<Integer, List<long[]>> mergedRows = new HashMap<>();
    List<Rewrite> rewrites = new ArrayList<>();
    boolean planRegions = !dvPositions.isEmpty();
    if (!merge.isEmpty() || planRegions) {
      ctx.phases()
          .part(
              "merge-read",
              () -> {
                for (int k : merge) {
                  mergedRows.put(k, readLive(k));
                }
                if (planRegions) {
                  rewrites.addAll(rewrites(dvPositions, false));
                }
              });
    }
    KeptFile out = new KeptFile(batchName(b));
    // with deferred merges: the files whose rows this batch writes elsewhere, which folds skip
    Set<Integer> elsewhere = new HashSet<>(merge);
    for (Rewrite r : rewrites) {
      if (r.whole()) {
        elsewhere.add(r.k());
      }
    }
    List<List<long[]>> foldedRows = new ArrayList<>();
    for (int i = 0; i < rewrites.size(); i++) {
      foldedRows.add(null);
    }
    for (int i = 0; i < rewrites.size(); i++) {
      if (rewrites.get(i).whole()) {
        continue; // an open tail file, read back whole and written as its own file
      }
      int at = i;
      ctx.phases()
          .part(
              "merge-read",
              () -> {
                Rewrite r = rewrites.get(at);
                List<long[]> live =
                    r.start() < 0
                        ? readLive(r.k())
                        : readLive(r.k(), held(r.k()), tailOf(r.k()), r.start(), r.length());
                rewrites.set(at, r.read(live));
                if (defer != null) {
                  foldedRows.set(at, fold(regionOf(r.k(), r.first()), elsewhere, versions,
                      dvPositions));
                }
              });
      ctx.phases()
          .part(
              "write",
              () -> {
                Rewrite r = rewrites.get(at);
                List<long[]> rows = changed(r.live(), versions);
                if (foldedRows.get(at) != null) {
                  rows.addAll(changed(foldedRows.get(at), versions));
                  foldedRows.set(at, null);
                }
                out.add(rows, false, defer == null ? -1 : regionOf(r.k(), r.first()));
                rowsHeld.release(
                    Held.Kind.ROWS, r.liveRows(), "a row group of " + out.name + " encoded");
                rewrites.set(at, r.dropped());
              });
    }
    if (readsUpdatedRows()) {
      Map<Integer, Set<Long>> whole = readWhole(merge, rewrites);
      for (long key : deltaReads.keySet()) {
        int k = (int) (key >>> 32);
        if (!whole.containsKey(k)) {
          whole.put(k, new HashSet<>());
        }
        if (whole.get(k) != null) {
          whole.get(k).add(key & 0xFFFFFFFFL);
        }
      }
      readUpdatedPart(b, whole);
    }
    // the updated rows whose regions are rewritten have their new versions there; the others' (in
    // merged files, or, with a cleaner, below its threshold) join the inserts
    Set<Long> inRegions = rewrittenUpdates(b, rewrites, replaced);
    if (clean != null) {
      carry(b, rewrites, merge, out);
    }
    ctx.phases()
        .part(
            "write",
            () -> {
              // the inserts, and the new versions of the rows outside the rewritten regions; with
              // deferred merges, those of rows outside the merged files wait in delta row groups
              List<long[]> rows = batchRows(b);
              rows.removeIf(r -> inRegions.contains(r[0]));
              rowsHeld.hold(Held.Kind.ROWS, rows.size(), "the batch's inserts to sort");
              for (List<long[]> rs : mergedRows.values()) {
                for (long[] r : rs) {
                  if (!replaced.contains(r[0])) {
                    rows.add(r);
                  }
                }
              }
              List<List<long[]>> deltas = defer == null ? List.of() : deltas(b, rows, replaced);
              out.add(rows, false, -1);
              for (List<long[]> d : deltas) {
                out.add(d, true, -1);
              }
              int k = out.finish(seq);
              for (int m : merge) {
                retire(m);
              }
              if (k >= 0 && b.insertIds().length > 0) {
                long bytes = files.get(k).fileSizeInBytes();
                bytesPerRow = bytes / (double) out.rows();
                policy.commit(plan, k, bytes);
              }
              rewriteOpen(b, rewrites, versions, seq);
              rowsHeld.close(); // the batch's files are written
            });
    return rewrites;
  }

  /** The updated rows, outside the merged files, whose regions this batch rewrites. */
  private Set<Long> rewrittenUpdates(OpLog.Batch b, List<Rewrite> rewrites, Set<Long> replaced) {
    Map<Integer, List<Rewrite>> byFile = new HashMap<>();
    for (Rewrite r : rewrites) {
      byFile.computeIfAbsent(r.k(), x -> new ArrayList<>()).add(r);
    }
    Set<Long> out = new HashSet<>();
    for (long id : b.updateIds()) {
      if (folded.contains(id)) {
        out.add(id); // its region's merge folded it in
        continue;
      }
      List<Rewrite> rs = replaced.contains(id) ? null : byFile.get(fileOfId[(int) id]);
      if (rs == null) {
        continue;
      }
      int pos = posOfId[(int) id];
      for (Rewrite r : rs) {
        if (r.whole() || (pos >= r.first() && pos < r.first() + r.rows())) {
          out.add(id);
          break;
        }
      }
    }
    return out;
  }

  /**
   * The cleaner: carries forward the live rows of the row groups {@link #cleanerPicks} chooses,
   * packed into row groups of about the writer's size in the batch's file, a row group at a time
   * (phase {@code clean}); each joins {@code rewrites}, so a DV covers it whole after the write.
   */
  private void carry(OpLog.Batch b, List<Rewrite> rewrites, Set<Integer> merge, KeptFile out)
      throws IOException {
    long own = b.insertIds().length + b.updateIds().length;
    Set<Integer> skip = new HashSet<>(merge);
    for (Rewrite r : rewrites) {
      if (r.whole()) {
        skip.add(r.k());
      } else {
        own += Math.max(0, r.liveRows());
      }
    }
    long budget = (long) (clean.budget * own);
    List<Rewrite> picks = new ArrayList<>();
    ctx.phases().part("clean", () -> picks.addAll(cleanerPicks(skip, budget)));
    long target = Math.max(1, (long) (0.95 * ctx.layout().rowGroupBytes / bytesPerRow));
    List<long[]> buffer = new ArrayList<>();
    for (Rewrite c : picks) {
      ctx.phases()
          .part(
              "clean",
              () -> {
                List<long[]> live =
                    c.start() < 0
                        ? readLive(c.k())
                        : readLive(c.k(), held(c.k()), tailOf(c.k()), c.start(), c.length());
                rewrites.add(c.read(live).dropped());
                buffer.addAll(live);
                if (buffer.size() >= target) {
                  flushCarried(buffer, out);
                }
              });
    }
    if (!buffer.isEmpty()) {
      ctx.phases().part("clean", () -> flushCarried(buffer, out));
    }
  }

  private void flushCarried(List<long[]> buffer, KeptFile out) throws IOException {
    out.add(new ArrayList<>(buffer), false, -1);
    rowsHeld.release(Held.Kind.ROWS, buffer.size(), "carried rows of " + out.name + " encoded");
    buffer.clear();
  }

  /**
   * The cleaner's picks this batch, as rewrites of no changed row: every live row group of each
   * cold file with fewer than {@code clean.drain} of its rows live (it then leaves the table); then
   * row groups with fewer than {@code clean.low} of their rows live, unchanged for {@code clean.age}
   * batches, by cost-benefit (1 - u) * age / (1 + u), within {@code budget} rows.
   *
   * <p>It judges from what the writer holds or reads for it, and counts what it reads. Each file's
   * dead rows are its DV's cardinality, as the manifest has it, at no read. A file with at least
   * {@code clean.examine} of its rows dead is judged row group by row group: its footer is read
   * (once in the batch, shared with the write's other uses of it), and its DV loaded, unless the
   * batch's locate loaded it and checked it against those row groups already. A file of one row
   * group needs neither. Each row group's age is the arm's history of the batches that changed it,
   * a few bytes a row group that a cleaner would keep beside the table.
   */
  private List<Rewrite> cleanerPicks(Set<Integer> skip, long budget) throws IOException {
    record Unit(Rewrite r, int liveRows, double u, int age) {}
    List<Rewrite> out = new ArrayList<>();
    List<Unit> sparse = new ArrayList<>();
    for (int k = live.nextSetBit(0); k >= 0; k = live.nextSetBit(k + 1)) {
      if (skip.contains(k) || policy.isOpen(k)) {
        continue;
      }
      DataFile df = files.get(k);
      long total = df.recordCount();
      DeleteFile[] ds = deletesOf(k);
      BitSet gone = dead.getOrDefault(k, new BitSet());
      long deadRows = 0; // as the manifest says: the DV's cardinality
      for (DeleteFile d : ds) {
        deadRows += d.recordCount();
      }
      if (deadRows != gone.cardinality()) {
        throw new IllegalStateException(
            "The manifest puts " + deadRows + " dead rows in " + df.location() + "; the arm "
                + gone.cardinality());
      }
      if (deadRows < clean.examine * total || deadRows >= total) {
        continue;
      }
      int bornAt = born.getOrDefault(k, -1);
      Map<Long, Integer> changes = changedAt.getOrDefault(k, Map.of());
      List<Unit> units = new ArrayList<>();
      boolean quiet = true;
      List<long[]> groups = rowGroups(k);
      long[] deadIn = deadByRowGroup(k, ds, groups, gone);
      for (int i = 0; i < groups.size(); i++) {
        long[] g = groups.get(i);
        int lr = (int) (g[1] - deadIn[i]);
        if (lr == 0) {
          continue;
        }
        int age = batch - Math.max(bornAt, changes.getOrDefault(g[0], -1));
        quiet &= age >= clean.age;
        units.add(
            new Unit(
                new Rewrite(k, false, g[0], g[1], 0, g[2], g[3], -1, null),
                lr,
                lr / (double) g[1],
                age));
      }
      if (quiet && total - deadRows < clean.drain * total) {
        for (Unit u : units) {
          out.add(u.r());
        }
        continue;
      }
      for (Unit u : units) {
        if (u.u() < clean.low && u.age() >= clean.age) {
          sparse.add(u);
        }
      }
    }
    sparse.sort(Comparator.comparingDouble((Unit u) -> -(1 - u.u()) * u.age() / (1 + u.u())));
    long used = 0;
    for (Unit u : sparse) {
      if (used + u.liveRows() <= budget) {
        out.add(u.r());
        used += u.liveRows();
      }
    }
    return out;
  }

  /**
   * The dead rows of each of data file {@code k}'s {@code groups}, for the cleaner: of a file of one
   * row group, its DV's cardinality (the manifest's); of a file whose DV this batch's locate loaded
   * and checked against its row groups, from that load; otherwise from its DV, loaded from storage
   * now. Each is checked against the positions the arm knows dead ({@code gone}).
   */
  private long[] deadByRowGroup(int k, DeleteFile[] ds, List<long[]> groups, BitSet gone) {
    long[] known = new long[groups.size()];
    for (int i = 0; i < known.length; i++) {
      long[] g = groups.get(i);
      known[i] = gone.get((int) g[0], (int) (g[0] + g[1])).cardinality();
    }
    if (groups.size() == 1 || locateLoadedDeletes(k)) {
      return known;
    }
    long[] loaded = new long[groups.size()];
    long[] firsts = new long[groups.size()];
    for (int i = 0; i < firsts.length; i++) {
      firsts[i] = groups.get(i)[0];
    }
    try (DeleteCache planning = new DeleteCache()) {
      PositionDeleteIndex index =
          Scans.loadDeletes(
              ctx, Reader.writer(ctx.layout()).footerPrefetchBytes(), files.get(k), ds, planning);
      index.forEach(
          pos -> {
            int i = Arrays.binarySearch(firsts, pos);
            loaded[i >= 0 ? i : -i - 2]++;
          });
    }
    if (!Arrays.equals(loaded, known)) {
      throw new IllegalStateException(
          "The DV of " + files.get(k).location() + " covers " + Arrays.toString(loaded)
              + " rows of its row groups; the arm knows " + Arrays.toString(known));
    }
    return loaded;
  }

  /**
   * With keep: a batch's new data file, written a row group at a time. Without footers it streams
   * to the store as each row group is encoded; with footers each row group is held for the file's
   * single create, as {@link MorArm#writeDataFile} holds a file it creates.
   */
  private final class KeptFile {
    final String name;
    final String location;
    final List<FileMetaData> rowGroups = new ArrayList<>();
    final List<RegionWriter.Region> waiting = new ArrayList<>(); // with footers: for the create
    final Held.Scope pending;
    long[] ids = new long[1 << 16];
    int n = 0;
    long base = 0;
    final List<long[]> deltaSpans = new ArrayList<>(); // its delta row groups: {first row, rows}
    final List<long[]> regionFirsts = new ArrayList<>(); // its merged regions: {first row, region}
    PositionOutputStream stream; // without footers: opened with the first row group

    KeptFile(String name) {
      this.name = name;
      this.location = String.format("%s/data/%s.parquet", ctx.root(), name);
      this.pending = Held.scope("created " + name);
    }

    long rows() {
      return n;
    }

    /**
     * Encodes {@code rows} ({id, version}) as the file's next row group, sorted by event time;
     * {@code delta}: rows that wait there for their regions' merges; {@code region}: the region
     * whose merged row group it is, or -1.
     */
    void add(List<long[]> rows, boolean delta, int region) throws IOException {
      if (rows.isEmpty()) {
        return;
      }
      if (delta) {
        deltaSpans.add(new long[] {n, rows.size()});
      }
      if (region >= 0) {
        regionFirsts.add(new long[] {n, region});
      }
      if (ctx.layout().sortWrites) {
        OpLog log = ctx.log();
        rows.sort(Comparator.comparingLong((long[] r) -> log.ts(r[0])).thenComparingLong(r -> r[0]));
      }
      RegionWriter.Region r = regions.write(location, base, new MadeRows(rows, ctx.log()));
      if (n + rows.size() > ids.length) {
        ids = Arrays.copyOf(ids, Math.max(ids.length * 2, n + rows.size()));
      }
      for (long[] row : rows) {
        ids[n++] = row[0];
      }
      rowGroups.add(r.footer());
      if (elide) {
        pending.hold(Held.Kind.PENDING, r.bytes().length, "a row group of " + name + " to create");
        waiting.add(r);
      } else {
        if (stream == null) {
          stream = ctx.io().newOutputFile(location).create();
        }
        stream.write(r.bytes());
      }
      base = r.end();
    }

    /** Writes the footer and adds the file to the table; returns its index, or -1 if empty. */
    int finish(long seq) throws IOException {
      if (rowGroups.isEmpty()) {
        pending.close();
        return -1;
      }
      FileMetaData footer = FooterCodec.merge(null, Set.of(), rowGroups, 0, 0);
      byte[] tail = FooterCodec.serialize(footer);
      long length = base + tail.length;
      if (elide) {
        ByteArrayOutputStream all = new ByteArrayOutputStream((int) length);
        for (RegionWriter.Region r : waiting) {
          all.write(r.bytes());
        }
        all.write(tail);
        ctx.io().create(location, all.toByteArray());
        waiting.clear();
      } else {
        stream.write(tail);
        stream.close();
      }
      pending.close();
      int k = addFile(RmowFile.describe(location, length, footer), seq, Arrays.copyOf(ids, n));
      if (!deltaSpans.isEmpty()) {
        Set<Long> firsts = new HashSet<>();
        for (long[] span : deltaSpans) {
          firsts.add(span[0]);
          for (long pos = span[0]; pos < span[0] + span[1]; pos++) {
            inDelta.set((int) ids[(int) pos]);
          }
        }
        deltaGroups.put(k, firsts);
      }
      for (long[] r : regionFirsts) {
        regionAt.put(((long) k << 32) | r[0], (int) r[1]);
      }
      return k;
    }
  }

  /**
   * With footers: the batch's rewritten row groups of sealed files, by file. At θ = 0 they are
   * every changed row's old place, so no row is left for a DV.
   */
  private static Map<Integer, List<Rewrite>> sealedRewrites(
      List<Rewrite> rewrites, Map<Integer, List<Integer>> dvPositions) {
    Map<Integer, List<Rewrite>> sealed = new TreeMap<>();
    Set<Integer> whole = new HashSet<>();
    for (Rewrite r : rewrites) {
      if (r.whole()) {
        whole.add(r.k());
      } else {
        sealed.computeIfAbsent(r.k(), x -> new ArrayList<>()).add(r);
      }
    }
    Set<Integer> covered = new HashSet<>(sealed.keySet());
    covered.addAll(whole);
    if (!covered.equals(dvPositions.keySet())) {
      throw new IllegalStateException("A changed row's region was not rewritten");
    }
    return sealed;
  }

  /**
   * Hides the rewritten row groups of each sealed file with a footer appended to it that lists its
   * other row groups; a file left with none leaves the table.
   */
  private void elide(Map<Integer, List<Rewrite>> sealed) throws IOException {
    for (Map.Entry<Integer, List<Rewrite>> e : sealed.entrySet()) {
      int k = e.getKey();
      Set<Long> firsts = new HashSet<>();
      long rows = 0;
      for (Rewrite r : e.getValue()) {
        firsts.add(r.first());
        rows += r.rows();
      }
      clearRemoved(k); // what the batch moved or deleted leaves the file's footer
      if (rows == files.get(k).recordCount()) {
        retire(k);
      } else {
        appendFooter(k, firsts);
      }
    }
  }

  /**
   * Appends a footer to data file {@code k} without the row groups that start at the rows {@code
   * firsts}, in one append at its length, and moves its later rows up.
   */
  private void appendFooter(int k, Set<Long> firsts) throws IOException {
    DataFile df = files.get(k);
    byte[] tail = tailOf(k);
    FooterCodec.Tail t = FooterCodec.locate(tail, tail.length);
    FileMetaData footer = FooterCodec.read(tail, t.footerStart(), t.footerLength());
    List<RowGroup> groups = footer.getRow_groups();
    Set<Integer> drop = new HashSet<>();
    long[] ids = idsOf.get(k);
    long[] kept = new long[ids.length];
    int n = 0;
    long first = 0;
    for (int i = 0; i < groups.size(); i++) {
      long rows = groups.get(i).getNum_rows();
      if (firsts.contains(first)) {
        drop.add(i);
      } else {
        System.arraycopy(ids, (int) first, kept, n, (int) rows);
        n += (int) rows;
      }
      first += rows;
    }
    if (drop.size() != firsts.size() || first != ids.length) {
      throw new IllegalStateException("Rewritten row groups out of step with " + df.location());
    }
    long version = appended.containsKey(k) ? versionOf(appended.get(k)) + 1 : 1;
    FileMetaData next = FooterCodec.merge(footer, drop, List.of(), df.fileSizeInBytes(), version);
    byte[] newTail = FooterCodec.serialize(next);
    long length = ctx.io().append(df.location(), df.fileSizeInBytes(), newTail);
    files.set(k, RmowFile.describe(df.location(), length, next));
    appended.put(k, next);
    appendedBytes.put(k, newTail.length);
    kept = Arrays.copyOf(kept, n);
    idsOf.put(k, kept);
    for (int pos = 0; pos < kept.length; pos++) {
      if (fileOfId[(int) kept[pos]] == k) {
        posOfId[(int) kept[pos]] = pos;
      }
    }
  }

  private static long versionOf(FileMetaData footer) {
    return Long.parseLong(FooterCodec.keyValues(footer).get(FooterCodec.VERSION));
  }

  /** A file a footer was appended to references its live row groups and that footer only. */
  @Override
  protected long liveDataBytes(int k) {
    FileMetaData footer = appended.get(k);
    if (footer == null) {
      return super.liveDataBytes(k);
    }
    long live = FooterCodec.MAGIC.length + appendedBytes.get(k);
    for (RowGroup rg : footer.getRow_groups()) {
      live += FooterCodec.liveBytes(rg);
    }
    return live;
  }

  /**
   * The regions the batch rewrites, of the files outside its merge whose rows at {@code changed}
   * it updates or deletes, with their live rows read back: each open tail file whole, and each
   * sealed file's row groups, where the batch changes at least θ of the region's live rows.
   *
   * @param readSealed whether to read the sealed files' row groups back now; if not, they are
   *     planned from footers alone, to be read one at a time
   */
  private List<Rewrite> rewrites(Map<Integer, List<Integer>> changed, boolean readSealed)
      throws IOException {
    List<Rewrite> out = new ArrayList<>();
    for (int k : new TreeSet<>(changed.keySet())) {
      List<Integer> positions = changed.get(k);
      BitSet gone = dead.getOrDefault(k, new BitSet());
      DataFile df = files.get(k);
      if (policy.isOpen(k)) {
        long liveRows = df.recordCount() - gone.cardinality();
        if (positions.size() >= rewriteShare * liveRows) {
          List<long[]> live = readLive(k);
          out.add(
              new Rewrite(
                  k, true, 0, df.recordCount(), positions.size(), -1, -1, live.size(), live));
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
        if (touched > 0) {
          changedAt.computeIfAbsent(k, x -> new HashMap<>()).put(first, batch);
        }
        if (defer != null) {
          // deferred: a region's own row group is merged once its dead rows and the batch's
          // changes reach θ of its rows; delta row groups wait to be folded into theirs
          if (touched == 0
              || deltaGroups.getOrDefault(k, Set.of()).contains(first)
              || (rows - liveRows) + touched < rewriteShare * rows) {
            continue;
          }
        } else if (touched == 0 || touched < rewriteShare * liveRows) {
          continue;
        }
        if (!readSealed) {
          out.add(new Rewrite(k, false, first, rows, touched, g[2], g[3], -1, null));
          continue;
        }
        List<long[]> live =
            g[2] < 0 ? readLive(k) : readLive(k, held(k), tailOf(k), g[2], g[3]);
        out.add(new Rewrite(k, false, first, rows, touched, g[2], g[3], live.size(), live));
      }
    }
    return out;
  }

  /**
   * The row groups and files the batch reads whole anyway, for the update read: file to the first
   * rows of its rewritten row groups, or to null for a file read whole (merged, or rewritten
   * whole).
   */
  private static Map<Integer, Set<Long>> readWhole(Set<Integer> merge, List<Rewrite> rewrites) {
    Map<Integer, Set<Long>> out = new HashMap<>();
    for (int m : merge) {
      out.put(m, null);
    }
    for (Rewrite r : rewrites) {
      if (r.whole()) {
        out.put(r.k(), null);
      } else if (!out.containsKey(r.k())) {
        out.put(r.k(), new HashSet<>(Set.of(r.first())));
      } else if (out.get(r.k()) != null) {
        out.get(r.k()).add(r.first());
      }
    }
    return out;
  }

  /**
   * The region whose row group starts at row {@code first} of data file {@code k}: numbered the
   * first time a write needs it, and carried to the row group its merge writes.
   */
  private int regionOf(int k, long first) {
    return regionAt.computeIfAbsent(((long) k << 32) | first, x -> nextRegion++);
  }

  /**
   * Deferred merges: the rows waiting in delta row groups for {@code region}, which the batch
   * merges, outside the files the batch writes {@code elsewhere} (those wait on). Each delta row
   * group that holds one is read whole, once a batch; DVs will hide the rows taken (added to
   * {@code dvPositions} unless the batch changes them, which put them there already). The writer
   * finds them in its index of the rows waiting for each region, as it would from the delta row
   * groups' id statistics: a region is a range of ids.
   */
  private List<long[]> fold(
      int region,
      Set<Integer> elsewhere,
      Map<Long, Integer> versions,
      Map<Integer, List<Integer>> dvPositions)
      throws IOException {
    List<Long> entered = waitingFor.remove(region);
    if (entered == null) {
      return List.of();
    }
    Map<Long, long[]> groups = new TreeMap<>(); // (file, first row) -> {file, row group...}
    Set<Long> wanted = new HashSet<>();
    List<Long> later = new ArrayList<>();
    for (long id : entered) {
      int i = (int) id;
      if (!inDelta.get(i) || regionOf[i] != region || folded.contains(id) || !wanted.add(id)) {
        continue; // deleted, or merged since (the list keeps every entry), or listed twice
      }
      int k = fileOfId[i];
      if (k < 0 || !live.get(k)) {
        throw new IllegalStateException("Row " + id + " waits in a delta that is not live");
      }
      if (elsewhere.contains(k)) {
        wanted.remove(id);
        later.add(id);
        continue;
      }
      long[] g = rowGroupOf(k, posOfId[i]);
      if (!deltaGroups.getOrDefault(k, Set.of()).contains(g[0])) {
        throw new IllegalStateException("Row " + id + " waits outside a delta row group");
      }
      groups.putIfAbsent(((long) k << 32) | g[0], new long[] {k, g[0], g[1], g[2], g[3]});
    }
    if (!later.isEmpty()) {
      waitingFor.put(region, later);
    }
    List<long[]> out = new ArrayList<>();
    for (Map.Entry<Long, long[]> e : groups.entrySet()) {
      long[] g = e.getValue();
      int k = (int) g[0];
      List<long[]> rows = deltaReads.get(e.getKey());
      if (rows == null) {
        rows = g[3] < 0 ? readLive(k) : readLive(k, held(k), tailOf(k), g[3], g[4]);
        deltaReads.put(e.getKey(), rows);
      }
      for (long[] row : rows) {
        if (wanted.contains(row[0]) && folded.add(row[0])) {
          out.add(row);
          if (!versions.containsKey(row[0])) {
            dvPositions.computeIfAbsent(k, x -> new ArrayList<>()).add(posOfId[(int) row[0]]);
            removed(k);
          }
        }
      }
    }
    if (out.size() != wanted.size()) {
      throw new IllegalStateException(
          "Folded " + out.size() + " of the " + wanted.size() + " rows waiting for a region");
    }
    return out;
  }

  /**
   * Deferred merges: the rows among {@code rows} (taken out of it) that wait for their regions'
   * merges: the new versions of the rows the batch updates outside its merged files, and rows of
   * merged tail files that waited already. A row waits for the region it waited for, or else the
   * region of the row group it left. One list a region ({@code deltas} {@code region}), or one
   * list ({@code write}).
   */
  private List<List<long[]>> deltas(OpLog.Batch b, List<long[]> rows, Set<Long> replaced)
      throws IOException {
    Set<Long> updated = new HashSet<>();
    for (long id : b.updateIds()) {
      if (!replaced.contains(id)) {
        updated.add(id);
      }
    }
    Map<Integer, List<long[]>> byRegion = new TreeMap<>();
    boolean perRegion = "region".equals(defer.deltas);
    for (var it = rows.iterator(); it.hasNext(); ) {
      long[] row = it.next();
      int id = (int) row[0];
      int region;
      if (updated.contains(row[0])) {
        int k = fileOfId[id];
        long first = rowGroupOf(k, posOfId[id])[0];
        region =
            deltaGroups.getOrDefault(k, Set.of()).contains(first) ? regionOf[id] : regionOf(k, first);
      } else if (inDelta.get(id)) {
        region = regionOf[id]; // waited in a merged tail file
      } else {
        continue;
      }
      it.remove();
      regionOf[id] = region;
      waitingFor.computeIfAbsent(region, x -> new ArrayList<>()).add(row[0]);
      byRegion.computeIfAbsent(perRegion ? region : 0, x -> new ArrayList<>()).add(row);
    }
    return new ArrayList<>(byRegion.values());
  }

  /**
   * A region's live rows with the batch's changes, as {id, version}: new versions, deleted rows
   * dropped.
   */
  private static List<long[]> changed(List<long[]> live, Map<Long, Integer> versions) {
    List<long[]> out = new ArrayList<>(live.size());
    for (long[] r : live) {
      Integer v = versions.get(r[0]);
      int version = v == null ? (int) r[1] : v;
      if (version >= 0) {
        out.add(v == null ? r : new long[] {r[0], version});
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
      List<Integer> ps = dvPositions.computeIfAbsent(r.k(), x -> new ArrayList<>());
      for (long p = r.first(); p < r.first() + r.rows(); p++) {
        ps.add((int) p);
      }
      // the row group's live rows the batch did not change are gone from it too
      for (int i = r.changed(); i < r.liveRows(); i++) {
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
