package org.rmow.bench.arms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;
import org.rmow.bench.io.Held;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.LogicalFile;
import org.rmow.bench.rmow.RegionWriter;
import org.rmow.bench.rmow.RegionWriter.Region;
import org.rmow.bench.rmow.RmowFile;
import org.rmow.bench.rmow.SplitFile;
import org.rmow.bench.rmow.VersionedInputFile;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * Region-merge-on-write. Each file a batch changes gets exactly one append: its new regions and a
 * footer that drops the row groups they replace.
 *
 * <ul>
 *   <li><b>Inserts</b> go to the tail file as one new row group, merged with the open tail row
 *       groups the {@link TailPolicy} picks (none under A; the open tail under B; tiers under C).
 *   <li><b>Updates and deletes</b> rewrite each row group they touch, in its own file, with its
 *       surviving rows and the updated versions (row-group-level copy-on-write). A touched row
 *       group the policy also merges is rewritten once, into the merged region.
 * </ul>
 *
 * The tail file rolls over once its live bytes reach the target file size or its footer lists
 * {@code maxRowGroupsPerFile} row groups, sealing every open tail. Replaced regions stay in their
 * object, where time travel still reads them.
 *
 * <p>A rewritten row group takes the replaced one's place in the footer ({@code footerOrder}
 * {@code logical}) or is listed last ({@code offset}). Inserts take new, larger ids and updates
 * rewrite rows within their row group, so under {@code logical} every footer lists its row groups
 * in id order, which is checked after each append.
 *
 * <p>Files are appendable objects ({@link RmowFile}) or, in the {@code -split} arms, a new object
 * per version for its regions and another for its footer ({@link SplitFile}), which need no
 * append and leave whole objects to delete as versions expire, instead of dead regions inside one.
 * Everything else, from the tail policy to the footer order, is the same.
 */
public final class RmowArm implements Arm {

  /**
   * One file at one version, with its footer then: what a snapshot's manifest would name. Footers
   * are never modified once written, so the version's own can be kept.
   */
  private record Version(LogicalFile.Version at, FileMetaData footer) {}

  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final ArmContext ctx;
  private final RegionWriter regions;
  private final TailPolicy policy;
  private final boolean inPlace; // footerOrder logical: rewritten row groups keep their place
  private final boolean split; // a new object per version (SplitFile), not one appendable object
  private final List<LogicalFile> files = new ArrayList<>();
  private final List<LogicalFile> rewritten = new ArrayList<>(); // replaced by compaction; stored
  private final List<List<Version>> history = new ArrayList<>(); // per step: each file's version
  private int compactions = 0;
  private final Map<Long, Integer> fileOfRg = new HashMap<>();
  private int tail = -1; // the file inserts go to; -1: start a new one
  private long nextRgId = 0;
  private final long[] rgOfId; // -1: absent or deleted
  private final int[] posInRg; // a row's position within its row group
  private double bytesPerRow = 40; // running estimate, for planning merges
  private long packBytes; // compaction's row-group target: a full row group of the initial load
  private final WriterFooters footers; // what the writer has read in this batch or compaction
  // what this batch's or compaction's writer holds (rows, regions waiting for their append);
  // null outside one
  private Held.Scope held;

  public RmowArm(ArmContext ctx, TailPolicy.Kind kind) {
    this(ctx, kind, false);
  }

  public RmowArm(ArmContext ctx, TailPolicy.Kind kind, boolean split) {
    this.split = split;
    this.ctx = ctx;
    this.regions = new RegionWriter(Rows.SCHEMA, ctx.layout().writerProperties());
    this.policy = new TailPolicy(kind, ctx.layout().tailSealBytes, ctx.layout().tierFanout);
    this.inPlace =
        switch (ctx.layout().footerOrder) {
          case "logical" -> true;
          case "offset" -> false;
          default ->
              throw new IllegalArgumentException("Unknown footer order: " + ctx.layout().footerOrder);
        };
    this.packBytes = ctx.layout().rowGroupBytes;
    this.footers = new WriterFooters(ctx);
    this.rgOfId = new long[(int) ctx.log().totalIds()];
    this.posInRg = new int[(int) ctx.log().totalIds()];
    Arrays.fill(rgOfId, -1);
  }

  @Override
  public String name() {
    return ctx.name();
  }

  /**
   * Adopts the initial files. Compaction packs row groups up to the on-disk size of the writer's
   * own: the largest row group of an initial file that has several (the writer's target counts
   * buffered bytes, so it is not the size on disk).
   */
  @Override
  public void load(List<InitialLoad.InitialFile> initial) throws IOException {
    long largest = 0;
    for (InitialLoad.InitialFile f : initial) {
      LogicalFile file =
          split
              ? SplitFile.adopt(ctx.setupIo(), f.location(), f.length(), () -> nextRgId++)
              : RmowFile.adopt(ctx.setupIo(), f.location(), f.length(), () -> nextRgId++);
      int k = files.size();
      files.add(file);
      long id = f.firstId();
      List<RowGroup> rgs = file.footer().getRow_groups();
      List<Long> rgIds = file.rgIds();
      for (int i = 0; i < rgs.size(); i++) {
        if (rgs.size() > 1) {
          largest = Math.max(largest, FooterCodec.compressedSize(rgs.get(i)));
        }
        fileOfRg.put(rgIds.get(i), k);
        for (int j = 0; j < rgs.get(i).getNum_rows(); j++, id++) {
          rgOfId[(int) id] = rgIds.get(i);
          posInRg[(int) id] = j;
        }
      }
      tail = file.liveBytes() < ctx.layout().targetFileBytes ? k : -1;
    }
    if (largest > 0) {
      packBytes = largest;
    }
    history.add(versions());
  }

  private List<Version> versions() {
    List<Version> v = new ArrayList<>(files.size());
    for (LogicalFile f : files) {
      v.add(new Version(f.current(), f.footer()));
    }
    return v;
  }

  /** The changes a batch makes to one live row group, and its rows once read, as {id, version}. */
  private static final class Touched {
    final Map<Long, Integer> updates = new HashMap<>();
    final Set<Long> deletes = new HashSet<>();
    List<long[]> rows;

    /** The row group's rows after this batch's updates and deletes. */
    List<long[]> merged() {
      List<long[]> out = new ArrayList<>(rows.size());
      for (long[] r : rows) {
        Integer v = updates.get(r[0]);
        if (v != null) {
          out.add(new long[] {r[0], v});
        } else if (!deletes.contains(r[0])) {
          out.add(r);
        }
      }
      return out;
    }
  }

  /** The regions one file gets in this batch. */
  private static final class Pending {
    final LogicalFile file; // null: a new file
    final String location; // where the regions go
    final List<Region> regions = new ArrayList<>();
    final List<Long> newRgIds = new ArrayList<>();
    final List<Long> inPlaceOf = new ArrayList<>(); // per region: the row group it replaces, or null
    final List<Long> drop = new ArrayList<>();

    Pending(LogicalFile file, String location) {
      this.file = file;
      this.location = location;
    }

    long nextBase() {
      if (!regions.isEmpty()) {
        return regions.get(regions.size() - 1).end();
      }
      return file == null ? 0 : file.regionBase();
    }
  }

  /** A new file's first object, from its stem. */
  private String newLocation(String stem) {
    return stem + (split ? ".r000.parquet" : ".parquet");
  }

  private LogicalFile create(Pending p) throws IOException {
    return split
        ? SplitFile.create(ctx.io(), p.location, p.regions, p.newRgIds)
        : RmowFile.create(ctx.io(), p.location, p.regions, p.newRgIds);
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    footers.begin();
    if (ctx.layout().locatesByScan()) {
      ctx.phases().run("locate", () -> locateByStats(b));
    }

    // every row group an update or delete touches is rewritten
    Map<Long, Touched> touched = new TreeMap<>();
    for (int i = 0; i < b.updateIds().length + b.deleteIds().length; i++) {
      boolean update = i < b.updateIds().length;
      long id = update ? b.updateIds()[i] : b.deleteIds()[i - b.updateIds().length];
      long rg = rgOfId[(int) id];
      if (update) {
        touched.computeIfAbsent(rg, k -> new Touched()).updates.put(id, b.updateVersions()[i]);
      } else {
        touched.computeIfAbsent(rg, k -> new Touched()).deletes.add(id);
      }
      if (!update) {
        rgOfId[(int) id] = -1;
      }
    }

    int newRows = b.insertIds().length;
    TailPolicy.Plan plan =
        newRows > 0
            ? policy.plan((long) (newRows * bytesPerRow))
            : new TailPolicy.Plan(List.of(), 0);
    Set<Long> mergeIds = new LinkedHashSet<>();
    for (TailPolicy.Unit u : plan.merge()) {
      mergeIds.add(u.id());
      touched.computeIfAbsent(u.id(), k -> new Touched());
    }

    // a row group at a time, as parts of the batch's merge-read and write: read it, rewrite it,
    // and drop its rows; the open tail row groups the policy merges are read, then written with
    // the inserts
    Map<Integer, Pending> pending = new TreeMap<>();
    Map<Long, List<Long>> idsOfNewRg = new HashMap<>();
    held = Held.scope("the batch's appends sent");
    try {
      for (Map.Entry<Long, Touched> e : touched.entrySet()) {
        long rg = e.getKey();
        if (mergeIds.contains(rg)) {
          continue;
        }
        Touched t = e.getValue();
        ctx.phases().part("merge-read", () -> t.rows = readRowGroup(rg));
        ctx.phases()
            .part(
                "write",
                () -> {
                  rewrite(rg, t, pending, idsOfNewRg);
                  held.release(Held.Kind.ROWS, t.rows.size(), "row group " + rg + " rewritten");
                });
        t.rows = null;
      }
      for (long rg : mergeIds) {
        Touched t = touched.get(rg);
        ctx.phases().part("merge-read", () -> t.rows = readRowGroup(rg));
      }
      ctx.phases()
          .part("write", () -> writeRegions(b, touched, plan, mergeIds, pending, idsOfNewRg));
    } finally {
      held.close();
      held = null;
    }
    history.add(versions());
  }

  /** A row group's rows, held until the writer has encoded them. */
  private List<long[]> readRowGroup(long rg) throws IOException {
    List<long[]> rows = readRowGroup(files.get(fileOfRg.get(rg)), rg);
    held.hold(Held.Kind.ROWS, rows.size(), "read back row group " + rg);
    return rows;
  }

  /**
   * The rows of one row group of {@code f}'s current version, read as a writer reads, as {id,
   * version} ({@link RowBatch#idVersions}).
   */
  private List<long[]> readRowGroup(LogicalFile f, long rgId) throws IOException {
    RowGroup rg = f.rowGroup(rgId);
    List<long[]> rows = new ArrayList<>((int) rg.getNum_rows());
    LogicalFile.RowGroupSource src = f.source(rgId);
    Scans.scan(
        ctx,
        Reader.writer(ctx.layout()),
        Scans.version(src.location(), src.length(), f.liveRows()),
        NO_DELETES,
        Rows.SCHEMA,
        Expressions.alwaysTrue(),
        footerFor(f, src),
        src.start(),
        src.size(),
        b -> b.idVersions(rows));
    if (rows.size() != rg.getNum_rows()) {
      throw new IllegalStateException(
          "Read " + rows.size() + " rows of row group " + rgId + ", expected " + rg.getNum_rows());
    }
    return rows;
  }

  /**
   * The footer a writer's read of {@code src} opens it with: the one the writer holds, or none
   * (the open reads it: a file of at most 1 MiB whole) on its first use in the transaction, or on
   * every use under {@code open}. For a footer-only version, the object's view of the version's
   * footer, derived in memory once that is read.
   */
  private byte[] footerFor(LogicalFile f, LogicalFile.RowGroupSource src) throws IOException {
    if (f.current().footerOnly()) {
      footer(f);
      return src.tail();
    }
    if (f.length() <= Scans.EAGER_BYTES || footers.mustRead(f.location(), f.length())) {
      return null;
    }
    return src.tail();
  }

  /**
   * {@code f}'s current footer as the writer has it: read from storage on its first use in the
   * transaction, or on every use under {@code open} ({@link WriterFooters}).
   */
  private FileMetaData footer(LogicalFile f) throws IOException {
    return footers.mustRead(f.location(), f.length()) ? readFooter(f) : f.footer();
  }

  private Pending pending(Map<Integer, Pending> pending, int k) {
    return pending.computeIfAbsent(
        k,
        i ->
            i < files.size()
                ? new Pending(files.get(i), files.get(i).regionLocation())
                : new Pending(null, newLocation(String.format("%s/data/rmow-%05d", ctx.root(), i))));
  }

  /**
   * Row-group-level copy-on-write of a touched row group the tail merge does not absorb: its rows
   * with the batch's changes, as a new region of its file.
   */
  private void rewrite(
      long rg, Touched t, Map<Integer, Pending> pending, Map<Long, List<Long>> idsOfNewRg)
      throws IOException {
    Pending p = pending(pending, fileOfRg.get(rg));
    p.drop.add(rg);
    Region r = addRegion(p, t.merged(), idsOfNewRg, inPlace ? rg : null);
    if (policy.isOpen(rg)) {
      policy.replace(
          rg,
          r == null ? -1 : p.newRgIds.get(p.newRgIds.size() - 1),
          r == null ? 0 : r.bytes().length);
    }
  }

  /** The inserts, with the open tail row groups merged into them, and every file's append. */
  private void writeRegions(
      OpLog.Batch b,
      Map<Long, Touched> touched,
      TailPolicy.Plan plan,
      Set<Long> mergeIds,
      Map<Integer, Pending> pending,
      Map<Long, List<Long>> idsOfNewRg)
      throws IOException {
    // the inserts, merged with the open tail row groups the policy picked
    if (b.insertIds().length > 0) {
      List<long[]> rows = new ArrayList<>(b.insertIds().length);
      for (long id : b.insertIds()) {
        rows.add(new long[] {id, 0});
      }
      held.hold(Held.Kind.ROWS, rows.size(), "the batch's inserts");
      Pending p = pending(pending, tail >= 0 ? tail : files.size());
      for (long rg : mergeIds) {
        rows.addAll(touched.get(rg).merged());
        p.drop.add(rg);
      }
      Region r = addRegion(p, rows, idsOfNewRg, null);
      bytesPerRow = r.bytes().length / (double) r.rows();
      policy.commit(plan, p.newRgIds.get(p.newRgIds.size() - 1), r.bytes().length);
      long merged = 0;
      for (long rg : mergeIds) {
        merged += touched.get(rg).rows.size();
      }
      held.release(Held.Kind.ROWS, b.insertIds().length + merged, "the inserts' region encoded");
    }

    for (Map.Entry<Integer, Pending> e : pending.entrySet()) {
      commit(e.getKey(), e.getValue(), idsOfNewRg);
    }
    // the new versions' manifest entries: column metrics from the merged footers
    for (int k : pending.keySet()) {
      files.get(k).dataFile();
    }
    if (tail >= 0
        && (files.get(tail).liveBytes() >= ctx.layout().targetFileBytes
            || files.get(tail).footer().getRow_groupsSize() >= ctx.layout().maxRowGroupsPerFile)) {
      tail = -1;
      policy.sealAll();
    }
  }

  /** Writes {@code p} as file {@code k}'s next version (or as new file {@code k}); indexes it. */
  private void commit(int k, Pending p, Map<Long, List<Long>> idsOfNewRg) throws IOException {
    if (p.file == null) {
      files.add(create(p));
      tail = k;
    } else {
      footer(p.file); // the new footer lists the old one's row groups
      p.file.commit(
          ctx.io(), ctx.io(), p.regions, p.drop, p.newRgIds, inPlace ? p.inPlaceOf : null);
    }
    footers.wrote(files.get(k).location(), files.get(k).length());
    long sent = 0;
    for (Region r : p.regions) {
      sent += r.bytes().length;
    }
    held.release(Held.Kind.PENDING, sent, "append to " + name(files.get(k).location()) + " sent");
    if (inPlace && outOfOrder(files.get(k)) > 0) {
      throw new IllegalStateException(
          "Footer of " + files.get(k).location() + " lists row groups out of id order");
    }
    for (long rg : p.drop) {
      fileOfRg.remove(rg);
    }
    for (long rg : p.newRgIds) {
      fileOfRg.put(rg, k);
      List<Long> ids = idsOfNewRg.get(rg);
      for (int pos = 0; pos < ids.size(); pos++) {
        rgOfId[ids.get(pos).intValue()] = rg;
        posInRg[ids.get(pos).intValue()] = pos;
      }
    }
  }

  /**
   * Adds {@code rows} as one new region of {@code p}'s file, to take the place of row group {@code
   * inPlaceOf} in the footer (null: listed last); null if there are no rows.
   */
  private Region addRegion(
      Pending p, List<long[]> rows, Map<Long, List<Long>> idsOfNewRg, Long inPlaceOf)
      throws IOException {
    if (rows.isEmpty()) {
      return null;
    }
    OpLog log = ctx.log();
    if (ctx.layout().sortWrites) {
      rows.sort(Comparator.comparingLong((long[] r) -> log.ts(r[0])).thenComparingLong(r -> r[0]));
    }
    long rg = nextRgId++;
    List<Long> ids = new ArrayList<>(rows.size());
    for (long[] r : rows) {
      ids.add(r[0]);
    }
    Region region = regions.write(p.location, p.nextBase(), new MadeRows(rows, log));
    held.hold(Held.Kind.PENDING, region.bytes().length, "a region of " + name(p.location));
    p.regions.add(region);
    p.newRgIds.add(rg);
    p.inPlaceOf.add(inPlaceOf);
    idsOfNewRg.put(rg, ids);
    return region;
  }

  private static String name(String location) {
    return location.substring(location.lastIndexOf('/') + 1);
  }

  // --- locating the rows a batch changes --------------------------------------------------------

  /**
   * Finds the row group of each updated and deleted row from the footers' {@code id} statistics:
   * updates rewrite rows within their row group and inserts take new ids, so row-group id ranges
   * stay disjoint and a range lookup is exact. Each file's footer is read from storage on its
   * first use in the batch ({@link WriterFooters}). Checks every row group found against the
   * index.
   */
  private void locateByStats(OpLog.Batch b) throws IOException {
    List<long[]> ranges = new ArrayList<>(); // {min id, max id, row group id}
    for (LogicalFile f : files) {
      if (f.liveRows() == 0) {
        continue;
      }
      FileMetaData footer = footer(f);
      List<Long> ids = f.rgIds();
      for (int i = 0; i < footer.getRow_groupsSize(); i++) {
        Statistics st =
            footer.getRow_groups().get(i).getColumns().get(0).getMeta_data().getStatistics();
        ranges.add(new long[] {le64(st.getMin_value()), le64(st.getMax_value()), ids.get(i)});
      }
    }
    ranges.sort(Comparator.comparingLong(r -> r[0]));
    for (int i = 1; i < ranges.size(); i++) {
      if (ranges.get(i)[0] <= ranges.get(i - 1)[1]) {
        throw new IllegalStateException("Row-group id ranges overlap: " + i);
      }
    }
    long[] mins = ranges.stream().mapToLong(r -> r[0]).toArray();
    for (long[] ids : new long[][] {b.updateIds(), b.deleteIds()}) {
      for (long id : ids) {
        int i = Arrays.binarySearch(mins, id);
        int at = i >= 0 ? i : -i - 2;
        if (at < 0 || id > ranges.get(at)[1] || ranges.get(at)[2] != rgOfId[(int) id]) {
          throw new IllegalStateException("Statistics do not place row " + id + " in its row group");
        }
      }
    }
  }

  /**
   * Reads a file's current footer from storage as an open does: tail, then footer, as
   * parquet-java reads it, or a version of up to 1 MiB whole, as Iceberg fetches it.
   */
  private FileMetaData readFooter(LogicalFile f) throws IOException {
    byte[] tail =
        Scans.readFooterObject(ctx, Reader.writer(ctx.layout()), f.location(), f.length());
    return FooterCodec.read(tail, 0, tail.length - 8);
  }

  /** A little-endian integer of up to 8 bytes (Parquet's plain encoding of INT32/INT64). */
  private static long le64(byte[] b) {
    long v = 0;
    for (int i = b.length - 1; i >= 0; i--) {
      v = (v << 8) | (b[i] & 0xFF);
    }
    return b.length == 4 ? (int) v : v;
  }

  // --- reads ------------------------------------------------------------------------------------


  @Override
  public TableHash read(Expression filter, Reader reader) throws IOException {
    InclusiveMetricsEvaluator prune = new InclusiveMetricsEvaluator(Rows.SCHEMA, filter);
    List<Scans.ReadFile> plain = new ArrayList<>();
    List<Scans.ReadFile> footerOnly = new ArrayList<>();
    for (LogicalFile f : files) {
      if (f.liveRows() > 0 && prune.eval(f.dataFile())) {
        (f.current().footerOnly() ? footerOnly : plain)
            .add(new Scans.ReadFile(f.dataFile(), NO_DELETES));
      }
    }
    return read(reader, plain, footerOnly, filter);
  }

  private TableHash read(
      Reader reader,
      List<Scans.ReadFile> plain,
      List<Scans.ReadFile> footerOnly,
      Expression filter)
      throws IOException {
    TableHash h = Scans.readAll(ctx, reader, plain, filter);
    if (!footerOnly.isEmpty()) {
      h.addAll(Scans.readSplit(ctx, reader, footerOnly, filter));
    }
    return h;
  }

  @Override
  public TableHash readSorted(Reader reader) throws IOException {
    List<Scans.ReadFile> toRead = new ArrayList<>();
    for (LogicalFile f : files) {
      if (f.liveRows() == 0) {
        continue;
      }
      if (f.current().footerOnly()) {
        throw new UnsupportedOperationException("A sorted read of split files: " + f.location());
      }
      toRead.add(new Scans.ReadFile(f.dataFile(), NO_DELETES));
    }
    return Scans.readSorted(ctx, reader, toRead);
  }

  /**
   * Old versions are planned as a snapshot's manifest entries would plan them: an ordinary
   * version with the split offsets of its own footer, so stock readers split it per row group as
   * they did while it was current. A footer-only version has none (its row groups are in other
   * objects).
   */
  @Override
  public TableHash readAfter(int steps, Reader reader) throws IOException {
    List<Scans.ReadFile> plain = new ArrayList<>();
    List<Scans.ReadFile> footerOnly = new ArrayList<>();
    for (Version v : history.get(steps)) {
      String location = v.at().location();
      long length = v.at().length();
      if (v.at().footerOnly()) {
        footerOnly.add(new Scans.ReadFile(Scans.version(location, length, 1), NO_DELETES));
      } else {
        DataFile df =
            Scans.version(
                location, length, v.footer().getNum_rows(), RmowFile.splitOffsets(v.footer()));
        plain.add(new Scans.ReadFile(df, NO_DELETES));
      }
    }
    return read(reader, plain, footerOnly, Expressions.alwaysTrue());
  }

  // --- compaction -------------------------------------------------------------------------------

  /**
   * Compaction, with one trigger and one action per goal (docs/DESIGN.md#compaction):
   *
   * <ul>
   *   <li><b>Space.</b> An appendable file whose garbage (bytes its current version does not use)
   *       reaches {@code compactGarbageRatio} of its length is {@linkplain #rewrite rewritten}
   *       into a new object.
   *       A split file instead has each object whose dead bytes reach {@code compactGarbageRatio}
   *       of its length {@linkplain #evacuees evacuated}: the object's live row groups are
   *       rewritten as they are into the next version's region object, so it can be deleted once
   *       older versions expire.
   *   <li><b>Reads.</b> A file holding {@code compactSmallRowGroups} sealed row groups under half
   *       the writer's row-group size (see {@link #load}) has each run of them that is adjacent in
   *       id order {@linkplain #packs packed} into row groups of up to that size. In an
   *       appendable file the packed row groups become garbage.
   * </ul>
   *
   * A file's evacuations and packing are one new version.
   */
  @Override
  public void compact() throws IOException {
    held = Held.scope("the compaction's appends sent");
    try {
      compactHeld();
    } finally {
      held.close();
      held = null;
    }
  }

  private void compactHeld() throws IOException {
    footers.begin();
    LayoutSpec layout = ctx.layout();
    for (int i = 0; i < files.size(); i++) {
      LogicalFile f = files.get(i);
      if (f.liveRows() == 0) {
        continue;
      }
      footer(f); // planning needs its row groups' sizes, and its garbage
      List<List<Long>> packs = packs(f);
      if (split) {
        Set<Long> evacuate = evacuees((SplitFile) f);
        if (!evacuate.isEmpty() || !packs.isEmpty()) {
          move(i, evacuate, packs);
        }
        continue;
      }
      double garbage = f.garbageBytes() / (double) (f.storedBytes() - f.reclaimableBytes());
      if (garbage >= layout.compactGarbageRatio) {
        rewrite(i);
      } else if (!packs.isEmpty()) {
        move(i, Set.of(), packs);
      }
    }
  }

  /**
   * The groups of row groups packing merges, each in id order: runs of small sealed row groups
   * (under half the writer's row-group size, and not open to the tail policy) adjacent in id
   * order, cut greedily into groups of up to that size; groups of one are left alone. None unless
   * the file holds {@code compactSmallRowGroups} small sealed row groups (0: never).
   */
  private List<List<Long>> packs(LogicalFile f) {
    long target = packBytes;
    int threshold = ctx.layout().compactSmallRowGroups;
    List<Long> ids = new ArrayList<>(f.rgIds());
    long small =
        ids.stream()
            .filter(rg -> !policy.isOpen(rg))
            .filter(rg -> FooterCodec.compressedSize(f.rowGroup(rg)) < target / 2)
            .count();
    if (threshold <= 0 || small < threshold) {
      return List.of();
    }
    ids.sort(Comparator.comparingLong(rg -> minId(f.rowGroup(rg))));
    List<List<Long>> packs = new ArrayList<>();
    List<Long> group = new ArrayList<>();
    long bytes = 0;
    for (long rg : ids) {
      long size = FooterCodec.compressedSize(f.rowGroup(rg));
      boolean packable = !policy.isOpen(rg) && size < target / 2;
      if (!packable || bytes + size > target) {
        if (group.size() > 1) {
          packs.add(group);
        }
        group = new ArrayList<>();
        bytes = 0;
      }
      if (packable) {
        group.add(rg);
        bytes += size;
      }
    }
    if (group.size() > 1) {
      packs.add(group);
    }
    return packs;
  }

  /**
   * The live row groups of each object of a split file whose dead bytes (replaced row groups, and
   * the footer of an object adopted as version 0) reach {@code compactGarbageRatio} of its length.
   * Objects holding a row group open to the tail policy are left alone: its merges rewrite them
   * soon.
   */
  private Set<Long> evacuees(SplitFile f) {
    Map<String, List<Long>> rgsOf = new LinkedHashMap<>();
    Map<String, Long> live = new HashMap<>();
    Set<String> open = new HashSet<>();
    for (long rg : f.rgIds()) {
      String object = f.objectOf(rg);
      rgsOf.computeIfAbsent(object, k -> new ArrayList<>()).add(rg);
      live.merge(object, FooterCodec.liveBytes(f.rowGroup(rg)), Long::sum);
      if (policy.isOpen(rg)) {
        open.add(object);
      }
    }
    Set<Long> evacuate = new LinkedHashSet<>();
    for (Map.Entry<String, List<Long>> e : rgsOf.entrySet()) {
      long length = f.objectLength(e.getKey());
      long dead = length - live.get(e.getKey());
      if (!open.contains(e.getKey()) && dead >= ctx.layout().compactGarbageRatio * length) {
        evacuate.addAll(e.getValue());
      }
    }
    return evacuate;
  }

  /**
   * Writes file {@code i}'s next version: each pack group's rows as one row group, and each other
   * row group in {@code evacuate} as it is, each taking the place of the (first) row group it
   * replaces.
   */
  private void move(int i, Set<Long> evacuate, List<List<Long>> packs) throws IOException {
    LogicalFile f = files.get(i);
    List<List<Long>> groups = new ArrayList<>(packs);
    Set<Long> packed = new HashSet<>();
    packs.forEach(packed::addAll);
    for (long rg : evacuate) {
      if (!packed.contains(rg)) {
        groups.add(List.of(rg));
      }
    }
    Pending p = new Pending(f, f.regionLocation());
    Map<Long, List<Long>> idsOfNewRg = new HashMap<>();
    for (List<Long> g : groups) {
      List<long[]> rows = new ArrayList<>();
      for (long rg : g) {
        rows.addAll(readRowGroup(f, rg));
      }
      held.hold(Held.Kind.ROWS, rows.size(), "read back a pack of " + name(f.location()));
      p.drop.addAll(g);
      addRegion(p, rows, idsOfNewRg, inPlace ? g.get(0) : null);
      held.release(Held.Kind.ROWS, rows.size(), "a pack of " + name(f.location()) + " encoded");
    }
    commit(i, p, idsOfNewRg);
    f.dataFile();
  }

  /** The smallest id in a row group, from its statistics. */
  private static long minId(RowGroup rg) {
    return le64(rg.getColumns().get(0).getMeta_data().getStatistics().getMin_value());
  }

  private void rewrite(int i) throws IOException {
    LogicalFile f = files.get(i);
    // the live rows, grouped by the row group that holds each one's live version
    Map<Long, List<long[]>> byRg = new LinkedHashMap<>();
    java.util.function.Consumer<RowBatch> group =
        b -> {
          for (int k = 0; k < b.size(); k++) {
            long id = b.id(k);
            byRg.computeIfAbsent(rgOfId[(int) id], g -> new ArrayList<>())
                .add(new long[] {id, b.version(k)});
          }
        };
    Reader writer = Reader.writer(ctx.layout());
    if (f.current().footerOnly()) {
      FileMetaData footer = footer(f);
      for (SplitFile.ObjectView v : SplitFile.views(f.location(), footer)) {
        Scans.scan(ctx, writer, Scans.version(v.location(), v.length(), v.rows()),
            NO_DELETES, Rows.SCHEMA, Expressions.alwaysTrue(), v.tail(), -1, -1, group);
      }
    } else {
      Scans.scan(
          ctx,
          writer,
          f.dataFile(),
          NO_DELETES,
          Rows.SCHEMA,
          Expressions.alwaysTrue(),
          f.length() <= Scans.EAGER_BYTES || footers.mustRead(f.location(), f.length())
              ? null
              : f.tail(),
          -1,
          -1,
          group);
    }

    long read = 0;
    for (List<long[]> rs : byRg.values()) {
      read += rs.size();
    }
    held.hold(Held.Kind.ROWS, read, "read back " + name(f.location()) + " whole");

    // pack the row groups in id order (footer order puts rewritten row groups last) into row
    // groups of up to the writer's size, as bin-packing does (MorArm.compact), so a full row
    // group stays as it is and the packed ones keep disjoint id ranges
    List<Long> order = new ArrayList<>(byRg.keySet());
    order.sort(Comparator.comparingLong(rg -> minId(byRg.get(rg))));
    String location =
        newLocation(String.format("%s/data/rmow-%05d-c%03d", ctx.root(), i, ++compactions));
    Pending p = new Pending(null, location);
    Map<Long, List<Long>> idsOfNewRg = new HashMap<>();
    List<long[]> packed = new ArrayList<>();
    long bytes = 0;
    for (long rg : order) {
      long size = FooterCodec.compressedSize(f.rowGroup(rg));
      if (!packed.isEmpty() && bytes + size > packBytes) {
        addRegion(p, packed, idsOfNewRg, null);
        packed = new ArrayList<>();
        bytes = 0;
      }
      packed.addAll(byRg.get(rg));
      bytes += size;
    }
    addRegion(p, packed, idsOfNewRg, null);
    LogicalFile compacted = create(p);
    held.release(Held.Kind.ROWS, read, name(f.location()) + " rewritten");
    long sent = 0;
    for (Region r : p.regions) {
      sent += r.bytes().length;
    }
    held.release(Held.Kind.PENDING, sent, name(compacted.location()) + " created");
    footers.wrote(compacted.location(), compacted.length());
    compacted.dataFile();
    for (long rg : f.rgIds()) {
      fileOfRg.remove(rg);
    }
    for (long rg : p.newRgIds) {
      fileOfRg.put(rg, i);
      List<Long> ids = idsOfNewRg.get(rg);
      for (int pos = 0; pos < ids.size(); pos++) {
        rgOfId[ids.get(pos).intValue()] = rg;
        posInRg[ids.get(pos).intValue()] = pos;
      }
    }
    files.set(i, compacted);
    rewritten.add(f);
    if (i == tail) {
      policy.sealAll(); // its open row groups are gone
    }
  }

  private static long minId(List<long[]> rows) {
    long min = Long.MAX_VALUE;
    for (long[] r : rows) {
      min = Math.min(min, r[0]);
    }
    return min;
  }

  @Override
  public ArmStats stats() {
    long stored = 0;
    long live = 0;
    long rowGroups = 0;
    long footers = 0;
    long outOfOrder = 0;
    long objects = 0;
    long used = 0;
    long reclaimable = 0;
    for (LogicalFile f : files) {
      used += f.usedObjects();
      stored += f.storedBytes() - f.reclaimableBytes();
      reclaimable += f.reclaimableBytes();
      objects += f.objects().size();
      live += f.liveBytes();
      rowGroups += f.footer().getRow_groupsSize();
      footers += f.tail().length;
      outOfOrder += outOfOrder(f);
    }
    for (LogicalFile f : rewritten) {
      reclaimable += f.storedBytes();
      objects += f.objects().size();
    }
    return new ArmStats(
        objects,
        stored + reclaimable,
        live,
        reclaimable,
        rowGroups,
        files.size(),
        0,
        footers,
        outOfOrder,
        used);
  }

  /** Row groups the footer lists after one holding a larger id. */
  private static long outOfOrder(LogicalFile f) {
    long count = 0;
    long maxSoFar = Long.MIN_VALUE;
    for (RowGroup rg : f.footer().getRow_groups()) {
      Statistics st = rg.getColumns().get(0).getMeta_data().getStatistics();
      if (le64(st.getMin_value()) < maxSoFar) {
        count++;
      }
      maxSoFar = Math.max(maxSoFar, le64(st.getMax_value()));
    }
    return count;
  }

  // --- for VersionDump and tests -----------------------------------------------------------

  ArmContext ctx() {
    return ctx;
  }

  /** The table's files at their current versions. */
  List<LogicalFile> files() {
    return List.copyOf(files);
  }

  /** Compaction's row-group target (see {@link #load}). */
  long packBytes() {
    return packBytes;
  }

  boolean isOpen(long rgId) {
    return policy.isOpen(rgId);
  }

  /** Every appendable object this arm adopted or wrote, those compaction replaced included. */
  List<LogicalFile> objects() {
    List<LogicalFile> all = new ArrayList<>(rewritten);
    all.addAll(files);
    return all;
  }

  /** The number of table states recorded: the initial load and one per batch. */
  int steps() {
    return history.size();
  }

  /** The (location, length) of each file version the table held after {@code step} batches. */
  List<Map.Entry<String, Long>> versionsAfter(int step) {
    List<Map.Entry<String, Long>> out = new ArrayList<>();
    for (Version v : history.get(step)) {
      out.add(Map.entry(v.at().location(), v.at().length()));
    }
    return out;
  }
}
