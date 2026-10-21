package org.rmow.bench.arms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.RegionWriter;
import org.rmow.bench.rmow.RegionWriter.Region;
import org.rmow.bench.rmow.RmowFile;
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
 *   <li>With <b>DVs on sealed row groups</b> ({@code -dv} arms), rows in a file's stable prefix
 *       (its leading row groups in footer order that no tail merge will touch) are deleted with a
 *       deletion vector keyed to the file, as Iceberg would apply it, and their updated versions
 *       join the batch's new tail row group, as in MoR. Positions in the prefix never change, so
 *       a DV stays valid for every later version of the file. Rows in open tail row groups are
 *       still merged or rewritten.
 * </ul>
 *
 * The tail file rolls over once its live bytes reach the target file size or its footer lists
 * {@code maxRowGroupsPerFile} row groups, sealing every open tail. Replaced regions stay in their
 * object, where time travel still reads them.
 */
public final class RmowArm implements Arm {

  /** One file at one version, with its DV then: what a snapshot's manifest would name. */
  private record Version(RmowFile file, long length, DeleteFile dv) {}

  private final ArmContext ctx;
  private final RegionWriter regions;
  private final TailPolicy policy;
  private final boolean dvSealed;
  private final List<RmowFile> files = new ArrayList<>();
  private final List<RmowFile> rewritten = new ArrayList<>(); // replaced by compaction; stored
  private final List<List<Version>> history = new ArrayList<>(); // per step: each file's version
  private final Map<RmowFile, DeleteFile> dvs = new IdentityHashMap<>();
  private final Map<String, Long> dvObjects = new LinkedHashMap<>(); // Puffin files written
  private int compactions = 0;
  private final Map<Long, Integer> fileOfRg = new HashMap<>();
  private int tail = -1; // the file inserts go to; -1: start a new one
  private long nextRgId = 0;
  private final long[] rgOfId; // -1: absent or deleted
  private final int[] posInRg; // a row's position within its row group
  private double bytesPerRow = 40; // running estimate, for planning merges

  public RmowArm(ArmContext ctx, TailPolicy.Kind kind, boolean dvSealed) {
    this.ctx = ctx;
    this.regions = new RegionWriter(Rows.SCHEMA, ctx.layout().writerProperties());
    this.policy = new TailPolicy(kind, ctx.layout().tailSealBytes, ctx.layout().tierFanout);
    this.dvSealed = dvSealed;
    this.rgOfId = new long[(int) ctx.log().totalIds()];
    this.posInRg = new int[(int) ctx.log().totalIds()];
    Arrays.fill(rgOfId, -1);
  }

  @Override
  public String name() {
    return ctx.name();
  }

  @Override
  public void load(List<InitialLoad.InitialFile> initial) throws IOException {
    for (InitialLoad.InitialFile f : initial) {
      RmowFile file = RmowFile.adopt(ctx.setupIo(), f.location(), f.length(), () -> nextRgId++);
      int k = files.size();
      files.add(file);
      long id = f.firstId();
      List<RowGroup> rgs = file.footer().getRow_groups();
      List<Long> rgIds = file.rgIds();
      for (int i = 0; i < rgs.size(); i++) {
        fileOfRg.put(rgIds.get(i), k);
        for (int j = 0; j < rgs.get(i).getNum_rows(); j++, id++) {
          rgOfId[(int) id] = rgIds.get(i);
          posInRg[(int) id] = j;
        }
      }
      tail = file.liveBytes() < ctx.layout().targetFileBytes ? k : -1;
    }
    history.add(versions());
  }

  private List<Version> versions() {
    List<Version> v = new ArrayList<>(files.size());
    for (RmowFile f : files) {
      v.add(new Version(f, f.length(), dvs.get(f)));
    }
    return v;
  }

  /** The changes a batch makes to one live row group, and its rows once read. */
  private static final class Touched {
    final Map<Long, Integer> updates = new HashMap<>();
    final Set<Long> deletes = new HashSet<>();
    List<Record> rows;

    /** The row group's rows after this batch's updates and deletes. */
    List<Record> merged() {
      List<Record> out = new ArrayList<>(rows.size());
      for (Record r : rows) {
        long id = (Long) r.get(Rows.ID);
        Integer v = updates.get(id);
        if (v != null) {
          out.add(Rows.record(id, (Long) r.get(Rows.TS), v));
        } else if (!deletes.contains(id)) {
          out.add(r);
        }
      }
      return out;
    }
  }

  /** The regions one file gets in this batch. */
  private static final class Pending {
    final RmowFile file; // null: a new file
    final String location;
    final List<Region> regions = new ArrayList<>();
    final List<Long> newRgIds = new ArrayList<>();
    final List<Long> drop = new ArrayList<>();

    Pending(RmowFile file, String location) {
      this.file = file;
      this.location = location;
    }

    long nextBase() {
      if (!regions.isEmpty()) {
        return regions.get(regions.size() - 1).end();
      }
      return file == null ? 0 : file.length();
    }
  }

  /** Each row group's first row position in a file's current footer order. */
  private static Map<Long, Long> rowStarts(RmowFile f) {
    Map<Long, Long> starts = new HashMap<>();
    List<Long> ids = f.rgIds();
    long pos = 0;
    for (int i = 0; i < ids.size(); i++) {
      starts.put(ids.get(i), pos);
      pos += f.footer().getRow_groups().get(i).getNum_rows();
    }
    return starts;
  }

  /** The row groups of a file whose positions are fixed: those before its first open one. */
  private Set<Long> stablePrefix(RmowFile f) {
    Set<Long> prefix = new HashSet<>();
    for (long rg : f.rgIds()) {
      if (policy.isOpen(rg)) {
        break;
      }
      prefix.add(rg);
    }
    return prefix;
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    if ("scan".equals(ctx.layout().locate)) {
      ctx.phases().run("locate", () -> locate(b));
    }

    // rows in a stable prefix get DVs (-dv arms); every other touched row group is rewritten
    Map<Long, Touched> touched = new TreeMap<>();
    Map<RmowFile, List<Long>> dvPositions = new IdentityHashMap<>();
    List<Record> moved = new ArrayList<>(); // updated versions of DV'd rows, for the tail
    Map<RmowFile, Set<Long>> prefixes = new IdentityHashMap<>();
    Map<RmowFile, Map<Long, Long>> starts = new IdentityHashMap<>();
    for (int i = 0; i < b.updateIds().length + b.deleteIds().length; i++) {
      boolean update = i < b.updateIds().length;
      long id = update ? b.updateIds()[i] : b.deleteIds()[i - b.updateIds().length];
      long rg = rgOfId[(int) id];
      RmowFile f = files.get(fileOfRg.get(rg));
      if (dvSealed && prefixes.computeIfAbsent(f, this::stablePrefix).contains(rg)) {
        long pos = starts.computeIfAbsent(f, RmowArm::rowStarts).get(rg) + posInRg[(int) id];
        dvPositions.computeIfAbsent(f, k -> new ArrayList<>()).add(pos);
        if (update) {
          moved.add(Rows.record(id, ctx.log().ts(id), b.updateVersions()[i]));
        }
      } else if (update) {
        touched.computeIfAbsent(rg, k -> new Touched()).updates.put(id, b.updateVersions()[i]);
      } else {
        touched.computeIfAbsent(rg, k -> new Touched()).deletes.add(id);
      }
      if (!update) {
        rgOfId[(int) id] = -1;
      }
    }

    int newRows = b.insertIds().length + moved.size();
    TailPolicy.Plan plan =
        newRows > 0
            ? policy.plan((long) (newRows * bytesPerRow))
            : new TailPolicy.Plan(List.of(), 0);
    Set<Long> mergeIds = new LinkedHashSet<>();
    for (TailPolicy.Unit u : plan.merge()) {
      mergeIds.add(u.id());
      touched.computeIfAbsent(u.id(), k -> new Touched());
    }

    if (!touched.isEmpty()) {
      ctx.phases().run("merge-read", () -> readTouched(touched));
    }
    ctx.phases().run("write", () -> writeRegions(b, touched, plan, mergeIds, moved));
    if (!dvPositions.isEmpty()) {
      ctx.phases().run("delete-write", () -> writeDVs(b.index(), dvPositions));
    }
    history.add(versions());
  }

  private void readTouched(Map<Long, Touched> touched) throws IOException {
    for (Map.Entry<Long, Touched> e : touched.entrySet()) {
      RmowFile f = files.get(fileOfRg.get(e.getKey()));
      RowGroup rg = f.rowGroup(e.getKey());
      List<Record> rows = new ArrayList<>((int) rg.getNum_rows());
      // open row groups hold no DV'd rows, so the row group is read without its file's DV
      Scans.scan(
          ctx,
          Reader.writer(ctx.layout()),
          Scans.version(f.location(), f.length(), f.liveRows()),
          new DeleteFile[0],
          Rows.SCHEMA,
          Expressions.alwaysTrue(),
          ctx.layout().writerCachesFooters ? f.tail() : null,
          FooterCodec.startOffset(rg),
          FooterCodec.compressedSize(rg),
          rows::add);
      if (rows.size() != rg.getNum_rows()) {
        throw new IllegalStateException(
            "Read " + rows.size() + " rows of row group " + e.getKey() + ", expected "
                + rg.getNum_rows());
      }
      e.getValue().rows = rows;
    }
  }

  private Pending pending(Map<Integer, Pending> pending, int k) {
    return pending.computeIfAbsent(
        k,
        i ->
            i < files.size()
                ? new Pending(files.get(i), files.get(i).location())
                : new Pending(null, String.format("%s/data/rmow-%05d.parquet", ctx.root(), i)));
  }

  private void writeRegions(
      OpLog.Batch b,
      Map<Long, Touched> touched,
      TailPolicy.Plan plan,
      Set<Long> mergeIds,
      List<Record> moved)
      throws IOException {
    Map<Integer, Pending> pending = new TreeMap<>();
    Map<Long, List<Long>> idsOfNewRg = new HashMap<>();

    // row-group-level copy-on-write of touched row groups the tail merge does not absorb
    for (Map.Entry<Long, Touched> e : touched.entrySet()) {
      long rg = e.getKey();
      if (mergeIds.contains(rg)) {
        continue;
      }
      Pending p = pending(pending, fileOfRg.get(rg));
      p.drop.add(rg);
      Region r = addRegion(p, e.getValue().merged(), idsOfNewRg);
      if (policy.isOpen(rg)) {
        policy.replace(
            rg,
            r == null ? -1 : p.newRgIds.get(p.newRgIds.size() - 1),
            r == null ? 0 : r.bytes().length);
      }
    }

    // the inserts (and moved updates), merged with the open tail row groups the policy picked
    if (b.insertIds().length + moved.size() > 0) {
      List<Record> rows = new ArrayList<>(b.insertIds().length + moved.size());
      for (long id : b.insertIds()) {
        rows.add(Rows.record(id, ctx.log().ts(id), 0));
      }
      rows.addAll(moved);
      Pending p = pending(pending, tail >= 0 ? tail : files.size());
      for (long rg : mergeIds) {
        rows.addAll(touched.get(rg).merged());
        p.drop.add(rg);
      }
      Region r = addRegion(p, rows, idsOfNewRg);
      bytesPerRow = r.bytes().length / (double) r.rows();
      policy.commit(plan, p.newRgIds.get(p.newRgIds.size() - 1), r.bytes().length);
    }

    for (Map.Entry<Integer, Pending> e : pending.entrySet()) {
      Pending p = e.getValue();
      if (p.file == null) {
        files.add(RmowFile.create(ctx.io(), p.location, p.regions, p.newRgIds));
        tail = e.getKey();
      } else {
        p.file.append(ctx.io(), p.regions, p.drop, p.newRgIds);
      }
      for (long rg : p.drop) {
        fileOfRg.remove(rg);
      }
      for (long rg : p.newRgIds) {
        fileOfRg.put(rg, e.getKey());
        List<Long> ids = idsOfNewRg.get(rg);
        for (int pos = 0; pos < ids.size(); pos++) {
          rgOfId[ids.get(pos).intValue()] = rg;
          posInRg[ids.get(pos).intValue()] = pos;
        }
      }
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

  /** Adds {@code rows} as one new region of {@code p}'s file; null if there are no rows. */
  private Region addRegion(Pending p, List<Record> rows, Map<Long, List<Long>> idsOfNewRg)
      throws IOException {
    if (rows.isEmpty()) {
      return null;
    }
    if (ctx.layout().sortWrites) {
      rows.sort(
          Comparator.comparingLong((Record r) -> (Long) r.get(Rows.TS))
              .thenComparingLong(r -> (Long) r.get(Rows.ID)));
    }
    long rg = nextRgId++;
    List<Long> ids = new ArrayList<>(rows.size());
    for (Record r : rows) {
      ids.add((Long) r.get(Rows.ID));
    }
    Region region = regions.write(p.location, p.nextBase(), rows);
    p.regions.add(region);
    p.newRgIds.add(rg);
    idsOfNewRg.put(rg, ids);
    return region;
  }

  private void writeDVs(int batch, Map<RmowFile, List<Long>> positions) throws IOException {
    String location = String.format("%s/deletes/dv-%05d.puffin", ctx.root(), batch);
    Map<String, List<Long>> byPath = new HashMap<>();
    Map<String, RmowFile> fileOfPath = new HashMap<>();
    for (Map.Entry<RmowFile, List<Long>> e : positions.entrySet()) {
      byPath.put(e.getKey().location(), e.getValue());
      fileOfPath.put(e.getKey().location(), e.getKey());
    }
    DeleteWriteResult result =
        Dvs.write(ctx, location, byPath, path -> dvs.get(fileOfPath.get(path)));
    for (DeleteFile dv : result.deleteFiles()) {
      dvs.put(fileOfPath.get(dv.referencedDataFile()), dv);
    }
    if (!result.deleteFiles().isEmpty()) {
      dvObjects.put(location, result.deleteFiles().get(0).fileSizeInBytes());
    }
  }

  // --- locating the rows a batch changes --------------------------------------------------------

  private void locate(OpLog.Batch b) throws IOException {
    if (dvSealed) {
      locateByScan(b);
    } else {
      locateByStats(b);
    }
  }

  /**
   * Finds the row group of each updated and deleted row from the footers' {@code id} statistics:
   * updates rewrite rows within their row group and inserts take new ids, so row-group id ranges
   * stay disjoint and a range lookup is exact. Footers come from the writer's cache, or from
   * storage (two requests per file, as parquet-java reads them) without it. Checks every row
   * group found against the index.
   */
  private void locateByStats(OpLog.Batch b) throws IOException {
    List<long[]> ranges = new ArrayList<>(); // {min id, max id, row group id}
    for (RmowFile f : files) {
      if (f.liveRows() == 0) {
        continue;
      }
      FileMetaData footer = ctx.layout().writerCachesFooters ? f.footer() : readFooter(f);
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
   * With DVs, updated rows move to the tail, so row-group id ranges overlap and statistics no
   * longer place a row: finds rows as MoR does, scanning {@code id} and {@code _pos} (DVs
   * applied) of every file whose id bounds may hold a target, row groups pruned to the targets'
   * range. Checks every position found against the index.
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
    for (RmowFile f : files) {
      if (f.liveRows() == 0 || !mayHoldAny(f.dataFile(), targets)) {
        continue;
      }
      Map<Long, Long> starts = rowStarts(f);
      DeleteFile dv = dvs.get(f);
      Scans.scan(
          ctx,
          Reader.writer(ctx.layout()),
          f.dataFile(),
          dv == null ? new DeleteFile[0] : new DeleteFile[] {dv},
          projection,
          range,
          ctx.layout().writerCachesFooters ? f.tail() : null,
          -1,
          -1,
          r -> {
            long id = (Long) r.get(0);
            if (Arrays.binarySearch(targets, id) >= 0) {
              long rg = rgOfId[(int) id];
              Long start = starts.get(rg);
              if (start == null || start + posInRg[(int) id] != (Long) r.get(1)) {
                throw new IllegalStateException("Located row " + id + " away from its index entry");
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

  private static boolean mayHoldAny(DataFile df, long[] targets) {
    int idField = Rows.SCHEMA.findField("id").fieldId();
    if (df.lowerBounds() == null || df.lowerBounds().get(idField) == null) {
      return true;
    }
    long lower = Conversions.<Long>fromByteBuffer(Types.LongType.get(), df.lowerBounds().get(idField));
    long upper = Conversions.<Long>fromByteBuffer(Types.LongType.get(), df.upperBounds().get(idField));
    int i = Arrays.binarySearch(targets, lower);
    int first = i >= 0 ? i : -i - 1;
    return first < targets.length && targets[first] <= upper;
  }

  /** Reads a file's current footer from storage the way parquet-java does: tail, then footer. */
  private FileMetaData readFooter(RmowFile f) throws IOException {
    byte[] tail = Scans.readTail(ctx, Reader.writer(ctx.layout()), f.location(), f.length());
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

  private static DeleteFile[] dvArray(DeleteFile dv) {
    return dv == null ? new DeleteFile[0] : new DeleteFile[] {dv};
  }

  @Override
  public TableHash read(Expression filter, Reader reader) throws IOException {
    InclusiveMetricsEvaluator prune = new InclusiveMetricsEvaluator(Rows.SCHEMA, filter);
    List<Scans.ReadFile> toRead = new ArrayList<>();
    for (RmowFile f : files) {
      if (f.liveRows() > 0 && prune.eval(f.dataFile())) {
        toRead.add(new Scans.ReadFile(f.dataFile(), dvArray(dvs.get(f))));
      }
    }
    return Scans.readAll(ctx, reader, toRead, filter);
  }

  /** Old versions are planned without split offsets (a snapshot's entry would carry them). */
  @Override
  public TableHash readAfter(int steps, Reader reader) throws IOException {
    List<Scans.ReadFile> toRead = new ArrayList<>();
    for (Version v : history.get(steps)) {
      toRead.add(
          new Scans.ReadFile(Scans.version(v.file().location(), v.length(), 1), dvArray(v.dv())));
    }
    return Scans.readAll(ctx, reader, toRead, Expressions.alwaysTrue());
  }

  // --- compaction -------------------------------------------------------------------------------

  /**
   * Rewrites each file whose dead bytes reach {@code compactGarbageRatio} of its length, whose DV
   * deletes {@code compactDeleteRatio} of its rows, or that holds {@code compactSmallRowGroups}
   * row groups under half the row-group target, into a new object: its live rows (DV applied),
   * grouped by row group and packed in id order into row groups of about the target size. The
   * old object stays stored, for time travel, and becomes reclaimable.
   */
  @Override
  public void compact() throws IOException {
    LayoutSpec layout = ctx.layout();
    for (int i = 0; i < files.size(); i++) {
      RmowFile f = files.get(i);
      if (f.liveRows() == 0) {
        continue;
      }
      long small = 0;
      for (RowGroup rg : f.footer().getRow_groups()) {
        if (FooterCodec.compressedSize(rg) < layout.rowGroupBytes / 2) {
          small++;
        }
      }
      DeleteFile dv = dvs.get(f);
      double deleted = dv == null ? 0 : dv.recordCount() / (double) f.liveRows();
      double garbage = f.deadBytes() / (double) f.length();
      if (garbage >= layout.compactGarbageRatio
          || deleted >= layout.compactDeleteRatio
          || small >= layout.compactSmallRowGroups) {
        rewrite(i);
      }
    }
  }

  private void rewrite(int i) throws IOException {
    RmowFile f = files.get(i);
    // the live rows, grouped by the row group that holds each one's live version
    Map<Long, List<Record>> byRg = new LinkedHashMap<>();
    Scans.scan(
        ctx,
        Reader.writer(ctx.layout()),
        f.dataFile(),
        dvArray(dvs.get(f)),
        Rows.SCHEMA,
        Expressions.alwaysTrue(),
        ctx.layout().writerCachesFooters ? f.tail() : null,
        -1,
        -1,
        r -> byRg.computeIfAbsent(rgOfId[((Long) r.get(Rows.ID)).intValue()], k -> new ArrayList<>()).add(r));

    // pack the row groups in id order (footer order puts rewritten row groups last) up to the
    // row-group target, so the packed row groups keep disjoint id ranges
    List<Long> order = new ArrayList<>(byRg.keySet());
    order.sort(Comparator.comparingLong(rg -> minId(byRg.get(rg))));
    String location = String.format("%s/data/rmow-%05d-c%03d.parquet", ctx.root(), i, ++compactions);
    Pending p = new Pending(null, location);
    Map<Long, List<Long>> idsOfNewRg = new HashMap<>();
    List<Record> packed = new ArrayList<>();
    long bytes = 0;
    for (int n = 0; n < order.size(); n++) {
      long rg = order.get(n);
      packed.addAll(byRg.get(rg));
      bytes += FooterCodec.compressedSize(f.rowGroup(rg));
      if (bytes >= ctx.layout().rowGroupBytes || n == order.size() - 1) {
        addRegion(p, packed, idsOfNewRg);
        packed = new ArrayList<>();
        bytes = 0;
      }
    }
    RmowFile compacted = RmowFile.create(ctx.io(), location, p.regions, p.newRgIds);
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
    dvs.remove(f);
    rewritten.add(f);
    if (i == tail) {
      policy.sealAll(); // its open row groups are gone
    }
  }

  private static long minId(List<Record> rows) {
    long min = Long.MAX_VALUE;
    for (Record r : rows) {
      min = Math.min(min, (Long) r.get(Rows.ID));
    }
    return min;
  }

  @Override
  public ArmStats stats() {
    long stored = 0;
    long live = 0;
    long rowGroups = 0;
    long footers = 0;
    for (RmowFile f : files) {
      stored += f.length();
      live += f.liveBytes();
      rowGroups += f.footer().getRow_groupsSize();
      footers += f.tail().length;
    }
    long reclaimable = 0;
    for (RmowFile f : rewritten) {
      reclaimable += f.length();
    }
    Map<String, Long> livePuffins = new HashMap<>();
    for (DeleteFile dv : dvs.values()) {
      live += dv.contentSizeInBytes();
      livePuffins.put(dv.location(), dv.fileSizeInBytes());
    }
    long puffins = dvObjects.values().stream().mapToLong(Long::longValue).sum();
    reclaimable += puffins - livePuffins.values().stream().mapToLong(Long::longValue).sum();
    return new ArmStats(
        files.size() + rewritten.size() + dvObjects.size(),
        stored + reclaimable + livePuffins.values().stream().mapToLong(Long::longValue).sum(),
        live,
        reclaimable,
        rowGroups,
        files.size(),
        dvs.size(),
        footers);
  }
}
