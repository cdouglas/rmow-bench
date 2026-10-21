package org.rmow.bench.rmow;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.rmow.RegionWriter.Region;

/**
 * Region-merge-on-write without append: each version's regions go to a new object, and its
 * footer to a footer-only object whose row groups name the objects they live in ({@code
 * ColumnChunk.file_path}, relative to the footer's object), as a {@code _metadata} summary file
 * names its data files. A reader reads the version's one footer, then each object's row groups.
 *
 * <p>All of a file's objects share a directory and a stem: {@code <stem>.rNNN.parquet} holds the
 * regions of version NNN, followed by a footer listing just them, so each is also a standalone
 * Parquet file; {@code <stem>.vNNN.parquet} is version NNN's footer. Version 0 is an ordinary
 * Parquet file (the initial file, or {@code <stem>.r000.parquet}) whose row groups are its own.
 * No footer mixes its own row groups with other objects': stock readers resolve every offset in
 * the file they opened, so a mixed footer reads wrong rows silently, while a footer-only one fails
 * loudly (DESIGN.md).
 *
 * <p>Objects no version uses any more can be deleted as their versions expire: the current
 * version's objects stay, the rest are {@linkplain #reclaimableBytes reclaimable}. Bytes of
 * replaced row groups inside objects still in use are garbage, as in an appendable object.
 */
public final class SplitFile implements LogicalFile {

  /** One object a footer-only version reads, as a virtual Parquet file (see {@link #views}). */
  public record ObjectView(String location, long length, byte[] tail, long rows) {}

  private static final String PREV_FILE = "rmow.prev-file";

  private final String stem;
  private final String dir;
  private FileMetaData footer;
  private byte[] tail;
  private final List<Long> rgIds = new ArrayList<>();
  private final List<Version> versions = new ArrayList<>();
  private final Map<String, Long> objectLengths = new LinkedHashMap<>(); // every object written
  private final Map<Long, String> objectOfRg = new HashMap<>(); // live row group -> its object
  private Map<String, ObjectView> views; // of the current version, by object; computed on demand
  private int next = 1; // the next version's number
  private DataFile dataFile;

  private SplitFile(String stem) {
    this.stem = stem;
    this.dir = stem.substring(0, stem.lastIndexOf('/') + 1);
  }

  /** Writes {@code regions} as a new file's version 0, {@code <stem>.r000.parquet}. */
  public static SplitFile create(
      FileIO io, String location, List<Region> regions, List<Long> newRgIds) throws IOException {
    String suffix = ".r000.parquet";
    if (!location.endsWith(suffix)) {
      throw new IllegalArgumentException("A split file starts as <stem>" + suffix + ": " + location);
    }
    SplitFile f = new SplitFile(location.substring(0, location.length() - suffix.length()));
    long length = writeRegionObject(io, location, regions);
    FileMetaData footer = FooterCodec.merge(null, Set.of(), RmowFile.footers(regions), 0, 0);
    f.start(location, length, footer, FooterCodec.serialize(footer), newRgIds);
    return f;
  }

  /** Adopts an ordinary Parquet object as a file's version 0 (setup I/O, not measured). */
  public static SplitFile adopt(FileIO io, String location, long length, LongSupplier nextRgId)
      throws IOException {
    if (!location.endsWith(".parquet")) {
      throw new IllegalArgumentException("Not a .parquet object: " + location);
    }
    RmowFile plain = RmowFile.adopt(io, location, length, nextRgId);
    SplitFile f = new SplitFile(location.substring(0, location.length() - ".parquet".length()));
    f.start(location, length, plain.footer(), plain.tail(), plain.rgIds());
    return f;
  }

  private void start(String location, long length, FileMetaData footer, byte[] tail, List<Long> ids) {
    objectLengths.put(location, length);
    versions.add(new Version(location, length, false));
    for (long id : ids) {
      objectOfRg.put(id, location);
    }
    rgIds.addAll(ids);
    this.footer = footer;
    this.tail = tail;
  }

  /** The regions, then a footer listing just them: a standalone Parquet file. */
  private static long writeRegionObject(FileIO io, String location, List<Region> regions)
      throws IOException {
    RmowFile.checkContiguous(regions, 0);
    byte[] own =
        FooterCodec.serialize(FooterCodec.merge(null, Set.of(), RmowFile.footers(regions), 0, 0));
    long length = own.length;
    try (OutputStream out = io.newOutputFile(location).createOrOverwrite()) {
      for (Region r : regions) {
        out.write(r.bytes());
        length += r.bytes().length;
      }
      out.write(own);
    }
    return length;
  }

  private static String name(String location) {
    return location.substring(location.lastIndexOf('/') + 1);
  }

  /** The new version's regions, then its footer: two PUTs (one if it only drops row groups). */
  @Override
  public void commit(
      FileIO io,
      RegionAppender appender,
      List<Region> regions,
      Collection<Long> dropRgIds,
      List<Long> newRgIds,
      List<Long> inPlaceOf)
      throws IOException {
    if (newRgIds.size() != regions.size()) {
      throw new IllegalArgumentException("One new row group id per region");
    }
    Set<Integer> drop = RmowFile.positions(rgIds, dropRgIds, location());
    List<Integer> slots = RmowFile.slots(rgIds, inPlaceOf);
    String region = regionLocation();
    List<FileMetaData> placed = new ArrayList<>(regions.size());
    if (!regions.isEmpty()) {
      objectLengths.put(region, writeRegionObject(io, region, regions));
      for (Region r : regions) {
        FileMetaData f = r.footer().deepCopy();
        f.setRow_groups(
            f.getRow_groups().stream().map(rg -> FooterCodec.inFile(rg, name(region))).toList());
        placed.add(f);
      }
    }

    // version 0 lists its own row groups: from now on they are named by its object
    FileMetaData previous = footer;
    if (!current().footerOnly()) {
      previous = footer.deepCopy();
      String own = name(location());
      previous.setRow_groups(
          footer.getRow_groups().stream().map(rg -> FooterCodec.inFile(rg, own)).toList());
    }
    FileMetaData merged =
        FooterCodec.merge(previous, drop, placed, slots, length(), versions.size());
    List<KeyValue> kv = new ArrayList<>(merged.getKey_value_metadata());
    kv.add(new KeyValue(PREV_FILE).setValue(name(location())));
    merged.setKey_value_metadata(kv);
    byte[] object = FooterCodec.footerOnly(merged);
    String version = String.format("%s.v%03d.parquet", stem, next++);
    try (OutputStream out = io.newOutputFile(version).createOrOverwrite()) {
      out.write(object);
    }
    objectLengths.put(version, (long) object.length);

    List<Long> ids = RmowFile.nextIds(rgIds, drop, slots, newRgIds);
    for (Long id : dropRgIds) {
      objectOfRg.remove(id);
    }
    for (Long id : newRgIds) {
      objectOfRg.put(id, region);
    }
    rgIds.clear();
    rgIds.addAll(ids);
    footer = merged;
    tail = Arrays.copyOfRange(object, FooterCodec.MAGIC.length, object.length);
    versions.add(new Version(version, object.length, true));
    views = null;
    dataFile = null;
  }

  /**
   * The objects a footer-only version reads, in footer order, each as a virtual Parquet file: the
   * object's bytes up to the end of its last row group in the version, followed by the footer it
   * would have if it held just those row groups. Reading one takes no request for its footer (the
   * tail is in memory) and never the object's length (no HEAD).
   *
   * @param versionLocation the footer's object, which {@code file_path} is relative to
   */
  public static List<ObjectView> views(String versionLocation, FileMetaData footer) {
    String base = versionLocation.substring(0, versionLocation.lastIndexOf('/') + 1);
    Map<String, long[]> extent = new LinkedHashMap<>(); // path -> {end, rows}
    for (RowGroup rg : footer.getRow_groups()) {
      String path = Objects.requireNonNull(FooterCodec.filePath(rg), "Not a footer-only version");
      long[] e = extent.computeIfAbsent(path, p -> new long[2]);
      e[0] = Math.max(e[0], FooterCodec.end(rg));
      e[1] += rg.getNum_rows();
    }
    List<ObjectView> out = new ArrayList<>(extent.size());
    for (Map.Entry<String, long[]> e : extent.entrySet()) {
      byte[] t = FooterCodec.serialize(FooterCodec.localFooter(footer, e.getKey()));
      out.add(new ObjectView(base + e.getKey(), e.getValue()[0] + t.length, t, e.getValue()[1]));
    }
    return out;
  }

  @Override
  public RowGroupSource source(long rgId) {
    RowGroup rg = rowGroup(rgId);
    long start = FooterCodec.startOffset(rg);
    long size = FooterCodec.compressedSize(rg);
    if (!current().footerOnly()) {
      return new RowGroupSource(location(), length(), tail, start, size);
    }
    if (views == null) {
      views = new HashMap<>();
      for (ObjectView v : views(location(), footer)) {
        views.put(v.location(), v);
      }
    }
    ObjectView v = views.get(dir + FooterCodec.filePath(rg));
    return new RowGroupSource(v.location(), v.length(), v.tail(), start, size);
  }

  @Override
  public Version current() {
    return versions.get(versions.size() - 1);
  }

  @Override
  public List<Version> versions() {
    return List.copyOf(versions);
  }

  @Override
  public FileMetaData footer() {
    return footer;
  }

  @Override
  public byte[] tail() {
    return tail;
  }

  @Override
  public List<Long> rgIds() {
    return List.copyOf(rgIds);
  }

  @Override
  public RowGroup rowGroup(long rgId) {
    int i = rgIds.indexOf(rgId);
    if (i < 0) {
      throw new IllegalArgumentException("Row group " + rgId + " is not live in " + location());
    }
    return footer.getRow_groups().get(i);
  }

  @Override
  public long liveBytes() {
    long live = FooterCodec.MAGIC.length + tail.length;
    for (RowGroup rg : footer.getRow_groups()) {
      live += FooterCodec.liveBytes(rg);
    }
    return live;
  }

  @Override
  public String regionLocation() {
    return String.format("%s.r%03d.parquet", stem, next);
  }

  @Override
  public long regionBase() {
    return 0;
  }

  @Override
  public List<String> objects() {
    return List.copyOf(objectLengths.keySet());
  }

  @Override
  public long storedBytes() {
    return objectLengths.values().stream().mapToLong(Long::longValue).sum();
  }

  /** The object the current version's row group {@code rgId} lives in. */
  public String objectOf(long rgId) {
    String object = objectOfRg.get(rgId);
    if (object == null) {
      throw new IllegalArgumentException("Row group " + rgId + " is not live in " + location());
    }
    return object;
  }

  /** The length of an object this file wrote or adopted. */
  public long objectLength(String object) {
    return objectLengths.get(object);
  }

  @Override
  public int usedObjects() {
    Set<String> used = new HashSet<>(objectOfRg.values());
    used.add(location());
    return used.size();
  }

  /** Objects holding no current row group, and footers of earlier versions. */
  @Override
  public long reclaimableBytes() {
    Set<String> used = new HashSet<>(objectOfRg.values());
    used.add(location());
    long bytes = 0;
    for (Map.Entry<String, Long> e : objectLengths.entrySet()) {
      if (!used.contains(e.getKey())) {
        bytes += e.getValue();
      }
    }
    return bytes;
  }

  /**
   * The current version as a manifest entry would describe it: the footer's object and column
   * metrics. A footer-only version has no split offsets (its row groups are in other objects).
   */
  @Override
  public DataFile dataFile() {
    if (dataFile == null) {
      try {
        ParquetMetadata md = new ParquetMetadataConverter().fromParquetMetadata(footer);
        DataFiles.Builder b =
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(location())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(length())
                .withMetrics(
                    ParquetUtil.footerMetrics(md, Stream.empty(), MetricsConfig.getDefault()));
        if (!current().footerOnly()) {
          b.withSplitOffsets(ParquetUtil.getSplitOffsets(md));
        }
        dataFile = b.build();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return dataFile;
  }
}
