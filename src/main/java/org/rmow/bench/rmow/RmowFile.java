package org.rmow.bench.rmow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.rmow.RegionWriter.Region;

/**
 * One appendable Parquet object: its committed length, its current footer, and which stable row
 * group id each footer entry carries. Each version is the object's prefix up to that version's
 * length; the versions' lengths are kept so any of them can be read again.
 *
 * <p>A new version is written as one request: the new regions followed by the merged footer,
 * appended at the committed length (or, for a new object, one PUT).
 */
public final class RmowFile implements LogicalFile {

  private final String location;
  private long length;
  private FileMetaData footer;
  private byte[] tail; // the current version's serialized footer, length and magic
  private final List<Long> rgIds = new ArrayList<>();
  private final List<Long> versionLengths = new ArrayList<>();
  private DataFile dataFile; // the current version's description, computed on demand

  private RmowFile(String location) {
    this.location = location;
  }

  /**
   * Writes a new object holding {@code regions} (the first at base 0) as its first version, in one
   * request, as the store's appender creates objects it can extend ({@link RegionAppender#create}).
   */
  public static RmowFile create(
      FileIO io,
      RegionAppender appender,
      String location,
      List<Region> regions,
      List<Long> newRgIds)
      throws IOException {
    RmowFile f = new RmowFile(location);
    FileMetaData merged =
        FooterCodec.merge(null, Set.of(), footers(regions), 0, 0);
    byte[] tail = FooterCodec.serialize(merged);
    checkContiguous(regions, 0);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Region r : regions) {
      out.write(r.bytes());
    }
    out.write(tail);
    appender.create(io, location, out.toByteArray());
    f.commit(regions.get(regions.size() - 1).end() + tail.length, merged, tail, newRgIds);
    return f;
  }

  /**
   * Adopts an existing ordinary Parquet object as an appendable file's first version, numbering
   * its row groups from {@code nextRgId}. Reads its footer with one ranged read (setup I/O; not
   * part of any measured phase).
   */
  public static RmowFile adopt(FileIO io, String location, long length, LongSupplier nextRgId)
      throws IOException {
    RmowFile f = new RmowFile(location);
    InputFile in = io.newInputFile(location, length);
    int probe = (int) Math.min(length, 1 << 20);
    byte[] buf = new byte[probe];
    try (SeekableInputStream s = in.newStream()) {
      ((RangeReadable) s).readFully(length - probe, buf, 0, probe);
    }
    FooterCodec.Tail t = FooterCodec.locate(buf, probe); // throws if the footer exceeds the probe
    FileMetaData footer = FooterCodec.read(buf, t.footerStart(), t.footerLength());
    List<Long> rgIds = new ArrayList<>(footer.getRow_groupsSize());
    for (int i = 0; i < footer.getRow_groupsSize(); i++) {
      rgIds.add(nextRgId.getAsLong());
    }
    byte[] tail = Arrays.copyOfRange(buf, t.footerStart(), probe);
    f.commit(length, footer, tail, rgIds);
    return f;
  }

  /**
   * Appends {@code regions} and a footer that drops the row groups {@code dropRgIds} and adds the
   * regions' row groups as {@code newRgIds}, listed last.
   */
  public void append(
      RegionAppender appender, List<Region> regions, Collection<Long> dropRgIds, List<Long> newRgIds)
      throws IOException {
    append(appender, regions, dropRgIds, newRgIds, null);
  }

  /**
   * Appends {@code regions}, one row group each, and a footer that drops the row groups {@code
   * dropRgIds} and adds the regions' row groups as {@code newRgIds}.
   *
   * @param inPlaceOf null to list the new row groups last; otherwise, per region, the dropped row
   *     group whose place in the footer its row group takes, or null to list it last
   */
  public void append(
      RegionAppender appender,
      List<Region> regions,
      Collection<Long> dropRgIds,
      List<Long> newRgIds,
      List<Long> inPlaceOf)
      throws IOException {
    Set<Integer> drop = positions(rgIds, dropRgIds, location);
    if (newRgIds.size() != regions.size()) {
      throw new IllegalArgumentException("One new row group id per region");
    }
    List<Integer> slots = slots(rgIds, inPlaceOf);
    checkContiguous(regions, length);
    FileMetaData merged =
        FooterCodec.merge(footer, drop, footers(regions), slots, length, versionLengths.size());
    byte[] newTail = FooterCodec.serialize(merged);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Region r : regions) {
      out.write(r.bytes());
    }
    out.write(newTail);
    long newLength = appender.append(location, length, out.toByteArray());
    commit(newLength, merged, newTail, nextIds(rgIds, drop, slots, newRgIds));
  }

  @Override
  public void commit(
      FileIO io,
      RegionAppender appender,
      List<Region> regions,
      Collection<Long> dropRgIds,
      List<Long> newRgIds,
      List<Long> inPlaceOf)
      throws IOException {
    append(appender, regions, dropRgIds, newRgIds, inPlaceOf);
  }

  /** The positions, in the current footer, of the row groups {@code ids}. */
  static Set<Integer> positions(List<Long> rgIds, Collection<Long> ids, String location) {
    Set<Integer> out = new HashSet<>();
    for (Long id : ids) {
      int i = rgIds.indexOf(id);
      if (i < 0) {
        throw new IllegalArgumentException("Row group " + id + " is not live in " + location);
      }
      out.add(i);
    }
    return out;
  }

  /** Per region, the position its row group takes (null: listed last); null for none in place. */
  static List<Integer> slots(List<Long> rgIds, List<Long> inPlaceOf) {
    if (inPlaceOf == null) {
      return null;
    }
    List<Integer> slots = new ArrayList<>(inPlaceOf.size());
    for (Long replaced : inPlaceOf) {
      slots.add(replaced == null ? null : rgIds.indexOf(replaced));
    }
    return slots;
  }

  /** The row group ids of the next footer, in its order. */
  static List<Long> nextIds(
      List<Long> rgIds, Set<Integer> drop, List<Integer> slots, List<Long> newRgIds) {
    Map<Integer, Long> slotted = new HashMap<>(); // position -> the new row group taking it
    for (int j = 0; slots != null && j < slots.size(); j++) {
      if (slots.get(j) != null) {
        slotted.put(slots.get(j), newRgIds.get(j));
      }
    }
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < rgIds.size(); i++) {
      if (slotted.containsKey(i)) {
        ids.add(slotted.get(i));
      } else if (!drop.contains(i)) {
        ids.add(rgIds.get(i));
      }
    }
    for (int j = 0; j < newRgIds.size(); j++) {
      if (slots == null || slots.get(j) == null) {
        ids.add(newRgIds.get(j));
      }
    }
    return ids;
  }

  private void commit(long newLength, FileMetaData merged, byte[] newTail, List<Long> ids) {
    if (ids.size() != merged.getRow_groupsSize()) {
      throw new IllegalStateException("Row group ids out of step with the footer");
    }
    this.length = newLength;
    this.footer = merged;
    this.tail = newTail;
    this.rgIds.clear();
    this.rgIds.addAll(ids);
    this.versionLengths.add(newLength);
    this.dataFile = null;
  }

  static List<FileMetaData> footers(List<Region> regions) {
    List<FileMetaData> fs = new ArrayList<>(regions.size());
    for (Region r : regions) {
      fs.add(r.footer());
    }
    return fs;
  }

  static void checkContiguous(List<Region> regions, long start) {
    long pos = start;
    for (Region r : regions) {
      if (r.base() != pos) {
        throw new IllegalArgumentException("Region at " + r.base() + " does not follow " + pos);
      }
      pos = r.end();
    }
  }

  /** The base the next pending region must be written at. */
  public long nextBase(List<Region> pending) {
    return pending.isEmpty() ? length : pending.get(pending.size() - 1).end();
  }

  @Override
  public Version current() {
    return new Version(location, length, false);
  }

  @Override
  public List<Version> versions() {
    return versionLengths.stream().map(l -> new Version(location, l, false)).toList();
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public FileMetaData footer() {
    return footer;
  }

  /** The current version's serialized footer, length and magic (the warm-footer cache). */
  @Override
  public byte[] tail() {
    return tail;
  }

  @Override
  public List<Long> rgIds() {
    return List.copyOf(rgIds);
  }

  @Override
  public String regionLocation() {
    return location;
  }

  @Override
  public long regionBase() {
    return length;
  }

  @Override
  public RowGroupSource source(long rgId) {
    RowGroup rg = rowGroup(rgId);
    return new RowGroupSource(
        location, length, tail, FooterCodec.startOffset(rg), FooterCodec.compressedSize(rg));
  }

  @Override
  public List<String> objects() {
    return List.of(location);
  }

  @Override
  public long storedBytes() {
    return length;
  }

  /** Nothing: the object holds the current version. */
  @Override
  public long reclaimableBytes() {
    return 0;
  }

  public int version() {
    return versionLengths.size() - 1;
  }

  public List<Long> versionLengths() {
    return List.copyOf(versionLengths);
  }

  /** The split offsets Iceberg derives from a version's footer, as its manifest entry holds them. */
  public static List<Long> splitOffsets(FileMetaData footer) {
    try {
      return ParquetUtil.getSplitOffsets(new ParquetMetadataConverter().fromParquetMetadata(footer));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The current version described as an Iceberg data file: its length, and the column metrics
   * and split offsets Iceberg derives from its footer, as a manifest entry for it would hold.
   */
  @Override
  public DataFile dataFile() {
    if (dataFile == null) {
      try {
        ParquetMetadata md = new ParquetMetadataConverter().fromParquetMetadata(footer);
        dataFile =
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(location)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(length)
                .withMetrics(
                    ParquetUtil.footerMetrics(md, Stream.empty(), MetricsConfig.getDefault()))
                .withSplitOffsets(ParquetUtil.getSplitOffsets(md))
                .build();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return dataFile;
  }

  @Override
  public RowGroup rowGroup(long rgId) {
    int i = rgIds.indexOf(rgId);
    if (i < 0) {
      throw new IllegalArgumentException("Row group " + rgId + " is not live in " + location);
    }
    return footer.getRow_groups().get(i);
  }

  @Override
  public long liveRows() {
    return footer.getNum_rows();
  }

  /** Bytes the current version references: magic, row groups with their indexes, and footer. */
  @Override
  public long liveBytes() {
    long live = FooterCodec.MAGIC.length + tail.length;
    for (RowGroup rg : footer.getRow_groups()) {
      live += FooterCodec.liveBytes(rg);
    }
    return live;
  }

  /** Bytes of earlier versions only: replaced regions and old footers. */
  public long deadBytes() {
    return length - liveBytes();
  }
}
