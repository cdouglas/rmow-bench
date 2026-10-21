package org.rmow.bench.rmow;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.io.FileIO;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.rmow.RegionWriter.Region;

/**
 * One data file of the table as region-merge-on-write sees it: a sequence of versions, each
 * summarized in exactly one footer, whose row groups carry stable ids across versions. How the
 * versions are stored is the implementation's:
 *
 * <ul>
 *   <li>{@link RmowFile}: one appendable object; a version is a prefix of it;
 *   <li>{@link SplitFile}: each version's regions in a new object, and its footer in a footer-only
 *       object naming the objects its row groups live in.
 * </ul>
 */
public interface LogicalFile {

  /**
   * A version: the object a manifest entry would name, and its length.
   *
   * @param footerOnly the object holds just a footer, whose row groups all live in other objects
   */
  record Version(String location, long length, boolean footerOnly) {}

  /**
   * Where a reader finds one row group: a file of {@code length} bytes whose last {@code
   * tail.length} bytes are a footer listing it (served from memory when the file is virtual), and
   * the row group's first byte and size.
   */
  record RowGroupSource(String location, long length, byte[] tail, long start, long size) {}

  /** The current version. */
  Version current();

  /** Every version, oldest first. */
  List<Version> versions();

  default String location() {
    return current().location();
  }

  default long length() {
    return current().length();
  }

  FileMetaData footer();

  /** The current footer serialized, with its length and magic: the writer's cached footer. */
  byte[] tail();

  List<Long> rgIds();

  RowGroup rowGroup(long rgId);

  default long liveRows() {
    return footer().getNum_rows();
  }

  /** Bytes the current version references: its row groups with their indexes, and its footer. */
  long liveBytes();

  /** The current version as a manifest entry would describe it. */
  DataFile dataFile();

  /** Where the next version's regions are written, and the base the first of them starts at. */
  String regionLocation();

  long regionBase();

  /**
   * Writes the next version: {@code regions} (written at {@link #regionLocation} from {@link
   * #regionBase}, one row group each, as {@code newRgIds}), dropping {@code dropRgIds}.
   *
   * @param inPlaceOf null to list the new row groups last; otherwise, per region, the dropped row
   *     group whose place in the footer its row group takes, or null to list it last
   */
  void commit(
      FileIO io,
      RegionAppender appender,
      List<Region> regions,
      Collection<Long> dropRgIds,
      List<Long> newRgIds,
      List<Long> inPlaceOf)
      throws IOException;

  /** Where the current version's row group {@code rgId} is read from. */
  RowGroupSource source(long rgId);

  /** Every object this file has stored, versions and regions. */
  List<String> objects();

  /** Objects the current version reads from. */
  default int usedObjects() {
    return 1;
  }

  long storedBytes();

  /**
   * Bytes of stored objects the current version does not use at all: deletable once older
   * versions expire.
   */
  long reclaimableBytes();

  /** Bytes the current version does not use inside objects it does use: only a rewrite frees them. */
  default long garbageBytes() {
    return storedBytes() - reclaimableBytes() - liveBytes();
  }
}
