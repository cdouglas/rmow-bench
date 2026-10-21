package org.rmow.bench.rmow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;

/**
 * Reads, merges and writes Parquet footers at the Thrift level.
 *
 * <p>A merged footer lists the live row groups of the previous version (minus the replaced ones)
 * followed by the new region's row groups. Its invariants:
 *
 * <ul>
 *   <li>the schema matches the previous version's (no schema evolution in this PoC);
 *   <li>row groups appear in ascending file-offset order, which the new ones satisfy since they
 *       sit after every earlier byte (parquet-java's PARQUET-2078 fallback checks this order);
 *   <li>{@code ordinal} is the list position and there are at most 32767 row groups (an i16; it
 *       is also part of the encryption AAD, so encrypted files are out of scope);
 *   <li>{@code num_rows} is the sum over the listed row groups;
 *   <li>the previous version's key-value metadata ({@code iceberg.schema}) and {@code created_by}
 *       are kept, and {@code rmow.prev-length} points back to the previous version's length.
 * </ul>
 */
public final class FooterCodec {

  public static final byte[] MAGIC = {'P', 'A', 'R', '1'};
  public static final String PREV_LENGTH = "rmow.prev-length";
  public static final String VERSION = "rmow.version";

  private FooterCodec() {}

  /** Where the footer sits in the trailing bytes of a Parquet file. */
  public record Tail(int footerStart, int footerLength) {}

  /** Locates the footer in {@code bytes}, whose last 8 bytes are the footer length and magic. */
  public static Tail locate(byte[] bytes, int end) {
    if (end < 12) {
      throw new IllegalArgumentException("Too short for a Parquet file: " + end);
    }
    if (!Arrays.equals(bytes, end - 4, end, MAGIC, 0, 4)) {
      throw new IllegalArgumentException("Missing trailing magic");
    }
    int len = ByteBuffer.wrap(bytes, end - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    int start = end - 8 - len;
    if (len < 0 || start < 0) {
      throw new IllegalArgumentException("Corrupt footer length: " + len);
    }
    return new Tail(start, len);
  }

  public static FileMetaData read(byte[] bytes, int offset, int length) {
    try {
      return Util.readFileMetaData(new ByteArrayInputStream(bytes, offset, length));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Serializes a footer followed by its 4-byte little-endian length and the magic. */
  public static byte[] serialize(FileMetaData footer) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
      Util.writeFileMetaData(footer, out);
      int len = out.size();
      out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(len).array());
      out.write(MAGIC);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Builds the footer of the next version.
   *
   * @param previous the current version's footer, or null for a new object
   * @param drop positions (in {@code previous}'s row-group list) of the row groups to omit
   * @param regions the footers the region writer produced, one per region, in file order
   * @param prevLength the current version's length (0 for a new object)
   * @param version the next version's number
   */
  public static FileMetaData merge(
      FileMetaData previous,
      Set<Integer> drop,
      List<FileMetaData> regions,
      long prevLength,
      long version) {
    if (previous == null && regions.isEmpty()) {
      throw new IllegalArgumentException("A new object needs at least one region");
    }
    FileMetaData base = previous != null ? previous : regions.get(0);
    FileMetaData merged = copyWithoutRowGroups(base);

    List<RowGroup> groups = new ArrayList<>();
    if (previous != null) {
      List<RowGroup> old = previous.getRow_groups();
      for (int i = 0; i < old.size(); i++) {
        if (!drop.contains(i)) {
          groups.add(shallowCopy(old.get(i)));
        }
      }
    }
    for (FileMetaData region : regions) {
      if (!region.getSchema().equals(base.getSchema())) {
        throw new IllegalArgumentException("Region schema differs from the file's schema");
      }
      for (RowGroup rg : region.getRow_groups()) {
        groups.add(shallowCopy(rg));
      }
    }
    if (groups.size() > Short.MAX_VALUE) {
      throw new IllegalStateException("More row groups than an i16 ordinal holds: " + groups.size());
    }

    long rows = 0;
    long prevStart = -1;
    for (int i = 0; i < groups.size(); i++) {
      RowGroup rg = groups.get(i);
      long start = startOffset(rg);
      if (start <= prevStart) {
        throw new IllegalStateException(
            "Row groups out of file order at " + i + ": " + start + " <= " + prevStart);
      }
      prevStart = start;
      rg.setOrdinal((short) i);
      rows += rg.getNum_rows();
    }
    merged.setRow_groups(groups);
    merged.setNum_rows(rows);

    List<KeyValue> kv = new ArrayList<>();
    if (base.isSetKey_value_metadata()) {
      for (KeyValue e : base.getKey_value_metadata()) {
        if (!e.getKey().startsWith("rmow.")) {
          kv.add(e.deepCopy());
        }
      }
    }
    kv.add(new KeyValue(PREV_LENGTH).setValue(Long.toString(prevLength)));
    kv.add(new KeyValue(VERSION).setValue(Long.toString(version)));
    merged.setKey_value_metadata(kv);
    return merged;
  }

  /** A deep copy of a footer's fields other than its row groups, which can be large. */
  private static FileMetaData copyWithoutRowGroups(FileMetaData footer) {
    List<RowGroup> groups = footer.getRow_groups();
    footer.setRow_groups(new ArrayList<>());
    try {
      return footer.deepCopy();
    } finally {
      footer.setRow_groups(groups);
    }
  }

  /**
   * A row group entry sharing its column chunks with {@code rg}. Versions share column-chunk
   * metadata, which is never modified; only a copy's own fields ({@code ordinal}) are set. Deep
   * copies would make each version's footer cost O(footer) more CPU than writing it.
   */
  private static RowGroup shallowCopy(RowGroup rg) {
    RowGroup c =
        new RowGroup()
            .setColumns(rg.getColumns())
            .setTotal_byte_size(rg.getTotal_byte_size())
            .setNum_rows(rg.getNum_rows());
    if (rg.isSetSorting_columns()) {
      c.setSorting_columns(rg.getSorting_columns());
    }
    if (rg.isSetFile_offset()) {
      c.setFile_offset(rg.getFile_offset());
    }
    if (rg.isSetTotal_compressed_size()) {
      c.setTotal_compressed_size(rg.getTotal_compressed_size());
    }
    if (rg.isSetOrdinal()) {
      c.setOrdinal(rg.getOrdinal());
    }
    return c;
  }

  /** The first byte of a row group: its first column chunk's dictionary or first data page. */
  public static long startOffset(RowGroup rg) {
    ColumnMetaData md = rg.getColumns().get(0).getMeta_data();
    long start = md.getData_page_offset();
    if (md.isSetDictionary_page_offset() && md.getDictionary_page_offset() > 0) {
      start = Math.min(start, md.getDictionary_page_offset());
    }
    return start;
  }

  /** Bytes of the row group's column chunks (they are contiguous within a row group). */
  public static long compressedSize(RowGroup rg) {
    long size = 0;
    for (ColumnChunk c : rg.getColumns()) {
      size += c.getMeta_data().getTotal_compressed_size();
    }
    return size;
  }

  /**
   * Bytes a row group keeps live: its column chunks plus its page indexes and bloom filters, which
   * the writer places after the chunks.
   */
  public static long liveBytes(RowGroup rg) {
    long size = compressedSize(rg);
    for (ColumnChunk c : rg.getColumns()) {
      if (c.isSetColumn_index_length()) {
        size += c.getColumn_index_length();
      }
      if (c.isSetOffset_index_length()) {
        size += c.getOffset_index_length();
      }
      ColumnMetaData md = c.getMeta_data();
      if (md.isSetBloom_filter_length()) {
        size += md.getBloom_filter_length();
      }
    }
    return size;
  }

  public static Map<String, String> keyValues(FileMetaData footer) {
    Map<String, String> kv = new java.util.LinkedHashMap<>();
    if (footer.isSetKey_value_metadata()) {
      for (KeyValue e : footer.getKey_value_metadata()) {
        kv.put(e.getKey(), e.getValue());
      }
    }
    return kv;
  }
}
