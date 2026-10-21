package org.rmow.bench.rmow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.TypeUtil;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.workload.Rows;

/**
 * What Iceberg's reader decodes from one version of an appendable object, as digests another
 * Parquet reader can recompute ({@code analysis/interop_check.py}). Each digest is a wrapping sum
 * over the rows, so it does not depend on the order rows are read in, except where positions are
 * hashed in:
 *
 * <ul>
 *   <li>{@code sum}: the {@link TableHash} sum over (id, version);
 *   <li>{@code contentSum}: over (id, ts, version), which with the payload check pins every
 *       column, since the payload is a function of those three ({@link Rows#record});
 *   <li>{@code posSum}: over (id, {@code _pos}), so readers must agree on each row's position,
 *       which follows the footer's row-group order.
 * </ul>
 *
 * Every decoded row's payload is checked against {@link Rows#record} as it is read.
 *
 * @param rowGroupRows each listed row group's row count, in footer order
 * @param offsetOrder whether the footer lists its row groups in ascending file-offset order
 * @param footerBytes the serialized footer's length
 */
public record VersionDigest(
    long rows,
    long sum,
    long contentSum,
    long posSum,
    List<Long> rowGroupRows,
    boolean offsetOrder,
    int footerBytes) {

  private static final Schema PROJECTION =
      TypeUtil.join(Rows.SCHEMA, new Schema(MetadataColumns.ROW_POSITION));

  /** The header of {@code versions.csv}, which {@code analysis/interop_check.py} reads. */
  public static final String CSV_HEADER =
      "arm,object,version,length,rows,sum,content_sum,pos_sum,row_groups,rg_rows,offset_order,"
          + "footer_bytes";

  public TableHash hash() {
    return new TableHash(rows, sum);
  }

  /** This version's row of {@code versions.csv}; {@code object} is relative to the CSV. */
  public String csv(String arm, String object, int version, long length) {
    return String.join(
        ",",
        arm,
        object,
        Integer.toString(version),
        Long.toString(length),
        Long.toString(rows),
        Long.toUnsignedString(sum),
        Long.toUnsignedString(contentSum),
        Long.toUnsignedString(posSum),
        Integer.toString(rowGroupRows.size()),
        rowGroupRows.stream().map(String::valueOf).collect(Collectors.joining(";")),
        Boolean.toString(offsetOrder),
        Integer.toString(footerBytes));
  }

  /** A row's contribution to {@code contentSum}. */
  public static long contentHash(long id, long ts, int version) {
    return Rows.mix64(Rows.rowHash(id, version) + ts * 0xC2B2AE3D27D4EB4FL);
  }

  /** A row's contribution to {@code posSum}. */
  public static long posHash(long id, long pos) {
    return Rows.mix64(id * 0x9E3779B97F4A7C15L ^ (pos * 0xD1B54A32D192ED03L + 1));
  }

  /** Reads the version of {@code location} that ends at {@code length}. */
  public static VersionDigest read(FileIO io, String location, long length) throws IOException {
    byte[] footerBytes = footer(io, location, length);
    FileMetaData footer = FooterCodec.read(footerBytes, 0, footerBytes.length);
    List<Long> rgRows = new ArrayList<>(footer.getRow_groupsSize());
    boolean ascending = true;
    long prev = -1;
    for (RowGroup rg : footer.getRow_groups()) {
      rgRows.add(rg.getNum_rows());
      long start = FooterCodec.startOffset(rg);
      ascending &= start > prev;
      prev = start;
    }

    long rows = 0;
    long sum = 0;
    long content = 0;
    long pos = 0;
    VersionedInputFile in = new VersionedInputFile(io.newInputFile(location, length), length);
    try (CloseableIterable<Record> rs =
        ParquetReads.open(in, PROJECTION, Expressions.alwaysTrue())) {
      for (Record r : rs) {
        Rows.verify(r);
        long id = (Long) r.get(Rows.ID);
        long ts = (Long) r.get(Rows.TS);
        int version = (Integer) r.get(Rows.VERSION);
        rows++;
        sum += Rows.rowHash(id, version);
        content += contentHash(id, ts, version);
        pos += posHash(id, (Long) r.get(Rows.SCHEMA.columns().size()));
      }
    }
    if (rows != footer.getNum_rows()) {
      throw new IllegalStateException(
          "Read " + rows + " rows of " + location + "@" + length + ", its footer lists "
              + footer.getNum_rows());
    }
    return new VersionDigest(rows, sum, content, pos, rgRows, ascending, footerBytes.length);
  }

  /** The serialized footer of the version ending at {@code length}. */
  private static byte[] footer(FileIO io, String location, long length) throws IOException {
    try (SeekableInputStream in = io.newInputFile(location, length).newStream()) {
      byte[] end = new byte[8];
      in.seek(length - 8);
      IOUtil.readFully(in, end, 0, 8);
      int len = ByteBuffer.wrap(end, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      byte[] buf = new byte[len];
      in.seek(length - 8 - len);
      IOUtil.readFully(in, buf, 0, len);
      return buf;
    }
  }
}
