package org.rmow.bench.rmow;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.format.FileMetaData;

/**
 * Writes one region: a single row group (with its page indexes and bloom filters) encoded by the
 * stock Iceberg Parquet writer, with the writer's own footer cut off. The writer is configured from
 * the same properties as every other arm's data files, so the encoding is identical across arms.
 */
public final class RegionWriter {

  /** A region's bytes, positioned at {@code base} of its object. */
  public record Region(long base, byte[] bytes, FileMetaData footer, Metrics metrics, long rows) {
    public long end() {
      return base + bytes.length;
    }
  }

  private final Schema schema;
  private final Map<String, String> properties;

  /**
   * @param writerProperties the shared Parquet writer properties; the row-group size is overridden
   *     so that a region is always one row group, whose size the merge policy chooses
   */
  public RegionWriter(Schema schema, Map<String, String> writerProperties) {
    this.schema = schema;
    Map<String, String> props = new LinkedHashMap<>(writerProperties);
    props.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(1L << 40));
    this.properties = Map.copyOf(props);
  }

  public Schema schema() {
    return schema;
  }

  /** Encodes {@code rows} as a region to be written at {@code base} of {@code location}. */
  public Region write(String location, long base, Iterable<Record> rows) throws IOException {
    OffsetOutputFile out = new OffsetOutputFile(location, base);
    FileAppender<Record> appender =
        Parquet.write(out)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::create)
            .setAll(properties)
            .overwrite()
            .build();
    try (FileAppender<Record> a = appender) {
      a.addAll(rows);
    }
    byte[] all = out.bytes();
    FooterCodec.Tail tail = FooterCodec.locate(all, all.length);
    FileMetaData footer = FooterCodec.read(all, tail.footerStart(), tail.footerLength());
    if (footer.getRow_groupsSize() != 1) {
      throw new IllegalStateException(
          "Expected one row group per region, got " + footer.getRow_groupsSize());
    }
    return new Region(
        base,
        Arrays.copyOf(all, tail.footerStart()),
        footer,
        appender.metrics(),
        footer.getNum_rows());
  }
}
