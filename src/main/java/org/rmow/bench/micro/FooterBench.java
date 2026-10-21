package org.rmow.bench.micro;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.RegionWriter;

/**
 * Spike S3: how a footer's size and its CPU costs grow with the number of row groups it lists.
 * Every appendable-file version rewrites the whole footer and every open reads and decodes it, so
 * this sets the rollover threshold on row groups per file.
 *
 * <p>Footers are built from one real row group (written by the stock writer, with its statistics
 * and page-index references) repeated at increasing offsets. Run the size table with {@code
 * main}, the timings with JMH:
 *
 * <pre>
 *   java -cp ... org.rmow.bench.micro.FooterBench            # sizes
 *   java -cp ... org.openjdk.jmh.Main FooterBench -rf json   # timings
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class FooterBench {

  @Param({"10", "100", "1000", "10000"})
  public int rowGroups;

  @Param({"11", "50"})
  public int columns;

  private FileMetaData footer;
  private byte[] serialized;
  private FileMetaData region;

  @Setup
  public void setup() throws IOException {
    FileMetaData one = oneRowGroup(columns);
    this.footer = repeat(one, rowGroups);
    this.serialized = FooterCodec.serialize(footer);
    this.region = repeat(one, 1);
    shift(region.getRow_groups().get(0), (long) rowGroups * span(one));
  }

  /** Decoding the Thrift footer: what every reader pays per open. */
  @Benchmark
  public FileMetaData parse() {
    FooterCodec.Tail t = FooterCodec.locate(serialized, serialized.length);
    return FooterCodec.read(serialized, t.footerStart(), t.footerLength());
  }

  /** parquet-java's conversion after decoding (what {@code ParquetFileReader.open} also does). */
  @Benchmark
  public ParquetMetadata convert() throws IOException {
    return new ParquetMetadataConverter().fromParquetMetadata(footer);
  }

  /** The writer's cost per version: merge (drop one row group, add one) and serialize. */
  @Benchmark
  public byte[] mergeAndSerialize() {
    return FooterCodec.serialize(
        FooterCodec.merge(footer, Set.of(rowGroups / 2), List.of(region), serialized.length, 1));
  }

  public static void main(String[] args) throws IOException {
    System.out.println("columns,row_groups,footer_bytes,bytes_per_row_group");
    for (int c : new int[] {11, 50}) {
      FileMetaData one = oneRowGroup(c);
      for (int n : new int[] {1, 10, 100, 1000, 10000}) {
        int bytes = FooterCodec.serialize(repeat(one, n)).length;
        System.out.printf("%d,%d,%d,%.1f%n", c, n, bytes, bytes / (double) n);
      }
    }
  }

  private static Schema schema(int columns) {
    List<Types.NestedField> fields = new ArrayList<>();
    fields.add(required(1, "id", Types.LongType.get()));
    for (int i = 1; i < columns; i++) {
      fields.add(
          i % 5 == 0
              ? required(i + 1, "s" + i, Types.StringType.get())
              : required(i + 1, "l" + i, Types.LongType.get()));
    }
    return new Schema(fields);
  }

  /** A footer with one row group of 10,000 rows, written by the stock Iceberg writer. */
  static FileMetaData oneRowGroup(int columns) throws IOException {
    Schema schema = schema(columns);
    List<Record> rows = new ArrayList<>();
    for (long id = 0; id < 10_000; id++) {
      GenericRecord r = GenericRecord.create(schema);
      r.set(0, id);
      for (int i = 1; i < columns; i++) {
        r.set(i, i % 5 == 0 ? "value-" + (id * i % 997) : id * 31 + i);
      }
      rows.add(r);
    }
    RegionWriter w = new RegionWriter(schema, Map.of("write.parquet.compression-codec", "zstd"));
    return w.write("mem://footer-bench", 0, rows).footer();
  }

  private static long span(FileMetaData one) {
    return FooterCodec.liveBytes(one.getRow_groups().get(0)) + 4;
  }

  /** {@code one}'s row group repeated {@code n} times, each shifted past the previous. */
  static FileMetaData repeat(FileMetaData one, int n) {
    FileMetaData f = one.deepCopy();
    RowGroup proto = one.getRow_groups().get(0);
    long span = span(one);
    List<RowGroup> rgs = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      RowGroup rg = proto.deepCopy();
      shift(rg, i * span);
      rg.setOrdinal((short) Math.min(i, Short.MAX_VALUE));
      rgs.add(rg);
    }
    f.setRow_groups(rgs);
    f.setNum_rows(n * proto.getNum_rows());
    return f;
  }

  private static void shift(RowGroup rg, long by) {
    if (rg.isSetFile_offset()) {
      rg.setFile_offset(rg.getFile_offset() + by);
    }
    for (ColumnChunk c : rg.getColumns()) {
      ColumnMetaData md = c.getMeta_data();
      md.setData_page_offset(md.getData_page_offset() + by);
      if (md.isSetDictionary_page_offset()) {
        md.setDictionary_page_offset(md.getDictionary_page_offset() + by);
      }
      if (md.isSetBloom_filter_offset()) {
        md.setBloom_filter_offset(md.getBloom_filter_offset() + by);
      }
      if (c.isSetColumn_index_offset()) {
        c.setColumn_index_offset(c.getColumn_index_offset() + by);
      }
      if (c.isSetOffset_index_offset()) {
        c.setOffset_index_offset(c.getOffset_index_offset() + by);
      }
    }
  }
}
