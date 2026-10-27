package org.rmow.bench.rmow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.TypeUtil;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.io.TracingFileIO;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.RegionWriter.Region;
import org.rmow.bench.workload.Rows;

/**
 * Spike S1: an appendable Parquet file with three versions, each read at its own length by stock
 * Iceberg and stock parquet-java.
 *
 * <ul>
 *   <li>v0: ids [0, n0), one row group (rg 0);
 *   <li>v1: v0 plus an appended region with ids [n0, n0 + n1) (rg 1);
 *   <li>v2: rg 0 replaced by a merged region (rg 2): ids [0, 50) updated to version 1, ids [50,
 *       100) deleted. The footer lists rg 2 last (offset order) or in rg 0's place (logical
 *       order, out of offset order).
 * </ul>
 */
class AppendableParquetTest {

  private static final String LOC = "mem://t/f.parquet";

  private MemFileIO io;
  private RegionAppender appender;
  private RegionWriter writer;

  @BeforeEach
  void setUp() {
    io = new MemFileIO();
    appender = new AtomicAppender(io);
    writer = new RegionWriter(Rows.SCHEMA, Map.of("write.parquet.compression-codec", "zstd"));
  }

  private static List<Record> rows(long from, long to, int version) {
    List<Record> rs = new ArrayList<>();
    for (long id = from; id < to; id++) {
      rs.add(Rows.record(id, id * 10, version));
    }
    return rs;
  }

  private static TableHash hash(List<Record> rows) {
    TableHash h = new TableHash();
    rows.forEach(r -> h.add(r, true));
    return h;
  }

  private record Versions(RmowFile file, long len0, long len1, long len2, List<Record> rows2) {}

  private Versions build(int n0, int n1) throws IOException {
    return build(LOC, n0, n1, false);
  }

  private Versions build(String loc, int n0, int n1, boolean inPlace) throws IOException {
    Region r0 = writer.write(loc, 0, rows(0, n0, 0));
    RmowFile f = RmowFile.create(appender, loc, List.of(r0), List.of(0L));
    long len0 = f.length();

    Region r1 = writer.write(loc, f.length(), rows(n0, n0 + n1, 0));
    f.append(appender, List.of(r1), List.of(), List.of(1L));
    long len1 = f.length();

    List<Record> rows2 = new ArrayList<>(rows(0, 50, 1));
    rows2.addAll(rows(100, n0, 0));
    Region r2 = writer.write(loc, f.length(), rows2);
    List<Long> inPlaceOf = new ArrayList<>();
    inPlaceOf.add(inPlace ? 0L : null);
    f.append(appender, List.of(r2), List.of(0L), List.of(2L), inPlaceOf);
    return new Versions(f, len0, len1, f.length(), rows2);
  }

  private TableHash readIceberg(InputFile in) throws IOException {
    TableHash h = new TableHash();
    try (CloseableIterable<Record> rs =
        ParquetReads.open(in, Rows.SCHEMA, Expressions.alwaysTrue())) {
      rs.forEach(r -> h.add(r, true));
    }
    return h;
  }

  private InputFile version(long length) {
    return new VersionedInputFile(io.newInputFile(LOC, length), length);
  }

  @Test
  void everyVersionReadsBackThroughIceberg() throws IOException {
    Versions v = build(2000, 1000);
    TableHash h0 = hash(rows(0, 2000, 0));
    TableHash h1 = hash(rows(0, 3000, 0));
    TableHash h2 = hash(v.rows2());
    h2.addAll(hash(rows(2000, 3000, 0)));

    assertThat(readIceberg(version(v.len0()))).isEqualTo(h0);
    assertThat(readIceberg(version(v.len1()))).isEqualTo(h1);
    assertThat(readIceberg(version(v.len2()))).isEqualTo(h2);
    // the newest version is also what a plain read of the whole object sees
    assertThat(readIceberg(io.newInputFile(LOC))).isEqualTo(h2);
  }

  @Test
  void everyVersionReadsBackThroughParquetJava() throws IOException {
    Versions v = build(2000, 1000);
    assertThat(readParquetJava(version(v.len0()))).isEqualTo(hash(rows(0, 2000, 0)));
    assertThat(readParquetJava(version(v.len1()))).isEqualTo(hash(rows(0, 3000, 0)));
    TableHash h2 = hash(v.rows2());
    h2.addAll(hash(rows(2000, 3000, 0)));
    assertThat(readParquetJava(version(v.len2()))).isEqualTo(h2);
  }

  @Test
  void footersChainBackAndKeepFileOrder() throws IOException {
    Versions v = build(2000, 1000);
    RmowFile f = v.file();
    assertThat(f.versionLengths()).containsExactly(v.len0(), v.len1(), v.len2());
    assertThat(FooterCodec.keyValues(f.footer()))
        .containsEntry(FooterCodec.PREV_LENGTH, Long.toString(v.len1()))
        .containsEntry(FooterCodec.VERSION, "2")
        .containsKey("iceberg.schema");
    assertThat(f.rgIds()).containsExactly(1L, 2L);

    List<RowGroup> rgs = f.footer().getRow_groups();
    assertThat(FooterCodec.startOffset(rgs.get(0))).isLessThan(FooterCodec.startOffset(rgs.get(1)));
    assertThat(rgs.get(0).getOrdinal()).isEqualTo((short) 0);
    assertThat(rgs.get(1).getOrdinal()).isEqualTo((short) 1);
    // rg 1 sits after v0's footer: its offsets are absolute
    assertThat(FooterCodec.startOffset(rgs.get(0))).isGreaterThanOrEqualTo(v.len0());
    // the replaced rg 0 and two old footers are dead
    assertThat(f.deadBytes()).isGreaterThan(0);
    assertThat(f.liveBytes() + f.deadBytes()).isEqualTo(v.len2());
  }

  @Test
  void rowPositionsFollowFooterOrder() throws IOException {
    Versions v = build(2000, 1000);
    Schema projection =
        TypeUtil.join(Rows.SCHEMA.select("id"), new Schema(MetadataColumns.ROW_POSITION));
    List<long[]> got = new ArrayList<>();
    try (CloseableIterable<Record> rs =
        ParquetReads.open(version(v.len2()), projection, Expressions.alwaysTrue())) {
      for (Record r : rs) {
        got.add(new long[] {(Long) r.get(0), (Long) r.get(1)});
      }
    }
    assertThat(got).hasSize(1000 + v.rows2().size());
    for (int i = 0; i < got.size(); i++) {
      assertThat(got.get(i)[1]).isEqualTo(i);
    }
    // footer order: the appended rg 1 (ids 2000..) first, then the merged rg 2
    assertThat(got.get(0)[0]).isEqualTo(2000);
    assertThat(got.get(1000)[0]).isEqualTo(0);
  }

  /** RK' in RK's slot: footer order is no longer offset order, and every reader path holds. */
  @Test
  void inPlaceRowGroupKeepsItsSlot() throws IOException {
    String loc = "mem://t/logical.parquet";
    Versions v = build(loc, 2000, 1000, true);
    RmowFile f = v.file();
    assertThat(f.rgIds()).containsExactly(2L, 1L);
    List<RowGroup> rgs = f.footer().getRow_groups();
    assertThat(FooterCodec.startOffset(rgs.get(0)))
        .isGreaterThan(FooterCodec.startOffset(rgs.get(1)));
    assertThat(rgs.get(0).getOrdinal()).isEqualTo((short) 0);

    InputFile in = new VersionedInputFile(io.newInputFile(loc, v.len2()), v.len2());
    TableHash want = hash(v.rows2());
    want.addAll(hash(rows(2000, 3000, 0)));
    assertThat(readIceberg(in)).isEqualTo(want);
    assertThat(readParquetJava(in)).isEqualTo(want);

    // row positions follow footer order: the rewritten rows come first again, as in v0
    Schema projection =
        TypeUtil.join(Rows.SCHEMA.select("id"), new Schema(MetadataColumns.ROW_POSITION));
    List<Long> ids = new ArrayList<>();
    try (CloseableIterable<Record> rs = ParquetReads.open(in, projection, Expressions.alwaysTrue())) {
      for (Record r : rs) {
        assertThat((Long) r.get(1)).isEqualTo(ids.size());
        ids.add((Long) r.get(0));
      }
    }
    assertThat(ids).isSorted();

    // Iceberg's split offsets are sorted (its spec requires it); planned splits still read every
    // row once, each split taking the row group whose midpoint it holds
    List<Long> offsets = f.dataFile().splitOffsets();
    assertThat(offsets).isSorted().hasSize(2);
    TableHash bySplit = new TableHash();
    for (int i = 0; i < offsets.size(); i++) {
      long start = offsets.get(i);
      long end = i + 1 < offsets.size() ? offsets.get(i + 1) : v.len2();
      try (CloseableIterable<Record> rs =
          ParquetReads.open(in, Rows.SCHEMA, Expressions.alwaysTrue(), start, end - start)) {
        rs.forEach(r -> bySplit.add(r, true));
      }
    }
    assertThat(bySplit).isEqualTo(want);
  }

  @Test
  void splitSelectsOneRegion() throws IOException {
    Versions v = build(2000, 1000);
    RowGroup merged = v.file().rowGroup(2L);
    long start = FooterCodec.startOffset(merged);
    long len = FooterCodec.compressedSize(merged);
    TableHash h = new TableHash();
    try (CloseableIterable<Record> rs =
        ParquetReads.open(version(v.len2()), Rows.SCHEMA, Expressions.alwaysTrue(), start, len)) {
      rs.forEach(r -> h.add(r, true));
    }
    assertThat(h).isEqualTo(hash(v.rows2()));
  }

  @Test
  void statsSkipRowGroupsOfOtherRegions() throws IOException {
    Versions v = build(2000, 1000);
    long minId = Long.MAX_VALUE;
    long n = 0;
    try (CloseableIterable<Record> rs =
        ParquetReads.open(
            version(v.len2()), Rows.SCHEMA, Expressions.greaterThanOrEqual("id", 2000L))) {
      for (Record r : rs) {
        minId = Math.min(minId, (Long) r.get(0));
        n++;
      }
    }
    // the merged rg 2 (ids < 2000) is skipped whole; rows are not filtered inside a row group
    assertThat(minId).isEqualTo(2000);
    assertThat(n).isEqualTo(1000);
  }

  /** Files over 1 MiB skip EagerInputFile; reads of an old version stay below its length. */
  @Test
  void largeOldVersionReadsStayInsideItsLength() throws IOException {
    Versions v = build(60_000, 20_000);
    assertThat(v.len0()).isGreaterThan(1 << 20);
    IoTrace trace = new IoTrace();
    trace.logEvents(true);
    TracingFileIO traced = new TracingFileIO(io, appender, trace);
    InputFile in = new VersionedInputFile(traced.newInputFile(LOC, v.len0()), v.len0());
    assertThat(readIceberg(in)).isEqualTo(hash(rows(0, 60_000, 0)));

    for (IoTrace.Event e : trace.events()) {
      if (e.kind() == IoTrace.Kind.GET_RANGE || e.kind() == IoTrace.Kind.GET_STREAM) {
        assertThat(e.offset()).isLessThan(v.len0());
      }
      if (e.kind() == IoTrace.Kind.GET_RANGE && e.offset() >= 0) {
        assertThat(e.offset() + e.length()).isLessThanOrEqualTo(v.len0());
      }
    }
    IoTrace.Counters c = trace.snapshot();
    assertThat(c.heads()).isZero();
    // the column chunks come through bounded ranged reads (vectored path), the footer through
    // the stream
    assertThat(c.getRange()).isPositive();
    assertThat(c.getRangeBytes()).isGreaterThan(v.len0() / 2);
  }

  /** Stock reads take two stream GETs for the footer; a prefetch that holds it takes one. */
  @Test
  void footerPrefetchTakesOneRequest() throws IOException {
    Versions v = build(60_000, 20_000);
    IoTrace stock = new IoTrace();
    IoTrace prefetch = new IoTrace();
    TracingFileIO a = new TracingFileIO(io, appender, stock);
    TracingFileIO b = new TracingFileIO(io, appender, prefetch);
    TableHash want = hash(rows(0, 60_000, 0));
    assertThat(readIceberg(new VersionedInputFile(a.newInputFile(LOC, v.len0()), v.len0())))
        .isEqualTo(want);
    assertThat(
            readIceberg(
                new VersionedInputFile(b.newInputFile(LOC, v.len0()), v.len0(), null, 64 << 10)))
        .isEqualTo(want);

    IoTrace.Counters s = stock.snapshot();
    IoTrace.Counters p = prefetch.snapshot();
    assertThat(s.getStream()).isEqualTo(2);
    assertThat(p.getStream()).isZero();
    assertThat(p.getRange()).isEqualTo(s.getRange() + 1);
    assertThat(p.gets()).isEqualTo(s.gets() - 1);
  }

  /**
   * Both files, offset order and logical order, in the layout {@code analysis/interop_check.py}
   * reads (pyarrow, DuckDB): each object once, and {@code versions.csv} with what Iceberg decodes
   * from each version, checked here against the rows written.
   */
  @Test
  void dumpVersionsForInterop() throws IOException {
    Path dir = Path.of("target", "interop");
    Files.createDirectories(dir);
    StringBuilder csv = new StringBuilder(VersionDigest.CSV_HEADER).append('\n');
    for (String order : List.of("offset", "logical")) {
      String loc = "mem://t/" + order + ".parquet";
      Versions v = build(loc, 2000, 1000, order.equals("logical"));
      long[] lens = {v.len0(), v.len1(), v.len2()};
      TableHash[] want = {
        hash(rows(0, 2000, 0)), hash(rows(0, 3000, 0)), hash(v.rows2()),
      };
      want[2].addAll(hash(rows(2000, 3000, 0)));
      byte[] bytes = new byte[(int) v.len2()];
      try (SeekableInputStream s = io.newInputFile(loc, v.len2()).newStream()) {
        org.apache.iceberg.io.IOUtil.readFully(s, bytes, 0, bytes.length);
      }
      try (OutputStream out = Files.newOutputStream(dir.resolve(order + ".parquet"))) {
        out.write(bytes);
      }
      for (int i = 0; i < lens.length; i++) {
        VersionDigest d = VersionDigest.read(io, loc, lens[i]);
        assertThat(d.hash()).isEqualTo(want[i]);
        assertThat(d.offsetOrder()).isEqualTo(i < 2 || order.equals("offset"));
        csv.append(d.csv("test", order + ".parquet", i, lens[i])).append('\n');
      }
    }
    Files.writeString(dir.resolve("versions.csv"), csv.toString());
  }

  private static TableHash readParquetJava(InputFile in) throws IOException {
    TableHash h = new TableHash();
    try (ParquetFileReader r = ParquetFileReader.open(parquetFile(in))) {
      MessageType schema = r.getFooter().getFileMetaData().getSchema();
      long rows = 0;
      for (BlockMetaData b : r.getFooter().getBlocks()) {
        rows += b.getRowCount();
      }
      PageReadStore pages;
      long seen = 0;
      while ((pages = r.readNextRowGroup()) != null) {
        RecordReader<Group> rr =
            new ColumnIOFactory()
                .getColumnIO(schema)
                .getRecordReader(pages, new GroupRecordConverter(schema));
        for (long i = 0; i < pages.getRowCount(); i++) {
          Group g = rr.read();
          h.add(g.getLong("id", 0), g.getInteger("version", 0));
          seen++;
        }
      }
      assertThat(seen).isEqualTo(rows);
    }
    return h;
  }

  private static org.apache.parquet.io.InputFile parquetFile(InputFile in) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() {
        return in.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() {
        SeekableInputStream s = in.newStream();
        return new DelegatingSeekableInputStream(s) {
          @Override
          public long getPos() throws IOException {
            return s.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            s.seek(newPos);
          }
        };
      }
    };
  }
}
