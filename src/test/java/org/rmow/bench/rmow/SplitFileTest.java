package org.rmow.bench.rmow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.api.Test;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.RegionWriter.Region;
import org.rmow.bench.workload.Rows;

/**
 * A split file: version 0 is an ordinary Parquet file of three row groups (ids [0, 300), 100
 * each); version 1 rewrites row group 0 into a new region object, in its place; version 2
 * rewrites row groups 1 and 2. Every version reads back; whole objects become reclaimable once
 * no row group of the current version lives in them; and stock Iceberg refuses a footer-only
 * version instead of reading wrong rows.
 */
class SplitFileTest {

  private static final String LOC = "mem://t/f.parquet";

  private final MemFileIO io = new MemFileIO();
  private final RegionWriter writer =
      new RegionWriter(Rows.SCHEMA, Map.of("write.parquet.compression-codec", "zstd"));

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

  /** The regions, in one object at {@code location}, each from its own rows. */
  private List<Region> regions(String location, List<List<Record>> each) throws IOException {
    List<Region> out = new ArrayList<>();
    long base = 0;
    for (List<Record> rows : each) {
      Region r = writer.write(location, base, rows);
      out.add(r);
      base = r.end();
    }
    return out;
  }

  @Test
  void versionsReadBackAndWholeObjectsBecomeReclaimable() throws IOException {
    List<List<Record>> initial = List.of(rows(0, 100, 0), rows(100, 200, 0), rows(200, 300, 0));
    RmowFile plain = RmowFile.create(io, io, LOC, regions(LOC, initial), List.of(0L, 1L, 2L));
    long id = 10;
    long[] next = {0};
    SplitFile f = SplitFile.adopt(io, LOC, plain.length(), () -> next[0]++);
    List<Long> ids = f.rgIds();
    assertThat(f.current().footerOnly()).isFalse();
    assertThat(f.reclaimableBytes()).isZero();

    // v1: row group 0 rewritten (ids [0, 50) updated, [50, 100) kept), in its place
    List<Record> rg0 = new ArrayList<>(rows(0, 50, 1));
    rg0.addAll(rows(50, 100, 0));
    String r1 = f.regionLocation();
    assertThat(r1).isEqualTo("mem://t/f.r001.parquet");
    f.commit(io, null, regions(r1, List.of(rg0)), List.of(ids.get(0)), List.of(id), List.of(ids.get(0)));
    assertThat(f.current()).isEqualTo(new LogicalFile.Version("mem://t/f.v001.parquet", f.length(), true));
    assertThat(f.footer().getRow_groups()).extracting(FooterCodec::filePath)
        .containsExactly("f.r001.parquet", "f.parquet", "f.parquet");
    // every object is in use: f.parquet still holds row groups 1 and 2; its row group 0 is garbage
    assertThat(f.reclaimableBytes()).isZero();
    assertThat(f.garbageBytes()).isPositive();

    // v2: row groups 1 and 2 rewritten (ids [100, 150) deleted)
    String r2 = f.regionLocation();
    List<Long> v1ids = f.rgIds();
    f.commit(
        io,
        null,
        regions(r2, List.of(rows(150, 200, 0), rows(200, 300, 2))),
        List.of(v1ids.get(1), v1ids.get(2)),
        List.of(id + 1, id + 2),
        List.of(v1ids.get(1), v1ids.get(2)));
    // f.parquet and v001 hold nothing of v2: both can go once v0 and v1 expire
    long plainBytes = plain.length();
    long v1Bytes = f.versions().get(1).length();
    assertThat(f.reclaimableBytes()).isEqualTo(plainBytes + v1Bytes);
    assertThat(f.objects())
        .containsExactly(
            LOC, "mem://t/f.r001.parquet", "mem://t/f.v001.parquet",
            "mem://t/f.r002.parquet", "mem://t/f.v002.parquet");

    // every version reads back, row positions in footer order
    TableHash[] want = {hash(rows(0, 300, 0)), hash(rg0), null};
    want[1].addAll(hash(rows(100, 300, 0)));
    want[2] = hash(rg0);
    want[2].addAll(hash(rows(150, 200, 0)));
    want[2].addAll(hash(rows(200, 300, 2)));
    List<LogicalFile.Version> versions = f.versions();
    for (int v = 0; v < versions.size(); v++) {
      VersionDigest d = VersionDigest.read(io, versions.get(v).location(), versions.get(v).length());
      assertThat(d.hash()).as("version " + v).isEqualTo(want[v]);
    }

    // a row group's source: its object as a virtual file whose footer lists what lives there
    RowGroup moved = f.rowGroup(id + 1);
    LogicalFile.RowGroupSource src = f.source(id + 1);
    assertThat(src.location()).isEqualTo(r2);
    assertThat(src.start()).isEqualTo(FooterCodec.startOffset(moved));
    FooterCodec.Tail t = FooterCodec.locate(src.tail(), src.tail().length);
    assertThat(FooterCodec.read(src.tail(), t.footerStart(), t.footerLength()).getRow_groupsSize())
        .isEqualTo(2);
  }

  /**
   * Stock Iceberg (parquet-java) resolves every offset in the file it opened: given a footer-only
   * version it fails. A footer that also listed row groups of its own object would be read
   * silently wrong, which is why no version mixes them.
   */
  @Test
  void stockReadersRefuseFooterOnlyVersions() throws IOException {
    RmowFile plain =
        RmowFile.create(io, io, LOC, regions(LOC, List.of(rows(0, 100, 0), rows(100, 200, 0))),
            Arrays.asList(0L, 1L));
    long[] next = {0};
    SplitFile f = SplitFile.adopt(io, LOC, plain.length(), () -> next[0]++);
    List<Long> ids = f.rgIds();
    f.commit(io, null, regions(f.regionLocation(), List.of(rows(0, 100, 1))), List.of(ids.get(0)),
        List.of(5L), List.of(ids.get(0)));
    assertThatThrownBy(
            () -> {
              try (CloseableIterable<Record> rs =
                  ParquetReads.open(
                      new VersionedInputFile(io.newInputFile(f.location(), f.length()), f.length()),
                      Rows.SCHEMA,
                      Expressions.alwaysTrue())) {
                rs.forEach(r -> {});
              }
            })
        .isInstanceOf(RuntimeException.class);
  }
}
