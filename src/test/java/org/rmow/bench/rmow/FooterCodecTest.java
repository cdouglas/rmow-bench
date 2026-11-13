package org.rmow.bench.rmow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.data.Record;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.api.Test;
import org.rmow.bench.rmow.RegionWriter.Region;
import org.rmow.bench.workload.Rows;

class FooterCodecTest {

  private final RegionWriter writer =
      new RegionWriter(Rows.SCHEMA, Map.of("write.parquet.compression-codec", "zstd"));

  private List<Region> threeRegions() throws IOException {
    List<Region> regions = new ArrayList<>();
    long base = 0;
    for (int i = 0; i < 3; i++) {
      List<Record> rows = new ArrayList<>();
      for (long id = i * 100L; id < (i + 1) * 100L; id++) {
        rows.add(Rows.record(id, id, 0));
      }
      Region r = writer.write("mem://f", base, rows);
      regions.add(r);
      base = r.end();
    }
    return regions;
  }

  private static List<FileMetaData> footers(List<Region> rs) {
    return rs.stream().map(Region::footer).toList();
  }

  @Test
  void mergeKeepsRowGroupsAndLeavesThePreviousFooterAlone() throws IOException {
    List<Region> rs = threeRegions();
    FileMetaData v0 = FooterCodec.merge(null, Set.of(), footers(rs), 0, 0);
    FileMetaData before = v0.deepCopy();

    FileMetaData v1 = FooterCodec.merge(v0, Set.of(1), List.of(), 1000, 1);

    assertThat(v0).isEqualTo(before);
    assertThat(v1.getRow_groupsSize()).isEqualTo(2);
    assertThat(v1.getNum_rows()).isEqualTo(200);
    for (int i = 0; i < 2; i++) {
      RowGroup kept = v1.getRow_groups().get(i).deepCopy();
      assertThat(kept.getOrdinal()).isEqualTo((short) i);
      RowGroup source = v0.getRow_groups().get(i == 0 ? 0 : 2);
      kept.setOrdinal(source.getOrdinal());
      assertThat(kept).isEqualTo(source);
    }
    // the serialized form decodes to the same footer
    byte[] bytes = FooterCodec.serialize(v1);
    FooterCodec.Tail t = FooterCodec.locate(bytes, bytes.length);
    assertThat(FooterCodec.read(bytes, t.footerStart(), t.footerLength())).isEqualTo(v1);
  }

  @Test
  void inPlaceRegionTakesTheReplacedSlot() throws IOException {
    List<Region> rs = threeRegions();
    FileMetaData v0 = FooterCodec.merge(null, Set.of(), footers(rs.subList(0, 2)), 0, 0);
    // region 2, after both, replaces row group 0 in its place: the list leaves offset order
    FileMetaData v1 =
        FooterCodec.merge(v0, Set.of(0), footers(rs.subList(2, 3)), Arrays.asList(0), 1000, 1);
    assertThat(v1.getRow_groups())
        .extracting(FooterCodec::startOffset)
        .containsExactly(start(rs.get(2)), start(rs.get(1)));
    assertThat(v1.getRow_groups()).extracting(RowGroup::getOrdinal).containsExactly((short) 0, (short) 1);
    assertThat(v1.getNum_rows()).isEqualTo(200);
    // later versions keep it, listing new row groups last
    FileMetaData v2 = FooterCodec.merge(v0, Set.of(1), List.of(), Arrays.asList(), 2000, 2);
    assertThat(v2.getRow_groupsSize()).isEqualTo(1);

    // a region can take only a dropped row group's place
    assertThatThrownBy(
            () ->
                FooterCodec.merge(v0, Set.of(), footers(rs.subList(2, 3)), Arrays.asList(0), 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    // several regions take one place in their order (a row group the event-time merge cut)
    FileMetaData v3 =
        FooterCodec.merge(v0, Set.of(0, 1), footers(rs.subList(1, 3)), Arrays.asList(0, 0), 1, 1);
    assertThat(v3.getRow_groups())
        .extracting(FooterCodec::startOffset)
        .containsExactly(start(rs.get(1)), start(rs.get(2)));
  }

  private static long start(Region r) {
    return FooterCodec.startOffset(r.footer().getRow_groups().get(0));
  }

  @Test
  void rejectsRowGroupsOutOfFileOrder() throws IOException {
    List<Region> rs = threeRegions();
    List<FileMetaData> reversed = new ArrayList<>(footers(rs));
    java.util.Collections.reverse(reversed);
    assertThatThrownBy(() -> FooterCodec.merge(null, Set.of(), reversed, 0, 0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("out of file order");
  }
}
