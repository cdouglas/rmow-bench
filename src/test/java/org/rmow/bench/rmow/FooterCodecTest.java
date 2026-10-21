package org.rmow.bench.rmow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
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
  void rejectsRowGroupsOutOfFileOrder() throws IOException {
    List<Region> rs = threeRegions();
    List<FileMetaData> reversed = new ArrayList<>(footers(rs));
    java.util.Collections.reverse(reversed);
    assertThatThrownBy(() -> FooterCodec.merge(null, Set.of(), reversed, 0, 0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("out of file order");
  }
}
