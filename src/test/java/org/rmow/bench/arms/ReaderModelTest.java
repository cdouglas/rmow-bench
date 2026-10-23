package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;
import org.rmow.bench.bench.Phases;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.io.TracingFileIO;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.RegionWriter;
import org.rmow.bench.rmow.RegionWriter.Region;
import org.rmow.bench.rmow.RmowFile;
import org.rmow.bench.workload.Rows;

/**
 * Pins the requests each reader model issues to read one version of an appendable file: the
 * version is summarized in exactly one footer, which the one-footer reader reads once (one
 * request, with its prefetch); stock Iceberg readers re-read it for every row-group split.
 */
class ReaderModelTest {

  private static final String LOC = "mem://t/rmow.parquet";

  @Test
  void oneFooterPerVersion() throws IOException {
    MemFileIO mem = new MemFileIO();
    RegionWriter writer =
        new RegionWriter(Rows.SCHEMA, Map.of("write.parquet.compression-codec", "zstd"));
    TableHash want = new TableHash();
    RmowFile f = null;
    for (int region = 0; region < 3; region++) {
      List<Record> rows = new ArrayList<>();
      for (long id = region * 40_000L; id < (region + 1) * 40_000L; id++) {
        Record r = Rows.record(id, id, 0);
        rows.add(r);
        want.add(r, false);
      }
      Region r = writer.write(LOC, f == null ? 0 : f.length(), rows);
      if (f == null) {
        f = RmowFile.create(mem, LOC, List.of(r), List.of(0L));
      } else {
        f.append(mem, List.of(r), List.of(), List.of((long) region));
      }
    }
    assertThat(f.length()).isGreaterThan(1 << 20);
    assertThat(f.footer().getRow_groupsSize()).isEqualTo(3);

    IoTrace.Counters oneFooter = read(mem, f, BenchReaders.oneFooter(), want);
    // one footer request, then one bounded ranged GET per live row group
    assertThat(oneFooter.gets()).isEqualTo(1 + 3);
    assertThat(oneFooter.getStream()).isZero();

    IoTrace.Counters stock = read(mem, f, BenchReaders.stock(), want);
    // each of the three row-group splits reads the footer again: two stream requests each
    assertThat(stock.getStream()).isEqualTo(3 * 2);
    assertThat(stock.gets()).isEqualTo(3 * 2 + 3);
  }

  private static IoTrace.Counters read(MemFileIO mem, RmowFile f, Reader reader, TableHash want)
      throws IOException {
    IoTrace trace = new IoTrace();
    ArmContext ctx =
        new ArmContext(
            "t",
            "mem://t",
            new TracingFileIO(mem, mem, trace),
            mem,
            new Phases(trace),
            new LayoutSpec(),
            null,
            true);
    TableHash got =
        Scans.readAll(
            ctx,
            reader,
            List.of(new Scans.ReadFile(f.dataFile(), new DeleteFile[0])),
            Expressions.alwaysTrue());
    assertThat(got).isEqualTo(want);
    return trace.snapshot();
  }

  /** The two reader models as the runner builds them from the default config. */
  private static final class BenchReaders {
    static Reader oneFooter() {
      return new Reader("one-footer", true, false, 64 << 10, 128L << 20, null);
    }

    static Reader stock() {
      return new Reader("stock", false, false, 0, 128L << 20, null);
    }
  }
}
