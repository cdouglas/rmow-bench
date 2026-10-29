package org.rmow.bench.workload;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpLogGeneratorTest {

  private static WorkloadSpec spec(long rows, int batches, int ops, WorkloadSpec.KeyDist keys) {
    WorkloadSpec w = new WorkloadSpec();
    w.initialRows = rows;
    w.batches = batches;
    w.opsPerBatch = ops;
    w.keyDist = keys;
    return w;
  }

  private static String sha(WorkloadSpec w) {
    return OpLogGenerator.generate(w).sha256();
  }

  /** Regions changed no log made without them: the hashes are those of the runs' manifests. */
  @Test
  void logsWithoutRegionsAreTheRunsLogs() {
    // the calibration (s3x-calibration-local, azure-calibration-azurite)
    assertThat(sha(spec(200_000, 20, 2_000, WorkloadSpec.KeyDist.RECENCY_EXP)))
        .isEqualTo("635d7dd05e6358cc909bf83362b06abe8cd4356d5d49695dca1a8897bdb15592");
    // local-compact-*, and the m1 matrix's recency and uniform settings
    assertThat(sha(spec(1_000_000, 60, 10_000, WorkloadSpec.KeyDist.RECENCY_EXP)))
        .isEqualTo("03a752cee1062744c8df513e08260a05a5fc2deb4ce6b235a3eda90e88fb51f7");
    assertThat(sha(spec(1_000_000, 60, 10_000, WorkloadSpec.KeyDist.UNIFORM)))
        .isEqualTo("3fb664e9e72fae9ee4df764ea738375cdcb3ad33f36d42f922dfec13988ed355");
  }

  /** The region knobs leave a log without regions alone. */
  @Test
  void regionKnobsDoNotMoveOtherLogs() {
    WorkloadSpec a = spec(50_000, 10, 1_000, WorkloadSpec.KeyDist.RECENCY_EXP);
    WorkloadSpec b = spec(50_000, 10, 1_000, WorkloadSpec.KeyDist.RECENCY_EXP);
    b.regionRows = 777;
    b.regionSkew = 2.5;
    assertThat(sha(b)).isEqualTo(sha(a));
  }

  @Test
  void everyBatchTouchesDistinctLiveRows() {
    for (WorkloadSpec.KeyDist keys : WorkloadSpec.KeyDist.values()) {
      for (double share : new double[] {0, 0.05, 1}) {
        WorkloadSpec w = spec(20_000, 15, 2_000, keys);
        w.regionRows = 1_000;
        w.updateShare = share;
        OpLog log = OpLogGenerator.generate(w);
        BitSet deleted = new BitSet();
        for (OpLog.Batch b : log.batches()) {
          assertThat(b.updateIds()).hasSize(w.updates()).doesNotHaveDuplicates();
          assertThat(b.deleteIds()).hasSize(w.deletes());
          BitSet touched = new BitSet();
          for (long[] ids : new long[][] {b.updateIds(), b.deleteIds()}) {
            for (long id : ids) {
              String at = keys + " " + share + ": id " + id;
              assertThat(deleted.get((int) id)).as("%s deleted", at).isFalse();
              assertThat(touched.get((int) id)).as("%s twice", at).isFalse();
              touched.set((int) id);
            }
          }
          for (long id : b.deleteIds()) {
            deleted.set((int) id);
          }
        }
      }
    }
  }

  @Test
  void aBatchIsAPrefixOfALongerRun() {
    WorkloadSpec w = spec(20_000, 6, 1_000, WorkloadSpec.KeyDist.REGIONS);
    w.regionRows = 500;
    w.updateShare = 0.1;
    OpLog shorter = OpLogGenerator.generate(w);
    w.batches = 12;
    OpLog longer = OpLogGenerator.generate(w);
    for (int i = 0; i < 6; i++) {
      OpLog.Batch a = shorter.batches().get(i);
      OpLog.Batch b = longer.batches().get(i);
      assertThat(b.updateIds()).isEqualTo(a.updateIds());
      assertThat(b.deleteIds()).isEqualTo(a.deleteIds());
    }
  }

  /** Updates per region over the whole log. */
  private static Map<Long, Integer> updatesByRegion(OpLog log, long regionRows) {
    Map<Long, Integer> n = new HashMap<>();
    for (OpLog.Batch b : log.batches()) {
      for (long id : b.updateIds()) {
        n.merge(id / regionRows, 1, Integer::sum);
      }
    }
    return n;
  }

  /** The hottest tenth of the regions' share of the updates. */
  private static double hottestTenth(Map<Long, Integer> byRegion, long regions) {
    int[] n = byRegion.values().stream().mapToInt(Integer::intValue).sorted().toArray();
    long all = Arrays.stream(n).sum();
    long top = 0;
    for (int i = 0; i < regions / 10; i++) {
      top += n[n.length - 1 - i];
    }
    return (double) top / all;
  }

  @Test
  void skewConcentratesUpdatesAndZeroIsUniform() {
    WorkloadSpec w = spec(200_000, 20, 2_000, WorkloadSpec.KeyDist.REGIONS);
    w.regionRows = 1_000; // 200 regions at the start, 228 at the end
    w.regionSkew = 0;
    double uniform = hottestTenth(updatesByRegion(OpLogGenerator.generate(w), 1_000), 200);
    w.regionSkew = 1;
    double zipf = hottestTenth(updatesByRegion(OpLogGenerator.generate(w), 1_000), 200);
    w.regionSkew = 2;
    double steep = hottestTenth(updatesByRegion(OpLogGenerator.generate(w), 1_000), 200);
    // uniform: about a tenth (the busiest tenth by chance, 10,000 updates over 200 regions)
    assertThat(uniform).isBetween(0.10, 0.14);
    // Zipf's law with exponent 1 over 228 regions puts about half the weight on the top tenth
    assertThat(zipf).isBetween(0.4, 0.8);
    assertThat(steep).isGreaterThan(zipf);
  }

  /** The regions the ids fall in, and the ids in the busiest. */
  private static long[] spread(long regionRows, long[]... ids) {
    Map<Long, Integer> n = new HashMap<>();
    for (long[] some : ids) {
      for (long id : some) {
        n.merge(id / regionRows, 1, Integer::sum);
      }
    }
    return new long[] {n.size(), n.values().stream().mapToInt(Integer::intValue).max().orElse(0)};
  }

  @Test
  void aStatementChangesAShareOfOneRegion() {
    WorkloadSpec w = spec(100_000, 10, 2_000, WorkloadSpec.KeyDist.REGIONS);
    w.regionRows = 1_000;
    for (OpLog.Batch b : OpLogGenerator.generate(w).batches()) {
      assertThat(spread(1_000, b.updateIds())[0]).as("one row an update").isGreaterThan(30);
      assertThat(spread(1_000, b.deleteIds())[0]).as("one row a delete").isGreaterThan(30);
    }
    // a statement takes a tenth of a region's live rows (80 to 100 here, as deletes thin them),
    // and a batch's 500 updates and 100 deletes take at most eight, deletes included
    w.updateShare = 0.1;
    for (OpLog.Batch b : OpLogGenerator.generate(w).batches()) {
      long[] changes = spread(1_000, b.updateIds(), b.deleteIds());
      assertThat(changes[0]).isBetween(1L, 8L);
      assertThat(changes[1]).isGreaterThanOrEqualTo(80);
    }
    // a statement would take the whole region; the batch's 600 changes cap it
    w.updateShare = 1;
    for (OpLog.Batch b : OpLogGenerator.generate(w).batches()) {
      assertThat(spread(1_000, b.updateIds(), b.deleteIds())).containsExactly(1, 600);
    }
  }

  @Test
  void anUpdateShareWorksOnTheIngestWindowToo() {
    WorkloadSpec w = spec(100_000, 10, 2_000, WorkloadSpec.KeyDist.RECENCY_EXP);
    w.regionRows = 1_000;
    w.updateShare = 0.2;
    // statements of 190 to 200 rows in full regions, fewer in the newest, which is filling
    for (OpLog.Batch b : OpLogGenerator.generate(w).batches()) {
      long[] changes = spread(1_000, b.updateIds(), b.deleteIds());
      assertThat(changes[0]).isBetween(1L, 6L);
      assertThat(changes[1]).isGreaterThanOrEqualTo(150);
    }
  }
}
