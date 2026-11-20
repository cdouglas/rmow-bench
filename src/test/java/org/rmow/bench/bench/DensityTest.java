package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.arms.LayoutSpec;
import org.rmow.bench.workload.WorkloadSpec;

/**
 * What a write reads of the rows it updates ({@code layout.updateRead}), and RMoR with deferred
 * merges ({@code rmor-density}). The runner checks every read against the reference model, so
 * each run here is also a correctness check.
 */
class DensityTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** Updates and deletes only, clustered by region, on a table laid out by key. */
  private static BenchConfig updates() {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 40_000;
    cfg.workload.initialJitterRows = 0;
    cfg.workload.batches = 10;
    cfg.workload.opsPerBatch = 1_500;
    cfg.workload.insertFrac = 0;
    cfg.workload.updateFrac = 0.8;
    cfg.workload.deleteFrac = 0.2;
    cfg.workload.keyDist = WorkloadSpec.KeyDist.REGIONS;
    cfg.workload.regionRows = 4_000;
    cfg.workload.updateShare = 0.03;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.tailSealBytes = 128 << 10;
    cfg.layout.tierFanout = 3;
    cfg.layout.locate = "scan";
    cfg.layout.writerSkipsDeadRowGroups = true;
    cfg.reads.every = 2;
    cfg.reads.travelBack = 2;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.cached(), BenchConfig.ReaderSpec.skipping());
    return cfg;
  }

  private static List<JsonNode> run(BenchConfig cfg, Path dir) throws IOException {
    new Runner(cfg, dir, "t").run();
    List<JsonNode> out = new ArrayList<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      out.add(JSON.readTree(l));
    }
    return out;
  }

  private static long sum(List<JsonNode> steps, String arm, String phase, String f) {
    long n = 0;
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && r.get("phase").asText().equals(phase)) {
        n += r.get(f).asLong();
      }
    }
    return n;
  }

  private static long count(List<JsonNode> steps, String arm, String phase) {
    return steps.stream()
        .filter(r -> r.get("arm").asText().equals(arm) && r.get("phase").asText().equals(phase))
        .count();
  }

  /** By default a new version is made from its id and version: no write reads its old row. */
  @Test
  void byDefaultUpdatesReadNoRows(@TempDir Path dir) throws IOException {
    BenchConfig cfg = updates();
    cfg.arms = List.of("mor-v3", "rmor-c-kept");
    List<JsonNode> steps = run(cfg, dir);
    assertThat(count(steps, "mor-v3", "update-read")).isZero();
    assertThat(count(steps, "rmor-c-kept", "update-read")).isZero();
  }

  /**
   * Under {@code row}, MoR reads every row group holding a row it updates, after a locate that
   * reads what it read before. RMoR, which rewrites every row group it changes, reads them whole
   * anyway and has nothing left to read.
   */
  @Test
  void rowReadsTheRowGroupsOfUpdatedRows(@TempDir Path id, @TempDir Path row) throws IOException {
    BenchConfig cfg = updates();
    cfg.arms = List.of("mor-v3", "rmor-c-kept");
    List<JsonNode> before = run(cfg, id);
    cfg.layout.updateRead = "row";
    List<JsonNode> after = run(cfg, row);
    assertThat(sum(after, "mor-v3", "update-read", "bytes_read"))
        .as("MoR's update read")
        .isGreaterThan(sum(after, "mor-v3", "locate", "bytes_read"));
    assertThat(sum(after, "mor-v3", "locate", "bytes_read"))
        .isEqualTo(sum(before, "mor-v3", "locate", "bytes_read"));
    assertThat(sum(after, "rmor-c-kept", "update-read", "bytes_read")).isZero();
    assertThat(sum(after, "rmor-c-kept", "merge-read", "bytes_read"))
        .isEqualTo(sum(before, "rmor-c-kept", "merge-read", "bytes_read"));
  }

  /** Under {@code scan}, MoR's locate reads every column: more than its id, and no second pass. */
  @Test
  void scanReadsEveryColumnInTheLocate(@TempDir Path id, @TempDir Path scan) throws IOException {
    BenchConfig cfg = updates();
    cfg.arms = List.of("mor-v3");
    List<JsonNode> before = run(cfg, id);
    cfg.layout.updateRead = "scan";
    List<JsonNode> after = run(cfg, scan);
    assertThat(sum(after, "mor-v3", "locate", "bytes_read"))
        .isGreaterThan(3 * sum(before, "mor-v3", "locate", "bytes_read"));
    assertThat(count(after, "mor-v3", "update-read")).isZero();
  }

  @Test
  void anUnknownUpdateReadIsRefused(@TempDir Path dir) {
    BenchConfig cfg = updates();
    cfg.layout.updateRead = "rows";
    cfg.arms = List.of("mor-v3");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("updateRead");
  }

  /**
   * Deferred merges write less than RMoR, which rewrites every row group it changes, and still
   * merge: a region's row group, once its dead rows and the write's changes reach θ of it, with
   * its waiting rows folded in. Both placements of the waiting rows.
   */
  @Test
  void deferredMergesWriteLessAndFold(@TempDir Path region, @TempDir Path write)
      throws IOException {
    BenchConfig cfg = updates();
    cfg.layout.updateRead = "row";
    cfg.layout.defer = new LayoutSpec.Defer();
    cfg.layout.defer.share = 0.1;
    cfg.arms = List.of("rmor-c-kept", "rmor-density");
    List<JsonNode> steps = run(cfg, region);
    assertThat(sum(steps, "rmor-density", "write", "bytes_written"))
        .isLessThan(sum(steps, "rmor-c-kept", "write", "bytes_written") / 2);
    assertThat(sum(steps, "rmor-density", "merge-read", "bytes_read"))
        .as("regions merged")
        .isPositive();
    assertThat(sum(steps, "rmor-density", "update-read", "bytes_read"))
        .as("the rows it leaves DVs on, read to make their new versions")
        .isPositive();

    cfg.layout.defer.deltas = "write";
    cfg.arms = List.of("rmor-density");
    List<JsonNode> perWrite = run(cfg, write);
    assertThat(sum(perWrite, "rmor-density", "merge-read", "bytes_read")).isPositive();
  }

  /** With inserts too (a tail of merged insert files beside the regions), every read is right. */
  @Test
  void deferredMergesWithInserts(@TempDir Path dir) throws IOException {
    BenchConfig cfg = updates();
    cfg.workload.insertFrac = 0.5;
    cfg.workload.updateFrac = 0.4;
    cfg.workload.deleteFrac = 0.1;
    cfg.layout.updateRead = "row";
    cfg.layout.defer = new LayoutSpec.Defer();
    cfg.layout.defer.share = 0.05;
    cfg.arms = List.of("rmor-density");
    List<JsonNode> steps = run(cfg, dir);
    assertThat(sum(steps, "rmor-density", "merge-read", "bytes_read")).isPositive();
  }

  /**
   * A tail file every row of which goes dead under DVs leaves the table, and must leave the tail
   * policy's open units too: the fuzzer's seed 1592 (its configuration, cut to this arm and nine
   * batches) found the policy merging such a file, read back whole with its DV gone, so its rows
   * came back beside their newer versions. A seal size above the whole table keeps every file
   * open, and the newest rows are changed again and again.
   */
  @Test
  void aTailFileThatDiesLeavesTheTail(@TempDir Path dir) throws IOException {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    WorkloadSpec w = cfg.workload;
    w.seed = 471179891185L;
    w.initialRows = 3795;
    w.batches = 9;
    w.opsPerBatch = 73;
    w.insertFrac = 1 / 73.0;
    w.updateFrac = 53 / 73.0;
    w.deleteFrac = 19 / 73.0;
    w.keyDist = WorkloadSpec.KeyDist.RECENCY_EXP;
    w.recencyMeanBatches = 0.2851650271847598;
    w.tsStepMicros = 1;
    w.initialJitterRows = 276.8677388183578;
    w.lateMedianBatches = 0.02759587100326767;
    w.lateP99Batches = 0.19581047863234427;
    w.regionRows = 2074;
    cfg.layout.codec = "uncompressed";
    cfg.layout.rowGroupBytes = 84696;
    cfg.layout.targetFileBytes = 169392;
    cfg.layout.tailSealBytes = 4127091;
    cfg.layout.tierFanout = 3;
    cfg.layout.maxRowGroupsPerFile = 26;
    cfg.layout.defer = new LayoutSpec.Defer();
    cfg.layout.defer.share = 0.09412906274394002;
    cfg.reads.every = 3;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.cached());
    cfg.arms = List.of("rmor-density");
    run(cfg, dir);
  }

  /** The arm's last state record's {@code field}. */
  private static long last(List<JsonNode> steps, String arm, String field) {
    long v = -1;
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && r.get("phase").asText().equals("state")) {
        v = r.get(field).asLong();
      }
    }
    return v;
  }

  /**
   * RMoR-density with the cut, and the merge compaction after every batch: tiers of 2, cold after
   * 2 batches, merged row groups of 64 KiB (a quarter of the writer's), on more batches.
   */
  private static BenchConfig merged() {
    BenchConfig cfg = updates();
    cfg.workload.batches = 16;
    cfg.layout.updateRead = "row";
    cfg.layout.locate = "keys";
    cfg.layout.defer = new LayoutSpec.Defer();
    cfg.layout.defer.share = 0.1;
    cfg.layout.cut = new LayoutSpec.CutSpec();
    cfg.layout.cut.strideRows = 200;
    cfg.layout.compactEvery = 1;
    cfg.layout.merge = new LayoutSpec.Merge();
    cfg.layout.merge.fanout = 2;
    cfg.layout.merge.cold = 2;
    cfg.layout.merge.rowGroupBytes = 64 << 10;
    cfg.arms = List.of("rmor-density");
    return cfg;
  }

  /**
   * The merge compaction keeps fewer row groups than RMoR-density without it, in a phase of its
   * own, and every read is right (the runner checks each one).
   */
  @Test
  void theMergeCompactionKeepsFewerRowGroups(@TempDir Path without, @TempDir Path with)
      throws IOException {
    BenchConfig cfg = merged();
    cfg.layout.merge = null;
    cfg.layout.compactEvery = 0;
    List<JsonNode> alone = run(cfg, without);
    List<JsonNode> steps = run(merged(), with);
    assertThat(last(steps, "rmor-density", "row_groups"))
        .isLessThan(last(alone, "rmor-density", "row_groups"));
    assertThat(sum(steps, "rmor-density", "compact", "bytes_written")).isPositive();
    assertThat(sum(steps, "rmor-density", "compact", "bytes_read")).isPositive();
    assertThat(count(alone, "rmor-density", "compact")).isZero();
  }

  /**
   * Draining and merging back with no batch-age wait (cold 0), and the delta row groups of one
   * list a batch ({@code deltas} {@code write}), whose rows wait for several regions: what a fold
   * takes from one is not moved again.
   */
  @Test
  void theMergeCompactionAtItsMostEager(@TempDir Path region, @TempDir Path write)
      throws IOException {
    BenchConfig cfg = merged();
    cfg.layout.merge.cold = 0;
    cfg.layout.merge.drain = 0.9;
    cfg.layout.merge.small = 1;
    List<JsonNode> steps = run(cfg, region);
    assertThat(sum(steps, "rmor-density", "compact", "bytes_written")).isPositive();
    cfg.layout.defer.deltas = "write";
    steps = run(cfg, write);
    assertThat(sum(steps, "rmor-density", "compact", "bytes_written")).isPositive();
  }

  /**
   * RMoR-density that never copies (θ above 1) is MoR with its new versions in a delta row group
   * per region; with the merge compaction, those merge in tiers, and fewer row groups stay.
   */
  @Test
  void neverCopyingWithTheMergeCompaction(@TempDir Path without, @TempDir Path with)
      throws IOException {
    BenchConfig cfg = merged();
    cfg.layout.defer.share = 9;
    cfg.layout.merge = null;
    cfg.layout.compactEvery = 0;
    List<JsonNode> alone = run(cfg, without);
    cfg = merged();
    cfg.layout.defer.share = 9;
    List<JsonNode> steps = run(cfg, with);
    assertThat(sum(steps, "rmor-density", "compact", "bytes_written")).isPositive();
    assertThat(last(steps, "rmor-density", "row_groups"))
        .isLessThan(last(alone, "rmor-density", "row_groups"));
  }

  /**
   * Leveled under a cap of 1 or 2 (every batch's delta row groups merge into a region's one or two),
   * every read is right, and no more row groups stay than with tiers.
   */
  @Test
  void theMergeCompactionLeveled(@TempDir Path tiers, @TempDir Path one, @TempDir Path two)
      throws IOException {
    BenchConfig cfg = merged();
    cfg.layout.merge.cap = 0;
    List<JsonNode> tiered = run(cfg, tiers);
    cfg.layout.merge.cap = 1;
    List<JsonNode> steps = run(cfg, one);
    assertThat(sum(steps, "rmor-density", "compact", "bytes_written")).isPositive();
    assertThat(last(steps, "rmor-density", "row_groups"))
        .isLessThanOrEqualTo(last(tiered, "rmor-density", "row_groups"));
    cfg.layout.merge.cap = 2;
    cfg.layout.defer.deltas = "write";
    run(cfg, two);
  }

  /**
   * With delta files ({@code defer.deltaFiles}), a write's and the merge compaction's delta row
   * groups go to files of their own (the arm refuses a file that mixes them), every read is right,
   * and windows by id send fewer requests: the copies' files keep narrow id bounds. Also when the
   * arm never copies.
   */
  @Test
  void deltaFilesKeepCopiesApart(@TempDir Path mixed, @TempDir Path apart, @TempDir Path never)
      throws IOException {
    BenchConfig cfg = merged();
    cfg.reads.windows = new BenchConfig.WindowSpec();
    List<JsonNode> together = run(cfg, mixed);
    cfg.layout.defer.deltaFiles = true;
    List<JsonNode> steps = run(cfg, apart);
    assertThat(sum(steps, "rmor-density", "compact", "bytes_written")).isPositive();
    assertThat(sum(steps, "rmor-density", "read_window", "requests"))
        .isLessThan(sum(together, "rmor-density", "read_window", "requests"));
    cfg.layout.defer.share = 9;
    run(cfg, never);
  }

  @Test
  void anUnknownMergeIsRefused(@TempDir Path dir) {
    BenchConfig cfg = merged();
    cfg.layout.merge.fanout = 1;
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("merge");
  }

  @Test
  void anUnknownDeltaPlacementIsRefused(@TempDir Path dir) {
    BenchConfig cfg = updates();
    cfg.layout.defer = new LayoutSpec.Defer();
    cfg.layout.defer.deltas = "file";
    cfg.arms = List.of("rmor-density");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("defer.deltas");
  }
}
