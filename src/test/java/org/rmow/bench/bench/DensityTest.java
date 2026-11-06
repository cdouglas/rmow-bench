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
