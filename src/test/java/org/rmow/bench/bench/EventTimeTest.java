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

/**
 * The event-time merge ({@code layout.inserts} {@code event-time}): the region designs put each
 * insert into the row group its event time falls in, so their row groups stay disjoint in event
 * time. Each arm checks that itself (its index refuses an overlap, and its locate checks every
 * placement against the footers' statistics), and the runner checks every read against the
 * reference model, so each run here is also a correctness check.
 */
class EventTimeTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final List<String> REGION_ARMS =
      List.of("rmor-c-kept", "rmor-c-footer-kept", "rmow-c", "rmor-density");

  /** Inserts only, by event time, some late: the inserts workload, small. */
  private static BenchConfig inserts(String placement) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 40_000;
    cfg.workload.initialJitterRows = 0;
    cfg.workload.batches = 10;
    cfg.workload.opsPerBatch = 2_000;
    cfg.workload.insertFrac = 1;
    cfg.workload.updateFrac = 0;
    cfg.workload.deleteFrac = 0;
    cfg.workload.lateMedianBatches = 0.05;
    cfg.workload.lateP99Batches = 5;
    cfg.layout.rowGroupBytes = 128 << 10;
    cfg.layout.targetFileBytes = 1 << 20;
    cfg.layout.tailSealBytes = 128 << 10;
    cfg.layout.locate = "scan";
    cfg.layout.writerSkipsDeadRowGroups = true;
    cfg.layout.eagerBytes = 16L << 10;
    cfg.layout.inserts = placement;
    List<String> arms = new ArrayList<>(List.of("mor-v3"));
    arms.addAll(REGION_ARMS);
    cfg.arms = arms;
    BenchConfig.ReaderSpec skipping = BenchConfig.ReaderSpec.skipping();
    skipping.sortedRead = true;
    cfg.reads.readers = List.of(skipping, BenchConfig.ReaderSpec.stock());
    cfg.reads.every = 2;
    cfg.reads.travelBack = 2;
    cfg.verifyRows = true;
    return cfg;
  }

  private static List<JsonNode> run(BenchConfig cfg, Path dir) throws IOException {
    Files.createDirectories(dir);
    new Runner(cfg, dir, "t").run();
    List<JsonNode> out = new ArrayList<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      out.add(JSON.readTree(l));
    }
    return out;
  }

  /** The largest value of {@code field} of an arm's {@code phase} records at {@code step}. */
  private static long at(List<JsonNode> steps, String arm, String phase, int step, String field) {
    long max = 0;
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && r.get("phase").asText().equals(phase)
          && r.get("step").asInt() == step && r.has(field)) {
        max = Math.max(max, r.get(field).asLong());
      }
    }
    return max;
  }

  private static long sum(List<JsonNode> steps, String arm, String phase, String field) {
    long n = 0;
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && r.get("phase").asText().equals(phase)
          && r.has(field)) {
        n += r.get(field).asLong();
      }
    }
    return n;
  }

  /**
   * Every region design reads the table right and in event-time order, and, but for RMoR-density
   * (whose waiting rows' delta row groups overlap), holds fewer bytes to do so than when inserts go
   * to the tail, whose row groups each reach back to their latest late row. Each writes more for
   * that than MoR, which appends. (These files are under 1 MiB, which a read fetches and sorts
   * whole, so the RMoR family's files, which mix row groups of several times, are held whole.)
   */
  @Test
  void theRegionDesignsKeepTheirRowGroupsApartInEventTime(@TempDir Path dir) throws IOException {
    List<JsonNode> merged = run(inserts("event-time"), dir.resolve("event-time"));
    List<JsonNode> tail = run(inserts(null), dir.resolve("tail"));
    int last = 10;
    for (String arm : REGION_ARMS) {
      long heldMerged = at(merged, arm, "read_sorted", last, "held_bytes");
      long heldTail = at(tail, arm, "read_sorted", last, "held_bytes");
      if (!arm.equals("rmor-density")) { // its waiting rows' delta row groups overlap the others
        assertThat(heldMerged).as(arm + ": bytes the sorted read holds").isLessThan(heldTail);
      }
      assertThat(sum(merged, arm, "batch", "bytes_written"))
          .as(arm + ": bytes written")
          .isGreaterThan(sum(merged, "mor-v3", "batch", "bytes_written"));
    }
    // RMoW's files hold runs of event time, one after another: the read holds about one row group
    assertThat(4 * at(merged, "rmow-c", "read_sorted", last, "held_bytes"))
        .isLessThan(at(tail, "rmow-c", "read_sorted", last, "held_bytes"));
    // MoR appends either way
    assertThat(sum(merged, "mor-v3", "batch", "bytes_written"))
        .isEqualTo(sum(tail, "mor-v3", "batch", "bytes_written"));
  }

  @Test
  void aMixedWorkloadIsRefused(@TempDir Path dir) {
    BenchConfig cfg = inserts("event-time");
    cfg.workload.insertFrac = 0.7;
    cfg.workload.updateFrac = 0.3;
    cfg.arms = List.of("rmow-c");
    assertThatThrownBy(() -> run(cfg, dir))
        .hasMessage(
            "The event-time merge places inserts; a workload that also updates or deletes is not"
                + " built for it");
  }

  @Test
  void compactionAndTheOtherRegionArmsAreRefused(@TempDir Path dir) {
    BenchConfig compacted = inserts("event-time");
    compacted.layout.compactEvery = 4;
    compacted.arms = List.of("rmor-c-kept");
    assertThatThrownBy(() -> run(compacted, dir.resolve("a")))
        .hasMessageContaining("compaction is not built for it");
    BenchConfig mixedFile = inserts("event-time");
    mixedFile.arms = List.of("rmor-c");
    assertThatThrownBy(() -> run(mixedFile, dir.resolve("b")))
        .hasMessageContaining("built for the arms that keep row groups");
    BenchConfig split = inserts("event-time");
    split.arms = List.of("rmow-c-split");
    assertThatThrownBy(() -> run(split, dir.resolve("c")))
        .hasMessageContaining("not built for split files");
  }

  @Test
  void anInitialLoadOutOfEventTimeOrderIsRefused(@TempDir Path dir) {
    BenchConfig cfg = inserts("event-time");
    cfg.workload.initialJitterRows = 2_000;
    cfg.arms = List.of("rmow-c");
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.stock());
    assertThatThrownBy(() -> run(cfg, dir))
        .hasMessageContaining("needs an initial load in event-time order");
  }
}
