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
import org.rmow.bench.workload.WorkloadSpec;

/**
 * The sorted read: every arm's merge of its row groups gives the model's table in event-time order
 * (the runner checks the rows; the merge checks the order), and needs every run sorted, which an
 * initial load with jittered event times is not. Every phase records what it held, and a run with
 * the held log writes every change.
 */
class SortedReadTest {

  private static BenchConfig config() {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 40_000;
    cfg.workload.batches = 8;
    cfg.workload.opsPerBatch = 2_000;
    cfg.workload.keyDist = WorkloadSpec.KeyDist.REGIONS;
    cfg.workload.regionRows = 4_000;
    cfg.workload.updateShare = 0.05;
    cfg.workload.initialJitterRows = 0;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 1 << 20;
    cfg.layout.tailSealBytes = 256 << 10;
    cfg.layout.locate = "scan";
    cfg.layout.writerSkipsDeadRowGroups = true;
    cfg.arms = List.of("mor-v3", "tailcow-c", "rmor-c", "rmor-c-footer", "rmow-c");
    BenchConfig.ReaderSpec cached = BenchConfig.ReaderSpec.cached();
    cached.sortedRead = true;
    cfg.reads.readers = List.of(cached, BenchConfig.ReaderSpec.stock());
    cfg.reads.every = 4;
    cfg.reads.travelBack = 4;
    cfg.verifyRows = true;
    return cfg;
  }

  private static List<JsonNode> lines(Path file) throws IOException {
    ObjectMapper json = new ObjectMapper();
    List<JsonNode> out = new ArrayList<>();
    for (String l : Files.readAllLines(file)) {
      out.add(json.readTree(l));
    }
    return out;
  }

  @Test
  void everyArmReadsTheTableSortedAndRecordsWhatItHeld(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config();
    new Runner(cfg, dir, "t", new Runner.Options(null, false, false, false, true)).run();
    List<JsonNode> steps = lines(dir.resolve("steps.jsonl"));

    // a sorted read by the cached reader at each probe (0, 4, 8), none by the stock one
    List<JsonNode> sorted = steps.stream().filter(s -> s.get("phase").asText().equals("read_sorted")).toList();
    assertThat(sorted).hasSize(cfg.arms.size() * 3);
    assertThat(sorted).allMatch(s -> s.get("reader").asText().equals("cached"));
    for (JsonNode s : sorted) {
      JsonNode full =
          steps.stream()
              .filter(f -> f.get("phase").asText().equals("read_full")
                  && f.get("reader").asText().equals("cached")
                  && f.get("arm").equals(s.get("arm"))
                  && f.get("step").equals(s.get("step")))
              .findFirst()
              .orElseThrow();
      assertThat(s.get("rows").asLong()).isEqualTo(full.get("rows").asLong());
      assertThat(s.get("held_bytes").asLong()).isPositive();
    }
    // writers hold rows to rewrite, and the region writers encoded bytes to send
    assertThat(steps)
        .filteredOn(s -> s.get("phase").asText().equals("write"))
        .allMatch(s -> s.has("held_rows") && s.has("held_writer"));
    assertThat(steps)
        .filteredOn(s -> s.get("phase").asText().equals("write")
            && s.get("arm").asText().equals("rmow-c") && s.get("step").asInt() > 0)
        .allMatch(s -> s.get("held_pending").asLong() > 0);

    // the held log: every change, in order, never below nothing
    List<JsonNode> held = lines(dir.resolve("held.jsonl"));
    assertThat(held).isNotEmpty();
    assertThat(held).allMatch(e -> e.get("bytes").asLong() >= 0 && e.get("rows").asLong() >= 0);
    assertThat(held).anyMatch(e -> e.get("phase").asText().equals("read_sorted"));
  }

  @Test
  void anInitialLoadWithJitteredEventTimesCannotBeReadSorted(@TempDir Path dir) {
    BenchConfig cfg = config();
    cfg.workload.initialJitterRows = 100;
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("out of order");
  }
}
