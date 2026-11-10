package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.workload.WorkloadSpec;

/**
 * The locate pruned by key ({@code locate} {@code keys}): a MoR-family writer reads only the row
 * groups whose {@code id} statistics may hold a row the batch changes, where {@code scan} reads
 * every row group in the targets' one id range. It must find the same rows (the locate checks each
 * against the index, and the runner every read against the model), change nothing but the locate,
 * and read less of a table whose changes cluster in a few regions.
 */
class LocateByKeyTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final List<String> ARMS =
      List.of("mor-v3", "tailcow-c", "rmor-c-kept", "rmor-c-footer-kept", "rmor-density", "rmow-c");

  private static BenchConfig config(String locate) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 60_000;
    cfg.workload.batches = 6;
    cfg.workload.opsPerBatch = 1_500;
    cfg.workload.insertFrac = 0.4;
    cfg.workload.updateFrac = 0.5;
    cfg.workload.deleteFrac = 0.1;
    cfg.workload.keyDist = WorkloadSpec.KeyDist.REGIONS;
    cfg.workload.regionRows = 3_000;
    cfg.workload.updateShare = 0.05;
    cfg.layout.rowGroupBytes = 64 << 10;
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.tailSealBytes = 64 << 10;
    cfg.layout.locate = locate;
    cfg.layout.writerSkipsDeadRowGroups = true;
    cfg.layout.updateRead = "row";
    cfg.arms = ARMS;
    cfg.reads.every = 3;
    cfg.reads.travelBack = 3;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.skipping());
    cfg.verifyRows = true;
    return cfg;
  }

  private static Map<String, JsonNode> run(String locate, Path dir) throws IOException {
    Files.createDirectories(dir);
    new Runner(config(locate), dir, "t").run();
    Map<String, JsonNode> out = new HashMap<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      JsonNode r = JSON.readTree(l);
      out.put(r.get("arm").asText() + " " + r.get("step") + " " + r.get("phase").asText() + " "
          + r.path("reader").asText(), r);
    }
    return out;
  }

  private static long sum(Map<String, JsonNode> steps, String arm, String phase, String field) {
    return steps.entrySet().stream()
        .filter(e -> e.getKey().startsWith(arm + " ") && e.getValue().get("phase").asText().equals(phase))
        .mapToLong(e -> e.getValue().get(field).asLong())
        .sum();
  }

  @Test
  void itFindsTheSameRowsReadingLessAndChangesNothingElse(@TempDir Path dir) throws IOException {
    Map<String, JsonNode> scan = run("scan", dir.resolve("scan"));
    Map<String, JsonNode> keys = run("keys", dir.resolve("keys"));
    assertThat(keys.keySet()).isEqualTo(scan.keySet());
    List<String> counts = List.of("requests", "gets", "bytes_read", "bytes_written", "puts");
    for (String k : scan.keySet()) {
      String phase = scan.get(k).get("phase").asText();
      if (phase.equals("locate") || phase.equals("batch") || phase.equals("state")) {
        continue; // the locate, and the batch that holds it
      }
      for (String f : counts) {
        assertThat(keys.get(k).path(f).asLong()).as(k + " " + f).isEqualTo(scan.get(k).path(f).asLong());
      }
    }
    for (String arm : ARMS) {
      for (String f : List.of("requests", "bytes_read")) {
        assertThat(sum(keys, arm, "locate", f)).as(arm + " locate " + f)
            .isLessThanOrEqualTo(sum(scan, arm, "locate", f));
      }
    }
    // MoR's changes cluster in a few regions, so most row groups in the targets' range hold none
    assertThat(sum(keys, "mor-v3", "locate", "bytes_read"))
        .as("MoR's locate reads less by key")
        .isLessThan(sum(scan, "mor-v3", "locate", "bytes_read"));
    // RMoW finds its rows from its footers' statistics either way
    assertThat(sum(keys, "rmow-c", "locate", "requests")).isEqualTo(sum(scan, "rmow-c", "locate", "requests"));
  }

  @Test
  void anUnknownLocateIsRefused(@TempDir Path dir) {
    BenchConfig cfg = config("key");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .hasStackTraceContaining("IllegalArgumentException")
        .hasStackTraceContaining("layout.locate: oracle, scan or keys");
  }
}
