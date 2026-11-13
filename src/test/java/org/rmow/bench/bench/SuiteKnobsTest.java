package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The knobs of the 128 MiB suite (docs/HYPOTHESES.md, 2026-11-12): range reads between the probes
 * ({@code reads.rangeEvery}), and MoR's compaction planned as Iceberg's bin-pack ({@code
 * layout.compactMinInputFiles}).
 */
class SuiteKnobsTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static BenchConfig config() {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 30_000;
    cfg.workload.initialJitterRows = 0;
    cfg.workload.batches = 12;
    cfg.workload.opsPerBatch = 1_000;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.locate = "scan";
    cfg.reads.every = 4;
    cfg.reads.travelBack = 2;
    BenchConfig.WindowSpec windows = new BenchConfig.WindowSpec();
    windows.by = List.of("ts");
    windows.count = 2;
    windows.fraction = 0.01;
    cfg.reads.windows = windows;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.cached());
    cfg.arms = List.of("mor-v3", "rmow-c");
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

  private static List<Integer> stepsOf(List<JsonNode> steps, String arm, String phase) {
    return steps.stream()
        .filter(s -> s.get("arm").asText().equals(arm) && s.get("phase").asText().equals(phase))
        .map(s -> s.get("step").asInt())
        .distinct()
        .sorted()
        .toList();
  }

  /** Each write's requests and bytes, by arm, step and phase. */
  private static Map<String, String> writes(List<JsonNode> steps) {
    Map<String, String> out = new TreeMap<>();
    for (JsonNode s : steps) {
      String phase = s.get("phase").asText();
      if (!phase.startsWith("read") && !phase.equals("state")) {
        out.put(
            s.get("arm").asText() + " " + s.get("step") + " " + phase,
            s.get("requests") + " " + s.get("bytes_written") + " " + s.get("bytes_read"));
      }
    }
    return out;
  }

  @Test
  void rangeReadsBetweenTheProbesChangeNoWrite(@TempDir Path dir) throws IOException {
    List<JsonNode> probes = run(config(), dir.resolve("probes"));
    BenchConfig cfg = config();
    cfg.reads.rangeEvery = 1;
    List<JsonNode> every = run(cfg, dir.resolve("every"));

    List<Integer> all = new ArrayList<>();
    for (int i = 0; i <= 12; i++) {
      all.add(i);
    }
    assertThat(stepsOf(every, "rmow-c", "read_window")).isEqualTo(all);
    assertThat(stepsOf(every, "rmow-c", "read_recent")).isEqualTo(all);
    assertThat(stepsOf(every, "rmow-c", "read_full")).containsExactly(0, 4, 8, 12);
    assertThat(stepsOf(every, "rmow-c", "read_old")).containsExactly(4, 8, 12);
    assertThat(writes(every)).isEqualTo(writes(probes)).isNotEmpty();
  }

  @Test
  void icebergsBinPackWaitsForFiveSmallFiles(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config();
    cfg.workload.insertFrac = 1;
    cfg.workload.updateFrac = 0;
    cfg.workload.deleteFrac = 0;
    cfg.layout.targetFileBytes = 8 << 20; // the initial file is small too
    cfg.layout.compactEvery = 1;
    cfg.layout.compactMinInputFiles = 5;
    cfg.arms = List.of("mor-v3");
    List<JsonNode> steps = run(cfg, dir);
    List<Integer> compacted =
        steps.stream()
            .filter(s -> s.get("phase").asText().equals("compact")
                && s.get("bytes_written").asLong() > 0)
            .map(s -> s.get("step").asInt())
            .toList();
    // the initial file and four writes' files make five; then the merged file and four more
    assertThat(compacted).containsExactly(4, 8, 12);
  }
}
