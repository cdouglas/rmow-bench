package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.workload.WorkloadSpec;

/**
 * A measured run (no {@code --verify}) runs no oracle in its reads, which only count their rows;
 * a verification run of the same config hashes every row against the reference model. The oracle
 * reads nothing, so the two must issue the same requests and bytes, leave the same table and
 * return the same rows, record for record: that is what lets a verification run vouch for a
 * measured one.
 */
class MeasuredRunTest {

  /** The fields a run's I/O and table determine: equal in every run of a config. */
  private static final List<String> DETERMINED =
      List.of(
          "requests", "gets", "get_range", "get_stream", "heads", "puts", "appends",
          "append_splits", "deletes", "aborts", "bytes_read", "bytes_skipped_through",
          "bytes_written", "rows", "objects", "stored_bytes", "live_bytes", "reclaimable_bytes",
          "row_groups", "data_files", "delete_files", "footer_bytes", "live_objects");

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
    cfg.layout.locate = "keys";
    cfg.layout.writerSkipsDeadRowGroups = true;
    cfg.arms = List.of("mor-v3", "rmor-c-kept", "rmor-c-footer-kept", "rmow-c", "rmor-density");
    BenchConfig.ReaderSpec cached = BenchConfig.ReaderSpec.cached();
    cached.sortedRead = true;
    cached.skipDeadRowGroups = true;
    cfg.reads.readers = List.of(cached, BenchConfig.ReaderSpec.stock());
    cfg.reads.every = 4;
    cfg.reads.travelBack = 4;
    cfg.verifyRows = true;
    return cfg;
  }

  /** Each record's determined fields, by arm, step, phase, reader and window. */
  private static Map<String, Map<String, String>> determined(Path dir) throws IOException {
    ObjectMapper json = new ObjectMapper();
    Map<String, Map<String, String>> out = new LinkedHashMap<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      JsonNode s = json.readTree(l);
      String key =
          String.join(
              "/", s.path("arm").asText(), s.path("step").asText(), s.path("phase").asText(),
              s.path("reader").asText(), s.path("window_by").asText(), s.path("window_at").asText());
      Map<String, String> fields = new LinkedHashMap<>();
      for (String f : DETERMINED) {
        if (s.has(f)) {
          fields.put(f, s.get(f).asText());
        }
      }
      assertThat(out.put(key, fields)).as("one record for %s", key).isNull();
    }
    return out;
  }

  @Test
  void aMeasuredRunIssuesWhatItsVerificationRunDoes(@TempDir Path dir) throws IOException {
    Path measured = dir.resolve("measured");
    Path verified = dir.resolve("verified");
    Files.createDirectories(measured);
    Files.createDirectories(verified);
    new Runner(config(), measured, "t", new Runner.Options(null, false, false, false, false, false))
        .run();
    new Runner(config(), verified, "t", new Runner.Options(null, false, false, false, false, true))
        .run();

    Map<String, Map<String, String>> m = determined(measured);
    Map<String, Map<String, String>> v = determined(verified);
    assertThat(m.keySet()).containsExactlyElementsOf(v.keySet());
    for (String key : m.keySet()) {
      assertThat(m.get(key)).as(key).isEqualTo(v.get(key));
    }
    // every read returned rows, and both runs counted the same ones
    assertThat(m.entrySet())
        .filteredOn(e -> e.getKey().contains("/read_"))
        .isNotEmpty()
        .allSatisfy(e -> assertThat(Long.parseLong(e.getValue().get("rows"))).isPositive());
  }
}
