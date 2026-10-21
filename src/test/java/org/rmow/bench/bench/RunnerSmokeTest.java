package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.workload.WorkloadSpec;

/**
 * End to end on both stores: every arm replays the op log and every probe read (current, recent
 * window, time travel) matches the reference model; the runner throws otherwise.
 */
class RunnerSmokeTest {

  private static BenchConfig config(String store, Path root) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = store;
    cfg.store.root = store.equals("mem") ? "mem://test" : root.toString();
    cfg.workload.initialRows = 30_000;
    cfg.workload.batches = 12;
    cfg.workload.opsPerBatch = 1_000;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 1 << 20;
    cfg.layout.tailSealBytes = 128 << 10;
    cfg.layout.tierFanout = 3;
    cfg.arms =
        List.of(
            "mor-v2-file",
            "mor-v2-partition",
            "mor-v3",
            "cow",
            "tailcow-b",
            "tailcow-c",
            "rmow-a",
            "rmow-b",
            "rmow-c",
            "rmow-a-dv",
            "rmow-b-dv",
            "rmow-c-dv",
            "rmow-b-split",
            "rmow-c-split");
    cfg.layout.compactEvery = 4;
    cfg.layout.compactSmallRowGroups = 4;
    cfg.reads.every = 4;
    cfg.reads.travelBack = 5;
    return cfg;
  }

  private static List<JsonNode> run(BenchConfig cfg, Path dir) throws IOException {
    new Runner(cfg, dir, "t").run();
    ObjectMapper json = new ObjectMapper();
    return Files.readAllLines(dir.resolve("steps.jsonl")).stream()
        .map(
            l -> {
              try {
                return json.readTree(l);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  @Test
  void memStoreRecencySkew(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config("mem", dir);
    // a parallel one-footer reader of small split tasks, beside the stock reader
    BenchConfig.ReaderSpec parallel = BenchConfig.ReaderSpec.oneFooter();
    parallel.name = "parallel";
    parallel.parallelism = 4;
    parallel.splitTargetBytes = 256 << 10;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.stock(), parallel);
    List<JsonNode> recs = run(cfg, dir);
    assertThat(recs).anyMatch(r -> r.get("phase").asText().equals("read_old"));
    // MoR-v3 never appends; RMoW-A never writes delete files
    assertThat(recs)
        .filteredOn(r -> r.get("arm").asText().startsWith("mor-") && r.has("appends"))
        .allMatch(r -> r.get("appends").asLong() == 0);
    assertThat(recs)
        .filteredOn(
            r -> r.get("arm").asText().startsWith("rmow-") && r.get("phase").asText().equals("batch"))
        .allMatch(r -> r.get("appends").asLong() + r.get("puts").asLong() >= 1);
    // compaction ran and rewrote something in every family
    assertThat(recs)
        .filteredOn(r -> r.get("phase").asText().equals("compact") && r.get("bytes_written").asLong() > 0)
        .extracting(r -> r.get("arm").asText().split("-")[0])
        .contains("mor", "cow", "tailcow", "rmow");
    // file-scoped v2 deletes are merged, so a data file has at most one live delete file;
    // a regression here means Iceberg no longer sees them as file-scoped (truncated bounds)
    assertThat(recs)
        .filteredOn(r -> r.get("arm").asText().equals("mor-v2-file") && r.get("phase").asText().equals("state"))
        .allMatch(r -> r.get("delete_files").asLong() <= r.get("data_files").asLong());
    // no arm pays a HEAD stock Iceberg would not (I/O parity rule 2): reads none, and writes only
    // the existence check of each Puffin file, so only arms that write DVs
    assertThat(recs)
        .filteredOn(r -> r.has("heads") && r.get("phase").asText().startsWith("read"))
        .allMatch(r -> r.get("heads").asLong() == 0);
    assertThat(recs)
        .filteredOn(r -> r.has("heads") && r.get("heads").asLong() > 0)
        .extracting(r -> r.get("arm").asText())
        .allMatch(a -> a.equals("mor-v3") || a.startsWith("tailcow-") || a.endsWith("-dv"));
    assertThat(recs)
        .filteredOn(
            r ->
                r.has("heads")
                    && r.get("phase").asText().equals("delete-write")
                    && !r.get("arm").asText().startsWith("mor-v2-"))
        .allMatch(r -> r.get("heads").asLong() == 1);
  }

  @Test
  void localStoreUniformKeys(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config("local", dir.resolve("store"));
    cfg.workload.keyDist = WorkloadSpec.KeyDist.UNIFORM;
    // every arm finds its rows by scanning, checked against the index, footers from storage
    cfg.layout.locate = "scan";
    cfg.layout.writerCachesFooters = false;
    cfg.layout.footerPrefetchBytes = 64 << 10;
    Files.createDirectories(dir.resolve("out"));
    List<JsonNode> recs = run(cfg, dir.resolve("out"));
    assertThat(recs).anyMatch(r -> r.get("phase").asText().equals("state"));

    // H2, a validating case: uniform updates touch every row group every batch, so region-merge-on
    // -write rewrites every live row, whatever the layout, as copy-on-write does (batches before
    // the first compaction, which changes each arm's layout differently)
    java.util.Map<String, Long> written = new java.util.TreeMap<>();
    recs.stream()
        .filter(r -> r.get("phase").asText().equals("batch") && r.get("step").asInt() < 4)
        .forEach(r -> written.merge(r.get("arm").asText(), r.get("bytes_written").asLong(), Long::sum));
    double cow = written.get("cow");
    for (String arm : List.of("rmow-a", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split")) {
      // footers aside (split files' region objects carry their own): measured 0.996 to 1.016
      assertThat(written.get(arm) / cow).as(arm + " bytes written / CoW's").isBetween(0.95, 1.05);
    }
  }
}
