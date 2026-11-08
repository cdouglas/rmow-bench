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
        .allMatch(a -> a.equals("mor-v3") || a.startsWith("tailcow-"));
    assertThat(recs)
        .filteredOn(
            r ->
                r.has("heads")
                    && r.get("phase").asText().equals("delete-write")
                    && !r.get("arm").asText().startsWith("mor-v2-"))
        .allMatch(r -> r.get("heads").asLong() == 1);
  }

  @Test
  void timeTravelReadsLikeTheCurrentRead(@TempDir Path dir) throws IOException {
    // read_old at step s reads the state read_full read at step s - travelBack, when it was
    // current, so with no compaction (history is recorded before it) the two must cost the same:
    // an old version is planned from its own split offsets, as a snapshot's manifest entry would
    // plan it (H4). Files of 2 MiB hold several row groups, so stock splits per row group.
    BenchConfig cfg = config("mem", dir);
    cfg.arms =
        List.of("mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split");
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.compactEvery = 0;
    cfg.reads.every = 3;
    cfg.reads.travelBack = 3;
    List<JsonNode> recs = run(cfg, dir);
    java.util.Map<String, JsonNode> full = new java.util.HashMap<>();
    recs.stream()
        .filter(r -> r.get("phase").asText().equals("read_full"))
        .forEach(r -> full.put(key(r, r.get("step").asInt()), r));
    List<JsonNode> old =
        recs.stream().filter(r -> r.get("phase").asText().equals("read_old")).toList();
    assertThat(old).hasSize(cfg.arms.size() * cfg.reads.readers.size() * 4);
    for (JsonNode o : old) {
      JsonNode c = full.get(key(o, o.get("step").asInt() - cfg.reads.travelBack));
      for (String col :
          List.of("requests", "get_range", "get_stream", "aborts", "bytes_read",
              "bytes_skipped_through", "rows")) {
        assertThat(o.get(col).asLong()).as(key(o, o.get("step").asInt()) + " " + col)
            .isEqualTo(c.get(col).asLong());
      }
    }
  }

  @Test
  void cachedReaderLoadsEachFilesDeletesOnce(@TempDir Path dir) throws IOException {
    // the cached reader loads a data file's deletes once per read, for all its splits: no read
    // costs more under it than under one-footer, arms without deletes read exactly alike, and
    // an arm whose files of several row groups carry DVs (MoR-v3, TailCoW-C) reads with fewer GETs:
    // uniform updates put DVs on the initial load's 2 MiB files
    BenchConfig cfg = config("mem", dir);
    cfg.arms = List.of("mor-v3", "tailcow-c", "rmow-c");
    cfg.workload.initialRows = 100_000;
    cfg.workload.keyDist = WorkloadSpec.KeyDist.UNIFORM;
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.compactEvery = 0;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.oneFooter(), BenchConfig.ReaderSpec.cached());
    List<JsonNode> recs = run(cfg, dir);
    java.util.Map<String, JsonNode> oneFooter = new java.util.HashMap<>();
    for (JsonNode r : recs) {
      if (r.has("reader") && r.get("reader").asText().equals("one-footer")) {
        oneFooter.put(r.get("arm").asText() + " " + r.get("phase").asText() + " " + r.get("step"), r);
      }
    }
    boolean fewer = false;
    for (JsonNode c : recs) {
      if (!c.has("reader") || !c.get("reader").asText().equals("cached")) {
        continue;
      }
      String at = c.get("arm").asText() + " " + c.get("phase").asText() + " " + c.get("step");
      JsonNode o = oneFooter.get(at);
      assertThat(c.get("rows").asLong()).as(at + " rows").isEqualTo(o.get("rows").asLong());
      assertThat(c.get("gets").asLong()).as(at + " GETs").isLessThanOrEqualTo(o.get("gets").asLong());
      if (c.get("arm").asText().equals("rmow-c")) {
        assertThat(c.get("gets").asLong()).as(at + " GETs").isEqualTo(o.get("gets").asLong());
        assertThat(c.get("bytes_read").asLong()).as(at).isEqualTo(o.get("bytes_read").asLong());
      }
      fewer |= !c.get("arm").asText().equals("rmow-c")
          && c.get("gets").asLong() < o.get("gets").asLong();
    }
    assertThat(fewer).as("a DV arm loads each file's DV once under the cached reader").isTrue();
  }

  @Test
  void warmupChangesNoRequestOrByte(@TempDir Path dir) throws IOException {
    // the warmup only warms the JIT: the measured run's records must match a run without one
    BenchConfig cfg = config("mem", dir);
    cfg.arms = List.of("mor-v3", "tailcow-c", "rmow-c", "rmow-c-split");
    cfg.workload.batches = 6;
    cfg.reads.every = 3;
    cfg.reads.travelBack = 3;
    Path cold = Files.createDirectories(dir.resolve("cold"));
    Path warm = Files.createDirectories(dir.resolve("warm"));
    List<JsonNode> without = run(cfg, cold);
    cfg.warmup.batches = 2;
    cfg.warmup.initialRows = 10_000;
    List<JsonNode> with = run(cfg, warm);
    assertThat(with).hasSameSizeAs(without);
    for (int i = 0; i < without.size(); i++) {
      JsonNode a = without.get(i);
      JsonNode b = with.get(i);
      String at = a.get("arm").asText() + " step " + a.get("step") + " " + a.get("phase").asText();
      for (String col :
          List.of("arm", "step", "phase", "requests", "gets", "puts", "appends", "heads",
              "aborts", "bytes_read", "bytes_written", "rows", "stored_bytes", "live_bytes")) {
        assertThat(String.valueOf(b.get(col))).as(at + " " + col).isEqualTo(String.valueOf(a.get(col)));
      }
    }
  }

  @Test
  void windowReadsAddRecordsAndChangeNoOther(@TempDir Path dir) throws IOException {
    // each window is a read of its own, checked against the model; the other records stay as a
    // run without windows sends them, and a configuration without windows keeps its hash
    BenchConfig cfg = config("mem", dir);
    cfg.arms = List.of("mor-v3", "rmor-c-kept", "rmow-c");
    cfg.workload.batches = 6;
    cfg.reads.every = 3;
    cfg.reads.travelBack = 3;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.oneFooter(), BenchConfig.ReaderSpec.cached());
    ObjectMapper json = new ObjectMapper();
    String plain = json.writeValueAsString(json.valueToTree(cfg));
    assertThat(plain).doesNotContain("windows");
    List<JsonNode> without = run(cfg, Files.createDirectories(dir.resolve("plain")));
    cfg.reads.windows = new BenchConfig.WindowSpec();
    cfg.reads.windows.by = List.of("id", "ts");
    cfg.reads.windows.count = 3;
    List<JsonNode> with = run(cfg, Files.createDirectories(dir.resolve("windows")));

    List<JsonNode> windows =
        with.stream().filter(r -> r.get("phase").asText().equals("read_window")).toList();
    // probes after the load and batches 3 and 6: 3 arms x 2 readers x 2 columns x 3 windows
    assertThat(windows).hasSize(3 * 3 * 2 * 2 * 3);
    assertThat(windows.stream().map(r -> r.get("window_at").asDouble()).distinct())
        .containsExactlyInAnyOrder(1 / 6.0, 0.5, 5 / 6.0);
    assertThat(windows).allMatch(r -> r.get("rows").asLong() > 0);
    List<JsonNode> rest =
        with.stream().filter(r -> !r.get("phase").asText().equals("read_window")).toList();
    assertThat(rest).hasSameSizeAs(without);
    for (int i = 0; i < without.size(); i++) {
      JsonNode a = without.get(i);
      JsonNode b = rest.get(i);
      String at = a.get("arm").asText() + " step " + a.get("step") + " " + a.get("phase").asText();
      for (String col :
          List.of("arm", "step", "phase", "reader", "requests", "bytes_read", "bytes_written",
              "rows", "stored_bytes", "live_bytes", "held_bytes")) {
        assertThat(String.valueOf(b.get(col))).as(at + " " + col).isEqualTo(String.valueOf(a.get(col)));
      }
    }
  }

  private static String key(JsonNode r, int step) {
    return r.get("arm").asText() + "/" + r.get("reader").asText() + "/" + step;
  }

  @Test
  void localStoreUniformKeys(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config("local", dir.resolve("store"));
    cfg.workload.keyDist = WorkloadSpec.KeyDist.UNIFORM;
    // every arm finds its rows by scanning, checked against the index, footers from storage
    cfg.layout.locate = "scan";
    cfg.layout.writerFooters = "open";
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
