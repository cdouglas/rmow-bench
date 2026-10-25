package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The request log ({@code --log-requests}) and the injected store latency ({@code
 * store.latency}): the log holds every request each phase counted, and with latency injected, a
 * phase's wall time is at least what its logged requests would take on that store, which is what
 * the lab's replay computes.
 */
class RequestLogTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static BenchConfig config(Path root) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 30_000;
    cfg.workload.batches = 6;
    cfg.workload.opsPerBatch = 1_000;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.tailSealBytes = 128 << 10;
    cfg.layout.tierFanout = 3;
    cfg.layout.locate = "scan";
    cfg.layout.compactEvery = 3;
    cfg.reads.every = 3;
    cfg.reads.travelBack = 3;
    cfg.arms = List.of("mor-v3", "tailcow-c", "rmow-c", "rmow-c-split");
    return cfg;
  }

  private static List<JsonNode> lines(Path file) throws IOException {
    List<JsonNode> out = new ArrayList<>();
    for (String l : Files.readAllLines(file)) {
      out.add(JSON.readTree(l));
    }
    return out;
  }

  private static String key(JsonNode r) {
    return r.get("arm").asText() + " " + r.get("step") + " " + r.get("phase").asText() + " "
        + (r.hasNonNull("reader") ? r.get("reader").asText() : "");
  }

  private static List<JsonNode> run(BenchConfig cfg, Path dir) throws IOException {
    new Runner(cfg, dir, "t", new Runner.Options(null, false, false, true)).run();
    return lines(dir.resolve("steps.jsonl"));
  }

  @Test
  void theLogHoldsEveryCountedRequest(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config(dir);
    List<JsonNode> steps = run(cfg, dir);
    Map<String, Map<String, Long>> logged = new HashMap<>();
    Map<String, Long> bytes = new HashMap<>();
    for (JsonNode e : lines(dir.resolve("requests.jsonl"))) {
      String kind = e.get("kind").asText();
      logged.computeIfAbsent(key(e), k -> new HashMap<>()).merge(kind, 1L, Long::sum);
      if (kind.equals("get_range") || kind.equals("stream_bytes") || kind.equals("skip_through")) {
        bytes.merge(key(e), e.get("length").asLong(), Long::sum);
      }
    }
    // the outermost phases: batches (their sub-phases' requests included), compactions, probes
    for (JsonNode s : steps) {
      String phase = s.get("phase").asText();
      if (!List.of("batch", "compact", "read_full", "read_recent", "read_old").contains(phase)) {
        continue;
      }
      Map<String, Long> n = logged.getOrDefault(key(s), Map.of());
      String at = key(s);
      assertThat(n.getOrDefault("get_range", 0L)).as(at).isEqualTo(s.get("get_range").asLong());
      assertThat(n.getOrDefault("get_stream", 0L)).as(at).isEqualTo(s.get("get_stream").asLong());
      assertThat(n.getOrDefault("head", 0L)).as(at).isEqualTo(s.get("heads").asLong());
      assertThat(n.getOrDefault("put", 0L)).as(at).isEqualTo(s.get("puts").asLong());
      assertThat(n.getOrDefault("append", 0L)).as(at).isEqualTo(s.get("appends").asLong());
      assertThat(n.getOrDefault("delete", 0L)).as(at).isEqualTo(s.get("deletes").asLong());
      assertThat(bytes.getOrDefault(key(s), 0L)).as(at + " bytes").isEqualTo(s.get("bytes_read").asLong());
      if (phase.startsWith("read") && s.get("gets").asLong() > 0) {
        assertThat(n).as(at + " tasks").containsKey("task");
      }
    }
  }

  @Test
  void phasesCarryTheirStartTime(@TempDir Path dir) throws IOException {
    // a campaign's load samples are matched to phases by time (docker/campaign.py, the lab's
    // phase_load): every timed phase says when it started on the wall clock, within the run, and
    // a batch's parts lie inside the batch
    long before = System.currentTimeMillis();
    List<JsonNode> steps = run(config(dir), dir);
    long after = System.currentTimeMillis();
    Map<String, JsonNode> batches = new HashMap<>();
    for (JsonNode s : steps) {
      if (s.get("phase").asText().equals("batch")) {
        batches.put(s.get("arm").asText() + " " + s.get("step"), s);
      }
    }
    int parts = 0;
    for (JsonNode s : steps) {
      if (!s.hasNonNull("wall_ns")) {
        continue; // state records are not timed
      }
      long start = s.get("start_ms").asLong();
      double end = start + s.get("wall_ns").asLong() / 1e6;
      assertThat(start).as(key(s)).isBetween(before, after);
      assertThat(end).as(key(s)).isLessThanOrEqualTo(after + 1);
      JsonNode batch = batches.get(s.get("arm").asText() + " " + s.get("step"));
      if (List.of("locate", "merge-read", "write", "delete-write").contains(s.get("phase").asText())) {
        long b0 = batch.get("start_ms").asLong();
        assertThat(start).as(key(s)).isGreaterThanOrEqualTo(b0);
        assertThat(end).as(key(s)).isLessThanOrEqualTo(b0 + batch.get("wall_ns").asLong() / 1e6 + 1);
        parts++;
      }
    }
    assertThat(parts).isPositive();
  }

  /** Footer reads per arm, step and object in batches: a stock read aborts its 8-byte probe. */
  private static Map<String, Long> footerReads(List<JsonNode> log) {
    Map<String, Long> out = new HashMap<>();
    for (JsonNode e : log) {
      if (e.get("phase").asText().equals("batch") && e.get("kind").asText().equals("abort")) {
        out.merge(e.get("arm").asText() + " " + e.get("step") + " " + e.get("object").asText(),
            1L, Long::sum);
      }
    }
    return out;
  }

  @Test
  void writersReadEachFooterOncePerTransaction(@TempDir Path dir) throws IOException {
    // I/O parity rule 6: under `transaction` every arm's writer reads a file version's footer at
    // most once per batch and reuses it there, and nothing carries over to the next batch; under
    // `open` (the control) every open reads it again. Files of over 1 MiB, whose footers are read
    // as parquet-java reads them: probe the last 8 bytes, abort, read the footer
    Map<String, List<JsonNode>> steps = new HashMap<>();
    Map<String, Map<String, Long>> reads = new HashMap<>();
    long largestBatchFile = 0;
    for (String scope : List.of("transaction", "open")) {
      BenchConfig cfg = config(dir);
      cfg.workload.initialRows = 80_000;
      cfg.workload.opsPerBatch = 40_000;
      cfg.workload.batches = 4;
      cfg.layout.compactEvery = 0;
      cfg.layout.writerFooters = scope;
      cfg.arms = List.of("mor-v3", "tailcow-c", "rmow-c", "rmow-c-split");
      Path out = dir.resolve(scope);
      Files.createDirectories(out);
      steps.put(scope, run(cfg, out));
      List<JsonNode> log = lines(out.resolve("requests.jsonl"));
      reads.put(scope, footerReads(log));
      for (JsonNode e : log) {
        if (e.get("kind").asText().equals("put") && e.get("object").asText().contains("/data/batch-")) {
          largestBatchFile = Math.max(largestBatchFile, e.get("length").asLong());
        }
      }
    }
    assertThat(largestBatchFile).as("MoR-v3's batch files exceed 1 MiB").isGreaterThan(1L << 20);

    // at most once per batch, and the control re-reads
    assertThat(reads.get("transaction").values()).allMatch(n -> n == 1);
    assertThat(reads.get("open")).anySatisfy((k, n) -> assertThat(n).as(k).isGreaterThan(1));
    // nothing carries over: every batch of RMoW-C reads its files' footers again
    for (int step = 1; step <= 4; step++) {
      String prefix = "rmow-c " + step + " ";
      assertThat(reads.get("transaction").keySet()).as(prefix).anyMatch(k -> k.startsWith(prefix));
    }
    // MoR-v3 opens each file once per batch, so the scope changes none of its requests or bytes;
    // no arm's batch is dearer for holding a footer
    Map<String, JsonNode> open = new HashMap<>();
    for (JsonNode s : steps.get("open")) {
      if (s.get("phase").asText().equals("batch")) {
        open.put(key(s), s);
      }
    }
    int mor = 0;
    for (JsonNode s : steps.get("transaction")) {
      if (!s.get("phase").asText().equals("batch")) {
        continue;
      }
      JsonNode o = open.get(key(s));
      assertThat(s.get("requests").asLong()).as(key(s)).isLessThanOrEqualTo(o.get("requests").asLong());
      if (s.get("arm").asText().equals("mor-v3")) {
        assertThat(s.get("requests").asLong()).as(key(s)).isEqualTo(o.get("requests").asLong());
        assertThat(s.get("bytes_read").asLong()).as(key(s)).isEqualTo(o.get("bytes_read").asLong());
        mor++;
      }
    }
    assertThat(mor).isEqualTo(4);
    assertThat(reads.get("transaction").keySet()).anyMatch(k -> k.startsWith("mor-v3 "));
  }

  @Test
  void phasesCountTheirInstructions(@TempDir Path dir) throws IOException {
    // ThreadCounters: every timed phase counts its thread's user-mode instructions and cycles, a
    // read on a pool also its tasks'; the request log's split tasks carry theirs, and a pool's add
    // up to its phase's pool counts exactly. Needs perf_event_open (perf_event_paranoid <= 2)
    assumeTrue(ThreadCounters.unavailable() == null, "no counters: " + ThreadCounters.unavailable());
    BenchConfig cfg = config(dir);
    BenchConfig.ReaderSpec pooled = BenchConfig.ReaderSpec.cached();
    pooled.name = "cached-p4";
    pooled.parallelism = 4;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.cached(), pooled);
    List<JsonNode> steps = run(cfg, dir);
    Map<String, Long> taskInstructions = new HashMap<>();
    for (JsonNode e : lines(dir.resolve("requests.jsonl"))) {
      if (e.get("kind").asText().equals("task")) {
        assertThat(e.get("instructions").asLong()).as(key(e)).isPositive();
        assertThat(e.get("cycles").asLong()).as(key(e)).isPositive();
        if (e.get("length").asLong() == 0) { // ran on the pool
          taskInstructions.merge(key(e), e.get("instructions").asLong(), Long::sum);
        }
      }
    }
    int pooledReads = 0;
    for (JsonNode s : steps) {
      if (!s.hasNonNull("wall_ns")) {
        continue;
      }
      String at = key(s);
      assertThat(s.get("instructions").asLong()).as(at).isPositive();
      assertThat(s.get("cycles").asLong()).as(at).isPositive();
      boolean pool = s.hasNonNull("reader") && s.get("reader").asText().equals("cached-p4");
      if (pool && s.get("gets").asLong() > 0) {
        assertThat(s.get("pool_instructions").asLong()).as(at).isPositive()
            .isEqualTo(taskInstructions.get(at));
        pooledReads++;
      } else {
        assertThat(s.get("pool_instructions").asLong()).as(at).isZero();
      }
    }
    assertThat(pooledReads).isPositive();
  }

  @Test
  void splitFilesArePlannedPerRowGroup(@TempDir Path dir) throws IOException {
    // a split file's objects are planned as an appendable file's version is: on their own split
    // offsets, one task per live row group, except that a footer-cache reader reads an object of
    // at most 1 MiB, which Iceberg fetches whole, in one task. So under stock a full read runs one
    // task per live row group, and under one-footer no object is fetched whole twice in a read.
    // Compaction leaves small objects; the initial file (under 1 MiB, not footer-only) is skipped
    BenchConfig cfg = config(dir);
    cfg.arms = List.of("rmow-c-split");
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.oneFooter(), BenchConfig.ReaderSpec.stock());
    List<JsonNode> steps = run(cfg, dir);
    Map<String, Long> tasks = new HashMap<>();
    Map<String, Long> wholeFetches = new HashMap<>();
    for (JsonNode e : lines(dir.resolve("requests.jsonl"))) {
      if (e.get("kind").asText().equals("task")) {
        tasks.merge(key(e), 1L, Long::sum);
      } else if (e.get("kind").asText().equals("get_stream")) {
        wholeFetches.merge(key(e) + " " + e.get("object").asText(), 1L, Long::sum);
      }
    }
    Map<Integer, Long> rowGroups = new HashMap<>();
    steps.stream()
        .filter(s -> s.get("phase").asText().equals("state"))
        .forEach(s -> rowGroups.put(s.get("step").asInt(), s.get("row_groups").asLong()));
    int checked = 0;
    for (JsonNode s : steps) {
      if (s.get("phase").asText().equals("read_full") && s.get("step").asInt() > 0
          && s.get("reader").asText().equals("stock")) {
        assertThat(tasks.get(key(s))).as(key(s)).isEqualTo(rowGroups.get(s.get("step").asInt()));
        checked++;
      }
    }
    assertThat(checked).isEqualTo(2);
    assertThat(wholeFetches).isNotEmpty();
    wholeFetches.forEach(
        (k, n) -> {
          if (k.contains(" one-footer ")) {
            assertThat(n).as(k).isEqualTo(1);
          }
        });
    assertThat(rowGroups.values()).anyMatch(n -> n > 1);
  }

  /** What the logged requests of each phase take on the store, in ns (the lab's replay). */
  private static Map<String, Double> replay(List<JsonNode> log, BenchConfig.LatencySpec l) {
    double get = l.getMs * Math.exp(l.getSigma * l.getSigma / 2) * 1e6;
    double put = l.putMs * Math.exp(l.putSigma * l.putSigma / 2) * 1e6;
    double append = l.appendMs * Math.exp(l.appendSigma * l.appendSigma / 2) * 1e6;
    double read = l.readMsPerMib * 1e6 / (1 << 20);
    double write = l.writeMsPerMib * 1e6 / (1 << 20);
    Map<String, Integer> parts = new HashMap<>();
    Map<String, Double> out = new HashMap<>();
    for (JsonNode e : log) {
      String object = e.get("arm").asText() + "/" + e.get("object").asText();
      long len = e.get("length").asLong();
      int p = parts.getOrDefault(object, 0);
      double ns =
          switch (e.get("kind").asText()) {
            case "get_range" -> get + p * l.partMs * 1e6 + len * read;
            case "get_stream" -> get + p * l.partMs * 1e6;
            case "stream_bytes", "skip_through" -> len * read;
            case "head" -> get;
            case "put" -> {
              parts.remove(object);
              yield put + len * write;
            }
            case "append" -> {
              parts.merge(object, 1, Integer::sum);
              yield append + len * write;
            }
            case "delete" -> {
              parts.remove(object);
              yield put;
            }
            default -> 0;
          };
      out.merge(key(e), ns, Double::sum);
    }
    return out;
  }

  @Test
  void injectedLatencyMakesPhasesTakeTheirRequestsTime(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config(dir);
    BenchConfig.LatencySpec l = new BenchConfig.LatencySpec();
    l.profile = "test";
    l.getMs = 1;
    l.putMs = 2;
    l.appendMs = 3;
    l.partMs = 0.05;
    l.readMsPerMib = 5;
    l.writeMsPerMib = 5;
    cfg.store.latency = l;
    cfg.reads.readers = List.of(BenchConfig.ReaderSpec.cached());
    List<JsonNode> steps = run(cfg, dir);
    Map<String, Double> modelled = replay(lines(dir.resolve("requests.jsonl")), l);
    int checked = 0;
    for (JsonNode s : steps) {
      Double ns = modelled.get(key(s));
      if (ns == null || !List.of("batch", "compact", "read_full", "read_recent", "read_old")
          .contains(s.get("phase").asText())) {
        continue;
      }
      // one thread: the phase slept its requests' time, and the rest is its own work
      long wall = s.get("wall_ns").asLong();
      assertThat((double) wall).as(key(s) + " wall").isGreaterThanOrEqualTo(ns);
      checked++;
    }
    assertThat(checked).isPositive();
    assertThat(modelled.values().stream().mapToDouble(Double::doubleValue).sum()).isPositive();
  }
}
