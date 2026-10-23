package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;

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
