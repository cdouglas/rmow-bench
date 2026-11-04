package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * RMoR-C ({@code rmor-c}): TailCoW-C whose batches rewrite the regions they change, a DV hiding
 * each old row group, and the reader that skips row groups their deletes cover whole. The runner
 * checks every read against the reference model, so each run here is also a correctness check.
 */
class RmorTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static BenchConfig config(double rewriteShare) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://test";
    cfg.workload.initialRows = 30_000;
    cfg.workload.batches = 8;
    cfg.workload.opsPerBatch = 1_000;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 2 << 20;
    cfg.layout.tailSealBytes = 128 << 10;
    cfg.layout.tierFanout = 3;
    cfg.layout.locate = "scan";
    cfg.layout.compactEvery = 4;
    cfg.layout.rmorRewriteShare = rewriteShare;
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

  private static String key(JsonNode r) {
    return r.get("step") + " " + r.get("phase").asText() + " "
        + (r.hasNonNull("reader") ? r.get("reader").asText() : "");
  }

  private static Map<String, JsonNode> of(List<JsonNode> steps, String arm) {
    Map<String, JsonNode> m = new HashMap<>();
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && !r.get("phase").asText().equals("state")) {
        m.put(key(r), r);
      }
    }
    return m;
  }

  private static long sum(List<JsonNode> steps, String arm, String phase, String reader, String f) {
    long n = 0;
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && r.get("phase").asText().equals(phase)
          && (reader == null || reader.equals(r.path("reader").asText()))) {
        n += r.get(f).asLong();
      }
    }
    return n;
  }

  /** Above 1 no region is rewritten, and RMoR-C is TailCoW-C, request for request. */
  @Test
  void aboveOneItIsTailCow(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config(2);
    cfg.arms = List.of("tailcow-c", "rmor-c");
    List<JsonNode> steps = run(cfg, dir);
    Map<String, JsonNode> tailcow = of(steps, "tailcow-c");
    Map<String, JsonNode> rmor = of(steps, "rmor-c");
    assertThat(rmor.keySet()).isEqualTo(tailcow.keySet());
    for (String k : tailcow.keySet()) {
      for (String f :
          List.of("gets", "get_range", "get_stream", "heads", "puts", "appends", "deletes")) {
        assertThat(rmor.get(k).get(f).asLong()).as(k + " " + f)
            .isEqualTo(tailcow.get(k).get(f).asLong());
      }
      // data files hold no paths; DV files name their data files, under each arm's own root
      if (k.endsWith(" write ")) {
        assertThat(rmor.get(k).get("bytes_written").asLong()).as(k)
            .isEqualTo(tailcow.get(k).get("bytes_written").asLong());
      }
    }
  }

  /**
   * At 0 every region a batch changes is rewritten: more writes than TailCoW-C's, and DVs that
   * cover old row groups whole, which the skipping reader does not fetch.
   */
  @Test
  void atZeroEveryChangedRegionIsRewritten(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config(0);
    cfg.arms = List.of("mor-v2-file", "mor-v3", "tailcow-c", "rmor-c");
    List<JsonNode> steps = run(cfg, dir);
    long rmorPut = sum(steps, "rmor-c", "write", null, "bytes_written");
    long tailcowPut = sum(steps, "tailcow-c", "write", null, "bytes_written");
    assertThat(rmorPut).as("regions rewritten").isGreaterThan(tailcowPut);
    assertThat(sum(steps, "rmor-c", "merge-read", null, "gets"))
        .as("regions read back")
        .isGreaterThan(sum(steps, "tailcow-c", "merge-read", null, "gets"));

    for (String arm : cfg.arms) {
      for (String phase : List.of("read_full", "read_recent", "read_old")) {
        long cached = sum(steps, arm, phase, "cached", "bytes_read");
        long skipping = sum(steps, arm, phase, "cached-skip", "bytes_read");
        assertThat(skipping).as(arm + " " + phase + " skipping reads no more").isLessThanOrEqualTo(cached);
      }
    }
    assertThat(sum(steps, "rmor-c", "read_full", "cached-skip", "bytes_read"))
        .as("RMoR's dead row groups go unread")
        .isLessThan(sum(steps, "rmor-c", "read_full", "cached", "bytes_read"));
  }

  /**
   * A writer that skips the row groups its deletes cover whole reads less to locate and merge
   * RMoR's rows, and writes the same.
   */
  @Test
  void aWriterThatSkipsDeadRowGroupsReadsLess(@TempDir Path dir) throws IOException {
    long[] read = new long[2];
    long[] written = new long[2];
    for (int i = 0; i < 2; i++) {
      BenchConfig cfg = config(0);
      cfg.arms = List.of("rmor-c");
      cfg.reads.readers = List.of(BenchConfig.ReaderSpec.skipping());
      cfg.layout.writerSkipsDeadRowGroups = i == 1;
      Path d = dir.resolve("skip-" + i);
      Files.createDirectories(d);
      List<JsonNode> steps = run(cfg, d);
      read[i] = sum(steps, "rmor-c", "batch", null, "bytes_read");
      written[i] = sum(steps, "rmor-c", "batch", null, "bytes_written");
    }
    assertThat(read[1]).as("batches' reads, skipping").isLessThan(read[0]);
    assertThat(written[1]).as("batches' writes").isEqualTo(written[0]);
  }

  /**
   * RMoR-C with footers: RMoR-C's rewrites at θ = 0, each old row group hidden by a footer
   * appended to its file instead of a DV. The batches write the same data files, append footers
   * where RMoR-C writes DV files, and leave no reader a dead row group to fetch or skip.
   */
  @Test
  void footersHideTheOldRowGroups(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config(0);
    cfg.layout.compactEvery = 0; // compaction triggers on DVs' delete ratios, which differ
    cfg.arms = List.of("rmor-c", "rmor-c-footer");
    cfg.reads.readers =
        List.of(
            BenchConfig.ReaderSpec.cached(),
            BenchConfig.ReaderSpec.skipping(),
            BenchConfig.ReaderSpec.stock());
    List<JsonNode> steps = run(cfg, dir);
    Map<String, JsonNode> dv = of(steps, "rmor-c");
    Map<String, JsonNode> footer = of(steps, "rmor-c-footer");
    for (String k : dv.keySet()) {
      if (k.endsWith(" write ")) {
        for (String f : List.of("puts", "bytes_written")) {
          assertThat(footer.get(k).get(f).asLong()).as(k + " " + f)
              .isEqualTo(dv.get(k).get(f).asLong());
        }
      }
    }
    assertThat(sum(steps, "rmor-c", "delete-write", null, "puts")).as("DV files").isPositive();
    assertThat(sum(steps, "rmor-c-footer", "delete-write", null, "puts")).as("DV files").isZero();
    assertThat(sum(steps, "rmor-c-footer", "delete-write", null, "appends"))
        .as("footers appended")
        .isPositive();
    for (String phase : List.of("read_full", "read_recent", "read_old")) {
      assertThat(sum(steps, "rmor-c-footer", phase, "cached-skip", "bytes_read"))
          .as(phase + ": nothing to skip")
          .isEqualTo(sum(steps, "rmor-c-footer", phase, "cached", "bytes_read"));
    }
    assertThat(sum(steps, "rmor-c-footer", "read_full", "stock", "gets"))
        .as("stock reads fetch no dead row group")
        .isLessThan(sum(steps, "rmor-c", "read_full", "stock", "gets"));
  }

  /**
   * With their row groups kept whole ({@code rmor-c-kept}, {@code rmor-c-footer-kept}): a batch's
   * new data file holds each rewritten row group as one row group of the same rows and the inserts
   * apart, as RMoW-C's regions are, written a row group at a time. Both write the same data files;
   * neither holds the batch's rows at once, as the mixed writer must to sort them; every read is
   * checked by the runner.
   */
  @Test
  void keptRowGroupsAreWrittenOneAtATime(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config(0);
    cfg.layout.compactEvery = 0;
    cfg.layout.tailSealBytes = 1; // every tail unit seals at once, as at the sweeps' settings
    cfg.arms = List.of("rmor-c", "rmor-c-footer", "rmor-c-kept", "rmor-c-footer-kept", "rmow-c");
    List<JsonNode> steps = run(cfg, dir);
    Map<String, JsonNode> dv = of(steps, "rmor-c-kept");
    Map<String, JsonNode> footer = of(steps, "rmor-c-footer-kept");
    for (String k : dv.keySet()) {
      if (k.endsWith(" write ")) {
        assertThat(footer.get(k).get("bytes_written").asLong()).as(k + " bytes_written")
            .isEqualTo(dv.get(k).get("bytes_written").asLong());
      }
    }
    for (String arm : List.of("rmor-c-kept", "rmor-c-footer-kept")) {
      assertThat(max(steps, arm, "batch", "held_rows"))
          .as(arm + ": rows held at once")
          .isLessThan(max(steps, "rmor-c", "batch", "held_rows"))
          .isLessThanOrEqualTo(max(steps, "rmow-c", "batch", "held_rows"));
    }
  }

  @Test
  void keptRowGroupsNeedEveryChangedRegionRewritten(@TempDir Path dir) {
    BenchConfig cfg = config(0.5);
    cfg.arms = List.of("rmor-c-kept");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rmorRewriteShare 0");
  }

  private static long max(List<JsonNode> steps, String arm, String phase, String f) {
    long m = 0;
    for (JsonNode r : steps) {
      if (r.get("arm").asText().equals(arm) && r.get("phase").asText().equals(phase)) {
        m = Math.max(m, r.path(f).asLong());
      }
    }
    return m;
  }

  /** A row group's removal moves its file's later rows up, so a DV would go stale: θ must be 0. */
  @Test
  void footersNeedEveryChangedRegionRewritten(@TempDir Path dir) {
    BenchConfig cfg = config(0.5);
    cfg.arms = List.of("rmor-c-footer");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rmorRewriteShare 0");
  }

  /** Between 0 and 1, a batch rewrites only the regions it changes enough of. */
  @Test
  void aShareRewritesFewerRegions(@TempDir Path dir) throws IOException {
    long[] written = new long[3];
    double[] shares = {0, 0.02, 2};
    for (int i = 0; i < shares.length; i++) {
      BenchConfig cfg = config(shares[i]);
      cfg.arms = List.of("rmor-c");
      cfg.reads.readers = List.of(BenchConfig.ReaderSpec.skipping());
      Path d = dir.resolve("share-" + i);
      Files.createDirectories(d);
      written[i] = sum(run(cfg, d), "rmor-c", "write", null, "bytes_written");
    }
    assertThat(written[0]).as("θ = 0 against θ = 0.02").isGreaterThan(written[1]);
    assertThat(written[1]).as("θ = 0.02 against TailCoW").isGreaterThanOrEqualTo(written[2]);
  }
}
