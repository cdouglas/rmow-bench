package org.rmow.bench.fuzz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import org.rmow.bench.arms.LayoutSpec;
import org.rmow.bench.bench.BenchConfig;
import org.rmow.bench.bench.Runner;
import org.rmow.bench.workload.WorkloadSpec;

/**
 * Random small configurations through the runner, each checked as a run checks itself and more.
 * A seed draws a whole configuration: the workload (mix, key distribution, regions, lateness),
 * every layout knob, a subset of the arms, and readers of random flags. The run is the real
 * runner, payload checks on, so every read of every arm under every reader must match the
 * reference model (the runner throws otherwise). Then:
 *
 * <ul>
 *   <li><b>footprint</b> (local store): each arm's objects and bytes on disk are what its last
 *       {@code state} record says it stores (arms never delete, so every object they wrote is
 *       counted);
 *   <li><b>state</b>: every {@code state} record is consistent (live and reclaimable bytes within
 *       what is stored, live objects within objects);
 *   <li><b>twin</b> (a share of seeds): the same configuration again, on the other store (in-memory
 *       or local), issues the same requests and bytes, returns the same rows, holds the same
 *       memory (readers on one thread) and ends in the same state, record for record;
 *   <li><b>versions</b> (a share of seeds): every version of every appendable object is dumped and
 *       checked against the model ({@link org.rmow.bench.arms.VersionDump}).
 * </ul>
 *
 * A share of seeds also log every request and every change of what the phases hold, as the cloud
 * runs do ({@code --log-requests}, {@code --log-held}).
 *
 * <pre>
 *   bin/fuzz.sh --out DIR [--from S] [--count N] [--minutes M | --until EPOCH_S]
 *       [--worker K --workers W] [--resume]
 *   bin/fuzz.sh --seed S [--out DIR]       # one seed, its outcome on stdout
 *   bin/fuzz.sh --print S                  # the configuration seed S draws
 *   ... --profile heavy                    # draws that favour nearly dead row groups
 * </pre>
 *
 * Worker K of W runs seeds S + K, S + K + W, ... Each outcome is a line of {@code
 * DIR/summary-K.jsonl}; a failure also writes {@code DIR/fail-<seed>/}: the configuration (runnable
 * with {@code bin/run.sh}), the error, and the run's records. A trial that runs past {@code
 * --timeout-minutes} (default 20) is recorded with every thread's stack and the JVM halts (exit 3),
 * as it does after an {@link Error} (exit 4); {@code bin/fuzz.sh} restarts the worker with {@code
 * --resume}, which skips the seeds its summary holds.
 */
public final class Fuzz {

  /** Every arm the runner makes. */
  static final List<String> ARMS =
      List.of(
          "mor-v2-file",
          "mor-v2-partition",
          "mor-v3",
          "cow",
          "rmow-a",
          "rmow-b",
          "rmow-c",
          "rmow-a-split",
          "rmow-b-split",
          "rmow-c-split",
          "tailcow-b",
          "tailcow-c",
          "rmor-c",
          "rmor-c-footer",
          "rmor-c-kept",
          "rmor-c-footer-kept",
          "rmor-c-clean",
          "rmor-density");

  /** The arms the results and the cloud runs use, drawn more often. */
  static final Set<String> MAIN =
      Set.of("mor-v3", "rmor-c", "rmor-c-kept", "rmor-c-footer-kept", "rmow-c", "rmor-density");

  /** The arms that rewrite every region they change, so need {@code rmorRewriteShare} 0. */
  static final Set<String> SHARE_ZERO = Set.of("rmor-c-footer", "rmor-c-kept", "rmor-c-footer-kept");

  /**
   * What the draws favour. {@code mixed}: everything, a share of each. {@code heavy}: every seed
   * changes rows in heavy statements (a share of 0.8 to 1 of a region's rows), with regions about
   * a row group or a few and a reader that skips dead row groups, so row groups go dead but for a
   * few rows, where skipping them is easiest to get wrong.
   */
  enum Profile {
    MIXED,
    HEAVY
  }

  /**
   * A seed's draw: the configuration, the checks beyond the run's own, and whether the run logs
   * every request and every change of what it holds, as the cloud runs do.
   */
  record Trial(long seed, BenchConfig cfg, boolean twin, boolean dump, boolean logs) {}

  private static final ObjectMapper JSON = new ObjectMapper();

  private Fuzz() {}

  // ------------------------------------------------------------------------------------------
  // the draw
  // ------------------------------------------------------------------------------------------

  static Trial draw(long seed) {
    return draw(seed, Profile.MIXED);
  }

  static Trial draw(long seed, Profile profile) {
    boolean heavy = profile == Profile.HEAVY;
    // not seed times SplittableRandom's own gamma: consecutive seeds would draw shifted copies
    SplittableRandom r = new SplittableRandom(seed ^ 0x632BE59BD9B4E019L);
    BenchConfig cfg = new BenchConfig();
    cfg.label = "fuzz-" + seed;
    cfg.verifyRows = true;
    cfg.store.type = chance(r, 0.25) ? "local" : "mem";
    cfg.store.root = cfg.store.type.equals("mem") ? "mem://fuzz" : "store";

    WorkloadSpec w = cfg.workload;
    w.seed = r.nextLong(1, 1L << 40);
    w.initialRows = logUniform(r, 2_000, 60_000);
    // some runs are long and of small batches, so tails go through many merges and seals
    boolean longRun = chance(r, 0.2);
    w.batches = longRun ? r.nextInt(17, 31) : r.nextInt(2, 17);
    int ops = (int) (longRun ? logUniform(r, 20, 300) : logUniform(r, 20, 3_000));
    int ins;
    int upd;
    int del;
    switch (heavy ? 2 * r.nextInt(2) : r.nextInt(3)) {
      case 0 -> { // updates and deletes
        ins = 0;
        del = (int) (ops * r.nextDouble(0, 0.4));
        upd = ops - del;
      }
      case 1 -> { // inserts
        ins = ops;
        upd = 0;
        del = 0;
      }
      default -> {
        double a = r.nextDouble();
        ins = (int) Math.round(ops * a);
        upd = Math.min(ops - ins, (int) Math.round(ops * r.nextDouble() * (1 - a)));
        del = ops - ins - upd;
      }
    }
    // rows changed per batch within a quarter of the table, deletes within half of it overall
    long cap = w.initialRows / 4;
    if (upd + del > cap) {
      double k = (double) cap / (upd + del);
      upd = (int) (upd * k);
      del = (int) (del * k);
    }
    del = (int) Math.min(del, w.initialRows / (2L * w.batches));
    if (ins + upd + del == 0 || heavy && upd + del == 0) {
      upd = (int) Math.max(1, Math.min(ins / 2, cap));
      ins -= Math.min(ins, upd);
    }
    ops = ins + upd + del;
    w.opsPerBatch = ops;
    w.insertFrac = (double) ins / ops;
    w.updateFrac = (double) upd / ops;
    w.deleteFrac = (double) del / ops;
    w.keyDist = WorkloadSpec.KeyDist.values()[r.nextInt(WorkloadSpec.KeyDist.values().length)];
    w.recencyMeanBatches = r.nextDouble(0.1, 5);
    w.tsStepMicros = pick(r, 1L, 10L, 1_000L);
    w.initialJitterRows = chance(r, 0.5) ? 0 : r.nextDouble(0, 2_000);
    w.lateMedianBatches = logUniform(r, 0.01, 1.0);
    w.lateP99Batches = w.lateMedianBatches * logUniform(r, 1.0, 100.0);
    w.regionRows = logUniform(r, 50, 50_000);
    w.regionSkew = r.nextDouble(0, 2);
    // heavy statements nearly empty a region, so row groups and files go (nearly) dead whole
    double share = r.nextDouble();
    w.updateShare =
        heavy || share >= 0.85
            ? r.nextDouble(0.8, 1)
            : share < 0.45 ? 0 : logUniform(r, 0.001, 0.5);

    LayoutSpec l = cfg.layout;
    l.codec = chance(r, 0.7) ? "zstd" : pick(r, "snappy", "gzip", "uncompressed");
    l.rowGroupBytes = logUniform(r, 16 << 10, 2 << 20);
    l.targetFileBytes = Math.max(64 << 10, l.rowGroupBytes * logUniform(r, 1, 32));
    l.sortWrites = chance(r, 0.85);
    l.writerFooters = chance(r, 0.8) ? "transaction" : "open";
    // a seal size under the row-group size makes sealed tail units small enough to pack
    l.tailSealBytes =
        chance(r, 0.3)
            ? (long) (l.rowGroupBytes * r.nextDouble(0.05, 0.6))
            : logUniform(r, 16 << 10, 4 << 20);
    l.tierFanout = r.nextInt(2, 9);
    l.maxRowGroupsPerFile = (int) logUniform(r, 2, 128);
    l.footerOrder = pick(r, "logical", "offset");
    l.footerPrefetchBytes = prefetch(r);
    l.locate = pick(r, "oracle", "scan", "keys");
    l.updateRead = pick(r, null, "id", "row", "scan");
    double every = r.nextDouble();
    l.compactEvery = every < 0.25 ? 0 : every < 0.65 ? r.nextInt(1, 4) : r.nextInt(4, 9);
    l.compactDeleteRatio = r.nextDouble(0.02, 0.9);
    l.compactSmallFileFraction = r.nextDouble(0.05, 1.0);
    l.compactGarbageRatio = r.nextDouble(0.05, 1.5);
    double small = r.nextDouble();
    l.compactSmallRowGroups = small < 0.3 ? 0 : small < 0.65 ? 2 : r.nextInt(3, 11);
    l.rmorRewriteShare = chance(r, 0.6) ? 0 : r.nextDouble(0.01, 1.5);
    l.writerSkipsDeadRowGroups = r.nextBoolean();
    if (chance(r, 0.5)) {
      LayoutSpec.Clean c = new LayoutSpec.Clean();
      c.rewriteShare = r.nextDouble(0.01, 1.2);
      c.low = r.nextDouble(0.05, 1);
      c.age = r.nextInt(1, 6);
      c.budget = r.nextDouble(0.05, 2);
      c.drain = r.nextDouble(0.05, 1);
      c.examine = r.nextDouble(0.01, 0.8);
      l.clean = c;
    }
    if (chance(r, 0.5)) {
      LayoutSpec.Defer d = new LayoutSpec.Defer();
      d.share = logUniform(r, 0.005, 1.2);
      d.deltas = pick(r, "region", "write");
      l.defer = d;
    }
    // the cut (2026-11-10), drawn from a stream of its own so every other draw of a seed stays as
    // it was: strides far smaller than Iceberg's page limit, since these row groups hold 270 to
    // 35,000 rows, but 2 to 64 of them a row group (128 MiB of the bench's rows holds 215 and
    // 16 MiB 27). Strides of a few rows cut row groups into thousands, whose footers every read
    // decodes: seeds 291 and 757 of round cut-r1, at 7 and 14 rows, ran past 20 minutes.
    SplittableRandom rc = new SplittableRandom(seed ^ 0x2545F4914F6CDD1DL);
    if (chance(rc, 0.5)) {
      LayoutSpec.CutSpec c = new LayoutSpec.CutSpec();
      c.minStrides = rc.nextInt(1, 6);
      long rgRows = Math.max(100, l.rowGroupBytes / 60);
      c.strideRows = (int) Math.max(5, rgRows / logUniform(rc, 2.0, 64.0));
      l.cut = c;
    }
    // planning's whole-file threshold (2026-11-10), from a stream of its own: Iceberg fetches files
    // of at most 1 MiB whole, and these tables (2,000 to 60,000 rows) seldom make a file larger, so
    // half the seeds plan files above 8 to 256 KiB row group by row group, reaching the paths that
    // work so (dead row groups skipped by readers and writers, the sorted read's runs, the locate by
    // key). Iceberg's reader still fetches such a file whole, which changes requests, not rows.
    SplittableRandom re = new SplittableRandom(seed ^ 0x9E3779B97F4A7C15L);
    if (chance(re, 0.5)) {
      l.eagerBytes = logUniform(re, 8 << 10, 256 << 10);
    }
    // A heavy statement empties most of a region only if the batch changes that many rows, and
    // leaves a row group dead but for a few rows only if its region spans the row group: regions
    // of about a row group or a few, and batches that change at least a region's rows.
    if (w.updateShare >= 0.8 && upd + del > 0) {
      long rgRows = Math.max(100, l.rowGroupBytes / 60);
      w.regionRows =
          Math.max(50, Math.min(w.initialRows / 4, (long) (rgRows * r.nextDouble(0.5, 3))));
      long want = Math.min(w.initialRows / 4, w.regionRows);
      if (upd + del < want) {
        double k = (double) want / (upd + del);
        upd = (int) (upd * k);
        del = (int) Math.min(del * k, w.initialRows / (2L * w.batches));
        ops = ins + upd + del;
        w.opsPerBatch = ops;
        w.insertFrac = (double) ins / ops;
        w.updateFrac = (double) upd / ops;
        w.deleteFrac = (double) del / ops;
      }
    }

    List<String> arms = new ArrayList<>();
    for (String a : ARMS) {
      if (chance(r, MAIN.contains(a) ? 0.55 : 0.3)) {
        arms.add(a);
      }
    }
    if (l.rmorRewriteShare != 0) {
      arms.removeAll(SHARE_ZERO);
    }
    // a sorted read needs sorted writes, an initial load in event-time order, and no split files
    boolean sorted = l.sortWrites && w.initialJitterRows == 0 && chance(r, 0.5);
    if (sorted) {
      arms.removeIf(a -> a.endsWith("-split"));
    }
    if (arms.isEmpty()) {
      arms.add(pick(r, "mor-v3", "rmor-c", "rmow-c", "rmor-density"));
    }
    cfg.arms = arms;

    BenchConfig.ReadSpec reads = cfg.reads;
    reads.every = r.nextInt(1, 5);
    reads.recentFraction = logUniform(r, 0.001, 0.5);
    reads.travelBack = r.nextInt(1, 9);
    if (chance(r, 0.6)) {
      BenchConfig.WindowSpec ws = new BenchConfig.WindowSpec();
      ws.by = pick(r, List.of("id"), List.of("ts"), List.of("id", "ts"));
      ws.count = r.nextInt(1, 7);
      ws.fraction = r.nextDouble(0.001, 1.0 / ws.count);
      reads.windows = ws;
    }
    List<BenchConfig.ReaderSpec> readers = new ArrayList<>();
    int n = r.nextInt(1, 5);
    for (int i = 0; i < n; i++) {
      BenchConfig.ReaderSpec s;
      if (chance(r, 0.4)) {
        s =
            switch (r.nextInt(4)) {
              case 0 -> BenchConfig.ReaderSpec.stock();
              case 1 -> BenchConfig.ReaderSpec.oneFooter();
              case 2 -> BenchConfig.ReaderSpec.cached();
              default -> BenchConfig.ReaderSpec.skipping();
            };
        s.name = s.name + "-" + i;
      } else {
        s = new BenchConfig.ReaderSpec();
        s.name = "r" + i;
        s.footerCache = chance(r, 0.7);
        s.deleteCache = chance(r, 0.5);
        s.footerPrefetchBytes = prefetch(r);
        s.skipDeadRowGroups = s.footerCache && chance(r, 0.5);
      }
      s.splitTargetBytes = chance(r, 0.5) ? s.splitTargetBytes : logUniform(r, 16 << 10, 128 << 20);
      s.parallelism = pick(r, 1, 1, 2, 4);
      readers.add(s);
    }
    if (heavy && readers.stream().noneMatch(x -> x.skipDeadRowGroups)) {
      BenchConfig.ReaderSpec s = BenchConfig.ReaderSpec.skipping();
      s.name = "skip";
      readers.add(s);
    }
    if (sorted) {
      BenchConfig.ReaderSpec s =
          readers.stream().filter(x -> x.footerCache).findFirst().orElse(null);
      if (s == null) {
        s = BenchConfig.ReaderSpec.cached();
        s.name = "sorted";
        readers.add(s);
      }
      s.sortedRead = true;
    }
    reads.readers = readers;

    if (chance(r, 0.1)) {
      // the warmup replays the mix on its own initial load, which must outlast its changes
      cfg.warmup.batches = r.nextInt(1, 3);
      cfg.warmup.initialRows =
          Math.max(
              logUniform(r, 1_000, 5_000),
              Math.max(4L * (upd + del), 2L * del * cfg.warmup.batches));
    }
    return new Trial(seed, cfg, chance(r, 0.2), chance(r, 0.15), chance(r, 0.3));
  }

  private static int prefetch(SplittableRandom r) {
    return pick(r, 0, 0, 8, 512, 64 << 10, r.nextInt(1, 200_000));
  }

  private static boolean chance(SplittableRandom r, double p) {
    return r.nextDouble() < p;
  }

  private static long logUniform(SplittableRandom r, long lo, long hi) {
    return Math.round(Math.exp(r.nextDouble(Math.log(lo), Math.log(hi))));
  }

  private static double logUniform(SplittableRandom r, double lo, double hi) {
    return lo == hi ? lo : Math.exp(r.nextDouble(Math.log(lo), Math.log(hi)));
  }

  @SafeVarargs
  private static <T> T pick(SplittableRandom r, T... values) {
    return values[r.nextInt(values.length)];
  }

  // ------------------------------------------------------------------------------------------
  // a trial
  // ------------------------------------------------------------------------------------------

  /** What a trial found: null when it passed, else its kind and message. */
  record Outcome(long seed, String kind, String message, double seconds) {
    boolean ok() {
      return kind == null;
    }
  }

  /** A check that failed after the run completed. */
  static final class CheckFailed extends RuntimeException {
    final String kind;

    CheckFailed(String kind, String message) {
      super(message);
      this.kind = kind;
    }
  }

  private static volatile long trialStart = 0;
  private static volatile long trialSeed = -1;

  /** Runs one seed in {@code scratch}; a failure's records go to {@code out/fail-<seed>/}. */
  static Outcome trial(long seed, Path scratch, Path out) throws IOException {
    return trial(seed, Profile.MIXED, scratch, out);
  }

  static Outcome trial(long seed, Profile profile, Path scratch, Path out) throws IOException {
    Trial t = draw(seed, profile);
    long t0 = System.nanoTime();
    trialSeed = seed;
    trialStart = System.currentTimeMillis();
    Path dir = scratch.resolve("t" + seed);
    try {
      deleteTree(dir);
      Files.createDirectories(dir);
      try {
        Path a = runOnce(t.cfg(), t.cfg().store.type, dir.resolve("a"), t.dump(), t.logs());
        if (t.twin()) {
          String other = t.cfg().store.type.equals("mem") ? "local" : "mem";
          Path b = runOnce(t.cfg(), other, dir.resolve("b"), false, false);
          List<String> diffs = diffSteps(t.cfg(), a, b);
          if (!diffs.isEmpty()) {
            throw new CheckFailed(
                "twin", "the " + other + " twin differs:\n  " + String.join("\n  ", diffs));
          }
        }
        return new Outcome(seed, null, null, secs(t0));
      } catch (Exception e) {
        String kind = e instanceof CheckFailed c ? c.kind : "run";
        Outcome o = new Outcome(seed, kind, firstLine(e), secs(t0));
        recordFailure(out, t, o, e, dir);
        return o;
      }
    } finally {
      trialStart = 0;
      deleteTree(dir);
    }
  }

  /** One run of the trial's configuration on {@code store}; checks it; returns its run directory. */
  private static Path runOnce(
      BenchConfig drawn, String store, Path runDir, boolean dump, boolean logs)
      throws IOException {
    BenchConfig cfg = JSON.readValue(JSON.writeValueAsBytes(drawn), BenchConfig.class);
    cfg.store.type = store;
    cfg.store.root = store.equals("mem") ? "mem://fuzz" : "store";
    boolean local = store.equals("local");
    Path storeRoot = runDir.resolve("store");
    Files.createDirectories(runDir);
    String runId = "run";
    Runner.Options opts =
        new Runner.Options(local ? storeRoot.toString() : null, local, dump, logs, logs);
    new Runner(cfg, runDir, runId, opts).run();
    Map<String, JsonNode> last = checkStates(runDir);
    if (local) {
      checkFootprint(cfg, storeRoot.resolve(runId), last);
      deleteTree(storeRoot);
    }
    return runDir;
  }

  /** Every state record is consistent; returns each arm's last. */
  private static Map<String, JsonNode> checkStates(Path runDir) throws IOException {
    Map<String, JsonNode> last = new LinkedHashMap<>();
    for (JsonNode n : records(runDir)) {
      if (!n.get("phase").asText().equals("state")) {
        continue;
      }
      String arm = n.get("arm").asText();
      long objects = n.get("objects").asLong();
      long stored = n.get("stored_bytes").asLong();
      long live = n.get("live_bytes").asLong();
      long reclaimable = n.get("reclaimable_bytes").asLong();
      long liveObjects = n.get("live_objects").asLong();
      List<String> bad = new ArrayList<>();
      if (live < 0 || reclaimable < 0 || live + reclaimable > stored) {
        bad.add("live " + live + " + reclaimable " + reclaimable + " vs stored " + stored);
      }
      if (liveObjects < 0 || liveObjects > objects) {
        bad.add("live objects " + liveObjects + " of " + objects);
      }
      if (!bad.isEmpty()) {
        throw new CheckFailed(
            "state", arm + " at step " + n.get("step").asInt() + ": " + String.join("; ", bad));
      }
      last.put(arm, n);
    }
    return last;
  }

  /** Each arm's objects on disk are those its last state record counts. */
  private static void checkFootprint(BenchConfig cfg, Path root, Map<String, JsonNode> last)
      throws IOException {
    List<String> bad = new ArrayList<>();
    for (String arm : cfg.arms) {
      long files = 0;
      long bytes = 0;
      Path dir = root.resolve(arm);
      if (Files.exists(dir)) {
        try (var paths = Files.walk(dir)) {
          for (Path p : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
            files++;
            bytes += Files.size(p);
          }
        }
      }
      JsonNode s = last.get(arm);
      long objects = s.get("objects").asLong();
      long stored = s.get("stored_bytes").asLong();
      if (files != objects || bytes != stored) {
        bad.add(
            String.format(
                "%s: %d objects, %d bytes on disk; its state says %d, %d",
                arm, files, bytes, objects, stored));
      }
    }
    if (!bad.isEmpty()) {
      throw new CheckFailed("footprint", String.join("; ", bad));
    }
  }

  /** Fields every record of a phase must repeat exactly, whatever the store. */
  private static final List<String> IO =
      List.of(
          "requests", "gets", "get_range", "get_stream", "heads", "puts", "appends",
          "append_splits", "deletes", "aborts", "bytes_read", "bytes_skipped_through",
          "bytes_written", "rows");

  private static final List<String> STATE =
      List.of(
          "objects", "stored_bytes", "live_bytes", "reclaimable_bytes", "row_groups", "data_files",
          "delete_files", "footer_bytes", "row_groups_out_of_order", "live_objects");

  /**
   * Differences between two runs of one configuration, record by record: requests, bytes and rows
   * of every phase, what it held (but under a reader on several threads, whose peaks depend on
   * timing), and every state.
   */
  static List<String> diffSteps(BenchConfig cfg, Path a, Path b) throws IOException {
    Set<String> parallel = new HashSet<>();
    for (BenchConfig.ReaderSpec s : cfg.reads.readers) {
      if (s.parallelism > 1) {
        parallel.add(s.name);
      }
    }
    Map<String, List<JsonNode>> ra = keyed(records(a));
    Map<String, List<JsonNode>> rb = keyed(records(b));
    List<String> diffs = new ArrayList<>();
    Set<String> keys = new java.util.TreeSet<>(ra.keySet());
    keys.addAll(rb.keySet());
    for (String k : keys) {
      List<JsonNode> la = ra.getOrDefault(k, List.of());
      List<JsonNode> lb = rb.getOrDefault(k, List.of());
      if (la.size() != lb.size()) {
        diffs.add(k + ": " + la.size() + " records against " + lb.size());
        continue;
      }
      for (int i = 0; i < la.size(); i++) {
        JsonNode x = la.get(i);
        JsonNode y = lb.get(i);
        boolean state = x.get("phase").asText().equals("state");
        List<String> fields = new ArrayList<>(state ? STATE : IO);
        String reader = x.path("reader").asText("");
        if (!state && !parallel.contains(reader)) {
          x.fieldNames().forEachRemaining(f -> {
            if (f.startsWith("held_")) {
              fields.add(f);
            }
          });
        }
        for (String f : fields) {
          if (!x.path(f).equals(y.path(f))) {
            diffs.add(k + "#" + i + " " + f + ": " + x.path(f) + " against " + y.path(f));
          }
        }
      }
      if (diffs.size() >= 20) {
        break;
      }
    }
    return diffs;
  }

  private static Map<String, List<JsonNode>> keyed(List<JsonNode> recs) {
    Map<String, List<JsonNode>> m = new TreeMap<>();
    for (JsonNode n : recs) {
      String k =
          String.join(
              "|",
              n.get("arm").asText(),
              n.get("step").asText(),
              n.get("phase").asText(),
              n.path("reader").asText(""),
              n.path("window_by").asText(""),
              n.path("window_at").asText(""));
      m.computeIfAbsent(k, x -> new ArrayList<>()).add(n);
    }
    return m;
  }

  private static List<JsonNode> records(Path runDir) throws IOException {
    List<JsonNode> out = new ArrayList<>();
    for (String line : Files.readAllLines(runDir.resolve("steps.jsonl"))) {
      out.add(JSON.readTree(line));
    }
    return out;
  }

  private static void recordFailure(Path out, Trial t, Outcome o, Throwable e, Path dir)
      throws IOException {
    Path f = out.resolve("fail-" + t.seed());
    Files.createDirectories(f);
    BenchConfig cfg = JSON.readValue(JSON.writeValueAsBytes(t.cfg()), BenchConfig.class);
    ObjectNode c = JSON.valueToTree(cfg);
    JSON.writerWithDefaultPrettyPrinter().writeValue(f.resolve("config.json").toFile(), c);
    StringWriter sw = new StringWriter();
    sw.write(
        String.format(
            "seed %d, %s, twin %s, dump %s, logs %s%n",
            t.seed(), o.kind(), t.twin(), t.dump(), t.logs()));
    if (e != null) {
      e.printStackTrace(new PrintWriter(sw));
    }
    Files.writeString(f.resolve("error.txt"), sw.toString());
    if (dir != null) {
      for (String run : List.of("a", "b")) {
        Path steps = dir.resolve(run).resolve("steps.jsonl");
        if (Files.exists(steps)) {
          Files.copy(steps, f.resolve("steps-" + run + ".jsonl"));
        }
      }
    }
  }

  private static String firstLine(Throwable e) {
    String m = e.getClass().getSimpleName() + ": " + e.getMessage();
    int nl = m.indexOf('\n');
    return nl < 0 || e instanceof CheckFailed ? m : m.substring(0, nl);
  }

  private static double secs(long t0) {
    return Math.round((System.nanoTime() - t0) / 1e7) / 100.0;
  }

  private static void deleteTree(Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (var paths = Files.walk(dir)) {
      for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(p);
      }
    }
  }

  // ------------------------------------------------------------------------------------------
  // the worker
  // ------------------------------------------------------------------------------------------

  public static void main(String[] args) throws Exception {
    Path out = Path.of("results/fuzz/adhoc");
    long from = 1;
    long count = Long.MAX_VALUE;
    double minutes = Double.MAX_VALUE;
    long until = Long.MAX_VALUE; // epoch ms: --minutes from a first start, kept across restarts
    double timeoutMinutes = 20;
    int worker = 0;
    int workers = 1;
    boolean resume = false;
    Long one = null;
    Long print = null;
    Profile profile = Profile.MIXED;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--out" -> out = Path.of(args[++i]);
        case "--from" -> from = Long.parseLong(args[++i]);
        case "--count" -> count = Long.parseLong(args[++i]);
        case "--minutes" -> minutes = Double.parseDouble(args[++i]);
        case "--until" -> until = Long.parseLong(args[++i]) * 1000;
        case "--timeout-minutes" -> timeoutMinutes = Double.parseDouble(args[++i]);
        case "--worker" -> worker = Integer.parseInt(args[++i]);
        case "--workers" -> workers = Integer.parseInt(args[++i]);
        case "--resume" -> resume = true;
        case "--seed" -> one = Long.parseLong(args[++i]);
        case "--print" -> print = Long.parseLong(args[++i]);
        case "--profile" -> profile = Profile.valueOf(args[++i].toUpperCase(java.util.Locale.ROOT));
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }
    if (print != null) {
      Trial t = draw(print, profile);
      System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(t.cfg()));
      System.out.println("twin " + t.twin() + ", dump " + t.dump() + ", logs " + t.logs());
      return;
    }
    Files.createDirectories(out);
    workerId = worker;
    Path scratch = out.resolve("scratch-" + worker);
    Fuzz.profile = profile;
    watchdog(out, (long) (timeoutMinutes * 60_000));
    if (one != null) {
      Outcome o = trial(one, profile, scratch, out);
      System.out.println(line(o, draw(one, profile)));
      System.exit(o.ok() ? 0 : 1);
    }
    Path summary = out.resolve("summary-" + worker + ".jsonl");
    Set<Long> done = new HashSet<>();
    if (resume && Files.exists(summary)) {
      for (String l : Files.readAllLines(summary)) {
        done.add(JSON.readTree(l).get("seed").asLong());
      }
    }
    long deadline =
        Math.min(until, System.currentTimeMillis() + (long) Math.min(minutes * 60_000, 1e15));
    long ran = 0;
    for (long k = 0; k < count && System.currentTimeMillis() < deadline; k++) {
      long seed = from + worker + k * workers;
      if (seed - from >= count) {
        break;
      }
      if (done.contains(seed)) {
        continue;
      }
      Outcome o;
      try {
        o = trial(seed, profile, scratch, out);
      } catch (Error e) {
        Outcome err = new Outcome(seed, "error", firstLine(e), 0);
        recordFailure(out, draw(seed, profile), err, e, null);
        append(summary, line(err, draw(seed, profile)));
        System.err.println("seed " + seed + ": " + firstLine(e) + "; the worker restarts");
        Runtime.getRuntime().halt(4);
        return;
      }
      append(summary, line(o, draw(seed, profile)));
      ran++;
      if (!o.ok()) {
        System.err.println("seed " + seed + " FAILED (" + o.kind() + "): " + o.message());
      } else if (ran % 10 == 0) {
        System.err.println("worker " + worker + ": " + ran + " seeds, last " + seed);
      }
    }
    deleteTree(scratch);
  }

  private static String line(Outcome o, Trial t) throws IOException {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("seed", o.seed());
    m.put("ok", o.ok());
    m.put("kind", o.kind());
    m.put("message", o.message());
    m.put("seconds", o.seconds());
    BenchConfig c = t.cfg();
    m.put("profile", profile.name().toLowerCase(java.util.Locale.ROOT));
    m.put("store", c.store.type);
    m.put("twin", t.twin());
    m.put("dump", t.dump());
    m.put("logs", t.logs());
    m.put("arms", c.arms);
    m.put("readers", c.reads.readers.size());
    m.put("initial_rows", c.workload.initialRows);
    m.put("batches", c.workload.batches);
    m.put("ops", c.workload.opsPerBatch);
    m.put("insert_frac", c.workload.insertFrac);
    m.put("rg_bytes", c.layout.rowGroupBytes);
    m.put("compact_every", c.layout.compactEvery);
    m.put("cut_stride_rows", c.layout.cut == null ? 0 : c.layout.cut.strideRows);
    return JSON.writeValueAsString(m);
  }

  private static synchronized void append(Path file, String line) throws IOException {
    Files.writeString(
        file,
        line + "\n",
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  /**
   * A trial past its time is recorded, with every thread's stack, and the JVM halts so the driver
   * restarts the worker past it.
   */
  private static void watchdog(Path out, long timeoutMs) {
    Thread w =
        new Thread(
            () -> {
              while (true) {
                try {
                  Thread.sleep(5_000);
                } catch (InterruptedException e) {
                  return;
                }
                long start = trialStart;
                long seed = trialSeed;
                if (start != 0 && System.currentTimeMillis() - start > timeoutMs) {
                  try {
                    Trial t = draw(seed, profile);
                    Outcome o =
                        new Outcome(seed, "timeout", "over " + timeoutMs / 60_000 + " minutes", 0);
                    recordFailure(out, t, o, null, null);
                    StringBuilder sb = new StringBuilder();
                    for (ThreadInfo ti :
                        ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
                      sb.append(ti);
                    }
                    Files.writeString(out.resolve("fail-" + seed).resolve("threads.txt"), sb);
                    append(out.resolve("summary-" + workerId + ".jsonl"), line(o, t));
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                  System.err.println("seed " + seed + " timed out; the worker restarts");
                  Runtime.getRuntime().halt(3);
                }
              }
            },
            "fuzz-watchdog");
    w.setDaemon(true);
    w.start();
  }

  /** This worker's number, for the watchdog's summary line. */
  private static volatile int workerId = 0;

  /** This worker's profile, for its summary lines. */
  private static volatile Profile profile = Profile.MIXED;
}
