package org.rmow.bench.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.rmow.bench.arms.Arm;
import org.rmow.bench.arms.ArmContext;
import org.rmow.bench.arms.ArmStats;
import org.rmow.bench.arms.CowArm;
import org.rmow.bench.arms.InitialLoad;
import org.rmow.bench.arms.MorV2Arm;
import org.rmow.bench.arms.Reader;
import org.rmow.bench.arms.MorV3Arm;
import org.rmow.bench.arms.RmowArm;
import org.rmow.bench.arms.TailCowArm;
import org.rmow.bench.arms.TailPolicy;
import org.rmow.bench.arms.VersionDump;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.RelocatedFileIO;
import org.rmow.bench.io.StoreLatency;
import org.rmow.bench.io.TracingFileIO;
import org.rmow.bench.oracle.ReferenceModel;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.OpLogGenerator;

/**
 * Replays one op log through every arm in lockstep. At each step the arms run in an order
 * shuffled from the seed, so drift in a shared store (a cloud region's load, a disk's cache)
 * lands on all of them alike. Every read is checked against the reference model; a mismatch stops
 * the run.
 *
 * <p>Writes {@code steps.jsonl}: one record per arm, step and phase (see {@link Phases}), plus a
 * {@code state} record per arm and step with its storage footprint.
 */
public final class Runner {

  /**
   * How a run executes, apart from what it measures (not part of the config hash).
   *
   * @param storeRoot the root a local store's objects go under, instead of the config's; null:
   *     the config's
   * @param keepStore keep the run's objects; by default they are deleted once the run ends
   * @param dumpVersions write every version of the appendable objects to {@code versions/} in
   *     the run directory ({@link VersionDump})
   * @param logRequests write every request each phase issued to {@code requests.jsonl} in the
   *     run directory, with its split task and each task's CPU, for latency models
   */
  public record Options(
      String storeRoot, boolean keepStore, boolean dumpVersions, boolean logRequests) {
    public static final Options DEFAULT = new Options(null, false, false, false);

    /** Where the run's objects go: {@code storeRoot} moves a local store's, never a cloud's. */
    String storeRoot(BenchConfig cfg) {
      return storeRoot != null && cfg.store.type.equals("local") ? storeRoot : cfg.store.root;
    }
  }

  private record ArmRun(Arm arm, Phases phases, IoTrace trace) {}

  /** The root every run's files are named under; the store holds them under the run's prefix. */
  static final String LOGICAL_ROOT = "bench://run";

  private final BenchConfig cfg;
  private final Path runDir;
  private final String runId;
  private final Options options;
  private final ObjectMapper json = new ObjectMapper();
  private final List<Reader> readers = new ArrayList<>();
  private BufferedWriter requestsOut; // the request log, while a run with one writes steps

  public Runner(BenchConfig cfg, Path runDir, String runId) {
    this(cfg, runDir, runId, Options.DEFAULT);
  }

  public Runner(BenchConfig cfg, Path runDir, String runId, Options options) {
    this.cfg = cfg;
    this.runDir = runDir;
    this.runId = runId;
    this.options = options;
  }

  /** Runs the benchmark; returns the op log's SHA-256 (for the run manifest). */
  public String run() throws IOException {
    if (cfg.warmup.batches > 0) {
      warmUp();
    }
    Store store = Store.open(cfg.store);
    try {
      return run(store);
    } finally {
      try {
        store.close();
      } catch (Exception e) {
        throw new IOException("Cannot close the store", e);
      }
    }
  }

  /**
   * The warmup ({@link BenchConfig.WarmupSpec}): a run of its own on the in-memory store, with
   * every probe each batch, in a temporary directory deleted after it.
   */
  private void warmUp() throws IOException {
    BenchConfig w = json.readValue(json.writeValueAsBytes(cfg), BenchConfig.class);
    w.store = new BenchConfig.StoreSpec();
    w.warmup.batches = 0;
    w.workload.seed = cfg.workload.seed + 1;
    w.workload.initialRows = cfg.warmup.initialRows;
    w.workload.batches = cfg.warmup.batches;
    w.reads.every = 1;
    w.reads.travelBack = 1;
    Path dir = Files.createTempDirectory("rmow-warmup-");
    try {
      new Runner(w, dir, runId + "-warmup").run();
    } finally {
      try (var paths = Files.walk(dir)) {
        for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.delete(p);
        }
      }
    }
  }

  /** Whether an arm appends to its files: the appendable RMoW arms. */
  private static boolean appends(String arm) {
    return arm.startsWith("rmow-") && !arm.endsWith("-split");
  }

  private String run(Store store) throws IOException {
    if (store.appender() == null) {
      for (String arm : cfg.arms) {
        if (appends(arm)) {
          throw new IllegalArgumentException(
              arm + " appends, and this store cannot (on S3 it needs S3 Express: append express)");
        }
      }
    }
    OpLog log = OpLogGenerator.generate(cfg.workload);
    log.write(runDir.resolve("oplog.bin"));

    // every run names its files alike (see RelocatedFileIO); its objects go under its own prefix
    String root = options.storeRoot(cfg) + "/" + runId;
    RelocatedFileIO io = new RelocatedFileIO(store.io(), store.appender(), LOGICAL_ROOT, root);
    List<InitialLoad.InitialFile> base =
        InitialLoad.write(io, LOGICAL_ROOT + "/base", log, cfg.layout);

    List<ExecutorService> pools = new ArrayList<>();
    for (BenchConfig.ReaderSpec r : cfg.reads.readers) {
      ExecutorService pool = r.parallelism > 1 ? Executors.newFixedThreadPool(r.parallelism) : null;
      if (pool != null) {
        pools.add(pool);
      }
      readers.add(
          new Reader(
              r.name, r.footerCache, r.deleteCache, r.footerPrefetchBytes, r.splitTargetBytes, pool));
    }
    BenchConfig.LatencySpec ls = cfg.store.latency;
    StoreLatency latency =
        ls == null
            ? null
            : new StoreLatency(
                ls.getMs, ls.getSigma, ls.putMs, ls.putSigma, ls.appendMs, ls.appendSigma,
                ls.partMs, ls.readMsPerMib, ls.writeMsPerMib);
    List<ArmRun> arms = new ArrayList<>();
    for (String name : cfg.arms) {
      IoTrace trace = new IoTrace();
      Phases phases = new Phases(trace, store.recorder());
      ArmContext ctx =
          new ArmContext(
              name,
              LOGICAL_ROOT + "/" + name,
              new TracingFileIO(io, io, trace, latency),
              io,
              phases,
              cfg.layout,
              log,
              cfg.verifyRows);
      Arm arm = create(name, ctx);
      arm.load(InitialLoad.copy(io, base, ctx.root(), appends(name) ? io : null));
      arms.add(new ArmRun(arm, phases, trace));
      trace.logEvents(options.logRequests());
    }

    ReferenceModel model = new ReferenceModel(log);
    long[] tsBounds = {Long.MAX_VALUE, Long.MIN_VALUE};
    for (long id = 0; id < log.initialRows(); id++) {
      widen(tsBounds, log.ts(id));
    }

    try (BufferedWriter out = Files.newBufferedWriter(runDir.resolve("steps.jsonl"));
        BufferedWriter requests =
            options.logRequests()
                ? Files.newBufferedWriter(runDir.resolve("requests.jsonl"))
                : null) {
      requestsOut = requests;
      probe(out, arms, model, 0, tsBounds);
      for (ArmRun a : arms) {
        emitState(out, a, 0);
      }
      for (OpLog.Batch b : log.batches()) {
        int step = b.index() + 1;
        boolean compact = cfg.layout.compactEvery > 0 && step % cfg.layout.compactEvery == 0;
        for (ArmRun a : shuffled(arms, step)) {
          a.phases().run("batch", () -> a.arm().apply(b));
          if (compact) {
            a.phases().run("compact", () -> a.arm().compact());
          }
          emitPhases(out, a, step);
        }
        model.apply(b);
        for (long id : b.insertIds()) {
          widen(tsBounds, log.ts(id));
        }
        if (step % cfg.reads.every == 0 || step == log.batches().size()) {
          probe(out, arms, model, step, tsBounds);
        }
        for (ArmRun a : arms) {
          emitState(out, a, step);
        }
        out.flush();
      }
    }
    pools.forEach(ExecutorService::shutdown);
    if (options.dumpVersions()) {
      List<Arm> all = new ArrayList<>();
      arms.forEach(a -> all.add(a.arm()));
      VersionDump.write(runDir.resolve("versions"), cfg.store.type, io, all, model::expectedAfter);
    }
    if (!options.keepStore()) {
      store.deleteRun(root);
    }
    return log.sha256();
  }

  private static Arm create(String name, ArmContext ctx) {
    return switch (name) {
      case "mor-v2-file" -> new MorV2Arm(ctx, DeleteGranularity.FILE);
      case "mor-v2-partition" -> new MorV2Arm(ctx, DeleteGranularity.PARTITION);
      case "mor-v3" -> new MorV3Arm(ctx);
      case "rmow-a" -> new RmowArm(ctx, TailPolicy.Kind.A);
      case "rmow-b" -> new RmowArm(ctx, TailPolicy.Kind.B);
      case "rmow-c" -> new RmowArm(ctx, TailPolicy.Kind.C);
      case "rmow-a-split" -> new RmowArm(ctx, TailPolicy.Kind.A, true);
      case "rmow-b-split" -> new RmowArm(ctx, TailPolicy.Kind.B, true);
      case "rmow-c-split" -> new RmowArm(ctx, TailPolicy.Kind.C, true);
      case "tailcow-b" -> new TailCowArm(ctx, TailPolicy.Kind.B);
      case "tailcow-c" -> new TailCowArm(ctx, TailPolicy.Kind.C);
      case "cow" -> new CowArm(ctx);
      default -> throw new IllegalArgumentException("Unknown arm: " + name);
    };
  }

  private static void widen(long[] range, long ts) {
    range[0] = Math.min(range[0], ts);
    range[1] = Math.max(range[1], ts);
  }

  private List<ArmRun> shuffled(List<ArmRun> arms, int step) {
    List<ArmRun> order = new ArrayList<>(arms);
    SplittableRandom rnd = new SplittableRandom(cfg.workload.seed * 0x9E3779B97F4A7C15L + step);
    for (int i = order.size() - 1; i > 0; i--) {
      int j = rnd.nextInt(i + 1);
      ArmRun t = order.get(i);
      order.set(i, order.get(j));
      order.set(j, t);
    }
    return order;
  }

  /** Full, recent-window and time-travel reads, each checked against the model. */
  private void probe(
      BufferedWriter out, List<ArmRun> arms, ReferenceModel model, int step, long[] tsBounds)
      throws IOException {
    long lo = tsBounds[1] - (long) (cfg.reads.recentFraction * (tsBounds[1] - tsBounds[0]));
    Expression recent = Expressions.greaterThanOrEqual("ts", lo);
    TableHash wantFull = model.expected();
    TableHash wantRecent = model.expectedTsAtLeast(lo);
    int oldSteps = step - cfg.reads.travelBack;
    TableHash wantOld = oldSteps >= 0 ? model.expectedAfter(oldSteps) : null;

    for (ArmRun a : shuffled(arms, -step - 1)) {
      for (Reader r : readers) {
        checkedRead(a, r, "read_full", step, () -> a.arm().read(Expressions.alwaysTrue(), r), wantFull);
        checkedRead(a, r, "read_recent", step, () -> a.arm().read(recent, r), wantRecent);
        if (wantOld != null) {
          checkedRead(a, r, "read_old", step, () -> a.arm().readAfter(oldSteps, r), wantOld);
        }
      }
      emitPhases(out, a, step);
    }
  }

  private static void checkedRead(
      ArmRun a,
      Reader reader,
      String phase,
      int step,
      Phases.IoSupplier<TableHash> read,
      TableHash want)
      throws IOException {
    Map<String, Object> extra = new LinkedHashMap<>();
    extra.put("reader", reader.name());
    TableHash got = a.phases().run(phase, read, extra);
    extra.put("rows", got.count());
    if (!got.equals(want)) {
      throw new IllegalStateException(
          String.format(
              "%s %s (%s reader) at step %d read %s, the model holds %s",
              a.arm().name(), phase, reader.name(), step, got, want));
    }
  }

  private Map<String, Object> base(ArmRun a, int step, String phase) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("label", cfg.label);
    m.put("run", runId);
    m.put("seed", cfg.workload.seed);
    m.put("arm", a.arm().name());
    m.put("store", cfg.store.type);
    m.put("step", step);
    m.put("phase", phase);
    return m;
  }

  private void emitPhases(BufferedWriter out, ArmRun a, int step) throws IOException {
    for (Phases.Measured p : a.phases().drain()) {
      Map<String, Object> m = base(a, step, p.phase());
      m.put("start_ms", p.startMs());
      m.put("wall_ns", p.wallNs());
      m.put("cpu_ns", p.cpuNs());
      m.put("pool_cpu_ns", p.poolCpuNs());
      m.put("user_ns", p.userNs());
      m.put("pool_user_ns", p.poolUserNs());
      if (p.counts() != null) {
        m.put("instructions", p.counts().instructions());
        m.put("cycles", p.counts().cycles());
        m.put("pool_instructions", p.poolCounts().instructions());
        m.put("pool_cycles", p.poolCounts().cycles());
      }
      m.put("process_cpu_ns", p.processCpuNs());
      IoTrace.Counters c = p.io();
      m.put("requests", c.requests());
      m.put("gets", c.gets());
      m.put("get_range", c.getRange());
      m.put("get_stream", c.getStream());
      m.put("heads", c.heads());
      m.put("puts", c.puts());
      m.put("appends", c.appends());
      m.put("deletes", c.deletes());
      m.put("aborts", c.aborts());
      m.put("bytes_read", c.bytesRead());
      m.put("bytes_skipped_through", c.skipThroughBytes());
      m.put("bytes_written", c.bytesWritten());
      m.putAll(p.extra());
      out.write(json.writeValueAsString(m));
      out.newLine();
      if (requestsOut != null) {
        emitRequests(a, step, p);
      }
    }
  }

  /** The request log: one record per request (or stream read, or split task) of the phase. */
  private void emitRequests(ArmRun a, int step, Phases.Measured p) throws IOException {
    for (IoTrace.Event e : p.events()) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("arm", a.arm().name());
      m.put("step", step);
      m.put("phase", p.phase());
      m.put("reader", p.extra().get("reader"));
      m.put("seq", e.seq());
      m.put("task", e.task());
      m.put("kind", e.kind().name().toLowerCase(java.util.Locale.ROOT));
      String path = e.path() == null ? "" : e.path();
      m.put("object", path.startsWith(LOGICAL_ROOT + "/") ? path.substring(LOGICAL_ROOT.length() + 1) : path);
      m.put("offset", e.offset());
      m.put("length", e.length());
      m.put("cpu_ns", e.cpuNs());
      if (e.instructions() >= 0) {
        m.put("instructions", e.instructions());
        m.put("cycles", e.cycles());
      }
      requestsOut.write(json.writeValueAsString(m));
      requestsOut.newLine();
    }
  }

  private void emitState(BufferedWriter out, ArmRun a, int step) throws IOException {
    ArmStats s = a.arm().stats();
    Map<String, Object> m = base(a, step, "state");
    m.put("objects", s.objects());
    m.put("stored_bytes", s.storedBytes());
    m.put("live_bytes", s.liveBytes());
    m.put("reclaimable_bytes", s.reclaimableBytes());
    m.put("row_groups", s.rowGroups());
    m.put("data_files", s.dataFiles());
    m.put("delete_files", s.deleteFiles());
    m.put("footer_bytes", s.footerBytes());
    m.put("row_groups_out_of_order", s.rowGroupsOutOfOrder());
    m.put("live_objects", s.liveObjects());
    out.write(json.writeValueAsString(m));
    out.newLine();
  }
}
