package org.rmow.bench.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.IcebergBuild;

/**
 * Runs one benchmark configuration.
 *
 * <pre>
 *   bin/run.sh &lt;config.json&gt; [--out &lt;dir&gt;] [--store-root &lt;dir&gt;] [--keep-store] [--dump-versions]
 *       [--log-requests] [--log-held] [--verify]
 * </pre>
 *
 * Results go to {@code <out>/<label>-<config hash>/<UTC start time>/}: {@code manifest.json}
 * (config, versions, host, op-log hash), {@code oplog.bin} and {@code steps.jsonl}. The config
 * hash names the experiment, so repeated runs of one configuration share a directory.
 *
 * <ul>
 *   <li>{@code --store-root} puts a local store's objects under another root than the config's
 *       (where a local store lives does not change what is measured, so it is not part of the
 *       hash); other stores keep their config's root;
 *   <li>{@code --keep-store} keeps the run's objects, which are otherwise deleted at the end;
 *   <li>{@code --dump-versions} writes every version of the appendable objects to {@code
 *       versions/} in the run directory, for {@code analysis/interop_check.py};
 *   <li>{@code --log-held} writes every change of what each phase's readers and writers hold to
 *       {@code held.jsonl} (rows, and bytes by kind), for drawing memory against the stages of a
 *       phase;
 *   <li>{@code --log-requests} writes every request each phase issued to {@code requests.jsonl},
 *       with its split task and each task's CPU, for latency models;
 *   <li>{@code --verify} makes the run a verification run: every read hashes each row it returns
 *       into the id-and-version oracle (and checks its payload, with the config's {@code
 *       verifyRows}) against the reference model. Without it the run measures: its reads only
 *       count their rows, so no check of correctness runs in a measured phase. A verification
 *       run issues the same requests and bytes as a measured run of its config, which it vouches
 *       for ({@code steps_diff}); the manifest says which it is ({@code verify}).
 * </ul>
 *
 * <p>{@code bin/matrix.sh} runs from a snapshot of the build and names it, and its git state, in
 * the system properties {@code rmow.build.git}, {@code rmow.build.dirty} and {@code
 * rmow.build.snapshot}; without them the manifest records the working tree's git state.
 */
public final class Main {

  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println(
          "usage: run.sh <config.json> [--out <dir>] [--store-root <dir>] [--keep-store]"
              + " [--dump-versions] [--log-requests] [--log-held] [--verify]");
      System.exit(2);
    }
    Path out = Path.of("results");
    String storeRoot = null;
    boolean keepStore = false;
    boolean dumpVersions = false;
    boolean logRequests = false;
    boolean logHeld = false;
    boolean verify = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--out") && i + 1 < args.length) {
        out = Path.of(args[++i]);
      } else if (args[i].equals("--store-root") && i + 1 < args.length) {
        storeRoot = args[++i];
      } else if (args[i].equals("--keep-store")) {
        keepStore = true;
      } else if (args[i].equals("--dump-versions")) {
        dumpVersions = true;
      } else if (args[i].equals("--log-requests")) {
        logRequests = true;
      } else if (args[i].equals("--log-held")) {
        logHeld = true;
      } else if (args[i].equals("--verify")) {
        verify = true;
      } else {
        throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }

    ObjectMapper json =
        JsonMapper.builder().enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY).build();
    ObjectWriter pretty = json.writerWithDefaultPrettyPrinter();
    BenchConfig cfg = json.readValue(Path.of(args[0]).toFile(), BenchConfig.class);
    // the canonical form names the experiment: every field, defaults included, sorted by name
    JsonNode canonical = json.valueToTree(cfg);
    String hash = sha256(json.writeValueAsString(canonical)).substring(0, 12);
    String started =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());
    Path runDir = out.resolve(cfg.label + "-" + hash).resolve(started);
    Files.createDirectories(runDir);

    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", cfg.label);
    manifest.put("config_hash", hash);
    manifest.put("config", canonical);
    manifest.put("started", started);
    manifest.put("versions", versions());
    manifest.put("host", host());
    Runner.Options options =
        new Runner.Options(storeRoot, keepStore, dumpVersions, logRequests, logHeld, verify);
    manifest.put("verify", verify);
    manifest.put("store_root", options.storeRoot(cfg));
    Path manifestFile = runDir.resolve("manifest.json");
    pretty.writeValue(manifestFile.toFile(), manifest);

    long t0 = System.nanoTime();
    String oplog;
    try {
      oplog = new Runner(cfg, runDir, started, options).run();
    } catch (Throwable t) {
      fail(t, manifest, manifestFile, pretty, t0);
      return;
    }
    manifest.put("oplog_sha256", oplog);
    manifest.put("elapsed_s", (System.nanoTime() - t0) / 1e9);
    manifest.put("finished", Instant.now().toString());
    pretty.writeValue(manifestFile.toFile(), manifest);
    System.out.println(runDir);
  }

  /**
   * A run that fails says so in its manifest ({@code failed}, which the lab reads as the run's
   * status) and ends the JVM with status 1, whatever threads are still alive, so that a script or a
   * VM running it moves on instead of waiting.
   */
  private static void fail(
      Throwable t, Map<String, Object> manifest, Path manifestFile, ObjectWriter pretty, long t0) {
    t.printStackTrace();
    try {
      manifest.put("failed", t.toString());
      manifest.put("elapsed_s", (System.nanoTime() - t0) / 1e9);
      pretty.writeValue(manifestFile.toFile(), manifest);
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(1);
  }

  private static Map<String, Object> versions() throws IOException, NoSuchAlgorithmException {
    Map<String, Object> v = new LinkedHashMap<>();
    String snapshot = System.getProperty("rmow.build.snapshot");
    if (snapshot != null) {
      v.put("rmow_bench_git", System.getProperty("rmow.build.git", ""));
      v.put("rmow_bench_dirty", Boolean.parseBoolean(System.getProperty("rmow.build.dirty")));
      v.put("build_snapshot", snapshot);
    } else {
      v.put("rmow_bench_git", git("rev-parse", "HEAD"));
      v.put("rmow_bench_dirty", !git("status", "--porcelain").isEmpty());
    }
    v.putAll(classpath());
    v.put("iceberg", IcebergBuild.fullVersion());
    v.put("parquet", org.apache.parquet.Version.FULL_VERSION);
    v.put("java", System.getProperty("java.version"));
    v.put("java_vm", System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version"));
    v.put("jvm_args", ManagementFactory.getRuntimeMXBean().getInputArguments());
    return v;
  }

  /**
   * The machine, as CPU is measured on it: its CPU model and the CPUs this process may run on. In
   * a container ({@code docker/run.sh}), the system properties {@code rmow.host.name} and {@code
   * rmow.host.os} name the host rather than the container, and {@code rmow.placement} says which
   * cores the campaign gave this run.
   */
  private static Map<String, Object> host() {
    Map<String, Object> h = new LinkedHashMap<>();
    String name = System.getProperty("rmow.host.name");
    if (name == null) {
      try {
        name = InetAddress.getLocalHost().getHostName();
      } catch (IOException e) {
        name = "unknown";
      }
    }
    h.put("name", name);
    h.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
    if (System.getProperty("rmow.host.os") != null) {
      h.put("distribution", System.getProperty("rmow.host.os"));
    }
    h.put("arch", System.getProperty("os.arch"));
    h.put("cpu_model", procField("/proc/cpuinfo", "model name"));
    h.put("cpus", Runtime.getRuntime().availableProcessors());
    h.put("cpus_allowed", procField("/proc/self/status", "Cpus_allowed_list"));
    // whether phases carry instructions and cycles, and if not, why
    h.put("thread_counters",
        ThreadCounters.unavailable() == null ? "available" : ThreadCounters.unavailable());
    h.put("thread_cpu_per_wall", threadCpuPerWall());
    h.put("max_heap_bytes", Runtime.getRuntime().maxMemory());
    if (System.getProperty("rmow.placement") != null) {
      h.put("container", true);
      h.put("placement", System.getProperty("rmow.placement"));
    }
    return h;
  }

  /**
   * Thread CPU time per nanosecond of wall time, spinning for 200 ms: 1 where the two clocks agree.
   * Under WSL2 on Hyper-V the thread CPU clock ran 11% fast (1.111), so a phase's CPU exceeded its
   * wall time; the lab's {@code phase_breakdown} divides CPU times by this.
   */
  private static double threadCpuPerWall() {
    var mx = ManagementFactory.getThreadMXBean();
    long cpu0 = mx.getCurrentThreadCpuTime();
    long t0 = System.nanoTime();
    long t;
    do {
      t = System.nanoTime();
    } while (t - t0 < 200_000_000L);
    return (mx.getCurrentThreadCpuTime() - cpu0) / (double) (t - t0);
  }

  /** The first {@code key: value} line's value in a /proc file; empty when there is none. */
  private static String procField(String file, String key) {
    try {
      for (String line : Files.readAllLines(Path.of(file))) {
        int colon = line.indexOf(':');
        if (colon > 0 && line.substring(0, colon).trim().equals(key)) {
          return line.substring(colon + 1).trim();
        }
      }
    } catch (IOException e) {
      // not Linux
    }
    return "";
  }

  /**
   * Digests of the jars the run loaded: the project jar's, and one over every classpath entry's
   * name and digest, so two runs can be checked to have run the same build.
   */
  private static Map<String, Object> classpath() throws IOException, NoSuchAlgorithmException {
    MessageDigest all = MessageDigest.getInstance("SHA-256");
    String own = "";
    for (String entry : System.getProperty("java.class.path").split(java.io.File.pathSeparator)) {
      Path p = Path.of(entry);
      if (!Files.isRegularFile(p)) {
        continue;
      }
      String digest =
          HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(p)));
      all.update((p.getFileName() + " " + digest + "\n").getBytes(StandardCharsets.UTF_8));
      if (p.getFileName().toString().startsWith("rmow-bench-")) {
        own = digest;
      }
    }
    Map<String, Object> v = new LinkedHashMap<>();
    v.put("rmow_bench_jar_sha256", own);
    v.put("classpath_sha256", HexFormat.of().formatHex(all.digest()));
    return v;
  }

  /** Best effort: the empty string when git or the repository is unavailable. */
  private static String git(String... args) {
    try {
      String[] cmd = new String[args.length + 1];
      cmd[0] = "git";
      System.arraycopy(args, 0, cmd, 1, args.length);
      Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      String outText = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      return p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0 ? outText : "";
    } catch (IOException e) {
      return "";
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "";
    }
  }

  private static String sha256(String s) throws NoSuchAlgorithmException {
    return HexFormat.of()
        .formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
  }
}
