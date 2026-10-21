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
 *   bin/run.sh &lt;config.json&gt; [--out &lt;dir&gt;]
 * </pre>
 *
 * Results go to {@code <out>/<label>-<config hash>/<UTC start time>/}: {@code manifest.json}
 * (config, versions, host, op-log hash), {@code oplog.bin} and {@code steps.jsonl}. The config
 * hash names the experiment, so repeated runs of one configuration share a directory.
 */
public final class Main {

  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("usage: run.sh <config.json> [--out <dir>]");
      System.exit(2);
    }
    Path out = Path.of("results");
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--out") && i + 1 < args.length) {
        out = Path.of(args[++i]);
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
    Path manifestFile = runDir.resolve("manifest.json");
    pretty.writeValue(manifestFile.toFile(), manifest);

    long t0 = System.nanoTime();
    String oplog = new Runner(cfg, runDir, started).run();
    manifest.put("oplog_sha256", oplog);
    manifest.put("elapsed_s", (System.nanoTime() - t0) / 1e9);
    manifest.put("finished", Instant.now().toString());
    pretty.writeValue(manifestFile.toFile(), manifest);
    System.out.println(runDir);
  }

  private static Map<String, Object> versions() {
    Map<String, Object> v = new LinkedHashMap<>();
    v.put("rmow_bench_git", git("rev-parse", "HEAD"));
    v.put("rmow_bench_dirty", !git("status", "--porcelain").isEmpty());
    v.put("iceberg", IcebergBuild.fullVersion());
    v.put("parquet", org.apache.parquet.Version.FULL_VERSION);
    v.put("java", System.getProperty("java.version"));
    v.put("java_vm", System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version"));
    v.put("jvm_args", ManagementFactory.getRuntimeMXBean().getInputArguments());
    return v;
  }

  private static Map<String, Object> host() {
    Map<String, Object> h = new LinkedHashMap<>();
    try {
      h.put("name", InetAddress.getLocalHost().getHostName());
    } catch (IOException e) {
      h.put("name", "unknown");
    }
    h.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
    h.put("arch", System.getProperty("os.arch"));
    h.put("cpus", Runtime.getRuntime().availableProcessors());
    h.put("max_heap_bytes", Runtime.getRuntime().maxMemory());
    return h;
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
