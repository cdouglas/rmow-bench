package org.rmow.bench.micro;

import com.azure.core.http.HttpClient;
import com.azure.core.util.Context;
import com.azure.core.util.HttpClientOptions;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;
import org.apache.iceberg.azure.AzureProperties;
import org.rmow.bench.io.AdlsClients;

/**
 * Whether one Azure Append Blob takes appends from many writers at once, for the catalog's shared
 * log rather than for RMoW (docs/AZURE_RUNBOOK.md, "Blind appends"). An Append Block need not name
 * a position: without {@code x-ms-blob-condition-appendpos} the service appends at the blob's end
 * and replies with where the block landed ({@code x-ms-blob-append-offset}). If concurrent writers'
 * blind appends all land, a log of records on Append Blobs, with checkpoints stored apart, needs no
 * conditional append (fileio-catalog's docs/LOG.md, "Unconditional appends"), and loses no races.
 * This measures what one blob takes, against the conditional appends the catalog makes now.
 *
 * <pre>
 *   java -cp ... org.rmow.bench.micro.AzureAppendSpike abfss://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;prefix&gt;
 *       [--writers 1,2,4,8,16,32,64] [--seconds 20] [--warmup 2] [--bytes 4096]
 *       [--modes blind,conditional] [--reps 1] [--keep] [--out dir] [--label l] [--endpoint url]
 * </pre>
 *
 * <p>A <b>level</b> is one mode, append size and number of writers, on a new Append Blob of its
 * own. Its writers are threads of this JVM sharing one HTTP connection pool (as large as the most
 * writers, twice over), each sending one Append Block at a time for {@code warmup + seconds}:
 *
 * <ul>
 *   <li><b>blind</b>: no condition. Every append should land; the reply says where.
 *   <li><b>conditional</b>: conditioned on the position the writer believes is the blob's end (the
 *       store's own append; the fork's adds the ETag, which changes nothing on a blob nobody
 *       replaces). A writer that loses ({@code 412 AppendPositionConditionNotMet}) learns the end
 *       with a HEAD and sends a new append there: the least a catalog writer does after a lost race.
 * </ul>
 *
 * <p>The SDK's retries are off (one try, 60 s), so each record is one request. A throttled or
 * failed append backs off (25 ms, doubling to 800 ms, reset by a success); a timeout or transport
 * failure is ambiguous (it may have landed), and the check below counts what landed unacknowledged.
 * Modes are interleaved at each writer count, so both see the store at about the same time.
 *
 * <p>Each append is a frame: a magic number, the level, the writer, its sequence number, the length,
 * filler, and a CRC32C of the rest. After each level the blob is read whole and checked frame by
 * frame: every frame whole (magic, length, CRC), no frame twice, every acknowledged append found at
 * the offset its reply named, and the blob's blocks as many as its frames. Then it is deleted
 * ({@code --keep} keeps it).
 *
 * <p>Writes {@code <out>/<label>-<UTC start>/} (default {@code results/spike},
 * {@code azure-appends}): {@code manifest.json}; {@code appends.jsonl}, one record per request
 * (level, mode, bytes, writers, writer, seq, op, outcome, status, code, offset, blocks, start_ns
 * from the level's start, ns, steady: after the warm-up and before the deadline); and {@code
 * levels.jsonl}, one summary per level: rates over the steady window, latency percentiles by op
 * and outcome, the client's CPU share, and the check. Not {@code spike.jsonl}, so the lab's latency
 * fits ({@code spike_requests}) never see these. Run it from a VM in the account's region.
 * {@code --endpoint} runs it against an emulator (with {@code AZURE_STORAGE_KEY}), which checks the
 * code, not Azure.
 */
public final class AzureAppendSpike {

  enum Mode {
    BLIND,
    CONDITIONAL;

    String label() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private static final int MAGIC = 0x424C4150; // "BLAP"
  private static final int HEADER = 24; // magic, level, writer, seq (8), length
  private static final int TRAILER = 4; // CRC32C
  private static final long BACKOFF_MIN_MS = 25;
  private static final long BACKOFF_MAX_MS = 800;

  /** One level: a mode, an append size and a number of writers, on a blob of its own. */
  record Level(int id, Mode mode, int bytes, int writers, int rep) {
    String name() {
      return String.format(Locale.ROOT, "%03d-%s-%d-w%d-r%d", id, mode.label(), bytes, writers, rep);
    }
  }

  /** One request, as its writer saw it. */
  record Req(
      int writer,
      long seq,
      String op,
      String outcome,
      int status,
      String code,
      long offset,
      int blocks,
      long startNs,
      long ns) {}

  private final AppendBlobFactory blobs;
  private final String prefix;
  private final long warmupNs;
  private final long steadyNs;
  private final boolean keep;
  private final BufferedWriter requestsOut;
  private final BufferedWriter levelsOut;
  private final ObjectMapper json = new ObjectMapper();

  private AzureAppendSpike(
      AppendBlobFactory blobs,
      String prefix,
      long warmupNs,
      long steadyNs,
      boolean keep,
      BufferedWriter requestsOut,
      BufferedWriter levelsOut) {
    this.blobs = blobs;
    this.prefix = prefix;
    this.warmupNs = warmupNs;
    this.steadyNs = steadyNs;
    this.keep = keep;
    this.requestsOut = requestsOut;
    this.levelsOut = levelsOut;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || !args[0].startsWith("abfs")) {
      System.err.println(
          "usage: AzureAppendSpike abfss://<container>@<account>.dfs.core.windows.net/<prefix>"
              + " [--writers 1,2,4,...] [--seconds s] [--warmup s] [--bytes n,...]"
              + " [--modes blind,conditional] [--reps n] [--keep] [--out dir] [--label l]"
              + " [--endpoint url]");
      System.exit(2);
    }
    String started =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());
    int[] writers = {1, 2, 4, 8, 16, 32, 64};
    int[] sizes = {4096};
    List<Mode> modes = List.of(Mode.BLIND, Mode.CONDITIONAL);
    double seconds = 20;
    double warmup = 2;
    int reps = 1;
    boolean keep = false;
    Path outRoot = Path.of("results", "spike");
    String label = "azure-appends";
    String endpoint = "";
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--writers" -> writers = ints(args[++i]);
        case "--bytes" -> sizes = ints(args[++i]);
        case "--modes" -> modes = modes(args[++i]);
        case "--seconds" -> seconds = Double.parseDouble(args[++i]);
        case "--warmup" -> warmup = Double.parseDouble(args[++i]);
        case "--reps" -> reps = Integer.parseInt(args[++i]);
        case "--keep" -> keep = true;
        case "--out" -> outRoot = Path.of(args[++i]);
        case "--label" -> label = args[++i];
        case "--endpoint" -> endpoint = args[++i];
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }
    for (int size : sizes) {
      if (size < HEADER + TRAILER) {
        throw new IllegalArgumentException(
            "An append must hold a frame's header and CRC: at least "
                + (HEADER + TRAILER)
                + " bytes, not "
                + size);
      }
    }
    int most = Arrays.stream(writers).max().orElse(1);

    Path dir = outRoot.resolve(label + "-" + started);
    Files.createDirectories(dir);
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", label);
    manifest.put("kind", "appends");
    manifest.put("started", started);
    manifest.put("root", args[0]);
    manifest.put("endpoint", endpoint);
    manifest.put("writers", writers);
    manifest.put("bytes", sizes);
    manifest.put("modes", modes.stream().map(Mode::label).toList());
    manifest.put("seconds", seconds);
    manifest.put("warmup", warmup);
    manifest.put("reps", reps);
    manifest.put("connections", 2 * most);
    manifest.put("sdk_tries", 1);
    manifest.put("host", InetAddress.getLocalHost().getHostName());
    manifest.put("vcpus", Runtime.getRuntime().availableProcessors());
    manifest.put("java", System.getProperty("java.version"));
    ObjectMapper json = new ObjectMapper();
    json.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("manifest.json").toFile(), manifest);

    String key = endpoint.isEmpty() ? null : System.getenv("AZURE_STORAGE_KEY");
    if (!endpoint.isEmpty()) {
      new AdlsClients(AdlsClients.properties(args[0], endpoint, key))
          .container(args[0])
          .createIfNotExists();
    }
    AppendBlobFactory blobs = new AppendBlobFactory(args[0], endpoint, key, 2 * most);
    String prefix = args[0] + "/appends-" + started;
    List<Map<String, Object>> summaries = new ArrayList<>();
    try (BufferedWriter requests = Files.newBufferedWriter(dir.resolve("appends.jsonl"));
        BufferedWriter levels = Files.newBufferedWriter(dir.resolve("levels.jsonl"))) {
      AzureAppendSpike spike =
          new AzureAppendSpike(
              blobs, prefix, nanos(warmup), nanos(seconds), keep, requests, levels);
      int id = 0;
      for (int rep = 0; rep < reps; rep++) {
        for (int size : sizes) {
          for (int n : writers) {
            for (Mode mode : modes) {
              summaries.add(spike.run(new Level(id++, mode, size, n, rep)));
            }
          }
        }
      }
    }
    manifest.put("finished", Instant.now().toString());
    json.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("manifest.json").toFile(), manifest);
    printTable(summaries);
    System.out.println(dir);
  }

  private static long nanos(double seconds) {
    return (long) (seconds * 1e9);
  }

  private static int[] ints(String list) {
    return Arrays.stream(list.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
  }

  private static List<Mode> modes(String list) {
    return Arrays.stream(list.split(","))
        .map(s -> Mode.valueOf(s.trim().toUpperCase(Locale.ROOT)))
        .toList();
  }

  /** One level: its writers for the warm-up and the steady window, then the blob's check. */
  private Map<String, Object> run(Level level) throws Exception {
    AppendBlobClient blob = blobs.appendBlob(prefix + "/" + level.name());
    blob.create(true);
    AtomicBoolean full = new AtomicBoolean();
    AtomicLong start = new AtomicLong();
    CountDownLatch go = new CountDownLatch(1);
    long deadline = warmupNs + steadyNs; // from the level's start
    ExecutorService pool = Executors.newFixedThreadPool(level.writers());
    List<Req> reqs = new ArrayList<>();
    long cpu0;
    long wallNs;
    long cpuNs;
    try {
      List<Future<List<Req>>> futures = new ArrayList<>();
      for (int w = 0; w < level.writers(); w++) {
        int writer = w;
        futures.add(
            pool.submit(
                () -> {
                  go.await();
                  return write(level, blob, writer, start.get(), deadline, full);
                }));
      }
      cpu0 = processCpuNs();
      start.set(System.nanoTime());
      go.countDown();
      for (Future<List<Req>> f : futures) {
        reqs.addAll(f.get());
      }
      wallNs = System.nanoTime() - start.get();
      cpuNs = processCpuNs() - cpu0;
    } finally {
      pool.shutdownNow();
    }

    Map<String, Object> check = check(blob, level, reqs);
    if (!keep) {
      blob.delete();
    }

    for (Req r : reqs) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("level", level.name());
      m.put("mode", level.mode().label());
      m.put("bytes", level.bytes());
      m.put("writers", level.writers());
      m.put("rep", level.rep());
      m.put("writer", r.writer());
      m.put("seq", r.seq());
      m.put("op", r.op());
      m.put("outcome", r.outcome());
      m.put("status", r.status());
      m.put("code", r.code());
      m.put("offset", r.offset());
      m.put("blocks", r.blocks());
      m.put("start_ns", r.startNs());
      m.put("ns", r.ns());
      m.put("steady", steady(r));
      requestsOut.write(json.writeValueAsString(m));
      requestsOut.newLine();
    }
    requestsOut.flush();

    Map<String, Object> summary = summarize(level, reqs, wallNs, cpuNs, full.get());
    summary.put("check", check);
    levelsOut.write(json.writeValueAsString(summary));
    levelsOut.newLine();
    levelsOut.flush();
    System.out.printf(
        Locale.ROOT,
        "%s: %.1f landed/s, %s%n",
        level.name(),
        (double) summary.get("landed_per_s"),
        Boolean.TRUE.equals(check.get("ok")) ? "check ok" : "CHECK FAILED " + check);
    return summary;
  }

  private boolean steady(Req r) {
    return r.startNs() >= warmupNs && r.startNs() < warmupNs + steadyNs;
  }

  /** One writer's appends until the deadline, or until the blob is full. */
  private static List<Req> write(
      Level level, AppendBlobClient blob, int writer, long start, long deadline, AtomicBoolean full) {
    List<Req> reqs = new ArrayList<>();
    SplittableRandom random = new SplittableRandom(31L * level.id() + writer);
    byte[] body = new byte[level.bytes()];
    long position = 0; // conditional: where this writer believes the blob ends
    long backoffMs = 0;
    for (long seq = 0; !full.get() && System.nanoTime() - start < deadline; seq++) {
      frame(body, level.id(), writer, seq, random);
      AppendBlobRequestConditions condition =
          level.mode() == Mode.CONDITIONAL
              ? new AppendBlobRequestConditions().setAppendPosition(position)
              : null;
      long t = System.nanoTime();
      try {
        AppendBlobItem item =
            blob.appendBlockWithResponse(
                    new ByteArrayInputStream(body), body.length, null, condition, null, Context.NONE)
                .getValue();
        long ns = System.nanoTime() - t;
        long offset = Long.parseLong(item.getBlobAppendOffset());
        Integer blocks = item.getBlobCommittedBlockCount();
        reqs.add(
            new Req(writer, seq, "append", "ok", 201, "", offset, blocks == null ? -1 : blocks,
                t - start, ns));
        position = offset + body.length;
        backoffMs = 0;
      } catch (BlobStorageException e) {
        long ns = System.nanoTime() - t;
        BlobErrorCode code = e.getErrorCode();
        String outcome;
        if (BlobErrorCode.APPEND_POSITION_CONDITION_NOT_MET.equals(code)) {
          outcome = "lost";
        } else if (BlobErrorCode.BLOCK_COUNT_EXCEEDS_LIMIT.equals(code)) {
          outcome = "full";
          full.set(true);
        } else if (e.getStatusCode() == 503 || e.getStatusCode() == 500) {
          outcome = "throttled";
        } else {
          outcome = "error";
        }
        reqs.add(
            new Req(writer, seq, "append", outcome, e.getStatusCode(),
                code == null ? "" : code.toString(), -1, -1, t - start, ns));
        if (outcome.equals("lost")) {
          position = head(blob, writer, seq, start, reqs, position);
        } else if (!outcome.equals("full")) {
          backoffMs = sleep(backoffMs);
        }
      } catch (RuntimeException e) {
        // a timeout or a transport failure: the append may or may not have landed
        reqs.add(
            new Req(writer, seq, "append", "error", 0, e.getClass().getSimpleName(), -1, -1,
                t - start, System.nanoTime() - t));
        backoffMs = sleep(backoffMs);
      }
    }
    return reqs;
  }

  /** After a lost race: the blob's end, from a HEAD; {@code fallback} if the HEAD fails. */
  private static long head(
      AppendBlobClient blob, int writer, long seq, long start, List<Req> reqs, long fallback) {
    long t = System.nanoTime();
    try {
      BlobProperties p = blob.getProperties();
      reqs.add(
          new Req(writer, seq, "head", "ok", 200, "", p.getBlobSize(),
              p.getCommittedBlockCount() == null ? -1 : p.getCommittedBlockCount(), t - start,
              System.nanoTime() - t));
      return p.getBlobSize();
    } catch (BlobStorageException e) {
      reqs.add(
          new Req(writer, seq, "head", "error", e.getStatusCode(),
              e.getErrorCode() == null ? "" : e.getErrorCode().toString(), -1, -1, t - start,
              System.nanoTime() - t));
    } catch (RuntimeException e) {
      reqs.add(
          new Req(writer, seq, "head", "error", 0, e.getClass().getSimpleName(), -1, -1,
              t - start, System.nanoTime() - t));
    }
    return fallback;
  }

  /** Backs off after a throttled or failed request; the next backoff. */
  private static long sleep(long backoffMs) {
    long next = backoffMs == 0 ? BACKOFF_MIN_MS : Math.min(BACKOFF_MAX_MS, 2 * backoffMs);
    try {
      Thread.sleep(next);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return next;
  }

  /** Fills {@code body} with one frame: header, filler, CRC32C of the rest. */
  static void frame(byte[] body, int level, int writer, long seq, SplittableRandom random) {
    ByteBuffer b = ByteBuffer.wrap(body);
    b.putInt(0, MAGIC).putInt(4, level).putInt(8, writer).putLong(12, seq).putInt(20, body.length);
    for (int i = HEADER; i < body.length - TRAILER; i++) {
      body[i] = (byte) random.nextInt(256);
    }
    CRC32C crc = new CRC32C();
    crc.update(body, 0, body.length - TRAILER);
    b.putInt(body.length - TRAILER, (int) crc.getValue());
  }

  static long key(int writer, long seq) {
    return ((long) writer << 40) | seq;
  }

  /** The blob read whole and checked frame by frame against the acknowledged appends. */
  private Map<String, Object> check(AppendBlobClient blob, Level level, List<Req> reqs) {
    Map<Long, Long> acked = new HashMap<>();
    for (Req r : reqs) {
      if (r.op().equals("append") && r.outcome().equals("ok")) {
        acked.put(key(r.writer(), r.seq()), r.offset());
      }
    }
    FrameCheck frames = new FrameCheck(level.id(), level.bytes(), acked);
    blob.downloadStreamWithResponse(frames, null, null, null, false, null, Context.NONE);
    BlobProperties p = blob.getProperties();
    return frames.result(p.getBlobSize(), p.getCommittedBlockCount());
  }

  /** Checks a blob's frames as its bytes arrive. */
  static final class FrameCheck extends OutputStream {
    private final int level;
    private final int bytes;
    private final Map<Long, Long> acked;
    private final Set<Long> seen = new HashSet<>();
    private final byte[] frame;
    private int filled = 0;
    private long position = 0;
    private long frames = 0;
    private long bad = 0;
    private long duplicates = 0;
    private long matched = 0;
    private long misplaced = 0;
    private long unacked = 0;

    FrameCheck(int level, int bytes, Map<Long, Long> acked) {
      this.level = level;
      this.bytes = bytes;
      this.acked = acked;
      this.frame = new byte[bytes];
    }

    @Override
    public void write(int b) {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      while (len > 0) {
        int n = Math.min(len, bytes - filled);
        System.arraycopy(b, off, frame, filled, n);
        filled += n;
        off += n;
        len -= n;
        if (filled == bytes) {
          examine();
          filled = 0;
          position += bytes;
        }
      }
    }

    private void examine() {
      frames++;
      ByteBuffer b = ByteBuffer.wrap(frame);
      CRC32C crc = new CRC32C();
      crc.update(frame, 0, bytes - TRAILER);
      if (b.getInt(0) != MAGIC
          || b.getInt(4) != level
          || b.getInt(20) != bytes
          || b.getInt(bytes - TRAILER) != (int) crc.getValue()) {
        bad++;
        return;
      }
      long key = key(b.getInt(8), b.getLong(12));
      if (!seen.add(key)) {
        duplicates++;
        return;
      }
      Long offset = acked.get(key);
      if (offset == null) {
        unacked++; // landed, but its writer was not told so
      } else if (offset == position) {
        matched++;
      } else {
        misplaced++;
      }
    }

    Map<String, Object> result(long blobBytes, Integer blobBlocks) {
      long missing = acked.size() - matched - misplaced;
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("blob_bytes", blobBytes);
      m.put("blob_blocks", blobBlocks == null ? -1 : blobBlocks);
      m.put("frames", frames);
      m.put("acked", acked.size());
      m.put("matched", matched);
      m.put("unacked_landed", unacked);
      m.put("bad", bad);
      m.put("duplicates", duplicates);
      m.put("misplaced", misplaced);
      m.put("missing", missing);
      m.put("tail_bytes", filled);
      m.put(
          "ok",
          bad == 0
              && duplicates == 0
              && misplaced == 0
              && missing == 0
              && filled == 0
              && (blobBlocks == null || blobBlocks == frames));
      return m;
    }
  }

  private Map<String, Object> summarize(
      Level level, List<Req> reqs, long wallNs, long cpuNs, boolean full) {
    double window = steadyNs / 1e9;
    Map<String, List<Long>> latencies = new LinkedHashMap<>();
    Map<String, Long> counts = new LinkedHashMap<>();
    Map<String, Long> steadyCounts = new LinkedHashMap<>();
    long lastEnd = 0;
    for (Req r : reqs) {
      String k = r.op() + "_" + r.outcome();
      counts.merge(k, 1L, Long::sum);
      lastEnd = Math.max(lastEnd, r.startNs() + r.ns());
      if (steady(r)) {
        steadyCounts.merge(k, 1L, Long::sum);
        latencies.computeIfAbsent(k, x -> new ArrayList<>()).add(r.ns());
      }
    }
    long landed = steadyCounts.getOrDefault("append_ok", 0L);
    long attempts =
        steadyCounts.entrySet().stream()
            .filter(e -> e.getKey().startsWith("append_"))
            .mapToLong(Map.Entry::getValue)
            .sum();
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("level", level.name());
    m.put("mode", level.mode().label());
    m.put("bytes", level.bytes());
    m.put("writers", level.writers());
    m.put("rep", level.rep());
    m.put("steady_s", window);
    m.put("landed_per_s", landed / window);
    m.put("attempts_per_s", attempts / window);
    m.put("landed_mib_per_s", landed * (double) level.bytes() / window / (1 << 20));
    m.put("lost_share", attempts == 0 ? 0.0 : steadyCounts.getOrDefault("append_lost", 0L) / (double) attempts);
    m.put("full", full);
    m.put("counts", counts);
    m.put("steady_counts", steadyCounts);
    Map<String, Object> ms = new LinkedHashMap<>();
    for (Map.Entry<String, List<Long>> e : latencies.entrySet()) {
      long[] v = e.getValue().stream().mapToLong(Long::longValue).sorted().toArray();
      Map<String, Object> q = new LinkedHashMap<>();
      q.put("n", v.length);
      q.put("p50", quantile(v, 0.50));
      q.put("p90", quantile(v, 0.90));
      q.put("p99", quantile(v, 0.99));
      q.put("max", v[v.length - 1] / 1e6);
      ms.put(e.getKey(), q);
    }
    m.put("latency_ms", ms);
    m.put("wall_s", wallNs / 1e9);
    m.put("last_end_s", lastEnd / 1e9);
    m.put("client_cpu_share", cpuNs / (wallNs * (double) Runtime.getRuntime().availableProcessors()));
    return m;
  }

  private static double quantile(long[] sorted, double q) {
    int i = (int) Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
    return sorted[i] / 1e6;
  }

  private static long processCpuNs() {
    return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
        .getProcessCpuTime();
  }

  @SuppressWarnings("unchecked")
  private static void printTable(List<Map<String, Object>> summaries) {
    System.out.printf(
        Locale.ROOT,
        "%n%-12s %6s %7s %10s %9s %9s %6s %9s %7s %s%n",
        "mode", "bytes", "writers", "landed/s", "p50 ms", "p99 ms", "lost", "throttled", "errors",
        "check");
    for (Map<String, Object> s : summaries) {
      Map<String, Object> lat = (Map<String, Object>) s.get("latency_ms");
      Map<String, Object> ok = (Map<String, Object>) lat.getOrDefault("append_ok", Map.of());
      Map<String, Long> counts = (Map<String, Long>) s.get("steady_counts");
      Map<String, Object> check = (Map<String, Object>) s.get("check");
      System.out.printf(
          Locale.ROOT,
          "%-12s %6d %7d %10.1f %9.1f %9.1f %5.0f%% %9d %7d %s%n",
          s.get("mode"),
          (int) s.get("bytes"),
          (int) s.get("writers"),
          (double) s.get("landed_per_s"),
          ((Number) ok.getOrDefault("p50", 0.0)).doubleValue(),
          ((Number) ok.getOrDefault("p99", 0.0)).doubleValue(),
          100 * (double) s.get("lost_share"),
          counts.getOrDefault("append_throttled", 0L),
          counts.getOrDefault("append_error", 0L),
          Boolean.TRUE.equals(check.get("ok"))
              ? "ok"
              : "FAILED (bad " + check.get("bad") + ", dup " + check.get("duplicates")
                  + ", misplaced " + check.get("misplaced") + ", missing " + check.get("missing")
                  + ")");
    }
  }

  /**
   * Append Blob clients with their own pipeline: Iceberg's credentials and endpoint ({@link
   * AzureProperties}, as {@link AdlsClients} uses), but one try per request and a connection pool
   * as large as the writers need, so no writer waits for a connection.
   */
  static final class AppendBlobFactory {
    private final DataLakeFileSystemClient fileSystem;

    AppendBlobFactory(String root, String endpoint, String accountKey, int connections) {
      AdlsClients.Location l = AdlsClients.Location.of(root);
      AzureProperties azure = new AzureProperties(AdlsClients.properties(root, endpoint, accountKey));
      HttpClient http =
          HttpClient.createDefault(
              new HttpClientOptions().setMaximumConnectionPoolSize(Math.max(16, connections)));
      DataLakeFileSystemClientBuilder b =
          new DataLakeFileSystemClientBuilder()
              .httpClient(http)
              .retryOptions(
                  new RequestRetryOptions(
                      RetryPolicyType.EXPONENTIAL, 1, Duration.ofSeconds(60), null, null, null))
              .fileSystemName(l.container());
      azure.applyClientConfiguration(l.host(), b);
      this.fileSystem = b.buildClient();
    }

    AppendBlobClient appendBlob(String location) {
      String path = AdlsClients.Location.of(location).path();
      String dfsUrl = fileSystem.getFileClient(path).getFileUrl();
      // an emulator's single endpoint serves both APIs and has no ".dfs." in its host
      String blobUrl = dfsUrl.replaceFirst("^(https?://[^/]*?)\\.dfs\\.", "$1.blob.");
      return new SpecializedBlobClientBuilder()
          .pipeline(fileSystem.getHttpPipeline())
          .endpoint(blobUrl)
          .buildAppendBlobClient();
    }
  }
}
