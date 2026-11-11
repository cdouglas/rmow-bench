package org.rmow.bench.micro;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.LeaseDurationType;
import com.azure.storage.blob.models.LeaseStateType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobAcquireLeaseOptions;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.options.AppendBlobSealOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rmow.bench.io.AdlsClients;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Does Append Blob Seal work on this account? Question Q1 of fileio-catalog's docs/AZURE_LOG.md,
 * whose log moves to a new segment by sealing the last one: on an account with a hierarchical
 * namespace, the REST reference documents Seal and {@code 409 BlobIsSealed}, but its table of
 * account kinds doesn't list such accounts.
 *
 * <pre>
 * AzureSealProbe root [--out results/spike] [--label azure-seal] [--endpoint url] [--keep]
 * </pre>
 *
 * <p>One writer, in order, on two Append Blobs beside {@code root} (no new directory): a segment
 * with three 1 KiB blocks, and an empty one. It checks what the log relies on, each step against
 * its expected status and error code:
 *
 * <ul>
 *   <li>an append past {@code x-ms-blob-condition-maxsize} fails with {@code 412
 *       MaxBlobSizeConditionNotMet}, and nothing lands;
 *   <li>Seal returns 200; then Get Blob Properties, Get Blob and a ranged Get Blob say {@code
 *       x-ms-blob-sealed: true}, with the length and bytes from before the seal, and a listing
 *       says the blob is sealed;
 *   <li>an append after the seal fails with {@code 409 BlobIsSealed}, blind or at the right
 *       position, and lands nothing;
 *   <li>a second Seal returns 200, also with the right {@code x-ms-blob-condition-appendpos};
 *   <li>a create with {@code If-None-Match: *} of the sealed blob fails with {@code 409
 *       BlobAlreadyExists};
 *   <li>an empty blob seals, with length 0;
 *   <li>a Seal of a blob that doesn't exist fails with {@code 404 BlobNotFound}, creating nothing.
 * </ul>
 *
 * <p>Two steps have no expectation and are recorded only: an append after the seal at a wrong
 * position, and a Seal at a wrong position (which of the two conditions the service reports).
 *
 * <p>{@code --fences lease} probes the other fence the log could use, where Seal is missing (a
 * hierarchical namespace: the service doesn't know {@code comp=seal}): an infinite lease under an
 * id derived from the blob's name, which no appender sends. On a third and fourth blob:
 *
 * <ul>
 *   <li>Acquire Lease returns 201; Get Blob Properties then says {@code leased}, {@code
 *       infinite}, with the length from before;
 *   <li>an append without the lease id fails with {@code 412 LeaseIdMissing}, blind or at the
 *       right position, and lands nothing; on an empty blob too;
 *   <li>an Acquire under another id fails with {@code 409 LeaseAlreadyPresent};
 *   <li>Get Blob, whole and ranged, returns the bytes from before;
 *   <li>a Put Blob without conditions (an overwrite) and a Delete without the id fail with {@code
 *       412 LeaseIdMissing}; a Delete with the id deletes;
 *   <li>an Acquire on a blob that doesn't exist fails with {@code 404 BlobNotFound}.
 * </ul>
 *
 * <p>Recorded only: an append without the id at a wrong position, an Acquire again under the same
 * id, and a create with {@code If-None-Match: *} of the leased blob.
 *
 * <p>{@code --fences fill} probes a fence made of Append Block's own size condition, under
 * concurrency: {@code --writers} threads append 1 KiB blocks blind, each with {@code
 * x-ms-blob-condition-maxsize} W = L - 16, until one fails; a rotator then pads the blob to exactly
 * L (an append of L - length at that length, with maxsize L), after which no writer's append can
 * fit. Even reps pad as soon as the blob is half full, racing the writers; odd reps pad once a
 * writer has found the blob full. Each rep is one record: the appends acknowledged, the pad's
 * offset and tries, the writers' failures, the blob's final length and whether its bytes are the
 * acknowledged blocks then the pad. It is ok when the final length is L, the pad ends there, and
 * no acknowledged block lies after the pad. With {@code --pace-ms P}, each writer waits a random 0
 * to 2P ms between appends, so that a racing pad can land while the writers still fit, and the
 * writers keep appending until a second after the pad landed.
 *
 * <p>Writes {@code <out>/<label>-<UTC start>/}: {@code manifest.json} (with {@code ok}, whether
 * every step met its expectation) and {@code seal.jsonl}, one record per step (step, status,
 * code, the {@code x-ms-blob-sealed} header, the append offset, the blob's length where a step
 * reads it, the {@code x-ms-lease-state} header, ms, the expectation and ok). Credentials as {@link AzureAppendSpike}'s, or the
 * account key in {@code AZURE_STORAGE_KEY}. The SDK tries each request once.
 */
public final class AzureSealProbe {
  private static final int BLOCK = 1024;
  private static final HttpHeaderName SEALED = HttpHeaderName.fromString("x-ms-blob-sealed");
  private static final HttpHeaderName OFFSET = HttpHeaderName.fromString("x-ms-blob-append-offset");
  private static final HttpHeaderName BLOCKS =
      HttpHeaderName.fromString("x-ms-blob-committed-block-count");
  /** No expectation: the step is recorded only. */
  private static final int ANY = -1;
  private static final HttpHeaderName LEASE = HttpHeaderName.fromString("x-ms-lease-state");

  private final List<Map<String, Object>> steps = new ArrayList<>();

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println(
          "usage: AzureSealProbe root [--out results/spike] [--label azure-seal] [--endpoint url]"
              + " [--fences seal,lease] [--keep]");
      System.exit(2);
    }
    String root = args[0].replaceAll("/+$", "");
    Path out = Path.of("results/spike");
    String label = "azure-seal";
    String endpoint = "";
    boolean keep = false;
    List<String> fences = List.of("seal", "lease");
    int writers = 16;
    int reps = 6;
    int limitKiB = 256;
    int paceMs = 0;
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--out" -> out = Path.of(args[++i]);
        case "--label" -> label = args[++i];
        case "--endpoint" -> endpoint = args[++i];
        case "--keep" -> keep = true;
        case "--fences" -> fences = List.of(args[++i].split(","));
        case "--writers" -> writers = Integer.parseInt(args[++i]);
        case "--reps" -> reps = Integer.parseInt(args[++i]);
        case "--limit-kib" -> limitKiB = Integer.parseInt(args[++i]);
        case "--pace-ms" -> paceMs = Integer.parseInt(args[++i]);
        default -> throw new IllegalArgumentException("unknown option " + args[i]);
      }
    }
    String started =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());
    Path dir = out.resolve(label + "-" + started);
    Files.createDirectories(dir);
    ObjectMapper json = new ObjectMapper();
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", label);
    manifest.put("kind", "seal");
    manifest.put("started", started);
    manifest.put("root", root);
    manifest.put("endpoint", endpoint);
    manifest.put("block_bytes", BLOCK);
    manifest.put("fences", fences);
    if (fences.contains("fill")) {
      manifest.put("writers", writers);
      manifest.put("reps", reps);
      manifest.put("limit_bytes", limitKiB * 1024L);
      manifest.put("pace_ms", paceMs);
    }
    manifest.put("sdk_tries", 1);
    manifest.put("host", InetAddress.getLocalHost().getHostName());
    manifest.put("java", System.getProperty("java.version"));
    json.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("manifest.json").toFile(), manifest);

    String key = System.getenv("AZURE_STORAGE_KEY");
    if (!endpoint.isEmpty()) {
      new AdlsClients(AdlsClients.properties(root, endpoint, key)).container(root).createIfNotExists();
    }
    AzureAppendSpike.AppendBlobFactory blobs =
        new AzureAppendSpike.AppendBlobFactory(root, endpoint, key, 2 * writers + 4);
    String stem =
        root + "/q1-" + started + "-" + Long.toHexString(ThreadLocalRandom.current().nextLong());
    AppendBlobClient seg = blobs.appendBlob(stem + "-seg");
    AppendBlobClient empty = blobs.appendBlob(stem + "-empty");
    AppendBlobClient missing = blobs.appendBlob(stem + "-missing");
    AppendBlobClient leased = blobs.appendBlob(stem + "-lease");
    AppendBlobClient leasedEmpty = blobs.appendBlob(stem + "-lease-empty");
    manifest.put(
        "blobs",
        List.of(
            seg.getBlobUrl(),
            empty.getBlobUrl(),
            missing.getBlobUrl(),
            leased.getBlobUrl(),
            leasedEmpty.getBlobUrl()));

    AzureSealProbe p = new AzureSealProbe();
    try {
      if (fences.contains("seal")) {
        p.probe(seg, empty, missing);
      }
      if (fences.contains("lease")) {
        p.probeLease(leased, leasedEmpty, missing);
      }
      if (fences.contains("fill")) {
        p.probeFill(blobs, stem, writers, reps, limitKiB * 1024L, paceMs, keep);
      }
    } finally {
      if (!keep) {
        if (fences.contains("seal")) {
          p.delete("delete seg", seg);
          p.delete("delete empty", empty);
        }
        if (fences.contains("lease")) {
          p.deleteLeased("lease: delete with the id", leased);
          p.deleteLeased("lease: delete empty with the id", leasedEmpty);
        }
      }
      try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("seal.jsonl"))) {
        for (Map<String, Object> s : p.steps) {
          w.write(json.writeValueAsString(s));
          w.newLine();
        }
      }
      boolean ok = p.steps.stream().allMatch(s -> !Boolean.FALSE.equals(s.get("ok")));
      manifest.put("ok", ok);
      manifest.put("finished", Instant.now().toString());
      json.writerWithDefaultPrettyPrinter()
          .writeValue(dir.resolve("manifest.json").toFile(), manifest);
      for (Map<String, Object> s : p.steps) {
        if (s.containsKey("pad_at")) {
          System.out.printf(
              "%-26s acked=%d pad_at=%d pad_bytes=%d tries=%d failures=%s length=%d sent_after=%d"
                  + " acked_after=%d %s%n",
              s.get("step"), s.get("acked"), s.get("pad_at"), s.get("pad_bytes"), s.get("pad_tries"),
              s.get("failures"), s.get("length"), s.get("sent_after_pad"), s.get("acked_after_pad"),
              Boolean.TRUE.equals(s.get("ok")) ? "ok" : "FAILED " + s.get("problem"));
          continue;
        }
        System.out.printf(
            "%-34s %4s %-32s sealed=%-5s lease=%-6s len=%-5s %s%n",
            s.get("step"),
            s.get("status"),
            s.get("code") == null ? "" : s.get("code"),
            s.get("sealed_header"),
            s.get("lease_state"),
            s.get("length") == null ? "" : s.get("length"),
            s.get("ok") == null ? "(recorded)" : (Boolean.TRUE.equals(s.get("ok")) ? "ok" : "FAILED"));
      }
      System.out.println((ok ? "all expectations met: " : "EXPECTATIONS FAILED: ") + dir);
    }
  }

  private void probe(AppendBlobClient seg, AppendBlobClient empty, AppendBlobClient missing) {
    byte[][] block = new byte[3][BLOCK];
    for (int i = 0; i < 3; i++) {
      Arrays.fill(block[i], (byte) (i + 1));
    }
    byte[] all = new byte[3 * BLOCK];
    for (int i = 0; i < 3; i++) {
      System.arraycopy(block[i], 0, all, i * BLOCK, BLOCK);
    }
    long length = 3L * BLOCK;

    step("create seg", 201, null, () -> create(seg), null);
    for (int i = 0; i < 3; i++) {
      byte[] b = block[i];
      long at = (long) i * BLOCK;
      step("append " + i, 201, null, () -> append(seg, b, null), r -> offsetIs(r, at));
    }
    step("props before seal", 200, null, () -> props(seg), r -> propsAre(r, false, length));
    step(
        "append past maxsize",
        412,
        "MaxBlobSizeConditionNotMet",
        () -> append(seg, block[0], new AppendBlobRequestConditions().setMaxSize(length + BLOCK - 1)),
        null);
    step("props after maxsize", 200, null, () -> props(seg), r -> propsAre(r, false, length));

    step("seal", 200, null, () -> seal(seg, null), null);
    step("props after seal", 200, null, () -> props(seg), r -> propsAre(r, true, length));
    step("get whole", 200, null, () -> get(seg, null), r -> bodyIs(r, all));
    step(
        "get ranged (block 1)",
        206,
        null,
        () -> get(seg, new BlobRange(BLOCK, (long) BLOCK)),
        r -> bodyIs(r, block[1]));
    step("list", 200, null, () -> listed(seg), null);

    step("append blind after seal", 409, "BlobIsSealed", () -> append(seg, block[0], null), null);
    step(
        "append at its end after seal",
        409,
        "BlobIsSealed",
        () -> append(seg, block[0], new AppendBlobRequestConditions().setAppendPosition(length)),
        null);
    step(
        "append at 0 after seal",
        ANY,
        null,
        () -> append(seg, block[0], new AppendBlobRequestConditions().setAppendPosition(0L)),
        null);
    step("seal again", 200, null, () -> seal(seg, null), null);
    step(
        "seal at its end",
        200,
        null,
        () -> seal(seg, new AppendBlobRequestConditions().setAppendPosition(length)),
        null);
    step(
        "seal at 0",
        ANY,
        null,
        () -> seal(seg, new AppendBlobRequestConditions().setAppendPosition(0L)),
        null);
    step("props at the end", 200, null, () -> props(seg), r -> propsAre(r, true, length));
    step("create sealed again", 409, "BlobAlreadyExists", () -> create(seg), null);

    step("create empty", 201, null, () -> create(empty), null);
    step("seal empty", 200, null, () -> seal(empty, null), null);
    step("props empty", 200, null, () -> props(empty), r -> propsAre(r, true, 0));
    step("append to sealed empty", 409, "BlobIsSealed", () -> append(empty, block[0], null), null);

    step("seal missing", 404, "BlobNotFound", () -> seal(missing, null), null);
    step("props missing", 404, null, () -> props(missing), null);
  }

  private void probeLease(AppendBlobClient blob, AppendBlobClient empty, AppendBlobClient missing) {
    byte[][] block = new byte[2][BLOCK];
    for (int i = 0; i < 2; i++) {
      Arrays.fill(block[i], (byte) (i + 1));
    }
    byte[] all = new byte[2 * BLOCK];
    System.arraycopy(block[0], 0, all, 0, BLOCK);
    System.arraycopy(block[1], 0, all, BLOCK, BLOCK);
    long length = 2L * BLOCK;
    String id = leaseId(blob);

    step("lease: create", 201, null, () -> create(blob), null);
    for (int i = 0; i < 2; i++) {
      byte[] b = block[i];
      long at = (long) i * BLOCK;
      step("lease: append " + i, 201, null, () -> append(blob, b, null), r -> offsetIs(r, at));
    }
    step("lease: acquire", 201, null, () -> acquire(blob, id), null);
    step("lease: props", 200, null, () -> props(blob), r -> leasedProps(r, length));
    step(
        "lease: append blind",
        412,
        "LeaseIdMissing",
        () -> append(blob, block[0], null),
        null);
    step(
        "lease: append at its end",
        412,
        "LeaseIdMissing",
        () -> append(blob, block[0], new AppendBlobRequestConditions().setAppendPosition(length)),
        null);
    step(
        "lease: append at 0",
        ANY,
        null,
        () -> append(blob, block[0], new AppendBlobRequestConditions().setAppendPosition(0L)),
        null);
    step("lease: acquire again, same id", ANY, null, () -> acquire(blob, id), null);
    step(
        "lease: acquire, another id",
        409,
        "LeaseAlreadyPresent",
        () -> acquire(blob, UUID.randomUUID().toString()),
        null);
    step("lease: get whole", 200, null, () -> get(blob, null), r -> leasedBodyIs(r, all));
    step(
        "lease: get ranged (block 1)",
        206,
        null,
        () -> get(blob, new BlobRange(BLOCK, (long) BLOCK)),
        r -> leasedBodyIs(r, block[1]));
    step("lease: props at the end", 200, null, () -> props(blob), r -> leasedProps(r, length));
    step("lease: create again", ANY, null, () -> create(blob), null);
    step(
        "lease: overwrite",
        412,
        "LeaseIdMissing",
        () -> blob.createWithResponse(new AppendBlobCreateOptions(), null, Context.NONE),
        null);
    step(
        "lease: delete without the id",
        412,
        "LeaseIdMissing",
        () -> blob.deleteWithResponse(null, null, null, Context.NONE),
        null);
    step("lease: props after all", 200, null, () -> props(blob), r -> leasedProps(r, length));

    step("lease: create empty", 201, null, () -> create(empty), null);
    step("lease: acquire empty", 201, null, () -> acquire(empty, leaseId(empty)), null);
    step(
        "lease: append to empty",
        412,
        "LeaseIdMissing",
        () -> append(empty, block[0], null),
        null);
    step("lease: props empty", 200, null, () -> props(empty), r -> leasedProps(r, 0));

    step("lease: acquire missing", 404, "BlobNotFound", () -> acquire(missing, leaseId(missing)), null);
    step("lease: props missing", 404, null, () -> props(missing), null);
  }

  /** The pad's smallest size: a writer's limit is this far below the segment's. */
  private static final int PAD_MIN = 16;

  private void probeFill(
      AzureAppendSpike.AppendBlobFactory blobs,
      String stem,
      int writers,
      int reps,
      long limit,
      int paceMs,
      boolean keep)
      throws InterruptedException {
    long writerLimit = limit - PAD_MIN;
    byte[] blk = new byte[BLOCK];
    Arrays.fill(blk, (byte) 7);
    for (int rep = 0; rep < reps; rep++) {
      boolean early = rep % 2 == 0;
      String name = "fill " + rep + (early ? " (racing)" : " (after full)");
      AppendBlobClient b = blobs.appendBlob(stem + "-fill-" + rep);
      step(name + ": create", 201, null, () -> create(b), null);
      ConcurrentLinkedQueue<Long> acked = new ConcurrentLinkedQueue<>();
      ConcurrentHashMap<String, AtomicInteger> failures = new ConcurrentHashMap<>();
      CountDownLatch firstFull = new CountDownLatch(1);
      ExecutorService pool = Executors.newFixedThreadPool(writers);
      CountDownLatch done = new CountDownLatch(writers);
      AtomicBoolean stop = new AtomicBoolean();
      AtomicInteger sentAfterPad = new AtomicInteger();
      AtomicBoolean padLanded = new AtomicBoolean();
      for (int w = 0; w < writers; w++) {
        pool.submit(
            () -> {
              try {
                while (!stop.get()) {
                  if (paceMs > 0) {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(2 * paceMs + 1));
                  }
                  if (padLanded.get()) {
                    sentAfterPad.incrementAndGet();
                  }
                  try {
                    Response<AppendBlobItem> r =
                        append(b, blk, new AppendBlobRequestConditions().setMaxSize(writerLimit));
                    acked.add(Long.parseLong(r.getHeaders().getValue(OFFSET)));
                  } catch (BlobStorageException e) {
                    String code = e.getErrorCode() == null ? "" + e.getStatusCode() : e.getErrorCode().toString();
                    failures.computeIfAbsent(code, k -> new AtomicInteger()).incrementAndGet();
                    firstFull.countDown();
                    if (paceMs == 0 || !code.equals("MaxBlobSizeConditionNotMet")) {
                      return;
                    }
                  } catch (RuntimeException e) {
                    failures
                        .computeIfAbsent(e.getClass().getSimpleName(), k -> new AtomicInteger())
                        .incrementAndGet();
                    return;
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      Map<String, Object> s = new LinkedHashMap<>();
      s.put("step", name);
      long t0 = System.nanoTime();
      String problem = null;
      long padAt = -1;
      long padBytes = 0;
      int padTries = 0;
      try {
        if (early) {
          while (b.getProperties().getBlobSize() < limit / 2 && firstFull.getCount() > 0) {
            Thread.sleep(5);
          }
        } else {
          firstFull.await(120, TimeUnit.SECONDS);
        }
        while (padAt < 0 && padTries < 1000) {
          long length = b.getProperties().getBlobSize();
          if (length > writerLimit) {
            problem = "the blob passed the writers' limit without the pad: " + length;
            break;
          }
          byte[] pad = new byte[(int) (limit - length)];
          Arrays.fill(pad, (byte) 0x50);
          padTries++;
          try {
            Response<AppendBlobItem> r =
                append(
                    b,
                    pad,
                    new AppendBlobRequestConditions().setAppendPosition(length).setMaxSize(limit));
            padAt = Long.parseLong(r.getHeaders().getValue(OFFSET));
            padBytes = pad.length;
            padLanded.set(true);
          } catch (BlobStorageException e) {
            if (e.getErrorCode() == null
                || !e.getErrorCode().toString().equals("AppendPositionConditionNotMet")) {
              problem = "pad failed: " + e.getStatusCode() + " " + e.getErrorCode();
              break;
            }
          }
        }
        if (paceMs > 0) {
          Thread.sleep(1000);
        }
        stop.set(true);
        if (!done.await(120, TimeUnit.SECONDS)) {
          problem = (problem == null ? "" : problem + "; ") + "writers still running";
        }
      } finally {
        pool.shutdownNow();
      }
      long finalLength = b.getProperties().getBlobSize();
      ByteArrayOutputStream body = new ByteArrayOutputStream();
      b.downloadStream(body);
      byte[] bytes = body.toByteArray();
      long maxAckEnd = acked.stream().mapToLong(o -> o + BLOCK).max().orElse(0);
      long padOffset = padAt;
      long afterPad = padOffset < 0 ? 0 : acked.stream().filter(o -> o >= padOffset).count();
      if (problem == null) {
        if (padAt < 0) {
          problem = "no pad landed in " + padTries + " tries";
        } else if (finalLength != limit) {
          problem = "final length " + finalLength + ", expected " + limit;
        } else if (padAt + padBytes != limit) {
          problem = "pad ends at " + (padAt + padBytes);
        } else if (afterPad > 0 || maxAckEnd > padAt) {
          problem = afterPad + " acknowledged blocks after the pad";
        } else if (acked.size() * (long) BLOCK != padAt) {
          problem = acked.size() + " blocks acknowledged before a pad at " + padAt;
        } else {
          for (int i = 0; i < bytes.length && problem == null; i++) {
            byte want = i < padAt ? (byte) 7 : (byte) 0x50;
            if (bytes[i] != want) {
              problem = "byte " + i + " is " + bytes[i];
            }
          }
        }
      }
      s.put("ms", (System.nanoTime() - t0) / 1e6);
      s.put("writers", writers);
      s.put("limit", limit);
      s.put("writer_limit", writerLimit);
      s.put("acked", acked.size());
      s.put("pad_at", padAt);
      s.put("pad_bytes", padBytes);
      s.put("pad_tries", padTries);
      s.put("failures", failures);
      s.put("length", finalLength);
      s.put("acked_after_pad", afterPad);
      s.put("sent_after_pad", sentAfterPad.get());
      s.put("ok", problem == null);
      s.put("problem", problem);
      steps.add(s);
      if (!keep) {
        delete(name + ": delete", b);
      }
    }
  }

  /** The lease id a fence uses: derived from the blob's name, so whoever fences it knows it. */
  private static String leaseId(AppendBlobClient b) {
    return UUID.nameUUIDFromBytes(b.getBlobUrl().getBytes(StandardCharsets.UTF_8)).toString();
  }

  private static Response<String> acquire(AppendBlobClient b, String id) {
    return new BlobLeaseClientBuilder()
        .blobClient(b)
        .leaseId(id)
        .buildClient()
        .acquireLeaseWithResponse(new BlobAcquireLeaseOptions(-1), null, Context.NONE);
  }

  private void deleteLeased(String name, AppendBlobClient b) {
    step(
        name,
        202,
        null,
        () ->
            b.deleteWithResponse(
                null, new BlobRequestConditions().setLeaseId(leaseId(b)), null, Context.NONE),
        null);
  }

  private static String leasedProps(Response<?> r, long length) {
    BlobProperties p = (BlobProperties) r.getValue();
    if (p.getLeaseState() != LeaseStateType.LEASED
        || p.getLeaseDuration() != LeaseDurationType.INFINITE) {
      return "lease " + p.getLeaseState() + " " + p.getLeaseDuration() + ", expected leased infinite";
    }
    return p.getBlobSize() == length ? null : "length " + p.getBlobSize() + ", expected " + length;
  }

  private static String leasedBodyIs(Response<?> r, byte[] expected) {
    byte[] body = (byte[]) r.getValue();
    if (!Arrays.equals(body, expected)) {
      return "body of " + body.length + " bytes differs from the " + expected.length + " expected";
    }
    return "leased".equals(r.getHeaders().getValue(LEASE)) ? null : "no x-ms-lease-state: leased";
  }

  private static Response<AppendBlobItem> create(AppendBlobClient b) {
    return b.createWithResponse(
        new AppendBlobCreateOptions().setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*")),
        null,
        Context.NONE);
  }

  private static Response<AppendBlobItem> append(
      AppendBlobClient b, byte[] bytes, AppendBlobRequestConditions conditions) {
    return b.appendBlockWithResponse(
        new ByteArrayInputStream(bytes), bytes.length, null, conditions, null, Context.NONE);
  }

  private static Response<Void> seal(AppendBlobClient b, AppendBlobRequestConditions conditions) {
    AppendBlobSealOptions o = new AppendBlobSealOptions();
    if (conditions != null) {
      o.setRequestConditions(conditions);
    }
    return b.sealWithResponse(o, null, Context.NONE);
  }

  private static Response<BlobProperties> props(AppendBlobClient b) {
    return b.getPropertiesWithResponse(null, null, Context.NONE);
  }

  /** A ranged or whole Get Blob, its body kept on the response's value. */
  private static Response<byte[]> get(AppendBlobClient b, BlobRange range) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    BlobDownloadResponse r =
        b.downloadStreamWithResponse(body, range, null, null, false, null, Context.NONE);
    return new SimpleResponse<>(r.getStatusCode(), r.getHeaders(), body.toByteArray());
  }

  /** The listing's entry for the blob: whether it says the blob is sealed. */
  private static Response<Boolean> listed(AppendBlobClient b) {
    String name = b.getBlobName();
    for (BlobItem item :
        b.getContainerClient().listBlobs(new ListBlobsOptions().setPrefix(name), null)) {
      if (item.getName().equals(name)) {
        Boolean sealed = item.getProperties().isSealed();
        HttpHeaders h = new HttpHeaders();
        h.set(SEALED, String.valueOf(sealed));
        return new SimpleResponse<>(200, h, sealed);
      }
    }
    return new SimpleResponse<>(404, new HttpHeaders(), null);
  }

  private void delete(String name, AppendBlobClient b) {
    step(name, 202, null, () -> b.deleteWithResponse(null, null, null, Context.NONE), null);
  }

  private static String offsetIs(Response<?> r, long at) {
    String o = r.getHeaders().getValue(OFFSET);
    return String.valueOf(at).equals(o) ? null : "offset " + o + ", expected " + at;
  }

  private static String propsAre(Response<?> r, boolean sealed, long length) {
    BlobProperties p = (BlobProperties) r.getValue();
    boolean isSealed = Boolean.TRUE.equals(p.isSealed());
    if (isSealed != sealed) {
      return "isSealed " + p.isSealed() + ", expected " + sealed;
    }
    return p.getBlobSize() == length ? null : "length " + p.getBlobSize() + ", expected " + length;
  }

  private static String bodyIs(Response<?> r, byte[] expected) {
    byte[] body = (byte[]) r.getValue();
    if (!Arrays.equals(body, expected)) {
      return "body of " + body.length + " bytes differs from the " + expected.length + " expected";
    }
    return "true".equals(r.getHeaders().getValue(SEALED)) ? null : "no x-ms-blob-sealed: true";
  }

  /**
   * Runs one step and records it. {@code check} says what is wrong with a successful response
   * (null if nothing); {@code expect} is the expected status, or {@link #ANY}.
   */
  private void step(
      String name,
      int expect,
      String expectCode,
      Supplier<Response<?>> call,
      Function<Response<?>, String> check) {
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("step", name);
    long t0 = System.nanoTime();
    Integer status;
    String code = null;
    HttpHeaders headers;
    String problem = null;
    try {
      Response<?> r = call.get();
      status = r.getStatusCode();
      headers = r.getHeaders();
      if (r.getValue() instanceof BlobProperties p) {
        s.put("length", p.getBlobSize());
        s.put("is_sealed", p.isSealed());
      } else if (r.getValue() instanceof byte[] body) {
        s.put("length", body.length);
      } else if (r.getValue() instanceof Boolean listedSealed) {
        s.put("is_sealed", listedSealed);
      }
      if (check != null) {
        problem = check.apply(r);
      }
    } catch (BlobStorageException e) {
      status = e.getStatusCode();
      code = e.getErrorCode() == null ? null : e.getErrorCode().toString();
      headers = e.getResponse() == null ? new HttpHeaders() : e.getResponse().getHeaders();
    } catch (RuntimeException e) {
      status = null;
      code = e.getClass().getSimpleName() + ": " + e.getMessage();
      headers = new HttpHeaders();
    }
    s.put("ms", (System.nanoTime() - t0) / 1e6);
    s.put("status", status);
    s.put("code", code);
    s.put("sealed_header", headers.getValue(SEALED));
    s.put("append_offset", headers.getValue(OFFSET));
    s.put("blocks", headers.getValue(BLOCKS));
    s.put("lease_state", headers.getValue(LEASE));
    s.put("request_id", headers.getValue(HttpHeaderName.X_MS_REQUEST_ID));
    if (expect == ANY) {
      s.put("ok", null);
    } else {
      s.put("expect_status", expect);
      s.put("expect_code", expectCode);
      boolean ok =
          status != null
              && status == expect
              && (expectCode == null || expectCode.equals(code))
              && problem == null;
      s.put("ok", ok);
    }
    s.put("problem", problem);
    steps.add(s);
  }

  /** A response with a value the probe made (a body, a listing's answer). */
  private record SimpleResponse<T>(int getStatusCode, HttpHeaders getHeaders, T getValue)
      implements Response<T> {
    @Override
    public com.azure.core.http.HttpRequest getRequest() {
      return null;
    }
  }
}
