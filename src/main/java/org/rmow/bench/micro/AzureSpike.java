package org.rmow.bench.micro;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.append.AzureAppenders;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.io.AdlsClients;

/**
 * What Azure's requests cost, for the latency model's {@code azure} profile (docs/AZURE_RUNBOOK.md);
 * the Azure counterpart of {@link S3ExpressSpike}. {@code AzureTest} checks the semantics.
 *
 * <pre>
 *   java -cp ... org.rmow.bench.micro.AzureSpike abfss://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;prefix&gt;
 *       [--reps n] [--out dir] [--endpoint url]
 * </pre>
 *
 * <ul>
 *   <li><b>put</b>, <b>append</b>: for sizes 64 KiB to 32 MiB, doubling, {@code reps} Put Blobs of
 *       new block blobs (Iceberg's writes, up to 256 MiB) and {@code reps} Append Blocks to one
 *       Append Blob (RMoW's appends);
 *   <li><b>create</b> (an empty Append Blob) and <b>head</b>, {@code reps} each;
 *   <li><b>get_full</b>, <b>get_tail</b> and <b>get_mid</b>: Append Blobs of 1, 10, 100 and 1000
 *       blocks of 64 KiB, read whole, as their last 64 KiB, and as 64 KiB from the middle;
 *   <li><b>iceberg_open</b> and <b>iceberg_mid</b>: what Iceberg 1.12's ADLSInputStream sends for
 *       the same reads of a 32 MiB file: opening a stream (a GET of the first 4 MiB) and a ranged
 *       read of 64 KiB at the middle (a GET of 4 MiB), against <b>get_mid_32m</b>, the exact read.
 * </ul>
 *
 * Writes {@code manifest.json} and {@code spike.jsonl} (one record per request: op, bytes, parts,
 * rep, ns) to {@code <out>/azure-spike-<UTC start>/} (default out: {@code results/spike}), and
 * deletes its objects. Run it from a VM in the account's region; elsewhere it measures the
 * network. {@code --endpoint} runs it against an emulator (with {@code AZURE_STORAGE_KEY}), which
 * checks the code, not Azure.
 */
public final class AzureSpike {

  private static final int KIB = 1024;
  private static final int PART = 64 * KIB;

  private final AdlsClients clients;
  private final ADLSFileIO adls;
  private final String prefix;
  private final int reps;
  private final BufferedWriter out;
  private final ObjectMapper json = new ObjectMapper();
  private final SplittableRandom random = new SplittableRandom(1);
  private int objects = 0;

  private AzureSpike(
      AdlsClients clients, ADLSFileIO adls, String prefix, int reps, BufferedWriter out) {
    this.clients = clients;
    this.adls = adls;
    this.prefix = prefix;
    this.reps = reps;
    this.out = out;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || !args[0].startsWith("abfs")) {
      System.err.println(
          "usage: AzureSpike abfss://<container>@<account>.dfs.core.windows.net/<prefix>"
              + " [--reps n] [--out dir] [--endpoint url]");
      System.exit(2);
    }
    String started =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(Instant.now());
    String prefix = args[0] + "/spike-" + started;
    int reps = 10;
    Path outRoot = Path.of("results", "spike");
    String endpoint = "";
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--reps" -> reps = Integer.parseInt(args[++i]);
        case "--out" -> outRoot = Path.of(args[++i]);
        case "--endpoint" -> endpoint = args[++i];
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }

    Path dir = outRoot.resolve("azure-spike-" + started);
    Files.createDirectories(dir);
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", "azure-spike");
    manifest.put("started", started);
    manifest.put("root", args[0]);
    manifest.put("endpoint", endpoint);
    manifest.put("reps", reps);
    manifest.put("part_bytes", PART);
    manifest.put("host", InetAddress.getLocalHost().getHostName());
    manifest.put("java", System.getProperty("java.version"));
    ObjectMapper json = new ObjectMapper();
    json.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("manifest.json").toFile(), manifest);

    String key = endpoint.isEmpty() ? null : System.getenv("AZURE_STORAGE_KEY");
    Map<String, String> props = AdlsClients.properties(args[0], endpoint, key);
    AdlsClients clients = new AdlsClients(props);
    if (!endpoint.isEmpty()) {
      clients.container(args[0]).createIfNotExists();
    }
    ADLSFileIO adls = new ADLSFileIO();
    adls.initialize(props);
    try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("spike.jsonl"))) {
      AzureSpike spike = new AzureSpike(clients, adls, prefix, reps, w);
      try {
        spike.putsAndAppends();
        spike.createsAndHeads();
        spike.getsByBlocks();
        spike.icebergReads();
      } finally {
        spike.deleteAll(endpoint.isEmpty());
        adls.close();
      }
    }
    manifest.put("finished", Instant.now().toString());
    json.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("manifest.json").toFile(), manifest);
    System.out.println(dir);
  }

  private byte[] payload(int n) {
    byte[] b = new byte[n];
    random.nextBytes(b); // incompressible, as compressed Parquet pages are
    return b;
  }

  private String location(String name) {
    return prefix + "/" + name + "-" + objects++;
  }

  private void record(String op, long bytes, int parts, int rep, long ns) throws IOException {
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("op", op);
    r.put("bytes", bytes);
    r.put("parts", parts);
    r.put("rep", rep);
    r.put("ns", ns);
    out.write(json.writeValueAsString(r));
    out.newLine();
  }

  private void putsAndAppends() throws IOException {
    RegionAppender appender = AzureAppenders.appendBlob(clients);
    for (int size = PART; size <= 32 * KIB * KIB; size *= 2) {
      byte[] body = payload(size);
      for (int i = 0; i < reps; i++) {
        String loc = location("put-" + size);
        long t = System.nanoTime();
        clients.blockBlob(loc).upload(BinaryData.fromBytes(body), true);
        record("put", size, 1, i, System.nanoTime() - t);
      }
      String loc = location("append-" + size);
      appender.create(adls, loc, body);
      long length = size;
      for (int i = 0; i < reps; i++) {
        long t = System.nanoTime();
        length = appender.append(loc, length, body);
        record("append", size, i + 2, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  private void createsAndHeads() throws IOException {
    for (int i = 0; i < reps; i++) {
      AppendBlobClient blob = clients.appendBlob(location("create"));
      long t = System.nanoTime();
      blob.create(true);
      record("create", 0, 0, i, System.nanoTime() - t);
      t = System.nanoTime();
      blob.getProperties();
      record("head", 0, 0, i, System.nanoTime() - t);
    }
    out.flush();
  }

  private byte[] get(String location, long offset, long count) {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    clients
        .appendBlob(location)
        .downloadStreamWithResponse(
            b, new BlobRange(offset, count), null, null, false, null, Context.NONE);
    return b.toByteArray();
  }

  private void getsByBlocks() throws IOException {
    RegionAppender appender = AzureAppenders.appendBlob(clients);
    byte[] part = payload(PART);
    for (int parts : new int[] {1, 10, 100, 1000}) {
      String loc = location("parts-" + parts);
      appender.create(adls, loc, part);
      long length = PART;
      for (int i = 1; i < parts; i++) {
        length = appender.append(loc, length, part);
      }
      for (int i = 0; i < reps; i++) {
        long t = System.nanoTime();
        int n = get(loc, 0, length).length;
        record("get_full", n, parts, i, System.nanoTime() - t);

        t = System.nanoTime();
        n = get(loc, length - PART, PART).length;
        record("get_tail", n, parts, i, System.nanoTime() - t);

        long mid = length / 2;
        t = System.nanoTime();
        n = get(loc, mid, PART).length;
        record("get_mid", n, parts, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  private void icebergReads() throws IOException {
    int size = 32 * KIB * KIB;
    String loc = location("iceberg");
    try (OutputStream o = adls.newOutputFile(loc).createOrOverwrite()) {
      o.write(payload(size));
    }
    byte[] b = new byte[PART];
    for (int i = 0; i < reps; i++) {
      long t = System.nanoTime();
      try (SeekableInputStream in = adls.newInputFile(loc, size).newStream()) {
        record("iceberg_open", 4 * KIB * KIB, 1, i, System.nanoTime() - t);
        t = System.nanoTime();
        ((RangeReadable) in).readFully(size / 2, b, 0, b.length);
        record("iceberg_mid", 4 * KIB * KIB, 1, i, System.nanoTime() - t);
      }
      t = System.nanoTime();
      int n = get(loc, size / 2, PART).length;
      record("get_mid_32m", n, 1, i, System.nanoTime() - t);
    }
    out.flush();
  }

  private void deleteAll(boolean azure) {
    if (azure) {
      adls.deletePrefix(prefix); // one recursive directory delete
      return;
    }
    BlobContainerClient container = clients.container(prefix);
    String path = AdlsClients.Location.of(prefix).path() + "/";
    for (BlobItem item : container.listBlobs(new ListBlobsOptions().setPrefix(path), null)) {
      container.getBlobClient(item.getName()).delete();
    }
  }
}
