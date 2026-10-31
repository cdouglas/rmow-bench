package org.rmow.bench.micro;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.SplittableRandom;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.append.EmulatorAppenders;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.io.AdlsClients;

/**
 * What Azure's requests cost, for the latency model's {@code azure} profile (docs/AZURE_RUNBOOK.md);
 * the Azure counterpart of {@link S3ExpressSpike}. {@code AzureTest} checks the semantics.
 *
 * <pre>
 *   java -cp ... org.rmow.bench.micro.AzureSpike abfss://&lt;container&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;prefix&gt;
 *       [--reps n] [--out dir] [--label l] [--endpoint url]
 * </pre>
 *
 * <ul>
 *   <li><b>put</b>, <b>append</b> and <b>append_sao</b>: for sizes 64 KiB to 128 MiB (a row group
 *       at Iceberg's default target), doubling, {@code reps} each, interleaved: Put Blobs of new
 *       block blobs (Iceberg's writes, up to 256 MiB); the store's own appends, Append Blocks
 *       conditioned on the blob's length alone; and RMoW's, the fork's APPEND ({@link
 *       AtomicAppender}), an Append Block conditioned on the length and the ETag, each to its own
 *       Append Blob. An Append Block takes at most 100 MiB, so a 128 MiB append is two, in order,
 *       on both (parts: the blob's blocks after it);
 *   <li><b>create</b> (an empty Append Blob), <b>create_own</b> (an empty Append Blob and an
 *       Append Block of 64 KiB, the store's own way to create an appendable file),
 *       <b>create_sao</b> (the fork's atomic create of 64 KiB: a staged Append Blob, an Append Block
 *       and a rename; not on an emulator, which cannot rename) and <b>head</b>, {@code reps} each;
 *   <li><b>get_size</b> and <b>get_range</b>: GETs by size, 64 KiB to 128 MiB, doubling, for the
 *       read rate per MiB: the block blobs the Put Blobs above wrote, read whole, and the same
 *       sizes read as a range from the middle of a 128 MiB one (as a row group is read), {@code
 *       reps} each, interleaved over the sizes;
 *   <li><b>get_full</b>, <b>get_tail</b> and <b>get_mid</b>: Append Blobs of 1, 10, 100 and 1000
 *       blocks of 64 KiB, read whole, as their last 64 KiB, and as 64 KiB from the middle;
 *   <li><b>iceberg_open</b> and <b>iceberg_mid</b>: Iceberg's reads of a 32 MiB file through
 *       ADLSFileIO, opening a stream (no request on the fork) and a ranged read of 64 KiB at the
 *       middle, against <b>get_mid_32m</b>, the same read through the SDK alone.
 * </ul>
 *
 * Writes {@code manifest.json} and {@code spike.jsonl} (one record per request: op, bytes, parts,
 * rep, ns) to {@code <out>/<label>-<UTC start>/} (default out: {@code results/spike}, label:
 * {@code azure-spike}; a label of its own keeps apart spikes that start in the same second), and
 * deletes its objects. A GET's body is drained into a buffer and dropped, so its time is the
 * request's, not a copy's. Run it from a VM in the account's region; elsewhere it measures the
 * network. {@code --endpoint} runs it against an emulator (with {@code AZURE_STORAGE_KEY}), which
 * checks the code, not Azure.
 */
public final class AzureSpike {

  private static final int KIB = 1024;
  private static final int PART = 64 * KIB;
  private static final int LARGEST = 128 * KIB * KIB;

  private final AdlsClients clients;
  private final ADLSFileIO adls;
  private final RegionAppender atomic; // RMoW's appender
  private final boolean emulator;
  private final String prefix;
  private final int reps;
  private final BufferedWriter out;
  private final ObjectMapper json = new ObjectMapper();
  private final SplittableRandom random = new SplittableRandom(1);
  private int objects = 0;
  // the Put Blobs' block blobs, by size, one per rep
  private final Map<Integer, List<String>> puts = new TreeMap<>();

  private AzureSpike(
      AdlsClients clients,
      ADLSFileIO adls,
      boolean emulator,
      String prefix,
      int reps,
      BufferedWriter out) {
    this.clients = clients;
    this.adls = adls;
    this.emulator = emulator;
    this.atomic =
        emulator ? EmulatorAppenders.appendBlobs(clients, adls) : new AtomicAppender(adls);
    this.prefix = prefix;
    this.reps = reps;
    this.out = out;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || !args[0].startsWith("abfs")) {
      System.err.println(
          "usage: AzureSpike abfss://<container>@<account>.dfs.core.windows.net/<prefix>"
              + " [--reps n] [--out dir] [--label l] [--endpoint url]");
      System.exit(2);
    }
    String started =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(Instant.now());
    String prefix = args[0] + "/spike-" + started;
    int reps = 10;
    Path outRoot = Path.of("results", "spike");
    String label = "azure-spike";
    String endpoint = "";
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--reps" -> reps = Integer.parseInt(args[++i]);
        case "--out" -> outRoot = Path.of(args[++i]);
        case "--label" -> label = args[++i];
        case "--endpoint" -> endpoint = args[++i];
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }

    Path dir = outRoot.resolve(label + "-" + started);
    Files.createDirectories(dir);
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", label);
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
      AzureSpike spike = new AzureSpike(clients, adls, !endpoint.isEmpty(), prefix, reps, w);
      try {
        spike.putsAndAppends();
        spike.getsBySize();
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

  /**
   * The store's own append: Append Blocks at {@code at}, each conditioned on its position alone,
   * one unless {@code body} is larger than an Append Block may be.
   */
  private static long appendBlock(AppendBlobClient blob, long at, byte[] body) {
    int max = blob.getMaxAppendBlockBytes();
    for (int off = 0; off < body.length; off += max) {
      int n = Math.min(max, body.length - off);
      blob.appendBlockWithResponse(
          new ByteArrayInputStream(body, off, n),
          n,
          null,
          new AppendBlobRequestConditions().setAppendPosition(at + off),
          null,
          Context.NONE);
    }
    return at + body.length;
  }

  /** The store's own appendable file: an empty Append Blob, then its Append Blocks. */
  private AppendBlobClient createOwn(String location, byte[] body) {
    AppendBlobClient blob = clients.appendBlob(location);
    blob.create(true);
    appendBlock(blob, 0, body);
    return blob;
  }

  private void putsAndAppends() throws IOException {
    for (int size = PART; size <= LARGEST; size *= 2) {
      byte[] body = payload(size);
      AppendBlobClient own = createOwn(location("append-" + size), body);
      String sao = location("append-sao-" + size);
      atomic.create(sao, body);
      long ownLength = size;
      long saoLength = size;
      int max = own.getMaxAppendBlockBytes();
      int blocks = (size + max - 1) / max; // the Append Blocks one append of this size takes
      for (int i = 0; i < reps; i++) {
        String loc = location("put-" + size);
        long t = System.nanoTime();
        clients.blockBlob(loc).upload(BinaryData.fromBytes(body), true);
        record("put", size, 1, i, System.nanoTime() - t);
        puts.computeIfAbsent(size, k -> new ArrayList<>()).add(loc);

        t = System.nanoTime();
        ownLength = appendBlock(own, ownLength, body);
        record("append", size, (i + 2) * blocks, i, System.nanoTime() - t);

        t = System.nanoTime();
        saoLength = atomic.append(sao, saoLength, body);
        record("append_sao", size, (i + 2) * blocks, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  private void createsAndHeads() throws IOException {
    byte[] body = payload(PART);
    for (int i = 0; i < reps; i++) {
      AppendBlobClient blob = clients.appendBlob(location("create"));
      long t = System.nanoTime();
      blob.create(true);
      record("create", 0, 0, i, System.nanoTime() - t);
      t = System.nanoTime();
      blob.getProperties();
      record("head", 0, 0, i, System.nanoTime() - t);

      String loc = location("create-own");
      t = System.nanoTime();
      createOwn(loc, body);
      record("create_own", PART, 1, i, System.nanoTime() - t);
      if (!emulator) {
        loc = location("create-sao");
        t = System.nanoTime();
        atomic.create(loc, body);
        record("create_sao", PART, 1, i, System.nanoTime() - t);
      }
    }
    out.flush();
  }

  private long get(String location, long offset, long count) {
    return get(clients.appendBlob(location), offset, count);
  }

  /** One GET of a range, its body dropped as it arrives; the bytes it delivered. */
  private static long get(BlobClientBase blob, long offset, long count) {
    Drain sink = new Drain();
    blob.downloadStreamWithResponse(
        sink, new BlobRange(offset, count), null, null, false, null, Context.NONE);
    return sink.bytes;
  }

  /** An output stream that counts what it is given and keeps none of it. */
  private static final class Drain extends OutputStream {
    long bytes = 0;

    @Override
    public void write(int b) {
      bytes++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      bytes += len;
    }
  }

  /** GETs by size, of the Put Blobs' blobs: whole, and as ranges of the largest. */
  private void getsBySize() throws IOException {
    for (int i = 0; i < reps; i++) {
      BlobClientBase big = clients.blockBlob(puts.get(LARGEST).get(i));
      for (Map.Entry<Integer, List<String>> e : puts.entrySet()) {
        int size = e.getKey();
        long t = System.nanoTime();
        long n = get(clients.blockBlob(e.getValue().get(i)), 0, size);
        record("get_size", n, 1, i, System.nanoTime() - t);

        t = System.nanoTime();
        n = get(big, (LARGEST - size) / 2, size);
        record("get_range", n, 1, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  private void getsByBlocks() throws IOException {
    byte[] part = payload(PART);
    for (int parts : new int[] {1, 10, 100, 1000}) {
      String loc = location("parts-" + parts);
      AppendBlobClient blob = createOwn(loc, part);
      long length = PART;
      for (int i = 1; i < parts; i++) {
        length = appendBlock(blob, length, part);
      }
      for (int i = 0; i < reps; i++) {
        long t = System.nanoTime();
        long n = get(loc, 0, length);
        record("get_full", n, parts, i, System.nanoTime() - t);

        t = System.nanoTime();
        n = get(loc, length - PART, PART);
        record("get_tail", n, parts, i, System.nanoTime() - t);

        long mid = length / 2;
        t = System.nanoTime();
        n = get(loc, mid, PART);
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
        record("iceberg_open", 0, 1, i, System.nanoTime() - t);
        t = System.nanoTime();
        ((RangeReadable) in).readFully(size / 2, b, 0, b.length);
        record("iceberg_mid", PART, 1, i, System.nanoTime() - t);
      }
      t = System.nanoTime();
      long n = get(loc, size / 2, PART);
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
