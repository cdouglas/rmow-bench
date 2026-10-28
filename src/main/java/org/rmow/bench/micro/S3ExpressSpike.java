package org.rmow.bench.micro;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.append.RegionAppender;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Spike S4 (PLAN.md, M3), its latency half: what an S3 Express append costs by size, the store's
 * own and RMoW's through the fork's atomic operations, against a PUT of a new object of the same
 * size, and what a GET costs against the number of parts an object was appended in. {@code
 * S3ExpressTest} checks the append's semantics.
 *
 * <pre>
 *   java -cp ... org.rmow.bench.micro.S3ExpressSpike s3://&lt;bucket&gt;--&lt;az id&gt;--x-s3/&lt;prefix&gt;
 *       [--region r] [--reps n] [--out dir] [--endpoint url --path-style]
 * </pre>
 *
 * <ul>
 *   <li><b>append</b>, <b>append_sao</b>, <b>put</b> and <b>create_sao</b>: for sizes 64 KiB to 128
 *       MiB (a row group at Iceberg's default target), doubling, {@code reps} each, interleaved:
 *       the store's own appends of that size to one object (a PutObject at its length, created by
 *       a PUT of the same size); RMoW's, the fork's APPEND ({@link AtomicAppender}: a PutObject at
 *       the length, conditioned on the ETag) to another; PUTs of new objects; and RMoW's creates of
 *       new objects, a PUT conditioned on their absence ({@code If-None-Match: *});
 *   <li><b>get_size</b> and <b>get_range</b>: GETs by size, 64 KiB to 128 MiB, doubling, for the
 *       read rate per MiB: the single-part objects the PUTs above wrote, read whole, and the same
 *       sizes read as a range from the middle of a 128 MiB one (as a row group is read), {@code
 *       reps} each, interleaved over the sizes;
 *   <li><b>get_full</b>, <b>get_tail</b> and <b>get_mid</b>: objects of 1, 10, 100 and 1000 parts
 *       of 64 KiB (a PUT, then appends), read whole, as their last 64 KiB (a footer prefetch), and
 *       as 64 KiB from the middle, {@code reps} times each.
 * </ul>
 *
 * Writes {@code manifest.json} and {@code spike.jsonl} (one record per request: op, bytes, parts,
 * rep, ns) to {@code <out>/s3x-spike-<UTC start>/} (default out: {@code results/spike}), and
 * deletes its objects. Bodies go up as streams over the payload, as the fork sends them, and a
 * GET's body is drained into a buffer and dropped, so a request's time holds no copy of it. Run it
 * from an instance in the bucket's zone; elsewhere it measures the
 * network. {@code --endpoint} runs it against an emulator, which checks the code, not S3 Express.
 */
public final class S3ExpressSpike {

  private static final int KIB = 1024;
  private static final int PART = 64 * KIB;
  private static final int LARGEST = 128 * KIB * KIB;

  private final S3Client s3;
  private final String bucket;
  private final String prefix;
  private final int reps;
  private final BufferedWriter out;
  private final ObjectMapper json = new ObjectMapper();
  private final SplittableRandom random = new SplittableRandom(1);
  private final byte[] buffer = new byte[1 << 20]; // what a GET's body is read into

  private S3ExpressSpike(S3Client s3, String bucket, String prefix, int reps, BufferedWriter out) {
    this.s3 = s3;
    this.bucket = bucket;
    this.prefix = prefix;
    this.reps = reps;
    this.out = out;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || !args[0].startsWith("s3://")) {
      System.err.println(
          "usage: S3ExpressSpike s3://<bucket>/<prefix> [--region r] [--reps n] [--out dir]"
              + " [--endpoint url --path-style]");
      System.exit(2);
    }
    String path = args[0].substring("s3://".length());
    String bucket = path.substring(0, path.indexOf('/'));
    String started =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(Instant.now());
    String prefix = path.substring(path.indexOf('/') + 1) + "/spike-" + started;
    String region = System.getenv().getOrDefault("AWS_REGION", "us-east-1");
    int reps = 10;
    Path outRoot = Path.of("results", "spike");
    String endpoint = "";
    boolean pathStyle = false;
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--region" -> region = args[++i];
        case "--reps" -> reps = Integer.parseInt(args[++i]);
        case "--out" -> outRoot = Path.of(args[++i]);
        case "--endpoint" -> endpoint = args[++i];
        case "--path-style" -> pathStyle = true;
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }

    Path dir = outRoot.resolve("s3x-spike-" + started);
    Files.createDirectories(dir);
    Map<String, Object> manifest = new LinkedHashMap<>();
    manifest.put("label", "s3x-spike");
    manifest.put("started", started);
    manifest.put("root", args[0]);
    manifest.put("region", region);
    manifest.put("endpoint", endpoint);
    manifest.put("reps", reps);
    manifest.put("part_bytes", PART);
    manifest.put("host", InetAddress.getLocalHost().getHostName());
    manifest.put("java", System.getProperty("java.version"));
    ObjectMapper json = new ObjectMapper();
    json.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("manifest.json").toFile(), manifest);

    S3ClientBuilder b =
        S3Client.builder()
            .httpClientBuilder(ApacheHttpClient.builder().maxConnections(16))
            .region(Region.of(region))
            .forcePathStyle(pathStyle);
    if (!endpoint.isEmpty()) {
      b.endpointOverride(URI.create(endpoint));
    }
    try (S3Client s3 = b.build();
        BufferedWriter w = Files.newBufferedWriter(dir.resolve("spike.jsonl"))) {
      S3ExpressSpike spike = new S3ExpressSpike(s3, bucket, prefix, reps, w);
      try {
        spike.appendsAndPuts();
        spike.getsBySize();
        spike.getsByParts();
      } finally {
        spike.deleteAll();
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

  private String location(String key) {
    return "s3://" + bucket + "/" + key;
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

  private static RequestBody body(byte[] body) {
    return RequestBody.fromInputStream(new ByteArrayInputStream(body), body.length);
  }

  private void put(String key, byte[] body) {
    s3.putObject(b -> b.bucket(bucket).key(key), body(body));
  }

  /** One GET, its body read through a buffer and dropped; the bytes it delivered. */
  private long get(Consumer<GetObjectRequest.Builder> request) throws IOException {
    long n = 0;
    try (InputStream in = s3.getObject(request)) {
      for (int r; (r = in.read(buffer)) >= 0; ) {
        n += r;
      }
    }
    return n;
  }

  /**
   * The store's own append: a PutObject at the object's length ({@code writeOffsetBytes}), which
   * S3 rejects unless it is the object's size, with no other condition.
   */
  private long append(String key, long at, byte[] body) {
    s3.putObject(b -> b.bucket(bucket).key(key).writeOffsetBytes(at), body(body));
    return at + body.length;
  }

  private void appendsAndPuts() throws IOException {
    RegionAppender atomic = new AtomicAppender(fileIO());
    for (int size = PART; size <= LARGEST; size *= 2) {
      byte[] body = payload(size);
      String own = prefix + "/append-" + size;
      put(own, body);
      String sao = location(prefix + "/append-sao-" + size);
      atomic.create(sao, body);
      long ownLength = size;
      long saoLength = size;
      for (int i = 0; i < reps; i++) {
        long t = System.nanoTime();
        ownLength = append(own, ownLength, body);
        record("append", size, i + 2, i, System.nanoTime() - t);

        t = System.nanoTime();
        saoLength = atomic.append(sao, saoLength, body);
        record("append_sao", size, i + 2, i, System.nanoTime() - t);

        String k = prefix + "/put-" + size + "-" + i;
        t = System.nanoTime();
        put(k, body);
        record("put", size, 1, i, System.nanoTime() - t);

        String c = location(prefix + "/create-sao-" + size + "-" + i);
        t = System.nanoTime();
        atomic.create(c, body);
        record("create_sao", size, 1, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  /** S3FileIO over the spike's client, which the spike closes; it builds no client of its own. */
  private S3FileIO fileIO() {
    S3Client client = s3;
    S3FileIO io =
        new S3FileIO(
            () -> client,
            () -> {
              throw new UnsupportedOperationException("The spike's S3 store is synchronous");
            });
    io.initialize(Map.of());
    return io;
  }

  /** GETs by size, of the PUTs' objects: whole, and as ranges of the largest. */
  private void getsBySize() throws IOException {
    for (int i = 0; i < reps; i++) {
      String big = prefix + "/put-" + LARGEST + "-" + i;
      for (int size = PART; size <= LARGEST; size *= 2) {
        String key = prefix + "/put-" + size + "-" + i;
        long t = System.nanoTime();
        long n = get(b -> b.bucket(bucket).key(key));
        record("get_size", n, 1, i, System.nanoTime() - t);

        String range = "bytes=" + (LARGEST - size) / 2 + "-" + ((LARGEST - size) / 2 + size - 1);
        t = System.nanoTime();
        n = get(b -> b.bucket(bucket).key(big).range(range));
        record("get_range", n, 1, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  private void getsByParts() throws IOException {
    byte[] part = payload(PART);
    for (int parts : new int[] {1, 10, 100, 1000}) {
      String key = prefix + "/parts-" + parts;
      put(key, part);
      long length = PART;
      for (int i = 1; i < parts; i++) {
        length = append(key, length, part);
      }
      for (int i = 0; i < reps; i++) {
        long t = System.nanoTime();
        long n = get(b -> b.bucket(bucket).key(key));
        record("get_full", n, parts, i, System.nanoTime() - t);
        long mid = n / 2; // the object's length as read, not as appended (an emulator ignores it)

        t = System.nanoTime();
        n = get(b -> b.bucket(bucket).key(key).range("bytes=-" + PART));
        record("get_tail", n, parts, i, System.nanoTime() - t);

        t = System.nanoTime();
        n = get(b -> b.bucket(bucket).key(key).range("bytes=" + mid + "-" + (mid + PART - 1)));
        record("get_mid", n, parts, i, System.nanoTime() - t);
      }
      out.flush();
    }
  }

  private void deleteAll() {
    List<String> keys = new ArrayList<>();
    for (S3Object o : s3.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(prefix + "/")).contents()) {
      keys.add(o.key());
    }
    for (String k : keys) {
      s3.deleteObject(b -> b.bucket(bucket).key(k));
    }
  }
}
