package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.io.S3Recorder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The S3 Express append, which no emulator implements (PLAN.md's S4 spike, its correctness half;
 * {@code micro.S3ExpressSpike} measures latencies). Runs only with {@code RMOW_S3X_ROOT} set to a
 * prefix in a directory bucket, {@code s3://<bucket>--<az id>--x-s3/<prefix>}, and credentials in
 * the default chain; {@code RMOW_S3X_REGION} (else {@code AWS_REGION}, else us-east-1) names its
 * region. Everything it writes goes under a prefix of its own, deleted at the end.
 *
 * <p>It checks what the benchmark's S3 Express path relies on, the atomicio fork's atomic
 * operations ({@link AtomicAppender} over S3FileIO): an append at the object's length lands; one
 * anywhere else but 0 (an unknown length to the fork, as to Iceberg) fails ({@code
 * InvalidWriteOffset}) and leaves the object as it was; the recorder
 * sees a create as one {@code PutObject}, conditioned on the object's absence, and an append as one
 * {@code PutObject} with a write offset, conditioned on its ETag, neither with a HEAD; and a run of
 * every arm family matches the trace's model request for request, appends included.
 */
class S3ExpressTest {

  private static String bucket;
  private static String prefix;
  private static String region;
  private static S3Client s3;
  // one S3FileIO over s3 for every appender: an S3FileIO closes its client when it is closed or
  // finalized, so one per appender, left to the collector, would close s3 under the other tests
  private static S3FileIO io;

  @BeforeAll
  static void open() {
    String root = System.getenv("RMOW_S3X_ROOT");
    assumeTrue(root != null && !root.isEmpty(), "RMOW_S3X_ROOT is not set");
    String path = root.substring(root.indexOf("://") + 3);
    bucket = path.substring(0, path.indexOf('/'));
    prefix = path.substring(path.indexOf('/') + 1) + "/test-" + Instant.now().toEpochMilli();
    region = System.getenv().getOrDefault("RMOW_S3X_REGION", System.getenv("AWS_REGION"));
    if (region == null || region.isEmpty()) {
      region = "us-east-1";
    }
    s3 = S3Client.builder().region(Region.of(region)).build();
    io = Store.s3FileIO(s3);
  }

  @AfterAll
  static void close() {
    if (s3 == null) {
      return;
    }
    List<String> keys = new ArrayList<>();
    for (S3Object o : s3.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(prefix + "/")).contents()) {
      keys.add(o.key());
    }
    for (String k : keys) {
      s3.deleteObject(b -> b.bucket(bucket).key(k));
    }
    io.close(); // closes s3 if it used it
    s3.close();
  }

  private static String location(String name) {
    return "s3://" + bucket + "/" + prefix + "/" + name;
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] read(String name) {
    return s3.getObjectAsBytes(b -> b.bucket(bucket).key(prefix + "/" + name)).asByteArray();
  }

  /** RMoW's appender over S3FileIO, with no snapshot of any object. */
  private static RegionAppender appender() {
    return new AtomicAppender(io);
  }

  @Test
  void appendsAtTheObjectsLength() throws IOException {
    RegionAppender appender = appender();
    appender.create(location("append"), bytes("first|"));
    long length = appender.append(location("append"), 6, bytes("second|"));
    length = appender.append(location("append"), length, bytes("third"));
    assertThat(length).isEqualTo(18);
    assertThat(new String(read("append"), StandardCharsets.UTF_8)).isEqualTo("first|second|third");
  }

  @Test
  void appendElsewhereFailsAndChangesNothing() throws IOException {
    appender().create(location("wrong"), bytes("0123456789"));
    // not at 0: the fork's snapshots, as Iceberg's, take a length of 0 for an unknown one, so an
    // APPEND from newInputFile(location, 0) extends the object at whatever length it has
    for (long offset : new long[] {1, 9, 11}) {
      // an appender with no snapshot of the object looks its ETag up
      assertThatThrownBy(() -> appender().append(location("wrong"), offset, bytes("x")))
          .as("append at " + offset)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("APPEND");
    }
    assertThat(read("wrong")).isEqualTo(bytes("0123456789"));
    assertThatThrownBy(() -> appender().create(location("wrong"), bytes("again")))
        .as("a create where an object is")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CAS");
    assertThat(read("wrong")).isEqualTo(bytes("0123456789"));
  }

  @Test
  void recorderSeesACreateAndAnAppendAndNoHead() throws IOException {
    S3Recorder recorder = new S3Recorder();
    // the S3FileIO closes its client
    try (S3FileIO recorded =
        Store.s3FileIO(
            S3Client.builder()
                .region(Region.of(region))
                .overrideConfiguration(o -> o.addExecutionInterceptor(recorder))
                .build())) {
      RegionAppender appender = new AtomicAppender(recorded);
      S3Recorder.Counts before = recorder.snapshot();
      appender.create(location("recorded"), bytes("abc"));
      S3Recorder.Counts create = recorder.snapshot().minus(before);
      assertThat(create.puts()).isEqualTo(1);
      assertThat(create.appends() + create.heads() + create.gets() + create.other()).isZero();

      before = recorder.snapshot();
      appender.append(location("recorded"), 3, bytes("defg"));
      S3Recorder.Counts append = recorder.snapshot().minus(before);
      assertThat(append.appends()).isEqualTo(1);
      assertThat(append.puts() + append.heads() + append.gets() + append.other()).isZero();
      assertThat(append.putBytes()).isEqualTo(4);
      // CreateSession goes to sdk_sessions if this client sends it; print what it did
      System.out.println("S3 Express create: " + create + "; append: " + append);
    }
  }

  private static BenchConfig config() {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "s3";
    cfg.store.root = "s3://" + bucket + "/" + prefix + "/runs";
    cfg.store.region = region;
    cfg.workload.initialRows = 30_000;
    cfg.workload.batches = 8;
    cfg.workload.opsPerBatch = 1_000;
    cfg.layout.rowGroupBytes = 256 << 10;
    cfg.layout.targetFileBytes = 1 << 20;
    cfg.layout.tailSealBytes = 128 << 10;
    cfg.layout.tierFanout = 3;
    cfg.layout.locate = "scan";
    cfg.layout.compactEvery = 4;
    cfg.layout.compactSmallRowGroups = 4;
    cfg.reads.every = 4;
    cfg.reads.travelBack = 4;
    cfg.arms =
        List.of("mor-v2-file", "mor-v3", "cow", "tailcow-c", "rmow-b", "rmow-c",
            "rmow-c-split");
    return cfg;
  }

  private static List<JsonNode> phases(Path dir) throws IOException {
    ObjectMapper json = new ObjectMapper();
    List<JsonNode> phases = new ArrayList<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      JsonNode r = json.readTree(l);
      if (r.has("sdk_requests")) {
        phases.add(r);
      }
    }
    assertThat(phases).isNotEmpty();
    return phases;
  }

  private static String at(JsonNode r) {
    return r.get("arm").asText() + " step " + r.get("step") + " " + r.get("phase").asText();
  }

  /**
   * Every arm family sends what the model counts. An append is one PutObject at the snapshot's
   * length, conditioned on its ETag, with no HEAD; a new appendable file is one PutObject
   * conditioned on its absence ({@code If-None-Match: *}), also with no HEAD.
   */
  @Test
  void everyArmFamilyMatchesTheModel(@TempDir Path dir) throws IOException {
    new Runner(config(), dir, "t").run();
    List<JsonNode> phases = phases(dir);
    for (JsonNode r : phases) {
      String at = at(r);
      assertThat(r.get("sdk_gets").asLong()).as(at + " GETs").isEqualTo(r.get("gets").asLong());
      assertThat(r.get("sdk_puts").asLong()).as(at + " PUTs").isEqualTo(r.get("puts").asLong());
      assertThat(r.get("sdk_appends").asLong()).as(at + " appends").isEqualTo(r.get("appends").asLong());
      assertThat(r.get("sdk_heads").asLong()).as(at + " HEADs").isEqualTo(r.get("heads").asLong());
      assertThat(r.get("sdk_deletes").asLong()).as(at).isEqualTo(r.get("deletes").asLong());
      assertThat(r.get("sdk_lists").asLong() + r.get("sdk_other").asLong()).as(at).isZero();
      assertThat(r.get("sdk_put_bytes").asLong()).as(at + " bytes written")
          .isEqualTo(r.get("bytes_written").asLong());
    }
    assertThat(phases).anyMatch(r -> r.get("sdk_appends").asLong() > 0);
    long retries = phases.stream().mapToLong(r -> r.get("sdk_retries").asLong()).sum();
    long sessions = phases.stream().mapToLong(r -> r.get("sdk_sessions").asLong()).sum();
    System.out.println("S3 Express run: " + retries + " retries, " + sessions + " CreateSession");
  }
}
