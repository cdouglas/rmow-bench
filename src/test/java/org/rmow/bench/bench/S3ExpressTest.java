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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.append.S3Appenders;
import org.rmow.bench.io.S3Recorder;
import software.amazon.awssdk.core.sync.RequestBody;
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
 * <p>It checks what the benchmark's S3 Express path relies on: an append at the object's length
 * lands, with the SDK's default checksums; one anywhere else fails with {@code
 * InvalidWriteOffset} and leaves the object as it was; the recorder sees an append as one {@code
 * PutObject} with a write offset, and no HEAD; and a run of every arm family matches the trace's
 * model request for request, appends included.
 */
class S3ExpressTest {

  private static String bucket;
  private static String prefix;
  private static String region;
  private static S3Client s3;

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

  private static void put(String name, byte[] body) {
    s3.putObject(b -> b.bucket(bucket).key(prefix + "/" + name), RequestBody.fromBytes(body));
  }

  @Test
  void appendsAtTheObjectsLength() throws IOException {
    put("append", bytes("first|"));
    RegionAppender appender = S3Appenders.express(s3);
    long length = appender.append(location("append"), 6, bytes("second|"));
    length = appender.append(location("append"), length, bytes("third"));
    assertThat(length).isEqualTo(18);
    assertThat(new String(read("append"), StandardCharsets.UTF_8)).isEqualTo("first|second|third");
  }

  @Test
  void appendElsewhereFailsAndChangesNothing() {
    put("wrong", bytes("0123456789"));
    RegionAppender appender = S3Appenders.express(s3);
    for (long offset : new long[] {0, 9, 11}) {
      assertThatThrownBy(() -> appender.append(location("wrong"), offset, bytes("x")))
          .as("append at " + offset)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("InvalidWriteOffset");
    }
    assertThat(read("wrong")).isEqualTo(bytes("0123456789"));
  }

  @Test
  void recorderSeesOneAppendAndNoHead() throws IOException {
    put("recorded", bytes("abc"));
    S3Recorder recorder = new S3Recorder();
    try (S3Client recorded =
        S3Client.builder()
            .region(Region.of(region))
            .overrideConfiguration(o -> o.addExecutionInterceptor(recorder))
            .build()) {
      S3Recorder.Counts before = recorder.snapshot();
      S3Appenders.express(recorded).append(location("recorded"), 3, bytes("defg"));
      S3Recorder.Counts d = recorder.snapshot().minus(before);
      assertThat(d.appends()).isEqualTo(1);
      assertThat(d.puts() + d.heads() + d.gets() + d.other()).isZero();
      assertThat(d.putBytes()).isEqualTo(4);
      // CreateSession goes to sdk_sessions if this client sends it; print what it did
      System.out.println("S3 Express append: " + d);
    }
  }

  @Test
  void everyArmFamilyMatchesTheModel(@TempDir Path dir) throws IOException {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "s3";
    cfg.store.root = "s3://" + bucket + "/" + prefix + "/runs";
    cfg.store.region = region;
    cfg.store.append = "express";
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
        List.of("mor-v2-file", "mor-v3", "cow", "tailcow-c", "rmow-b", "rmow-c", "rmow-c-dv",
            "rmow-c-split");
    new Runner(cfg, dir, "t").run();

    ObjectMapper json = new ObjectMapper();
    List<JsonNode> phases = new ArrayList<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      JsonNode r = json.readTree(l);
      if (r.has("sdk_requests")) {
        phases.add(r);
      }
    }
    assertThat(phases).isNotEmpty();
    for (JsonNode r : phases) {
      String at = r.get("arm").asText() + " step " + r.get("step") + " " + r.get("phase").asText();
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
