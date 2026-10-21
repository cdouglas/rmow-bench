package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The S3 store against versitygw, an S3 emulator: Iceberg's S3FileIO under every arm family, with
 * RMoW on the emulated append (versitygw ignores {@code writeOffsetBytes}; the S3 Express append
 * itself needs the real service). Every phase's requests, as the SDK sent them, must match what
 * {@code TracingFileIO} modelled, and the run must leave nothing behind.
 */
class S3StoreTest {

  private static final String IMAGE = "versity/versitygw:v1.9.0";
  private static final String ACCESS_KEY = "rmow-bench";
  private static final String SECRET_KEY = "rmow-bench-secret";
  private static final String BUCKET = "bench";

  private static GenericContainer<?> gateway;
  private static URI endpoint;
  private static final Map<String, String> savedProperties = new java.util.HashMap<>();

  @BeforeAll
  static void start() {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    gateway =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(7070)
            .withEnv("ROOT_ACCESS_KEY", ACCESS_KEY)
            .withEnv("ROOT_SECRET_KEY", SECRET_KEY)
            .withTmpFs(Map.of("/data", "rw"))
            .withCommand("posix", "/data")
            .waitingFor(Wait.forHttp("/").forPort(7070).forStatusCodeMatching(code -> code > 0));
    gateway.start();
    endpoint = URI.create("http://" + gateway.getHost() + ":" + gateway.getMappedPort(7070));
    try (S3Client s3 = client()) {
      s3.createBucket(b -> b.bucket(BUCKET));
    }
    // the store takes credentials from the default chain, which reads these first
    for (String[] kv :
        new String[][] {{"aws.accessKeyId", ACCESS_KEY}, {"aws.secretAccessKey", SECRET_KEY}}) {
      savedProperties.put(kv[0], System.getProperty(kv[0]));
      System.setProperty(kv[0], kv[1]);
    }
  }

  @AfterAll
  static void stop() {
    savedProperties.forEach(
        (k, v) -> {
          if (v == null) {
            System.clearProperty(k);
          } else {
            System.setProperty(k, v);
          }
        });
    if (gateway != null) {
      gateway.stop();
    }
  }

  private static S3Client client() {
    return S3Client.builder()
        .endpointOverride(endpoint)
        .region(Region.US_EAST_1)
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .build();
  }

  private static BenchConfig config(String append) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "s3";
    cfg.store.root = "s3://" + BUCKET + "/runs";
    cfg.store.endpoint = endpoint.toString();
    cfg.store.region = "us-east-1";
    cfg.store.pathStyle = true;
    cfg.store.append = append;
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
    return cfg;
  }

  private static List<JsonNode> records(Path dir) throws IOException {
    ObjectMapper json = new ObjectMapper();
    List<JsonNode> out = new ArrayList<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      out.add(json.readTree(l));
    }
    return out;
  }

  @Test
  void everyArmFamilyOnS3(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config("emulated");
    cfg.arms =
        List.of(
            "mor-v2-file",
            "mor-v2-partition",
            "mor-v3",
            "cow",
            "tailcow-c",
            "rmow-b",
            "rmow-c",
            "rmow-c-dv");
    new Runner(cfg, dir, "t").run();

    List<JsonNode> phases = records(dir).stream().filter(r -> r.has("sdk_requests")).toList();
    assertThat(phases).isNotEmpty();
    for (JsonNode r : phases) {
      String at = r.get("arm").asText() + " step " + r.get("step") + " " + r.get("phase").asText();
      // the trace's model of Iceberg's S3 streams, request by request
      assertThat(r.get("sdk_gets").asLong()).as(at + " GETs").isEqualTo(r.get("gets").asLong());
      assertThat(r.get("sdk_puts").asLong()).as(at + " PUTs").isEqualTo(r.get("puts").asLong());
      assertThat(r.get("sdk_heads").asLong()).as(at + " HEADs").isEqualTo(r.get("heads").asLong());
      assertThat(r.get("sdk_deletes").asLong()).as(at).isEqualTo(r.get("deletes").asLong());
      assertThat(r.get("sdk_appends").asLong()).as(at + " (emulated appends are unmeasured)").isZero();
      assertThat(r.get("sdk_lists").asLong() + r.get("sdk_other").asLong()).as(at).isZero();
      assertThat(r.get("sdk_retries").asLong()).as(at + " retries").isZero();
      // what PUTs sent is what the arms wrote, appends aside
      if (r.get("appends").asLong() == 0) {
        assertThat(r.get("sdk_put_bytes").asLong()).as(at).isEqualTo(r.get("bytes_written").asLong());
      }
    }
    assertThat(phases).anyMatch(r -> r.get("appends").asLong() > 0);
    assertThat(phases).anyMatch(r -> r.get("aborts").asLong() > 0 || r.get("get_stream").asLong() > 0);

    // the run deleted its objects
    try (S3Client s3 = client()) {
      assertThat(s3.listObjectsV2(b -> b.bucket(BUCKET).prefix("runs/")).contents()).isEmpty();
    }
  }

  /** Split files need no append: they run on standard S3 (and GCS), one object per write. */
  @Test
  void splitFilesOnStandardS3(@TempDir Path dir) throws IOException {
    BenchConfig cfg = config("none");
    cfg.arms = List.of("mor-v3", "rmow-b-split", "rmow-c-split");
    new Runner(cfg, dir, "t").run();
    List<JsonNode> phases = records(dir).stream().filter(r -> r.has("sdk_requests")).toList();
    assertThat(phases).filteredOn(r -> r.get("arm").asText().endsWith("-split"))
        .allMatch(r -> r.get("sdk_appends").asLong() == 0 && r.get("appends").asLong() == 0)
        .anyMatch(r -> r.get("sdk_puts").asLong() >= 2);
    for (JsonNode r : phases) {
      String at = r.get("arm").asText() + " step " + r.get("step") + " " + r.get("phase").asText();
      assertThat(r.get("sdk_gets").asLong()).as(at + " GETs").isEqualTo(r.get("gets").asLong());
      assertThat(r.get("sdk_puts").asLong()).as(at + " PUTs").isEqualTo(r.get("puts").asLong());
      assertThat(r.get("sdk_heads").asLong()).as(at + " HEADs").isEqualTo(r.get("heads").asLong());
    }
  }

  @Test
  void rmowNeedsAppend(@TempDir Path dir) {
    BenchConfig cfg = config("none");
    cfg.arms = List.of("mor-v3", "rmow-c");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("S3 Express");
  }
}
