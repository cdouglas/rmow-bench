package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.append.EmulatorAppenders;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.io.AdlsClients;
import org.rmow.bench.io.AzureRecorder;
import org.rmow.bench.io.AzureRecorder.Kind;

/**
 * What the Azure store relies on, against Azurite ({@link AzureStoreTest}) and Azure itself
 * ({@link AzureTest}): the atomicio fork's ADLSFileIO writes and reads, RMoW's Append Blobs through
 * its atomic operations, and what the recorder counts, under every arm family.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AzureStoreContract {

  static final int MIB = 1 << 20;

  /**
   * Whether the store renames, as the fork's atomic create on ADLS does. Azurite does not, so
   * there RMoW creates its files through a stand-in ({@link EmulatorAppenders#appendBlobs}).
   */
  boolean renames() {
    return false;
  }

  /** RMoW's appender on this store, as {@code Store} builds it. */
  RegionAppender appender() {
    return renames() ? new AtomicAppender(adls) : EmulatorAppenders.appendBlobs(clients, adls);
  }

  private String root; // this class's prefix: abfss://container@account.dfs.core.windows.net/...
  private String endpoint; // an emulator's, or empty for Azure
  private AdlsClients clients;
  private ADLSFileIO adls;
  private int objects = 0;

  /** Connects to the store under {@code root}; {@code key} signs for an emulator. */
  void connect(String root, String endpoint, String key) {
    this.root = root;
    this.endpoint = endpoint;
    Map<String, String> props = AdlsClients.properties(root, endpoint, key);
    clients = new AdlsClients(props);
    if (!endpoint.isEmpty()) {
      clients.container(root).createIfNotExists();
    }
    adls = new ADLSFileIO();
    adls.initialize(props);
  }

  ADLSFileIO adls() {
    return adls;
  }

  AdlsClients clients() {
    return clients;
  }

  @AfterAll
  void disconnect() {
    if (adls != null) {
      adls.close();
    }
  }

  private String location(String name) {
    return root + "/" + name + "-" + objects++;
  }

  private static byte[] random(int n) {
    byte[] b = new byte[n];
    new Random(n).nextBytes(b);
    return b;
  }

  private static void write(FileIO io, String location, byte[] bytes) throws IOException {
    try (OutputStream out = io.newOutputFile(location).create()) {
      out.write(bytes);
    }
  }

  private byte[] read(String location, long length) throws IOException {
    byte[] b = new byte[(int) length];
    try (SeekableInputStream in = adls.newInputFile(location, length).newStream()) {
      IOUtil.readFully(in, b, 0, b.length);
    }
    return b;
  }

  /** What the SDK sent while {@code body} ran: requests by kind, and GET bytes. */
  private record Sent(Map<Kind, Long> requests, long getBytes, long putBytes) {
    long n(Kind k) {
      return requests.get(k);
    }

    long total() {
      return requests.values().stream().mapToLong(Long::longValue).sum();
    }
  }

  private interface IoBody {
    void run() throws IOException;
  }

  private Sent sent(IoBody body) throws IOException {
    try (AzureRecorder r = AzureRecorder.install(root, endpoint)) {
      body.run();
      Map<Kind, Long> m = new LinkedHashMap<>();
      for (Kind k : Kind.values()) {
        m.put(k, r.requests(k));
      }
      return new Sent(m, r.getBytes(), r.putBytes());
    }
  }

  @Test
  void anIcebergWriteIsAHeadAndOnePutBlob() throws IOException {
    String loc = location("written");
    byte[] bytes = random(MIB);
    Sent s = sent(() -> write(adls, loc, bytes));
    assertThat(s.n(Kind.HEAD)).as("create()'s existence check").isEqualTo(1);
    assertThat(s.n(Kind.PUT)).isEqualTo(1);
    assertThat(s.total()).isEqualTo(2);
    assertThat(s.putBytes()).isEqualTo(bytes.length);
    assertThat(read(loc, bytes.length)).isEqualTo(bytes);
  }

  /**
   * Iceberg 1.12's ADLSInputStream opens with a GET of the file's first 4 MiB, and a ranged read
   * at an offset asks for {@code position + length} bytes. The atomicio fork fixes both: a stream
   * opens nothing, and a ranged read asks for exactly its range.
   */
  @Test
  void icebergReads() throws IOException {
    String loc = location("shipped");
    byte[] bytes = random(10 * MIB);
    write(adls, loc, bytes);
    byte[] b = new byte[64 << 10];
    SeekableInputStream[] in = new SeekableInputStream[1];
    Sent open = sent(() -> in[0] = adls.newInputFile(loc, bytes.length).newStream());
    assertThat(open.total()).isZero();

    Sent range = sent(() -> ((RangeReadable) in[0]).readFully(6 * MIB, b, 0, b.length));
    assertThat(range.n(Kind.GET)).isEqualTo(1);
    assertThat(range.total()).isEqualTo(1);
    assertThat(range.getBytes()).isEqualTo(b.length);
    assertThat(b).isEqualTo(Arrays.copyOfRange(bytes, 6 * MIB, 6 * MIB + b.length));

    Sent tail = sent(() -> ((RangeReadable) in[0]).readTail(b, 0, b.length));
    assertThat(tail.n(Kind.GET)).isEqualTo(1);
    assertThat(tail.getBytes()).isEqualTo(b.length);
    in[0].close();
  }

  /**
   * RMoW's files are Append Blobs, created and extended through the fork. A create is three
   * requests: on Azure, a staged Append Blob, an Append Block and a rename (counted as other); on
   * Azurite, an empty Append Blob, a HEAD and an Append Block. An append is one Append Block, at
   * the blob's length and conditioned on its ETag; one anywhere else fails and changes nothing.
   */
  @Test
  void appendBlobsAppendAtTheirLengthOnly() throws IOException {
    RegionAppender appender = appender();
    String loc = location("append");
    byte[] first = "first|".getBytes(StandardCharsets.UTF_8);
    Sent create = sent(() -> appender.create(loc, first));
    assertThat(create.n(Kind.BLOB_CREATE)).isEqualTo(1);
    assertThat(create.n(Kind.APPEND)).isEqualTo(1);
    assertThat(create.n(renames() ? Kind.OTHER : Kind.HEAD)).isEqualTo(1);
    assertThat(create.total()).isEqualTo(3);

    byte[] second = "second".getBytes(StandardCharsets.UTF_8);
    long[] length = new long[1];
    Sent append = sent(() -> length[0] = appender.append(loc, first.length, second));
    assertThat(append.n(Kind.APPEND)).isEqualTo(1);
    assertThat(append.total()).isEqualTo(1);
    assertThat(append.putBytes()).isEqualTo(second.length);
    assertThat(length[0]).isEqualTo(12);
    // ADLSFileIO reads an Append Blob, with the length it is given (PLAN.md's S5)
    assertThat(new String(read(loc, 12), StandardCharsets.UTF_8)).isEqualTo("first|second");

    // not at 0: the fork's snapshots, as Iceberg's, take a length of 0 for an unknown one, so an
    // APPEND from newInputFile(location, 0) extends the blob at whatever length it has
    for (long wrong : new long[] {1, 11, 13}) {
      // an appender with no snapshot of the blob looks its ETag up
      assertThatThrownBy(() -> new AtomicAppender(adls).append(loc, wrong, second))
          .as("append at " + wrong)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("APPEND");
    }
    assertThatThrownBy(() -> appender().create(loc, first))
        .as("a create where a blob is")
        .isInstanceOf(IOException.class);
    assertThat(new String(read(loc, 12), StandardCharsets.UTF_8)).isEqualTo("first|second");
  }

  @Test
  void aFileIcebergWroteCannotBeAppendedTo() throws IOException {
    String loc = location("block");
    write(adls, loc, random(1000));
    assertThatThrownBy(() -> new AtomicAppender(adls).append(loc, 1000, random(10)))
        .hasMessageContaining("InvalidBlobType");
  }

  private BenchConfig config() {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "azure";
    cfg.store.root = root + "/runs";
    cfg.store.endpoint = endpoint;
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
        List.of(
            "mor-v2-file",
            "mor-v2-partition",
            "mor-v3",
            "cow",
            "tailcow-c",
            "rmow-b",
            "rmow-c",
            "rmow-c-split");
    return cfg;
  }

  private static List<JsonNode> phases(Path dir) throws IOException {
    ObjectMapper json = new ObjectMapper();
    List<JsonNode> out = new ArrayList<>();
    for (String l : Files.readAllLines(dir.resolve("steps.jsonl"))) {
      JsonNode r = json.readTree(l);
      if (r.has("sdk_requests")) {
        out.add(r);
      }
    }
    return out;
  }

  private static long n(JsonNode r, String field) {
    return r.get(field).asLong();
  }

  private static String at(JsonNode r) {
    return r.get("arm").asText() + " step " + r.get("step") + " " + r.get("phase").asText()
        + (r.hasNonNull("reader") ? " " + r.get("reader").asText() : "");
  }

  /**
   * Every arm family sends what the model counts. A whole-object PUT is one Put Blob; a new
   * appendable file is three requests (an Append Blob, an Append Block, and a rename on Azure or a
   * HEAD on Azurite); an append is one Append Block; GETs and HEADs are the model's. Nothing else,
   * and no retries. Bytes read are printed, not checked: the SDK downloads a sequential read's 4
   * MiB chunk whole.
   */
  @Test
  void everyArmFamilyMatchesTheModel(@TempDir Path dir) throws IOException {
    new Runner(config(), dir, "t").run();
    List<JsonNode> phases = phases(dir);
    assertThat(phases).isNotEmpty();
    Map<String, long[]> byArm = new LinkedHashMap<>(); // model bytes read, sent
    for (JsonNode r : phases) {
      String at = at(r);
      long creates = n(r, "sdk_blob_creates");
      assertThat(n(r, "sdk_puts") + creates).as(at + " PUTs").isEqualTo(n(r, "puts"));
      assertThat(n(r, "sdk_appends")).as(at + " appends").isEqualTo(n(r, "appends") + creates);
      assertThat(n(r, "sdk_other")).as(at + " renames").isEqualTo(renames() ? creates : 0);
      assertThat(n(r, "sdk_heads"))
          .as(at + " HEADs")
          .isEqualTo(n(r, "heads") + (renames() ? 0 : creates));
      assertThat(n(r, "sdk_gets")).as(at + " GETs").isEqualTo(n(r, "gets"));
      assertThat(n(r, "sdk_put_bytes")).as(at + " bytes written").isEqualTo(n(r, "bytes_written"));
      assertThat(n(r, "sdk_deletes")).as(at + " deletes").isEqualTo(n(r, "deletes"));
      assertThat(n(r, "sdk_blocks") + n(r, "sdk_block_lists") + n(r, "sdk_lists"))
          .as(at + " other requests")
          .isZero();
      assertThat(n(r, "sdk_retries")).as(at + " retries").isZero();
      long[] t = byArm.computeIfAbsent(r.get("arm").asText(), k -> new long[2]);
      t[0] += n(r, "bytes_read");
      t[1] += n(r, "sdk_get_bytes");
    }
    assertThat(phases).anyMatch(r -> n(r, "sdk_appends") > 0);
    assertThat(phases).anyMatch(r -> n(r, "sdk_blob_creates") > 0);
    byArm.forEach(
        (arm, t) ->
            System.out.printf("%s: MB read %.1f sent / %.1f modelled%n", arm, t[1] / 1e6, t[0] / 1e6));
    String prefix = AdlsClients.Location.of(root).path() + "/runs/";
    assertThat(clients.container(root).listBlobsByHierarchy(prefix).stream())
        .as("the run's objects are deleted")
        .isEmpty();
  }
}
