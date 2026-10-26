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
import org.rmow.bench.append.AzureAppenders;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.io.AdlsClients;
import org.rmow.bench.io.AzureRecorder;
import org.rmow.bench.io.AzureRecorder.Kind;
import org.rmow.bench.io.ExactAdlsFileIO;

/**
 * What the Azure store relies on, against Azurite ({@link AzureStoreTest}) and Azure itself
 * ({@link AzureTest}): Iceberg's ADLSFileIO writes and reads, its reads as shipped and exact,
 * RMoW's Append Blobs, and what the recorder counts, under every arm family.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AzureStoreContract {

  static final int MIB = 1 << 20;

  /**
   * Whether this build is on the atomicio fork ({@code -Pfork}), whose ADLS reads are fixed (lazy
   * opens, exact ranges) and which has the fork appender.
   */
  static final boolean FORK = present("org.apache.iceberg.io.SupportsAtomicOperations");

  private static boolean present(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /** Whether the store renames, as the fork's CAS on ADLS needs (Azurite does not). */
  boolean renames() {
    return false;
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

  private static Sent sent(IoBody body) throws IOException {
    try (AzureRecorder r = AzureRecorder.install()) {
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
   * A canary for what {@code adlsReads: iceberg} measures: Iceberg 1.12's ADLSInputStream opens
   * with a GET of the file's first 4 MiB, and a ranged read at an offset asks for {@code position +
   * length} bytes. If this fails, Iceberg or the SDK changed, and so did that mode. The atomicio
   * fork fixes both (from core-fixes): it opens nothing, and reads exactly.
   */
  @Test
  void icebergReads() throws IOException {
    String loc = location("shipped");
    byte[] bytes = random(10 * MIB);
    write(adls, loc, bytes);
    byte[] b = new byte[64 << 10];
    SeekableInputStream[] in = new SeekableInputStream[1];
    Sent open = sent(() -> in[0] = adls.newInputFile(loc, bytes.length).newStream());
    assertThat(open.n(Kind.GET)).isEqualTo(FORK ? 0 : 1);
    assertThat(open.getBytes()).isEqualTo(FORK ? 0 : 4 * MIB);

    Sent range = sent(() -> ((RangeReadable) in[0]).readFully(6 * MIB, b, 0, b.length));
    assertThat(range.n(Kind.GET)).isEqualTo(1);
    assertThat(range.getBytes()).isEqualTo(FORK ? b.length : 4 * MIB);
    assertThat(b).isEqualTo(Arrays.copyOfRange(bytes, 6 * MIB, 6 * MIB + b.length));

    Sent tail = sent(() -> ((RangeReadable) in[0]).readTail(b, 0, b.length));
    assertThat(tail.n(Kind.GET)).isEqualTo(1);
    assertThat(tail.getBytes()).isEqualTo(b.length);
    in[0].close();
  }

  @Test
  void exactReadsSendTheModelsRequests() throws IOException {
    String loc = location("exact");
    byte[] bytes = random(10 * MIB);
    write(adls, loc, bytes);
    FileIO io = new ExactAdlsFileIO(adls, clients);
    byte[] b = new byte[64 << 10];
    SeekableInputStream[] in = new SeekableInputStream[1];
    assertThat(sent(() -> in[0] = io.newInputFile(loc, bytes.length).newStream()).total()).isZero();

    Sent range = sent(() -> ((RangeReadable) in[0]).readFully(6 * MIB, b, 0, b.length));
    assertThat(range.total()).isEqualTo(1);
    assertThat(range.getBytes()).isEqualTo(b.length);
    assertThat(b).isEqualTo(Arrays.copyOfRange(bytes, 6 * MIB, 6 * MIB + b.length));

    Sent tail = sent(() -> ((RangeReadable) in[0]).readTail(b, 0, b.length));
    assertThat(tail.total()).isEqualTo(1);
    assertThat(b).isEqualTo(Arrays.copyOfRange(bytes, bytes.length - b.length, bytes.length));

    // a sequential read opens one request; a short forward seek reads through, a backward one
    // abandons it and opens another
    byte[] seq = new byte[MIB];
    Sent first =
        sent(
            () -> {
              in[0].seek(4 * MIB);
              IOUtil.readFully(in[0], seq, 0, seq.length);
            });
    assertThat(first.n(Kind.GET)).isEqualTo(1);
    assertThat(seq).isEqualTo(Arrays.copyOfRange(bytes, 4 * MIB, 5 * MIB));
    Sent skip =
        sent(
            () -> {
              in[0].seek(5 * MIB + MIB / 2);
              IOUtil.readFully(in[0], seq, 0, seq.length);
            });
    assertThat(skip.total()).isZero();
    assertThat(seq).isEqualTo(Arrays.copyOfRange(bytes, 5 * MIB + MIB / 2, 6 * MIB + MIB / 2));
    Sent back =
        sent(
            () -> {
              in[0].seek(0);
              IOUtil.readFully(in[0], seq, 0, seq.length);
            });
    assertThat(back.n(Kind.GET)).isEqualTo(1);
    assertThat(seq).isEqualTo(Arrays.copyOfRange(bytes, 0, MIB));
    in[0].close();
  }

  @Test
  void appendBlobsAppendAtTheirLengthOnly() throws IOException {
    RegionAppender appender = AzureAppenders.appendBlob(clients);
    String loc = location("append");
    byte[] first = "first|".getBytes(StandardCharsets.UTF_8);
    Sent create = sent(() -> appender.create(adls, loc, first));
    assertThat(create.n(Kind.BLOB_CREATE)).isEqualTo(1);
    assertThat(create.n(Kind.APPEND)).isEqualTo(1);
    assertThat(create.total()).isEqualTo(2);

    byte[] second = "second".getBytes(StandardCharsets.UTF_8);
    long[] length = new long[1];
    Sent append = sent(() -> length[0] = appender.append(loc, first.length, second));
    assertThat(append.n(Kind.APPEND)).isEqualTo(1);
    assertThat(append.total()).isEqualTo(1);
    assertThat(append.putBytes()).isEqualTo(second.length);
    assertThat(length[0]).isEqualTo(12);
    // ADLSFileIO reads an Append Blob, with the length it is given (PLAN.md's S5)
    assertThat(new String(read(loc, 12), StandardCharsets.UTF_8)).isEqualTo("first|second");

    for (long wrong : new long[] {0, 11, 13}) {
      assertThatThrownBy(() -> appender.append(loc, wrong, second))
          .as("append at " + wrong)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("AppendPositionConditionNotMet");
    }
    assertThat(new String(read(loc, 12), StandardCharsets.UTF_8)).isEqualTo("first|second");
  }

  @Test
  void aFileIcebergWroteCannotBeAppendedTo() throws IOException {
    String loc = location("block");
    write(adls, loc, random(1000));
    assertThatThrownBy(() -> AzureAppenders.appendBlob(clients).append(loc, 1000, random(10)))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("InvalidBlobType");
  }

  private BenchConfig config(String reads) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "azure";
    cfg.store.root = root + "/runs-" + reads;
    cfg.store.endpoint = endpoint;
    cfg.store.adlsReads = reads;
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

  /**
   * Writes as the model counts them, whatever the reads: a whole-object PUT is one Put Blob, or,
   * for a new appendable file, an empty Append Blob and its first block; an append is one Append
   * Block. Nothing else, and no retries.
   */
  private static void writesMatchTheModel(JsonNode r, String at) {
    long creates = n(r, "sdk_blob_creates");
    assertThat(n(r, "sdk_puts") + creates).as(at + " PUTs").isEqualTo(n(r, "puts"));
    assertThat(n(r, "sdk_appends")).as(at + " appends").isEqualTo(n(r, "appends") + creates);
    assertThat(n(r, "sdk_put_bytes")).as(at + " bytes written").isEqualTo(n(r, "bytes_written"));
    assertThat(n(r, "sdk_deletes")).as(at + " deletes").isEqualTo(n(r, "deletes"));
    assertThat(n(r, "sdk_blocks") + n(r, "sdk_block_lists") + n(r, "sdk_lists") + n(r, "sdk_other"))
        .as(at + " other requests")
        .isZero();
    assertThat(n(r, "sdk_retries")).as(at + " retries").isZero();
  }

  private static String at(JsonNode r) {
    return r.get("arm").asText() + " step " + r.get("step") + " " + r.get("phase").asText()
        + (r.hasNonNull("reader") ? " " + r.get("reader").asText() : "");
  }

  @Test
  void everyArmFamilyMatchesTheModelWithExactReads(@TempDir Path dir) throws IOException {
    new Runner(config("exact"), dir, "t").run();
    List<JsonNode> phases = phases(dir);
    assertThat(phases).isNotEmpty();
    for (JsonNode r : phases) {
      String at = at(r);
      writesMatchTheModel(r, at);
      assertThat(n(r, "sdk_gets")).as(at + " GETs").isEqualTo(n(r, "gets"));
      assertThat(n(r, "sdk_heads")).as(at + " HEADs").isEqualTo(n(r, "heads"));
    }
    assertThat(phases).anyMatch(r -> n(r, "sdk_appends") > 0);
    assertThat(phases).anyMatch(r -> n(r, "sdk_blob_creates") > 0);
    String prefix = AdlsClients.Location.of(root).path() + "/runs-exact/";
    assertThat(clients.container(root).listBlobsByHierarchy(prefix).stream())
        .as("the run's objects are deleted")
        .isEmpty();
  }

  /**
   * The fork appender ({@code append fork}) on the fork's atomic operations, with exact reads. An
   * append is one Append Block, to the snapshot the last write returned, so no lookup. A new
   * appendable file is a CAS against what is there: its absence looked up (a HEAD), a temporary
   * Append Blob staged (created, then one Append Block) and renamed onto the file (counted as
   * other). Azure only, since Azurite does not rename.
   */
  @Test
  void everyArmFamilyWithForkAppends(@TempDir Path dir) throws IOException {
    assumeTrue(FORK, "needs a -Pfork build");
    assumeTrue(renames(), "the fork's CAS on ADLS renames, which this store does not");
    BenchConfig cfg = config("exact");
    cfg.store.root = root + "/runs-fork";
    cfg.store.append = "fork";
    new Runner(cfg, dir, "t").run();
    List<JsonNode> phases = phases(dir);
    for (JsonNode r : phases) {
      String at = at(r);
      long creates = n(r, "sdk_blob_creates");
      assertThat(n(r, "sdk_puts") + creates).as(at + " PUTs").isEqualTo(n(r, "puts"));
      assertThat(n(r, "sdk_appends")).as(at + " appends").isEqualTo(n(r, "appends") + creates);
      assertThat(n(r, "sdk_other")).as(at + " renames").isEqualTo(creates);
      assertThat(n(r, "sdk_heads")).as(at + " HEADs").isEqualTo(n(r, "heads") + creates);
      assertThat(n(r, "sdk_gets")).as(at + " GETs").isEqualTo(n(r, "gets"));
      assertThat(n(r, "sdk_put_bytes")).as(at + " bytes written").isEqualTo(n(r, "bytes_written"));
      assertThat(n(r, "sdk_retries")).as(at + " retries").isZero();
    }
    assertThat(phases).anyMatch(r -> n(r, "sdk_appends") > 0);
  }

  /**
   * Iceberg's own reads: as shipped they send more than the model counts; on the fork, whose reads
   * are fixed, what it counts (sequential reads here are shorter than the SDK's 4 MiB chunk, each
   * a request).
   */
  @Test
  void icebergReadsAgainstTheModel(@TempDir Path dir) throws IOException {
    new Runner(config("iceberg"), dir, "t").run();
    List<JsonNode> phases = phases(dir);
    Map<String, long[]> byArm = new LinkedHashMap<>(); // model GETs, sent GETs, model bytes, sent
    for (JsonNode r : phases) {
      String at = at(r);
      writesMatchTheModel(r, at);
      if (FORK) {
        assertThat(n(r, "sdk_gets")).as(at + " GETs").isEqualTo(n(r, "gets"));
        assertThat(n(r, "sdk_heads")).as(at + " HEADs").isEqualTo(n(r, "heads"));
      } else {
        assertThat(n(r, "sdk_gets")).as(at + " GETs").isGreaterThanOrEqualTo(n(r, "gets"));
      }
      long[] t = byArm.computeIfAbsent(r.get("arm").asText(), k -> new long[4]);
      t[0] += n(r, "gets");
      t[1] += n(r, "sdk_gets");
      t[2] += n(r, "bytes_read");
      t[3] += n(r, "sdk_get_bytes");
    }
    if (!FORK) {
      assertThat(byArm.values()).anyMatch(t -> t[1] > t[0]);
    }
    byArm.forEach(
        (arm, t) ->
            System.out.printf(
                "iceberg reads, %s: GETs %d sent / %d modelled, MB %.1f / %.1f%n",
                arm, t[1], t[0], t[3] / 1e6, t[2] / 1e6));
  }
}
