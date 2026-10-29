package org.rmow.bench.bench;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileIO;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.append.EmulatorAppenders;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.io.AdlsClients;
import org.rmow.bench.io.AzureRecorder;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.io.PreadFileIO;
import org.rmow.bench.io.S3Recorder;
import org.rmow.bench.io.SdkRecorder;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * The store a configuration names: its FileIO, its appender (null for a store without append),
 * and, in the cloud, the recorder of the requests its SDK sends. Every FileIO has the atomicio
 * fork's {@code SupportsAtomicOperations}, and every appender is {@link AtomicAppender} over it,
 * apart from an emulator's stand-ins ({@link EmulatorAppenders}).
 *
 * <ul>
 *   <li>{@code mem}: {@link MemFileIO};
 *   <li>{@code local}: {@link PreadFileIO}, positional reads and appends on a local filesystem;
 *   <li>{@code s3}: Iceberg's {@link S3FileIO} over one SDK client, built here with a {@link
 *       S3Recorder} and credentials from the default chain (environment, system properties,
 *       profile, instance), so none are in the configuration or its manifest. It appends only in
 *       a directory bucket (S3 Express One Zone, {@code <name>--<az id>--x-s3}); standard S3 runs
 *       no appendable arm. On an emulator ({@code endpoint}), appends rewrite the object, unmeasured.
 *   <li>{@code azure}: Iceberg's {@link ADLSFileIO} on a hierarchical-namespace account, RMoW's
 *       files as Append Blobs, and an {@link AzureRecorder}. Credentials come from the default
 *       chain, or, for an emulator ({@code endpoint}), the account key in {@code AZURE_STORAGE_KEY}
 *       (or the system property {@code azure.storage.key}).
 * </ul>
 */
final class Store implements AutoCloseable {

  /** Deletes everything under a run's root. */
  private interface RunDeleter {
    void delete(String root) throws IOException;
  }

  private final String type;
  private final FileIO io;
  private final RegionAppender appender;
  private final SdkRecorder recorder;
  private final List<AutoCloseable> owned;
  private final RunDeleter deleter; // null: by type

  private Store(
      String type,
      FileIO io,
      RegionAppender appender,
      SdkRecorder recorder,
      List<AutoCloseable> owned) {
    this(type, io, appender, recorder, owned, null);
  }

  private Store(
      String type,
      FileIO io,
      RegionAppender appender,
      SdkRecorder recorder,
      List<AutoCloseable> owned,
      RunDeleter deleter) {
    this.type = type;
    this.io = io;
    this.appender = appender;
    this.recorder = recorder;
    this.owned = owned;
    this.deleter = deleter;
  }

  static Store open(BenchConfig.StoreSpec spec) {
    return switch (spec.type) {
      case "mem" -> {
        MemFileIO mem = new MemFileIO();
        yield new Store("mem", mem, new AtomicAppender(mem), null, List.of());
      }
      case "local" -> {
        PreadFileIO local = new PreadFileIO(spec.fsync);
        yield new Store("local", local, new AtomicAppender(local), null, List.of());
      }
      case "s3" -> s3(spec);
      case "azure" -> azure(spec);
      default -> throw new IllegalArgumentException("Unknown store type: " + spec.type);
    };
  }

  private static Store s3(BenchConfig.StoreSpec spec) {
    S3Recorder recorder = new S3Recorder();
    S3FileIO io = s3FileIO(client(spec, recorder));
    if (!spec.endpoint.isEmpty()) {
      // versitygw ignores write offsets: appends rewrite the object, on a client nothing records
      S3FileIO unmeasured = s3FileIO(client(spec, null));
      return new Store(
          "s3", io, EmulatorAppenders.rewriting(io, unmeasured), recorder, List.of(io, unmeasured));
    }
    RegionAppender appender = directoryBucket(spec.root) ? new AtomicAppender(io) : null;
    return new Store("s3", io, appender, recorder, List.of(io));
  }

  /**
   * S3FileIO over {@code client}, which it closes. It is given an async client supplier only so
   * that it does not build clients of its own: nothing here reads through an async client (the
   * analytics accelerator is off), and any read that did would go unmeasured, so it fails instead.
   */
  static S3FileIO s3FileIO(S3Client client) {
    S3FileIO io =
        new S3FileIO(
            () -> client,
            () -> {
              throw new UnsupportedOperationException("The benchmark's S3 store is synchronous");
            });
    io.initialize(Map.of());
    return io;
  }

  /** Whether {@code root} is in a directory bucket, the only S3 buckets that append. */
  static boolean directoryBucket(String root) {
    String path = root.substring(root.indexOf("://") + 3);
    int slash = path.indexOf('/');
    return (slash < 0 ? path : path.substring(0, slash)).endsWith("--x-s3");
  }

  private static Store azure(BenchConfig.StoreSpec spec) {
    boolean emulator = !spec.endpoint.isEmpty();
    String key = System.getProperty("azure.storage.key", System.getenv("AZURE_STORAGE_KEY"));
    var props = AdlsClients.properties(spec.root, spec.endpoint, emulator ? key : null);
    AzureRecorder recorder = AzureRecorder.install(spec.root, spec.endpoint);
    ADLSFileIO adls = new ADLSFileIO();
    adls.initialize(props);
    RunDeleter deleter;
    RegionAppender appender;
    if (emulator) {
      AdlsClients clients = new AdlsClients(props);
      // an emulator starts empty, and has no Data Lake endpoint for a directory delete
      BlobContainerClient container = clients.container(spec.root);
      container.createIfNotExists();
      deleter =
          root -> {
            String prefix = AdlsClients.Location.of(root).path() + "/";
            for (BlobItem b : container.listBlobs(new ListBlobsOptions().setPrefix(prefix), null)) {
              container.getBlobClient(b.getName()).delete();
            }
          };
      // Azurite cannot rename, as the fork's atomic create does
      appender = EmulatorAppenders.appendBlobs(clients, adls);
    } else {
      deleter = adls::deletePrefix; // one recursive directory delete
      appender = new AtomicAppender(adls);
    }
    return new Store("azure", adls, appender, recorder, List.of(adls, recorder), deleter);
  }

  private static S3Client client(BenchConfig.StoreSpec spec, S3Recorder recorder) {
    String region = !spec.region.isEmpty() ? spec.region : System.getenv("AWS_REGION");
    S3ClientBuilder b =
        S3Client.builder()
            .httpClientBuilder(ApacheHttpClient.builder().maxConnections(spec.maxConnections))
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .region(Region.of(region == null || region.isEmpty() ? "us-east-1" : region))
            .forcePathStyle(spec.pathStyle);
    if (!spec.endpoint.isEmpty()) {
      b.endpointOverride(URI.create(spec.endpoint));
    }
    if (recorder != null) {
      b.overrideConfiguration(o -> o.addExecutionInterceptor(recorder));
    }
    return b.build();
  }

  FileIO io() {
    return io;
  }

  /** Null if the store cannot append. */
  RegionAppender appender() {
    return appender;
  }

  /** Null unless the store is in the cloud. */
  SdkRecorder recorder() {
    return recorder;
  }

  /** Deletes a run's objects: everything under {@code root}, which names that run alone. */
  void deleteRun(String root) throws IOException {
    if (deleter != null) {
      deleter.delete(root);
      return;
    }
    switch (type) {
      case "local" -> {
        Path dir = Path.of(root.startsWith("file:") ? root.substring(5) : root);
        if (Files.exists(dir)) {
          try (var paths = Files.walk(dir)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
              Files.delete(p);
            }
          }
        }
      }
      case "s3" -> ((S3FileIO) io).deletePrefix(root + "/");
      default -> {} // in-memory objects go with the process
    }
  }

  @Override
  public void close() throws Exception {
    for (AutoCloseable c : owned) {
      c.close();
    }
  }
}
