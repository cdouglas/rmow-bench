package org.rmow.bench.bench;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.append.S3Appenders;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.io.PreadFileIO;
import org.rmow.bench.io.S3Recorder;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * The store a configuration names: its FileIO, its appender (null for a store without append),
 * and, for S3, the recorder of the requests its client sends.
 *
 * <ul>
 *   <li>{@code mem}: {@link MemFileIO};
 *   <li>{@code local}: {@link PreadFileIO}, positional reads and appends on a local filesystem;
 *   <li>{@code s3}: Iceberg's {@link S3FileIO} over one SDK client, built here with a {@link
 *       S3Recorder} and credentials from the default chain (environment, system properties,
 *       profile, instance), so none are in the configuration or its manifest. Appends are {@code
 *       express} (S3 Express One Zone), {@code emulated} (for emulators, unmeasured) or {@code
 *       none} (standard S3).
 * </ul>
 */
final class Store implements AutoCloseable {

  private final String type;
  private final FileIO io;
  private final RegionAppender appender;
  private final S3Recorder recorder;
  private final List<AutoCloseable> owned;

  private Store(
      String type,
      FileIO io,
      RegionAppender appender,
      S3Recorder recorder,
      List<AutoCloseable> owned) {
    this.type = type;
    this.io = io;
    this.appender = appender;
    this.recorder = recorder;
    this.owned = owned;
  }

  static Store open(BenchConfig.StoreSpec spec) {
    return switch (spec.type) {
      case "mem" -> {
        MemFileIO mem = new MemFileIO();
        yield new Store("mem", mem, mem, null, List.of());
      }
      case "local" -> {
        PreadFileIO local = new PreadFileIO(spec.fsync);
        yield new Store("local", local, local, null, List.of());
      }
      case "s3" -> s3(spec);
      default -> throw new IllegalArgumentException("Unknown store type: " + spec.type);
    };
  }

  private static Store s3(BenchConfig.StoreSpec spec) {
    S3Recorder recorder = new S3Recorder();
    S3Client client = client(spec, recorder);
    // S3FileIO closes the client. It is given an async client supplier only so that it does not
    // build clients of its own: nothing here reads through an async client (the analytics
    // accelerator is off), and any read that did would go unmeasured, so it fails instead.
    S3FileIO io =
        new S3FileIO(
            () -> client,
            () -> {
              throw new UnsupportedOperationException("The benchmark's S3 store is synchronous");
            });
    io.initialize(Map.of());
    return switch (spec.append) {
      case "express" -> new Store("s3", io, S3Appenders.express(client), recorder, List.of(io));
      case "emulated" -> {
        S3Client unmeasured = client(spec, null);
        yield new Store(
            "s3", io, S3Appenders.emulated(unmeasured), recorder, List.of(io, unmeasured));
      }
      case "none" -> new Store("s3", io, null, recorder, List.of(io));
      default -> throw new IllegalArgumentException("Unknown S3 append mode: " + spec.append);
    };
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

  /** Null unless the store is S3. */
  S3Recorder recorder() {
    return recorder;
  }

  /** Deletes a run's objects: everything under {@code root}, which names that run alone. */
  void deleteRun(String root) throws IOException {
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
