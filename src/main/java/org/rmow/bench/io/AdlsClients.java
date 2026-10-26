package org.rmow.bench.io;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.azure.AzureProperties;

/**
 * Azure clients for {@code abfss://} locations, configured as Iceberg's ADLSFileIO configures its
 * own ({@link AzureProperties#applyClientConfiguration}), so the appender and the exact reads reach
 * the same endpoint with the same credentials. Their pipelines carry {@link AzureRecorder}'s
 * policies, as ADLSFileIO's do.
 */
public final class AdlsClients {

  /** {@code abfss://<container>@<account>.dfs.core.windows.net/<path>}. */
  public record Location(String container, String host, String path) {
    public static Location of(String location) {
      int scheme = location.indexOf("://");
      int at = location.indexOf('@', scheme + 3);
      int slash = location.indexOf('/', scheme + 3);
      if (scheme < 0 || at < 0 || slash < at) {
        throw new IllegalArgumentException(
            "Not an ADLS location (abfss://container@account.dfs.core.windows.net/path): "
                + location);
      }
      return new Location(
          location.substring(scheme + 3, at),
          location.substring(at + 1, slash),
          location.substring(slash + 1));
    }

    public String account() {
      int dot = host.indexOf('.');
      return dot < 0 ? host : host.substring(0, dot);
    }
  }

  /**
   * ADLSFileIO's properties for a store under {@code root}. {@code endpoint} replaces Azure's (an
   * emulator's, such as Azurite's {@code http://127.0.0.1:10000/devstoreaccount1}); {@code
   * accountKey} signs with the root's account key. Without a key, the default credential chain
   * applies (a VM's managed identity, environment, Azure CLI), so no secret is in a configuration.
   */
  public static Map<String, String> properties(String root, String endpoint, String accountKey) {
    Location l = Location.of(root);
    Map<String, String> p = new HashMap<>();
    if (endpoint != null && !endpoint.isEmpty()) {
      // Iceberg uses this property's value as the account's endpoint
      p.put(AzureProperties.ADLS_CONNECTION_STRING_PREFIX + l.host(), endpoint);
    }
    if (accountKey != null && !accountKey.isEmpty()) {
      p.put(AzureProperties.ADLS_SHARED_KEY_ACCOUNT_NAME, l.account());
      p.put(AzureProperties.ADLS_SHARED_KEY_ACCOUNT_KEY, accountKey);
    }
    return p;
  }

  private final AzureProperties azure;
  private final HttpClient http = HttpClient.createDefault(); // ADLSFileIO's kind of client
  private final Map<String, DataLakeFileSystemClient> fileSystems = new ConcurrentHashMap<>();
  private final Map<String, DataLakeFileSystemAsyncClient> asyncFileSystems =
      new ConcurrentHashMap<>();

  public AdlsClients(Map<String, String> properties) {
    this.azure = new AzureProperties(properties);
  }

  private DataLakeFileSystemClientBuilder builder(Location l) {
    DataLakeFileSystemClientBuilder b =
        new DataLakeFileSystemClientBuilder().httpClient(http).fileSystemName(l.container());
    azure.applyClientConfiguration(l.host(), b);
    return b;
  }

  private DataLakeFileSystemClient fileSystem(Location l) {
    return fileSystems.computeIfAbsent(
        l.host() + "/" + l.container(), k -> builder(l).buildClient());
  }

  public DataLakeFileClient file(String location) {
    Location l = Location.of(location);
    return fileSystem(l).getFileClient(l.path());
  }

  public DataLakeFileAsyncClient fileAsync(String location) {
    Location l = Location.of(location);
    return asyncFileSystems
        .computeIfAbsent(l.host() + "/" + l.container(), k -> builder(l).buildAsyncClient())
        .getFileAsyncClient(l.path());
  }

  /** The Blob API's view of a file, as an Append Blob: same pipeline, blob endpoint. */
  public AppendBlobClient appendBlob(String location) {
    return blob(location).buildAppendBlobClient();
  }

  /** The Blob API's view of a file, as a block blob. */
  public BlockBlobClient blockBlob(String location) {
    return blob(location).buildBlockBlobClient();
  }

  private SpecializedBlobClientBuilder blob(String location) {
    DataLakeFileClient f = file(location);
    return new SpecializedBlobClientBuilder()
        .pipeline(f.getHttpPipeline())
        .endpoint(blobUrl(f.getFileUrl()));
  }

  /** The Blob API's view of a location's container. */
  public BlobContainerClient container(String location) {
    DataLakeFileSystemClient fs = fileSystem(Location.of(location));
    return new BlobContainerClientBuilder()
        .pipeline(fs.getHttpPipeline())
        .endpoint(blobUrl(fs.getFileSystemUrl()))
        .buildClient();
  }

  private static String blobUrl(String dfsUrl) {
    // an emulator's single endpoint serves both APIs and has no ".dfs." in its host
    return dfsUrl.replaceFirst("^(https?://[^/]*?)\\.dfs\\.", "$1.blob.");
  }
}
