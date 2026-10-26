package org.rmow.bench.bench;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The Azure store against Azurite, Microsoft's Blob emulator. Azurite has no Data Lake endpoint,
 * which only a directory delete needs; the store deletes an emulator's runs blob by blob.
 */
class AzureStoreTest extends AzureStoreContract {

  private static final String IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.37.0";
  private static final String ACCOUNT = "devstoreaccount1";
  // Azurite's well-known development key, not a secret
  private static final String KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  private GenericContainer<?> azurite;
  private String savedKey;

  @BeforeAll
  void start() {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    azurite =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(10000)
            .withCommand(
                "azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck", "--loose")
            .waitingFor(Wait.forLogMessage(".*successfully listens.*", 1));
    azurite.start();
    String endpoint =
        "http://" + azurite.getHost() + ":" + azurite.getMappedPort(10000) + "/" + ACCOUNT;
    // the store reads an emulator's key from here (or AZURE_STORAGE_KEY)
    savedKey = System.getProperty("azure.storage.key");
    System.setProperty("azure.storage.key", KEY);
    connect("abfss://bench@" + ACCOUNT + ".dfs.core.windows.net/t", endpoint, KEY);
  }

  @AfterAll
  void stop() {
    if (savedKey == null) {
      System.clearProperty("azure.storage.key");
    } else {
      System.setProperty("azure.storage.key", savedKey);
    }
    if (azurite != null) {
      azurite.stop();
    }
  }
}
