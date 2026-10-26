package org.rmow.bench.bench;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * The Azure store against Azure itself, which no emulator stands in for entirely (Azurite has no
 * Data Lake endpoint and is not the service). Runs only with {@code RMOW_AZURE_ROOT} set to a
 * prefix in a container of a hierarchical-namespace account, {@code
 * abfss://<container>@<account>.dfs.core.windows.net/<prefix>}, and credentials in the default
 * chain (a VM's managed identity, the environment, or the Azure CLI). Everything it writes goes
 * under a prefix of its own, deleted at the end.
 */
class AzureTest extends AzureStoreContract {

  private String prefix;

  @BeforeAll
  void open() {
    String root = System.getenv("RMOW_AZURE_ROOT");
    assumeTrue(root != null && !root.isEmpty(), "RMOW_AZURE_ROOT is not set");
    prefix = root + "/test-" + Instant.now().toEpochMilli();
    connect(prefix, "", null);
  }

  @AfterAll
  void close() {
    if (prefix != null) {
      adls().deletePrefix(prefix);
    }
  }
}
