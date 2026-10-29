package org.rmow.bench.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import org.junit.jupiter.api.Test;

/**
 * The recorder counts only requests to its store's hosts. On an Azure VM, Azure Identity's token
 * request to the instance metadata service passes through the same policies, and {@link
 * AzureRecorder#kind} takes it for a LIST: before the recorder kept to the store's hosts, it failed
 * {@code AzureTest}'s first write there (2026-10-28).
 */
class AzureRecorderTest {

  // the token request a VM's managed identity sends, as the recorder saw it on Azure
  private static final String TOKEN =
      "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2019-08-01"
          + "&resource=https%3A%2F%2Fstorage.azure.com";

  private static HttpRequest get(String url) {
    return new HttpRequest(HttpMethod.GET, url);
  }

  @Test
  void onAzureTheAccountsDataLakeAndBlobHostsCount() {
    try (AzureRecorder r = AzureRecorder.install("abfss://c@acct.dfs.core.windows.net/p", "")) {
      assertThat(r.counts(get("https://acct.dfs.core.windows.net/c/p/f"))).isTrue();
      assertThat(r.counts(get("https://acct.blob.core.windows.net/c/p/f"))).isTrue();
      assertThat(r.counts(get("https://other.blob.core.windows.net/c/p/f"))).isFalse();
      assertThat(r.counts(get(TOKEN))).isFalse();
      assertThat(r.counts(get("https://login.microsoftonline.com/t/oauth2/v2.0/token"))).isFalse();
    }
  }

  @Test
  void onAnEmulatorItsHostCounts() {
    try (AzureRecorder r =
        AzureRecorder.install(
            "abfss://bench@devstoreaccount1.dfs.core.windows.net/t",
            "http://localhost:32768/devstoreaccount1")) {
      assertThat(r.counts(get("http://localhost:32768/devstoreaccount1/bench/t/f"))).isTrue();
      assertThat(r.counts(get(TOKEN))).isFalse();
    }
  }

  @Test
  void theTokenRequestWouldCountAsAList() {
    assertThat(AzureRecorder.kind(get(TOKEN))).isEqualTo(AzureRecorder.Kind.LIST);
  }
}
