package org.rmow.bench.io;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.AfterRetryPolicyProvider;
import com.azure.core.http.policy.BeforeRetryPolicyProvider;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.BinaryData;
import java.net.URI;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import reactor.core.publisher.Mono;

/**
 * Counts the requests the Azure SDK sends. Iceberg's ADLSFileIO builds its clients itself, so
 * this records through the SDK's own hook: azure-core adds the policies of every {@link
 * BeforeRetryPolicyProvider} and {@link AfterRetryPolicyProvider} on the classpath ({@code
 * META-INF/services}) to every pipeline a storage builder makes. {@link Requests} sees each request
 * once, {@link Attempts} each transmission. The policies record into the {@linkplain
 * #install(String, String) installed} recorder, if any; one store at a time per JVM.
 *
 * <p>Only requests to the store's hosts count. Azure Identity's pipelines get the policies too, and
 * on a VM its token request to the instance metadata service (a GET with {@code resource=} in its
 * query) would count as a LIST, its token as GET bytes.
 *
 * <p>Requests are classified by what reaches the Blob service: Iceberg's output streams upload a
 * block blob (one {@code PutBlob} up to 256 MiB, else blocks and a block list), its reads are
 * ranged GETs, and RMoW's files are Append Blobs, created and then extended by {@code
 * AppendBlock}.
 */
public final class AzureRecorder implements SdkRecorder, AutoCloseable {

  /** What a request asked the service for. */
  public enum Kind {
    GET, // Get Blob (a read, ranged or not)
    HEAD, // Get Blob Properties
    PUT, // Put Blob: a whole block blob in one request
    BLOCK, // Put Block: one block of a staged upload
    BLOCK_LIST, // Put Block List: commits a staged upload
    BLOB_CREATE, // Put Blob of an Append Blob: an empty one
    APPEND, // Append Block
    DELETE,
    LIST,
    OTHER // container and Data Lake calls, none of them in a measured phase
  }

  private static volatile AzureRecorder active;

  private final Set<String> hosts; // the store's
  private final Map<Kind, LongAdder> requests = new EnumMap<>(Kind.class);
  private final LongAdder attempts = new LongAdder();
  private final LongAdder getBytes = new LongAdder();
  private final LongAdder putBytes = new LongAdder();

  private AzureRecorder(Set<String> hosts) {
    this.hosts = hosts;
    for (Kind k : Kind.values()) {
      requests.put(k, new LongAdder());
    }
  }

  /**
   * A new recorder of the requests to the store under {@code root}, which the providers' policies
   * record into until it is closed. {@code endpoint} is an emulator's, as {@link
   * AdlsClients#properties} takes it, or empty for Azure, where the store's hosts are the root's
   * account's Data Lake and Blob endpoints.
   */
  public static AzureRecorder install(String root, String endpoint) {
    Set<String> hosts;
    if (endpoint != null && !endpoint.isEmpty()) {
      hosts = Set.of(URI.create(endpoint).getHost());
    } else {
      String host = AdlsClients.Location.of(root).host();
      hosts = Set.of(host, host.replace(".dfs.", ".blob."));
    }
    AzureRecorder r = new AzureRecorder(hosts);
    active = r;
    return r;
  }

  /** Whether {@code request} goes to the store. */
  boolean counts(HttpRequest request) {
    return hosts.contains(request.getUrl().getHost());
  }

  @Override
  public void close() {
    if (active == this) {
      active = null;
    }
  }

  public long requests(Kind kind) {
    return requests.get(kind).sum();
  }

  public long getBytes() {
    return getBytes.sum();
  }

  public long putBytes() {
    return putBytes.sum();
  }

  @Override
  public Map<String, Long> fields() {
    Map<String, Long> m = new LinkedHashMap<>();
    long total = 0;
    for (LongAdder a : requests.values()) {
      total += a.sum();
    }
    m.put("sdk_requests", total);
    for (Kind k : Kind.values()) {
      m.put("sdk_" + plural(k), requests(k));
    }
    m.put("sdk_retries", attempts.sum() - total);
    m.put("sdk_get_bytes", getBytes.sum());
    m.put("sdk_put_bytes", putBytes.sum());
    return m;
  }

  private static String plural(Kind k) {
    return switch (k) {
      case BLOCK_LIST -> "block_lists";
      case BLOB_CREATE -> "blob_creates";
      case OTHER -> "other";
      default -> k.name().toLowerCase(Locale.ROOT) + "s";
    };
  }

  /** The kind of a request, from its method, its query and the blob type it declares. */
  public static Kind kind(HttpRequest r) {
    String query = r.getUrl().getQuery();
    String comp = param(query, "comp");
    HttpMethod method = r.getHttpMethod();
    if (method == HttpMethod.HEAD) {
      return Kind.HEAD;
    } else if (method == HttpMethod.DELETE) {
      return Kind.DELETE;
    } else if (method == HttpMethod.GET) {
      if ("list".equals(comp) || param(query, "resource") != null) {
        return Kind.LIST;
      }
      return comp == null ? Kind.GET : Kind.OTHER;
    } else if (method == HttpMethod.PUT && param(query, "restype") == null
        && param(query, "resource") == null) {
      if (comp == null) {
        String type = r.getHeaders().getValue(HttpHeaderName.fromString("x-ms-blob-type"));
        if ("AppendBlob".equals(type)) {
          return Kind.BLOB_CREATE;
        }
        return "BlockBlob".equals(type) ? Kind.PUT : Kind.OTHER;
      }
      return switch (comp) {
        case "appendblock" -> Kind.APPEND;
        case "block" -> Kind.BLOCK;
        case "blocklist" -> Kind.BLOCK_LIST;
        default -> Kind.OTHER;
      };
    }
    return Kind.OTHER;
  }

  private static String param(String query, String name) {
    if (query == null) {
      return null;
    }
    for (String kv : query.split("&")) {
      int eq = kv.indexOf('=');
      if ((eq < 0 ? kv : kv.substring(0, eq)).equals(name)) {
        return eq < 0 ? "" : kv.substring(eq + 1);
      }
    }
    return null;
  }

  private void attempted(HttpRequest request, HttpResponse response) {
    attempts.increment();
    if (request.getHttpMethod() == HttpMethod.GET && response != null) {
      // what the response declares; an abandoned stream transfers fewer
      String length = response.getHeaderValue(HttpHeaderName.CONTENT_LENGTH);
      if (length != null) {
        getBytes.add(Long.parseLong(length));
      }
    } else if (request.getHttpMethod() == HttpMethod.PUT) {
      String length = request.getHeaders().getValue(HttpHeaderName.CONTENT_LENGTH);
      if (length != null) {
        putBytes.add(Long.parseLong(length));
      } else {
        BinaryData body = request.getBodyAsBinaryData();
        Long n = body == null ? null : body.getLength();
        if (n != null) {
          putBytes.add(n);
        }
      }
    }
  }

  /** Before the retry policy: once per request. */
  public static final class Requests implements BeforeRetryPolicyProvider {
    @Override
    public HttpPipelinePolicy create() {
      return new HttpPipelinePolicy() {
        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext ctx, HttpPipelineNextPolicy next) {
          count(ctx.getHttpRequest());
          return next.process();
        }

        @Override
        public HttpResponse processSync(
            HttpPipelineCallContext ctx, HttpPipelineNextSyncPolicy next) {
          count(ctx.getHttpRequest());
          return next.processSync();
        }

        private void count(HttpRequest request) {
          AzureRecorder r = active;
          if (r != null && r.counts(request)) {
            r.requests.get(kind(request)).increment();
          }
        }
      };
    }
  }

  /** After the retry policy: once per transmission, with its bytes. */
  public static final class Attempts implements AfterRetryPolicyProvider {
    @Override
    public HttpPipelinePolicy create() {
      return new HttpPipelinePolicy() {
        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext ctx, HttpPipelineNextPolicy next) {
          AzureRecorder r = active;
          if (r == null || !r.counts(ctx.getHttpRequest())) {
            return next.process();
          }
          return next.process().doOnNext(response -> r.attempted(ctx.getHttpRequest(), response));
        }

        @Override
        public HttpResponse processSync(
            HttpPipelineCallContext ctx, HttpPipelineNextSyncPolicy next) {
          AzureRecorder r = active;
          HttpResponse response = next.processSync();
          if (r != null && r.counts(ctx.getHttpRequest())) {
            r.attempted(ctx.getHttpRequest(), response);
          }
          return response;
        }
      };
    }
  }
}
