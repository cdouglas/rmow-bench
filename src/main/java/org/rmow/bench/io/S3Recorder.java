package org.rmow.bench.io;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.services.s3.model.CreateSessionRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Counts the requests an S3 client sends, as an interceptor on the one client that S3FileIO's
 * streams and the appender share. {@link TracingFileIO} models what Iceberg's S3 streams send;
 * these counts check that model against the SDK on a real store (aborts stay modelled only: the
 * SDK does not see a stream being abandoned).
 */
public final class S3Recorder implements ExecutionInterceptor, SdkRecorder {

  /**
   * Requests by operation, transmission attempts (a retry is a second attempt of one request), and
   * bytes: what GET responses declare (an aborted stream transfers fewer) and what PUTs send.
   * {@code sessions} are S3 Express's {@code CreateSession} requests, which the client sends for
   * credentials, not for the data: they count in {@link #requests} and cost, not in the model.
   */
  public record Counts(
      long gets,
      long puts,
      long appends,
      long heads,
      long deletes,
      long lists,
      long sessions,
      long other,
      long attempts,
      long getBytes,
      long putBytes) {

    public Counts minus(Counts o) {
      return new Counts(
          gets - o.gets,
          puts - o.puts,
          appends - o.appends,
          heads - o.heads,
          deletes - o.deletes,
          lists - o.lists,
          sessions - o.sessions,
          other - o.other,
          attempts - o.attempts,
          getBytes - o.getBytes,
          putBytes - o.putBytes);
    }

    public long requests() {
      return gets + puts + appends + heads + deletes + lists + sessions + other;
    }

    /** As step-record fields, prefixed {@code sdk_}. */
    public Map<String, Long> fields() {
      Map<String, Long> m = new LinkedHashMap<>();
      m.put("sdk_requests", requests());
      m.put("sdk_gets", gets);
      m.put("sdk_puts", puts);
      m.put("sdk_appends", appends);
      m.put("sdk_heads", heads);
      m.put("sdk_deletes", deletes);
      m.put("sdk_lists", lists);
      m.put("sdk_sessions", sessions);
      m.put("sdk_other", other);
      m.put("sdk_retries", attempts - requests());
      m.put("sdk_get_bytes", getBytes);
      m.put("sdk_put_bytes", putBytes);
      return m;
    }
  }

  private final LongAdder gets = new LongAdder();
  private final LongAdder puts = new LongAdder();
  private final LongAdder appends = new LongAdder();
  private final LongAdder heads = new LongAdder();
  private final LongAdder deletes = new LongAdder();
  private final LongAdder lists = new LongAdder();
  private final LongAdder sessions = new LongAdder();
  private final LongAdder other = new LongAdder();
  private final LongAdder attempts = new LongAdder();
  private final LongAdder getBytes = new LongAdder();
  private final LongAdder putBytes = new LongAdder();

  public Counts snapshot() {
    return new Counts(
        gets.sum(),
        puts.sum(),
        appends.sum(),
        heads.sum(),
        deletes.sum(),
        lists.sum(),
        sessions.sum(),
        other.sum(),
        attempts.sum(),
        getBytes.sum(),
        putBytes.sum());
  }

  @Override
  public Map<String, Long> fields() {
    return snapshot().fields();
  }

  @Override
  public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes attributes) {
    SdkRequest r = context.request();
    if (r instanceof GetObjectRequest) {
      gets.increment();
    } else if (r instanceof PutObjectRequest p) {
      (p.writeOffsetBytes() != null ? appends : puts).increment();
    } else if (r instanceof HeadObjectRequest) {
      heads.increment();
    } else if (r instanceof DeleteObjectRequest || r instanceof DeleteObjectsRequest) {
      deletes.increment();
    } else if (r instanceof ListObjectsV2Request || r instanceof ListObjectsRequest) {
      lists.increment();
    } else if (r instanceof CreateSessionRequest) {
      sessions.increment(); // S3 Express credentials, if the SDK sends them through this client
    } else {
      other.increment();
    }
  }

  @Override
  public void beforeTransmission(
      Context.BeforeTransmission context, ExecutionAttributes attributes) {
    attempts.increment();
  }

  @Override
  public void afterTransmission(Context.AfterTransmission context, ExecutionAttributes attributes) {
    if (context.httpRequest().method() == SdkHttpMethod.GET) {
      context
          .httpResponse()
          .firstMatchingHeader("Content-Length")
          .ifPresent(v -> getBytes.add(Long.parseLong(v)));
    } else if (context.httpRequest().method() == SdkHttpMethod.PUT) {
      // with a streamed checksum the body is aws-chunked; the decoded length is the payload's
      context
          .httpRequest()
          .firstMatchingHeader("x-amz-decoded-content-length")
          .or(() -> context.httpRequest().firstMatchingHeader("Content-Length"))
          .ifPresent(v -> putBytes.add(Long.parseLong(v)));
    }
  }
}
