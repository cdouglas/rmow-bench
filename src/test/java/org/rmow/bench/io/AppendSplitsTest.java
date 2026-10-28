package org.rmow.bench.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.rmow.bench.append.RegionAppender;

/**
 * Where a store caps an append (Azure's Append Block, 100 MiB), the fork splits a larger append or
 * appendable create into several: the trace counts the extra ones apart from the model's requests,
 * and injected latency prices each.
 */
class AppendSplitsTest {

  private static final long CAP = IoTrace.CAPPED_APPEND_BYTES;

  /** An appender that writes nothing: only the sizes matter here. */
  private static final RegionAppender NOWHERE =
      new RegionAppender() {
        @Override
        public long append(String location, long expectedLength, byte[] bytes) {
          return expectedLength + bytes.length;
        }

        @Override
        public void create(String location, byte[] bytes) {}
      };

  @Test
  void aWriteSplitsOncePerCapItExceeds() {
    assertThat(IoTrace.splits(0)).isZero();
    assertThat(IoTrace.splits(1)).isZero();
    assertThat(IoTrace.splits(CAP)).isZero();
    assertThat(IoTrace.splits(CAP + 1)).isEqualTo(1);
    assertThat(IoTrace.splits(2 * CAP)).isEqualTo(1);
    assertThat(IoTrace.splits(2 * CAP + 1)).isEqualTo(2);
  }

  @Test
  void theTraceCountsSplitsApartFromRequests() throws IOException {
    IoTrace trace = new IoTrace();
    TracingFileIO io = new TracingFileIO(new MemFileIO(), NOWHERE, trace);
    io.create("mem://t/a", new byte[(int) (2 * CAP + 1)]); // three parts
    io.append("mem://t/a", 2 * CAP + 1, new byte[(int) CAP + 1]); // two
    io.append("mem://t/a", 3 * CAP + 2, new byte[1024]); // one
    IoTrace.Counters c = trace.snapshot();
    assertThat(c.puts()).isEqualTo(1);
    assertThat(c.appends()).isEqualTo(2);
    assertThat(c.requests()).isEqualTo(3);
    assertThat(c.appendSplits()).isEqualTo(3);
  }

  @Test
  void injectedLatencyPricesEachPart() throws IOException {
    // 30 ms a PUT, 20 ms an append; no transfer time
    StoreLatency capped = new StoreLatency(0, 0, 30, 0, 20, 0, 0, 0, 0, 100);
    long t = System.nanoTime();
    capped.append("mem://t/a", CAP + 1);
    assertThat(System.nanoTime() - t).isGreaterThanOrEqualTo(40_000_000L);
    t = System.nanoTime();
    capped.create("mem://t/b", 2 * CAP + 1);
    assertThat(System.nanoTime() - t).isGreaterThanOrEqualTo(70_000_000L);

    assertThatThrownBy(() -> new StoreLatency(0, 0, 30, 0, 20, 0, 0, 0, 0, 4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxAppendMib");
  }
}
