package org.rmow.bench.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

/**
 * A simulated store's latency, injected into requests ({@link TracingFileIO}): each request sleeps
 * its expected latency, so a phase's wall time becomes what it would take on that store, with the
 * overlap of parallel readers included. The model is the one {@code lab/views/40_latency.sql}
 * replays over the request log:
 *
 * <ul>
 *   <li>a GET or HEAD costs {@code get}, a PUT or DELETE {@code put}, an append {@code append},
 *       each a lognormal's mean (median × e^(σ²/2));
 *   <li>a GET of an object that took appends costs {@code part} more per appended part;
 *   <li>each byte read or written costs {@code 1 / rate}.
 * </ul>
 *
 * Latencies are their expected values, not samples, so a replay of the same requests predicts
 * the same time.
 */
public final class StoreLatency {

  private static final double MIB = 1024 * 1024;

  private final long getNs;
  private final long putNs;
  private final long appendNs;
  private final long partNs;
  private final double readNsPerByte;
  private final double writeNsPerByte;
  private final Map<String, Integer> appended = new ConcurrentHashMap<>(); // parts after the first

  /**
   * @param getMs median GET (and HEAD) latency, with its lognormal {@code getSigma}
   * @param putMs median PUT (and DELETE) latency
   * @param appendMs median append latency
   * @param partMs extra GET latency per appended part of the object
   * @param readMsPerMib transfer time of a MiB read
   * @param writeMsPerMib transfer time of a MiB written
   */
  public StoreLatency(
      double getMs,
      double getSigma,
      double putMs,
      double putSigma,
      double appendMs,
      double appendSigma,
      double partMs,
      double readMsPerMib,
      double writeMsPerMib) {
    this.getNs = expectedNs(getMs, getSigma);
    this.putNs = expectedNs(putMs, putSigma);
    this.appendNs = expectedNs(appendMs, appendSigma);
    this.partNs = (long) (partMs * 1e6);
    this.readNsPerByte = readMsPerMib * 1e6 / MIB;
    this.writeNsPerByte = writeMsPerMib * 1e6 / MIB;
  }

  private static long expectedNs(double medianMs, double sigma) {
    return (long) (medianMs * Math.exp(sigma * sigma / 2) * 1e6);
  }

  private long parts(String path) {
    return appended.getOrDefault(path, 0);
  }

  /** A bounded ranged GET of {@code bytes}. */
  void get(String path, long bytes) {
    sleep(getNs + parts(path) * partNs + (long) (bytes * readNsPerByte));
  }

  /** The first byte of an open-ended GET; its bytes come through {@link #transfer}. */
  void open(String path) {
    sleep(getNs + parts(path) * partNs);
  }

  /** Bytes a stream delivered or read through: their transfer time, in ns, to sleep later. */
  long transfer(long bytes) {
    return (long) (bytes * readNsPerByte);
  }

  void head() {
    sleep(getNs);
  }

  void put(String path, long bytes) {
    appended.remove(path); // a PUT replaces the object: one part
    sleep(putNs + (long) (bytes * writeNsPerByte));
  }

  void append(String path, long bytes) {
    appended.merge(path, 1, Integer::sum);
    sleep(appendNs + (long) (bytes * writeNsPerByte));
  }

  void delete(String path) {
    appended.remove(path);
    sleep(putNs);
  }

  /** Sleeps {@code ns}, whatever the scheduler's early wake-ups. */
  static void sleep(long ns) {
    long deadline = System.nanoTime() + ns;
    for (long left = ns; left > 0; left = deadline - System.nanoTime()) {
      LockSupport.parkNanos(left);
    }
  }
}
