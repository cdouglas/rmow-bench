package org.rmow.bench.bench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.S3Recorder;

/**
 * Times named phases of one arm: wall-clock and thread CPU time, and the requests the arm's
 * {@link IoTrace} saw meanwhile. Arms run one at a time, so a trace delta belongs to the phase.
 *
 * <p>On S3, each phase also records the requests the SDK client sent meanwhile ({@code sdk_*}
 * fields), which check the trace's model of Iceberg's streams.
 *
 * <p>Phases nest: the runner times each whole batch as {@code batch}, and arms time their parts
 * inside it ({@code merge-read}, {@code write}, {@code delete-write}). Analysis must not add a
 * phase to the phase that contains it.
 *
 * <p>When the trace logs events (the request log), an outermost phase takes the events logged
 * while it ran, its nested phases' included; events logged outside every phase are dropped.
 */
public final class Phases {

  /**
   * One phase's measurements: when it started (epoch milliseconds, the clock a campaign's load
   * samples use), thread CPU of the arm's thread, and process CPU (which also counts read-pool
   * threads, and the JVM's own GC and JIT threads). {@code extra} holds phase-specific values
   * (e.g. rows read); {@code events}, the logged requests of an outermost phase.
   */
  public record Measured(
      String phase,
      long startMs,
      long wallNs,
      long cpuNs,
      long processCpuNs,
      IoTrace.Counters io,
      Map<String, Object> extra,
      List<IoTrace.Event> events) {}

  @FunctionalInterface
  public interface IoSupplier<T> {
    T get() throws IOException;
  }

  @FunctionalInterface
  public interface IoRunnable {
    void run() throws IOException;
  }

  private static final ThreadMXBean MX = ManagementFactory.getThreadMXBean();
  private static final com.sun.management.OperatingSystemMXBean OS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  private final IoTrace trace;
  private final S3Recorder recorder; // null unless the store is S3
  private final List<Measured> measured = new ArrayList<>();
  private int depth = 0;

  public Phases(IoTrace trace) {
    this(trace, null);
  }

  public Phases(IoTrace trace, S3Recorder recorder) {
    this.trace = trace;
    this.recorder = recorder;
  }

  public IoTrace trace() {
    return trace;
  }

  public <T> T run(String phase, IoSupplier<T> body) throws IOException {
    return run(phase, body, new LinkedHashMap<>());
  }

  /** Runs {@code body}; it may add phase-specific values to {@code extra} while it runs. */
  public <T> T run(String phase, IoSupplier<T> body, Map<String, Object> extra)
      throws IOException {
    boolean outermost = depth++ == 0;
    if (outermost && trace.logging()) {
      trace.drainEvents(); // what ran outside every phase is not measured
    }
    IoTrace.Counters before = trace.snapshot();
    S3Recorder.Counts sdk0 = recorder == null ? null : recorder.snapshot();
    long proc0 = OS.getProcessCpuTime();
    long cpu0 = MX.getCurrentThreadCpuTime();
    long startMs = System.currentTimeMillis();
    long t0 = System.nanoTime();
    T result;
    try {
      result = body.get();
    } finally {
      depth--;
    }
    long wall = System.nanoTime() - t0;
    long cpu = MX.getCurrentThreadCpuTime() - cpu0;
    long proc = OS.getProcessCpuTime() - proc0;
    if (recorder != null) {
      extra.putAll(recorder.snapshot().minus(sdk0).fields());
    }
    List<IoTrace.Event> events = outermost && trace.logging() ? trace.drainEvents() : List.of();
    measured.add(
        new Measured(
            phase, startMs, wall, cpu, proc, trace.snapshot().minus(before), extra, events));
    return result;
  }

  public void run(String phase, IoRunnable body) throws IOException {
    run(
        phase,
        () -> {
          body.run();
          return null;
        });
  }

  /** The phases measured since the last drain, in the order they finished. */
  public List<Measured> drain() {
    List<Measured> out = List.copyOf(measured);
    measured.clear();
    return out;
  }
}
