package org.rmow.bench.bench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.rmow.bench.io.IoTrace;

/**
 * Times named phases of one arm: wall-clock and thread CPU time, and the requests the arm's
 * {@link IoTrace} saw meanwhile. Arms run one at a time, so a trace delta belongs to the phase.
 *
 * <p>Phases nest: the runner times each whole batch as {@code batch}, and arms time their parts
 * inside it ({@code merge-read}, {@code write}, {@code delete-write}). Analysis must not add a
 * phase to the phase that contains it.
 */
public final class Phases {

  /**
   * One phase's measurements: thread CPU of the arm's thread, and process CPU (which also counts
   * read-pool threads, and the JVM's own GC and JIT threads). {@code extra} holds phase-specific
   * values (e.g. rows read).
   */
  public record Measured(
      String phase,
      long wallNs,
      long cpuNs,
      long processCpuNs,
      IoTrace.Counters io,
      Map<String, Object> extra) {}

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
  private final List<Measured> measured = new ArrayList<>();

  public Phases(IoTrace trace) {
    this.trace = trace;
  }

  public <T> T run(String phase, IoSupplier<T> body) throws IOException {
    return run(phase, body, new LinkedHashMap<>());
  }

  /** Runs {@code body}; it may add phase-specific values to {@code extra} while it runs. */
  public <T> T run(String phase, IoSupplier<T> body, Map<String, Object> extra)
      throws IOException {
    IoTrace.Counters before = trace.snapshot();
    long proc0 = OS.getProcessCpuTime();
    long cpu0 = MX.getCurrentThreadCpuTime();
    long t0 = System.nanoTime();
    T result = body.get();
    long wall = System.nanoTime() - t0;
    long cpu = MX.getCurrentThreadCpuTime() - cpu0;
    long proc = OS.getProcessCpuTime() - proc0;
    measured.add(new Measured(phase, wall, cpu, proc, trace.snapshot().minus(before), extra));
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
