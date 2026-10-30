package org.rmow.bench.bench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.rmow.bench.io.Held;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.SdkRecorder;
import org.rmow.bench.io.Spans;

/**
 * Times named phases of one arm: wall-clock and thread CPU time, and the requests the arm's
 * {@link IoTrace} saw meanwhile. Arms run one at a time, so a trace delta belongs to the phase.
 *
 * <p>On S3 and Azure, each phase also records the requests the SDK clients sent meanwhile ({@code
 * sdk_*} fields), which check the trace's model of Iceberg's streams.
 *
 * <p>Phases nest: the runner times each whole batch as {@code batch}, and arms time their parts
 * inside it ({@code merge-read}, {@code write}, {@code delete-write}). Analysis must not add a
 * phase to the phase that contains it.
 *
 * <p>When the trace logs events (the request log), an outermost phase takes the events logged
 * while it ran, its nested phases' included; events logged outside every phase are dropped.
 *
 * <p>A phase's thread is measured while it runs ({@link Spans}), so each phase records where its
 * time went: in requests, encoding, decoding, footers and deletes, each as wall and CPU time,
 * exclusive of one another, and the same for its split tasks on a read pool.
 *
 * <p>A phase may run in parts ({@link #part}): a writer that streams reads a region, writes it,
 * then reads the next, and still records one {@code merge-read} and one {@code write}, each the
 * sum of its parts.
 */
public final class Phases {

  /**
   * One phase's measurements: when it started (epoch milliseconds, the clock a campaign's load
   * samples use), thread CPU of the arm's thread, the thread CPU of its split tasks that ran on a
   * read pool, the user-mode part of each (without the kernel's: page faults, the local store's
   * reads), and process CPU (which also counts the JVM's own GC and JIT threads). {@code counts}
   * and {@code poolCounts} are the same threads' user-mode instructions and cycles ({@link
   * ThreadCounters}), null without counters. {@code extra} holds phase-specific values (e.g. rows
   * read); {@code events}, the logged requests of an outermost phase. {@code startNs} is its start
   * on {@code System.nanoTime}'s clock, the events'; {@code spans} its thread's time by kind of
   * work, and {@code poolSpans} and {@code poolWallNs} its pool tasks'. {@code held}: the most
   * its readers and writers held at once ({@link Held}); {@code heldEvents}, an outermost phase's
   * changes of what is held, when they are logged. {@code part}: one part of a phase run in parts
   * ({@link #part}).
   */
  public record Measured(
      String phase,
      long startMs,
      long startNs,
      long wallNs,
      long cpuNs,
      long poolCpuNs,
      long userNs,
      long poolUserNs,
      long processCpuNs,
      ThreadCounters.Sample counts,
      ThreadCounters.Sample poolCounts,
      Spans.Totals spans,
      Spans.Totals poolSpans,
      long poolWallNs,
      IoTrace.Counters io,
      Map<String, Object> extra,
      List<IoTrace.Event> events,
      Held.Peak held,
      List<Held.Event> heldEvents,
      boolean part) {

    /** Two parts of one phase as one: measurements added, the start the earlier's. */
    Measured plus(Measured o) {
      Map<String, Object> x = new LinkedHashMap<>(extra);
      o.extra.forEach((k, v) -> x.merge(k, v, Measured::add));
      List<IoTrace.Event> ev = new ArrayList<>(events);
      ev.addAll(o.events);
      List<Held.Event> hev = new ArrayList<>(heldEvents);
      hev.addAll(o.heldEvents);
      return new Measured(
          phase, startMs, startNs, wallNs + o.wallNs, cpuNs + o.cpuNs, poolCpuNs + o.poolCpuNs,
          userNs + o.userNs, poolUserNs + o.poolUserNs, processCpuNs + o.processCpuNs,
          counts == null ? null : counts.plus(o.counts),
          poolCounts == null ? null : poolCounts.plus(o.poolCounts), spans.plus(o.spans),
          poolSpans.plus(o.poolSpans), poolWallNs + o.poolWallNs, io.plus(o.io), x, ev,
          held.plus(o.held), hev, true);
    }

    private static Object add(Object a, Object b) {
      return a instanceof Long x && b instanceof Long y ? (Object) (x + y) : a;
    }
  }

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
  private final SdkRecorder recorder; // null unless the store is in the cloud
  private final List<Measured> measured = new ArrayList<>();
  private int depth = 0;

  public Phases(IoTrace trace) {
    this(trace, null);
  }

  public Phases(IoTrace trace, SdkRecorder recorder) {
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
    return run(phase, body, extra, false);
  }

  /**
   * Runs {@code body} as a part of {@code phase}: a phase's parts between two drains are recorded
   * as one phase, at the place of the first.
   */
  public void part(String phase, IoRunnable body) throws IOException {
    run(
        phase,
        () -> {
          body.run();
          return null;
        },
        new LinkedHashMap<>(),
        true);
  }

  private <T> T run(String phase, IoSupplier<T> body, Map<String, Object> extra, boolean part)
      throws IOException {
    boolean outermost = depth++ == 0;
    Held.Peak held = Held.open(phase);
    if (outermost && trace.logging()) {
      trace.drainEvents(); // what ran outside every phase is not measured
    }
    if (outermost) {
      Held.drainEvents();
    }
    IoTrace.Counters before = trace.snapshot();
    Map<String, Long> sdk0 = recorder == null ? null : recorder.fields();
    long proc0 = OS.getProcessCpuTime();
    Spans.measure();
    Spans.Totals spans0 = Spans.totals();
    Spans.Totals poolSpans0 = trace.poolSpans();
    long poolWall0 = trace.poolWallNs();
    long pool0 = trace.poolCpuNs();
    long poolUser0 = trace.poolUserNs();
    long user0 = MX.getCurrentThreadUserTime();
    ThreadCounters.Sample pc0 =
        new ThreadCounters.Sample(trace.poolInstructions(), trace.poolCycles());
    ThreadCounters.Sample counts0 = ThreadCounters.now();
    long cpu0 = MX.getCurrentThreadCpuTime();
    long startMs = System.currentTimeMillis();
    long t0 = System.nanoTime();
    T result;
    try {
      result = body.get();
    } catch (RuntimeException | IOException | Error e) {
      Held.close(held);
      throw e;
    } finally {
      depth--;
      Spans.unmeasure();
    }
    long wall = System.nanoTime() - t0;
    if (outermost) {
      Held.closeOutermost(held, phase); // a hold an outermost phase leaves is a bug
    } else {
      Held.close(held);
    }
    long cpu = MX.getCurrentThreadCpuTime() - cpu0;
    Spans.Totals spans = Spans.totals().minus(spans0);
    Spans.Totals poolSpans = trace.poolSpans().minus(poolSpans0);
    long poolWall = trace.poolWallNs() - poolWall0;
    long user = MX.getCurrentThreadUserTime() - user0;
    long pool = trace.poolCpuNs() - pool0;
    long poolUser = trace.poolUserNs() - poolUser0;
    ThreadCounters.Sample counts = counts0 == null ? null : ThreadCounters.now().minus(counts0);
    ThreadCounters.Sample poolCounts =
        counts0 == null
            ? null
            : new ThreadCounters.Sample(trace.poolInstructions(), trace.poolCycles()).minus(pc0);
    long proc = OS.getProcessCpuTime() - proc0;
    if (recorder != null) {
      recorder.fields().forEach((k, v) -> extra.put(k, v - sdk0.getOrDefault(k, 0L)));
    }
    List<IoTrace.Event> events = outermost && trace.logging() ? trace.drainEvents() : List.of();
    List<Held.Event> heldEvents = outermost ? Held.drainEvents() : List.of();
    measured.add(
        new Measured(
            phase, startMs, t0, wall, cpu, pool, user, poolUser, proc, counts, poolCounts, spans,
            poolSpans, poolWall, trace.snapshot().minus(before), extra, events, held, heldEvents,
            part));
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

  /**
   * The phases measured since the last drain, in the order they finished; a phase run in parts as
   * one, where its first part finished.
   */
  public List<Measured> drain() {
    List<Measured> out = new ArrayList<>(measured.size());
    Map<String, Integer> partAt = new HashMap<>();
    for (Measured m : measured) {
      Integer at = m.part() ? partAt.putIfAbsent(m.phase(), out.size()) : null;
      if (at == null) {
        out.add(m);
      } else {
        out.set(at, out.get(at).plus(m));
      }
    }
    measured.clear();
    return out;
  }
}
