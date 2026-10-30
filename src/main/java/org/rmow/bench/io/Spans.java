package org.rmow.bench.io;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;

/**
 * Where a thread's time goes: wall and thread CPU time inside storage requests and inside named
 * kinds of work, each exclusive of the spans nested in it, so that a phase's wall time splits
 * into them and a remainder. {@link TracingFileIO} opens a {@link Kind#REQUEST} span around every
 * call into the store; the arms and the append layer open the others around Parquet encoding and
 * decoding, footer work and delete files. A span nested in another of the same kind counts once.
 *
 * <p>Spans count only on a measured thread ({@link #measure}): the arm's thread inside a phase,
 * and a read pool's thread inside a split task. Anywhere else (the setup, micro-benchmarks) a span
 * costs a thread-local read. A measured span reads the thread's CPU clock twice, about a
 * microsecond each on the machines used so far.
 */
public final class Spans {

  /** A kind of work, by its step-record field ({@code <field>_ns}, {@code <field>_cpu_ns}). */
  public enum Kind {
    /** A call into the store: the client's work (checksums, HTTP, TLS) and the wait for it. */
    REQUEST("io"),
    /** Parquet encoding of rows: data files and RMoW's regions. */
    ENCODE("encode"),
    /**
     * Parquet decoding in a scan: the footer, pages and rows, with the deletes applied to them
     * and what the scan's consumer does with each row (small: a hash, or a list to merge).
     */
    DECODE("decode"),
    /** RMoW's footers: Thrift decoding and encoding, and merging. */
    FOOTER("footer"),
    /** Delete files: DVs and position deletes built and encoded, loaded and decoded. */
    DELETES("deletes"),
    /**
     * Rows the benchmark makes for a writer from their id, event time and version (a row is a
     * function of them), a chunk at a time as the encoder takes them: work of the harness, not of
     * a layout, which before 2026-10-29 was the phase's remainder.
     */
    ROWS("rows");

    private final String field;

    Kind(String field) {
      this.field = field;
    }

    public String field() {
      return field;
    }
  }

  private static final Kind[] KINDS = Kind.values();
  private static final ThreadMXBean MX = ManagementFactory.getThreadMXBean();
  private static final ThreadLocal<State> STATE = ThreadLocal.withInitial(State::new);

  /** A thread's totals, and its open spans. */
  private static final class State {
    int measured = 0; // nesting of measure() calls
    final long[] wallNs = new long[KINDS.length];
    final long[] cpuNs = new long[KINDS.length];
    Span top = null;
  }

  /** Wall and CPU nanoseconds per kind, exclusive of nested spans. */
  public record Totals(long[] wallNs, long[] cpuNs) {
    public static final Totals ZERO = new Totals(new long[KINDS.length], new long[KINDS.length]);

    public Totals minus(Totals o) {
      long[] w = new long[KINDS.length];
      long[] c = new long[KINDS.length];
      for (int i = 0; i < w.length; i++) {
        w[i] = wallNs[i] - o.wallNs[i];
        c[i] = cpuNs[i] - o.cpuNs[i];
      }
      return new Totals(w, c);
    }

    public Totals plus(Totals o) {
      return minus(o.minus(o).minus(o));
    }

    public long wallNs(Kind k) {
      return wallNs[k.ordinal()];
    }

    public long cpuNs(Kind k) {
      return cpuNs[k.ordinal()];
    }
  }

  /** An open span; {@link #close} ends it. Its own times count once it closes. */
  public static final class Span implements AutoCloseable {
    private static final Span NONE = new Span(null, null, null, 0, 0);

    private final Kind kind;
    private final State state;
    private final Span parent;
    private final long startNs;
    private final long startCpuNs;
    private long childWallNs = 0;
    private long childCpuNs = 0;
    private long wallNs = 0;
    private long cpuNs = 0;

    private Span(Kind kind, State state, Span parent, long startNs, long startCpuNs) {
      this.kind = kind;
      this.state = state;
      this.parent = parent;
      this.startNs = startNs;
      this.startCpuNs = startCpuNs;
    }

    @Override
    public void close() {
      if (state == null || state.top != this) {
        return; // not measured, or closed already
      }
      wallNs = System.nanoTime() - startNs;
      cpuNs = MX.getCurrentThreadCpuTime() - startCpuNs;
      state.top = parent;
      if (parent != null) {
        parent.childWallNs += wallNs;
        parent.childCpuNs += cpuNs;
      }
      int k = kind.ordinal();
      state.wallNs[k] += wallNs - childWallNs;
      state.cpuNs[k] += cpuNs - childCpuNs;
    }

    /** When it started ({@code System.nanoTime}); 0 if not measured. */
    public long startNs() {
      return startNs;
    }

    /** Its wall time once closed, nested spans included; 0 if not measured. */
    public long wallNs() {
      return wallNs;
    }

    /** Its thread CPU once closed, nested spans included; 0 if not measured. */
    public long cpuNs() {
      return cpuNs;
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

  private Spans() {}

  /** Opens a span of {@code kind} on the calling thread; close it on the same thread. */
  public static Span open(Kind kind) {
    State s = STATE.get();
    if (s.measured == 0) {
      return Span.NONE;
    }
    Span span = new Span(kind, s, s.top, System.nanoTime(), MX.getCurrentThreadCpuTime());
    s.top = span;
    return span;
  }

  public static <T> T in(Kind kind, IoSupplier<T> body) throws IOException {
    try (Span s = open(kind)) {
      return body.get();
    }
  }

  public static void in(Kind kind, IoRunnable body) throws IOException {
    try (Span s = open(kind)) {
      body.run();
    }
  }

  /**
   * Starts counting spans on the calling thread, until the matching {@link #unmeasure}. Calls
   * nest; spans count while any is open.
   */
  public static void measure() {
    STATE.get().measured += 1;
  }

  public static void unmeasure() {
    State s = STATE.get();
    s.measured -= 1;
    if (s.measured == 0) {
      s.top = null;
    }
  }

  /** The calling thread's totals so far; a phase's are the difference of two. */
  public static Totals totals() {
    State s = STATE.get();
    return new Totals(
        Arrays.copyOf(s.wallNs, s.wallNs.length), Arrays.copyOf(s.cpuNs, s.cpuNs.length));
  }

  public static Kind[] kinds() {
    return KINDS.clone();
  }
}
