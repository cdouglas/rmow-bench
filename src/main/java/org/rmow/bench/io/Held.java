package org.rmow.bench.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * What the arm's readers and writers hold in memory at once: rows they keep to sort or merge, and
 * encoded bytes. Each phase records the peaks it saw ({@link Peak}), and a run with a held log
 * writes every change, in order, so a phase's memory can be drawn against the stages of its work
 * (a row group opened, a region buffered, an append sent) rather than against time.
 *
 * <p>The levels are counted where the code holds data, or, inside Iceberg's reader, from the
 * footer's row-group sizes when the reader opens a row group. They depend on the data and the
 * layout alone, so two runs record the same peaks and the same log, as they do requests and bytes;
 * a read whose splits run on a pool interleaves its row groups as the threads do. The JVM's heap
 * is not measured here: it holds the benchmark's own state too, and its peaks move with the
 * collector.
 *
 * <p>Arms run one at a time and every hold is released by the end of the outermost phase that
 * made it ({@link #closeOutermost} checks), so the levels are one arm's.
 */
public final class Held {

  /** What is held, by its step-record field ({@code held_<field>}). */
  public enum Kind {
    /** Rows kept to sort or merge (counted in rows, not bytes: their size is the engine's). */
    ROWS("rows"),
    /** Parquet column chunks a reader has fetched for the row group it is decoding. */
    CHUNKS("chunks"),
    /** A Parquet writer's buffered row group, as large as the row group it writes. */
    WRITER("writer"),
    /** Encoded data waiting for its request: regions for an append, a file for a single create. */
    PENDING("pending"),
    /** Deletes loaded or built: DVs and position deletes, at their encoded size. */
    DELETES("deletes");

    private final String field;

    Kind(String field) {
      this.field = field;
    }

    public String field() {
      return field;
    }

    boolean bytes() {
      return this != ROWS;
    }
  }

  private static final Kind[] KINDS = Kind.values();

  /** A change of a level, with every level after it, and the innermost phase it happened in. */
  public record Event(String phase, String what, long[] levels) {
    public long bytes() {
      long b = 0;
      for (Kind k : KINDS) {
        b += k.bytes() ? levels[k.ordinal()] : 0;
      }
      return b;
    }
  }

  /** The peaks of one phase: each kind's, and of all the bytes together. */
  public static final class Peak {
    private final long[] max = new long[KINDS.length];
    private long maxBytes;

    private void see(long[] levels) {
      long b = 0;
      for (Kind k : KINDS) {
        max[k.ordinal()] = Math.max(max[k.ordinal()], levels[k.ordinal()]);
        b += k.bytes() ? levels[k.ordinal()] : 0;
      }
      maxBytes = Math.max(maxBytes, b);
    }

    public long max(Kind k) {
      return max[k.ordinal()];
    }

    public long maxBytes() {
      return maxBytes;
    }

    /** The peaks of two parts of one phase, which ran one after the other. */
    public Peak plus(Peak o) {
      Peak p = new Peak();
      for (int i = 0; i < max.length; i++) {
        p.max[i] = Math.max(max[i], o.max[i]);
      }
      p.maxBytes = Math.max(maxBytes, o.maxBytes);
      return p;
    }
  }

  /** Holds released together: a read's deletes, a batch's rows. Safe across threads. */
  public static final class Scope implements AutoCloseable {
    private final long[] held = new long[KINDS.length];
    private final String what;

    private Scope(String what) {
      this.what = what;
    }

    public void hold(Kind k, long n, String why) {
      if (n == 0) {
        return;
      }
      synchronized (Held.class) {
        held[k.ordinal()] += n;
        change(k, n, why);
      }
    }

    public void release(Kind k, long n, String why) {
      hold(k, -n, why);
    }

    /** Holds {@code n} and releases it at once: a peak no other hold outlives. */
    public void pulse(Kind k, long n, String why) {
      hold(k, n, why);
      release(k, n, why);
    }

    /** Releases everything this scope holds. */
    @Override
    public void close() {
      synchronized (Held.class) {
        for (Kind k : KINDS) {
          long n = held[k.ordinal()];
          if (n != 0) {
            held[k.ordinal()] = 0;
            change(k, -n, what);
          }
        }
      }
    }
  }

  private static final long[] LEVELS = new long[KINDS.length];
  private static final List<Peak> OPEN = new ArrayList<>(); // the open phases' peaks
  private static final List<String> PHASES = new ArrayList<>(); // their names, innermost last
  private static boolean logging = false;
  private static final List<Event> EVENTS = new ArrayList<>();

  private Held() {}

  /** A scope of holds released together; close it to release them. */
  public static Scope scope(String what) {
    return new Scope(what);
  }

  /** Holds {@code n} and releases it at once: a peak no other hold outlives. */
  public static void pulse(Kind k, long n, String why) {
    try (Scope s = scope(why)) {
      s.pulse(k, n, why);
    }
  }

  private static void change(Kind k, long n, String why) {
    LEVELS[k.ordinal()] += n;
    if (LEVELS[k.ordinal()] < 0) {
      throw new IllegalStateException("Held " + k + " below 0 after " + why);
    }
    for (Peak p : OPEN) {
      p.see(LEVELS);
    }
    if (logging) {
      EVENTS.add(new Event(PHASES.isEmpty() ? null : PHASES.get(PHASES.size() - 1), why,
          LEVELS.clone()));
    }
  }

  /** Starts a phase's peaks, from the levels now. */
  public static synchronized Peak open(String phase) {
    Peak p = new Peak();
    p.see(LEVELS);
    OPEN.add(p);
    PHASES.add(phase);
    return p;
  }

  /** Ends a phase's peaks. */
  public static synchronized void close(Peak p) {
    int i = OPEN.lastIndexOf(p);
    OPEN.remove(i);
    PHASES.remove(i);
  }

  /** Ends an outermost phase, which must leave nothing held. */
  public static synchronized void closeOutermost(Peak p, String phase) {
    close(p);
    if (Arrays.stream(LEVELS).anyMatch(v -> v != 0)) {
      long[] left = LEVELS.clone();
      Arrays.fill(LEVELS, 0);
      throw new IllegalStateException(
          "Phase " + phase + " left held " + Arrays.toString(left) + " " + Arrays.toString(KINDS));
    }
  }

  /** Whether every change is logged ({@link #drainEvents}). */
  public static synchronized void logEvents(boolean on) {
    logging = on;
    EVENTS.clear();
  }

  /** The changes logged since the last drain. */
  public static synchronized List<Event> drainEvents() {
    List<Event> out = List.copyOf(EVENTS);
    EVENTS.clear();
    return out;
  }

  public static Kind[] kinds() {
    return KINDS.clone();
  }
}
