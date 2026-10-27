package org.rmow.bench.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counts the storage requests one arm issues. The counters model requests the way Iceberg's
 * object-store streams issue them (see {@link TracingFileIO}); on a real store the SDK-level
 * recorders give the ground truth to check this model against.
 */
public final class IoTrace {

  /**
   * One storage operation, kept only when event logging is on (tests, parity assertions and the
   * request log). {@code seq} orders an arm's events, and {@code task} is the split task the
   * request ran in ({@link #enterTask}), or -1 outside one. A request's {@code startNs} ({@code
   * System.nanoTime}), {@code durNs} and {@code cpuNs} are the wall and thread CPU time of the
   * calls into the store that served it ({@link Spans}): for a stream's GET, the read that opened
   * it, and for a stream's later reads their {@link Kind#STREAM_BYTES} events; for a PUT through an
   * output stream, its close, which uploads. They are 0 for the events that mark a request rather
   * than time one (skips, aborts), and on a thread that is not measured. A {@link Kind#TASK} event
   * closes a task: its start and wall time, {@code cpuNs} its thread CPU, {@code instructions} and
   * {@code cycles} its user-mode counts ({@code ThreadCounters}; -1 without counters), {@code
   * offset} its group (the first task id of the tasks scheduled together), and {@code length} 1 if
   * it ran on the phase's own thread, 0 on a read pool's.
   */
  public record Event(
      long seq,
      int task,
      Kind kind,
      String path,
      long offset,
      long length,
      long startNs,
      long durNs,
      long cpuNs,
      long instructions,
      long cycles) {}

  public enum Kind {
    GET_RANGE, // a bounded ranged GET (RangeReadable.readFully/readTail/readVectored)
    GET_STREAM, // a GET opened by a sequential stream (open-ended range)
    SKIP_THROUGH, // bytes a stream read through to serve a short forward seek
    ABORT, // a stream request abandoned before its end (seek away or close)
    HEAD, // a length or existence lookup
    PUT, // a whole-object write
    APPEND, // an append to an existing object
    DELETE,
    STREAM_BYTES, // logged only: bytes a stream request delivered to one read (length)
    TASK // logged only: a split task ended (cpuNs, and length 1 if it ran inline)
  }

  private static final ThreadLocal<Integer> CURRENT_TASK = ThreadLocal.withInitial(() -> -1);

  /** Marks the calling thread's requests as split task {@code id}'s, until {@link #exitTask}. */
  public static void enterTask(int id) {
    CURRENT_TASK.set(id);
  }

  public static void exitTask() {
    CURRENT_TASK.set(-1);
  }

  private final LongAdder getRange = new LongAdder();
  private final LongAdder getRangeBytes = new LongAdder();
  private final LongAdder getStream = new LongAdder();
  private final LongAdder getStreamBytes = new LongAdder();
  private final LongAdder skipThroughBytes = new LongAdder();
  private final LongAdder aborts = new LongAdder();
  private final LongAdder heads = new LongAdder();
  private final LongAdder puts = new LongAdder();
  private final LongAdder putBytes = new LongAdder();
  private final LongAdder appends = new LongAdder();
  private final LongAdder appendBytes = new LongAdder();
  private final LongAdder deletes = new LongAdder();
  private final List<Event> events = Collections.synchronizedList(new ArrayList<>());
  private final LongAdder poolCpu = new LongAdder();
  private final LongAdder poolUser = new LongAdder();
  private final LongAdder poolInstructions = new LongAdder();
  private final LongAdder poolCycles = new LongAdder();
  private final LongAdder poolWall = new LongAdder();
  private final LongAdder[] poolSpanWall = adders(Spans.kinds().length);
  private final LongAdder[] poolSpanCpu = adders(Spans.kinds().length);
  private final AtomicLong seq = new AtomicLong();
  private final AtomicLong taskIds = new AtomicLong();
  private volatile boolean logEvents = false;

  private static LongAdder[] adders(int n) {
    LongAdder[] a = new LongAdder[n];
    for (int i = 0; i < n; i++) {
      a[i] = new LongAdder();
    }
    return a;
  }

  public void logEvents(boolean enabled) {
    this.logEvents = enabled;
  }

  public boolean logging() {
    return logEvents;
  }

  /** The events logged since the last drain, in order; empties the log. */
  public List<Event> drainEvents() {
    synchronized (events) {
      List<Event> out = List.copyOf(events);
      events.clear();
      return out;
    }
  }

  /**
   * Adds the thread CPU of a split task that ran on a read pool, not the arm's own thread, and
   * the part of it spent in user mode.
   */
  public void addPoolCpu(long cpuNs, long userNs) {
    poolCpu.add(cpuNs);
    poolUser.add(userNs);
  }

  /** The thread CPU of every split task that has run on a read pool. */
  public long poolCpuNs() {
    return poolCpu.sum();
  }

  /** The user-mode part of {@link #poolCpuNs}. */
  public long poolUserNs() {
    return poolUser.sum();
  }

  /** Adds a pool task's user-mode instructions and cycles (ThreadCounters). */
  public void addPoolCounts(long instructions, long cycles) {
    poolInstructions.add(instructions);
    poolCycles.add(cycles);
  }

  /**
   * Adds a pool task's wall time and its spans ({@link Spans}): what its thread spent in requests,
   * encoding, decoding and the rest.
   */
  public void addPoolSpans(long wallNs, Spans.Totals spans) {
    poolWall.add(wallNs);
    for (int i = 0; i < poolSpanWall.length; i++) {
      poolSpanWall[i].add(spans.wallNs()[i]);
      poolSpanCpu[i].add(spans.cpuNs()[i]);
    }
  }

  /** The wall time of every split task that has run on a read pool. */
  public long poolWallNs() {
    return poolWall.sum();
  }

  /** The spans of every split task that has run on a read pool. */
  public Spans.Totals poolSpans() {
    long[] w = new long[poolSpanWall.length];
    long[] c = new long[poolSpanCpu.length];
    for (int i = 0; i < w.length; i++) {
      w[i] = poolSpanWall[i].sum();
      c[i] = poolSpanCpu[i].sum();
    }
    return new Spans.Totals(w, c);
  }

  public long poolInstructions() {
    return poolInstructions.sum();
  }

  public long poolCycles() {
    return poolCycles.sum();
  }

  /** Reserves {@code n} task ids, unique in this trace; returns the first. */
  public int nextTasks(int n) {
    return (int) taskIds.getAndAdd(n);
  }

  /**
   * Logs the end of split task {@code task} of {@code group}: its start and wall time, its thread
   * CPU and user-mode instructions and cycles (-1 without counters), and whether it ran inline.
   */
  public void taskDone(
      int task,
      int group,
      long startNs,
      long wallNs,
      long cpuNs,
      long instructions,
      long cycles,
      boolean inline) {
    if (logEvents) {
      events.add(
          new Event(seq.getAndIncrement(), task, Kind.TASK, "", group, inline ? 1 : 0, startNs,
              wallNs, cpuNs, instructions, cycles));
    }
  }

  public List<Event> events() {
    synchronized (events) {
      return List.copyOf(events);
    }
  }

  public void clearEvents() {
    events.clear();
  }

  /** Records a request that is not timed: a skip or an abort, which mark one. */
  void record(Kind kind, String path, long offset, long length) {
    record(kind, path, offset, length, 0, 0, 0);
  }

  /** Records a request, timed by the span of the calls that served it. */
  void record(Kind kind, String path, long offset, long length, Spans.Span span) {
    record(kind, path, offset, length, span.startNs(), span.wallNs(), span.cpuNs());
  }

  void record(
      Kind kind, String path, long offset, long length, long startNs, long durNs, long cpuNs) {
    switch (kind) {
      case GET_RANGE -> {
        getRange.increment();
        getRangeBytes.add(length);
      }
      case GET_STREAM -> getStream.increment();
      case SKIP_THROUGH -> skipThroughBytes.add(length);
      case ABORT -> aborts.increment();
      case HEAD -> heads.increment();
      case PUT -> {
        puts.increment();
        putBytes.add(length);
      }
      case APPEND -> {
        appends.increment();
        appendBytes.add(length);
      }
      case DELETE -> deletes.increment();
      default -> throw new IllegalArgumentException("Not a request: " + kind);
    }
    if (logEvents) {
      events.add(
          new Event(seq.getAndIncrement(), CURRENT_TASK.get(), kind, path, offset, length, startNs,
              durNs, cpuNs, -1, -1));
    }
  }

  /**
   * Bytes a stream request delivered to its reader (counted per read, not per request), and the
   * read's time: none if the read opened the request, whose GET carries it.
   */
  void streamBytes(String path, long n, long startNs, long durNs, long cpuNs) {
    getStreamBytes.add(n);
    if (logEvents) {
      events.add(
          new Event(seq.getAndIncrement(), CURRENT_TASK.get(), Kind.STREAM_BYTES, path, -1, n,
              startNs, durNs, cpuNs, -1, -1));
    }
  }

  public Counters snapshot() {
    return new Counters(
        getRange.sum(),
        getRangeBytes.sum(),
        getStream.sum(),
        getStreamBytes.sum(),
        skipThroughBytes.sum(),
        aborts.sum(),
        heads.sum(),
        puts.sum(),
        putBytes.sum(),
        appends.sum(),
        appendBytes.sum(),
        deletes.sum());
  }

  /** A point-in-time copy of the counters; {@link #minus} gives the requests of one phase. */
  public record Counters(
      long getRange,
      long getRangeBytes,
      long getStream,
      long getStreamBytes,
      long skipThroughBytes,
      long aborts,
      long heads,
      long puts,
      long putBytes,
      long appends,
      long appendBytes,
      long deletes) {

    public Counters minus(Counters o) {
      return new Counters(
          getRange - o.getRange,
          getRangeBytes - o.getRangeBytes,
          getStream - o.getStream,
          getStreamBytes - o.getStreamBytes,
          skipThroughBytes - o.skipThroughBytes,
          aborts - o.aborts,
          heads - o.heads,
          puts - o.puts,
          putBytes - o.putBytes,
          appends - o.appends,
          appendBytes - o.appendBytes,
          deletes - o.deletes);
    }

    public long gets() {
      return getRange + getStream;
    }

    /** Bytes transferred from storage, including bytes a stream read through to skip. */
    public long bytesRead() {
      return getRangeBytes + getStreamBytes + skipThroughBytes;
    }

    public long bytesWritten() {
      return putBytes + appendBytes;
    }

    public long requests() {
      return gets() + heads + puts + appends + deletes;
    }
  }
}
