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
   * request ran in ({@link #enterTask}), or -1 outside one. A {@link Kind#TASK} event closes a
   * task: {@code cpuNs} is its thread CPU, {@code offset} its group (the first task id of the
   * tasks scheduled together), and {@code length} is 1 if it ran on the phase's own thread, 0 on
   * a read pool's.
   */
  public record Event(
      long seq, int task, Kind kind, String path, long offset, long length, long cpuNs) {}

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
  private final AtomicLong seq = new AtomicLong();
  private final AtomicLong taskIds = new AtomicLong();
  private volatile boolean logEvents = false;

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

  /** Reserves {@code n} task ids, unique in this trace; returns the first. */
  public int nextTasks(int n) {
    return (int) taskIds.getAndAdd(n);
  }

  /**
   * Logs the end of split task {@code task} of {@code group}: its thread CPU, and whether it ran
   * inline.
   */
  public void taskDone(int task, int group, long cpuNs, boolean inline) {
    if (logEvents) {
      events.add(
          new Event(seq.getAndIncrement(), task, Kind.TASK, "", group, inline ? 1 : 0, cpuNs));
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

  void record(Kind kind, String path, long offset, long length) {
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
      events.add(new Event(seq.getAndIncrement(), CURRENT_TASK.get(), kind, path, offset, length, 0));
    }
  }

  /** Bytes a stream request delivered to its reader (counted per read, not per request). */
  void streamBytes(String path, long n) {
    getStreamBytes.add(n);
    if (logEvents) {
      events.add(
          new Event(seq.getAndIncrement(), CURRENT_TASK.get(), Kind.STREAM_BYTES, path, -1, n, 0));
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
