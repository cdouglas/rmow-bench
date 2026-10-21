package org.rmow.bench.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counts the storage requests one arm issues. The counters model requests the way Iceberg's
 * object-store streams issue them (see {@link TracingFileIO}); on a real store the SDK-level
 * recorders give the ground truth to check this model against.
 */
public final class IoTrace {

  /** One storage operation, kept only when event logging is on (tests and parity assertions). */
  public record Event(Kind kind, String path, long offset, long length) {}

  public enum Kind {
    GET_RANGE, // a bounded ranged GET (RangeReadable.readFully/readTail/readVectored)
    GET_STREAM, // a GET opened by a sequential stream (open-ended range)
    SKIP_THROUGH, // bytes a stream read through to serve a short forward seek
    ABORT, // a stream request abandoned before its end (seek away or close)
    HEAD, // a length or existence lookup
    PUT, // a whole-object write
    APPEND, // an append to an existing object
    DELETE
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
  private volatile boolean logEvents = false;

  public void logEvents(boolean enabled) {
    this.logEvents = enabled;
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
    }
    if (logEvents) {
      events.add(new Event(kind, path, offset, length));
    }
  }

  /** Bytes a stream request delivered to its reader (counted per read, not per request). */
  void streamBytes(long n) {
    getStreamBytes.add(n);
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
