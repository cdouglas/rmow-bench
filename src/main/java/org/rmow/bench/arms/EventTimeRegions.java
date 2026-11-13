package org.rmow.bench.arms;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * The event-time merge ({@code layout.inserts} {@code event-time}; docs/HYPOTHESES.md,
 * 2026-11-12): a region design keeps its row groups disjoint in event time, so a sorted read
 * concatenates them and a range of event time reads only the row groups that hold it. This is the
 * writer's index of them, by each live row group's event-time range.
 *
 * <p>Each insert joins the live row group whose range holds its event time, or, in a gap between
 * two, the earlier (before the first, the first): the row group with the greatest smallest event
 * time not above its own. So a row group's range grows only into the gap before the next, and the
 * ranges stay disjoint. The row group rows on time join is the newest, the one with the greatest
 * smallest event time.
 *
 * <p>A row group a write grows past {@code cap} rows is cut, in event-time order and never between
 * two rows of one event time ({@link #pieces}): the newest into full row groups of {@code cap} rows
 * and the rest, which stays the newest; an older one, which only late rows grow, once it holds the
 * rows of two full row groups, into equal row groups of at least {@code cap} rows. {@code cap} is
 * the rows of the initial load's largest row group, the size the writer cuts row groups at.
 */
final class EventTimeRegions {

  private static final int TS_FIELD = Rows.SCHEMA.findField("ts").fieldId();

  /** A live row group: the arm's key for it, and its rows' event-time range. */
  record Region(long key, long minTs, long maxTs) {}

  private final TreeMap<Long, Region> byMin = new TreeMap<>();
  private final Map<Long, Region> byKey = new HashMap<>();
  private final long cap;

  EventTimeRegions(long cap) {
    this.cap = Math.max(1, cap);
  }

  long cap() {
    return cap;
  }

  /**
   * Whether the event-time merge is on: the layout asks for it and the workload inserts. It places
   * inserts, so a workload without them runs as without it; it refuses a workload that also
   * updates or deletes (its writers would have to find those rows by key in a table laid out by
   * event time, which is not built), and compaction (which would pack row groups of one file
   * across others' ranges).
   */
  static boolean wanted(ArmContext ctx) {
    if (!ctx.layout().eventTime()) {
      return false;
    }
    boolean inserts = false;
    boolean changes = false;
    for (OpLog.Batch b : ctx.log().batches()) {
      inserts |= b.insertIds().length > 0;
      changes |= b.updateIds().length + b.deleteIds().length > 0;
    }
    if (!inserts) {
      return false;
    }
    if (changes) {
      throw new IllegalArgumentException(
          "The event-time merge places inserts; a workload that also updates or deletes is not"
              + " built for it");
    }
    if (ctx.layout().compactEvery > 0) {
      throw new IllegalArgumentException(
          "The event-time merge keeps its row groups apart itself; compaction is not built for it"
              + " (compactEvery " + ctx.layout().compactEvery + ")");
    }
    return true;
  }

  /**
   * Adds a live row group; it must not overlap another (an initial load out of event-time order,
   * {@code initialJitterRows} above 0, would).
   */
  void add(long key, long minTs, long maxTs) {
    Map.Entry<Long, Region> lo = byMin.floorEntry(minTs);
    Map.Entry<Long, Region> hi = byMin.ceilingEntry(minTs);
    if ((lo != null && lo.getValue().maxTs() >= minTs)
        || (hi != null && hi.getKey() <= maxTs)
        || byKey.containsKey(key)) {
      throw new IllegalStateException(
          "Row group " + key + " at event times " + minTs + ".." + maxTs + " overlaps "
              + (lo != null && lo.getValue().maxTs() >= minTs ? lo.getValue() : hi.getValue())
              + " (the event-time merge needs an initial load in event-time order:"
              + " initialJitterRows 0)");
    }
    Region r = new Region(key, minTs, maxTs);
    byMin.put(minTs, r);
    byKey.put(key, r);
  }

  /** Adds a live row group of {@code rows}, as {id, version}. */
  void add(long key, List<long[]> rows, OpLog log) {
    long[] range = range(rows, log);
    add(key, range[0], range[1]);
  }

  /** Adds a live row group of the rows {@code ids}. */
  void addIds(long key, List<Long> ids, OpLog log) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (long id : ids) {
      min = Math.min(min, log.ts(id));
      max = Math.max(max, log.ts(id));
    }
    add(key, min, max);
  }

  /** Removes a row group that left the table, if it is one of the index's. */
  void remove(long key) {
    Region r = byKey.remove(key);
    if (r != null) {
      byMin.remove(r.minTs());
    }
  }

  boolean contains(long key) {
    return byKey.containsKey(key);
  }

  Region region(long key) {
    return byKey.get(key);
  }

  /** The row group an insert at event time {@code ts} joins; null if there is none. */
  Region of(long ts) {
    Map.Entry<Long, Region> e = byMin.floorEntry(ts);
    if (e == null) {
      e = byMin.firstEntry();
    }
    return e == null ? null : e.getValue();
  }

  /** The newest row group, which rows on time join; null if there is none. */
  Region newest() {
    Map.Entry<Long, Region> e = byMin.lastEntry();
    return e == null ? null : e.getValue();
  }

  /** The live row groups, in event-time order. */
  List<Region> regions() {
    return new ArrayList<>(byMin.values());
  }

  /** Each insert's row group, by key, in the batch's order; {@code -1}: none (an empty table). */
  Map<Long, List<Long>> assign(long[] insertIds, OpLog log) {
    Map<Long, List<Long>> out = new HashMap<>();
    for (long id : insertIds) {
      Region r = of(log.ts(id));
      out.computeIfAbsent(r == null ? -1L : r.key(), k -> new ArrayList<>()).add(id);
    }
    return out;
  }

  /** Sorts rows ({id, version}) by event time, then id: the order a region is cut in. */
  static void sort(List<long[]> rows, OpLog log) {
    rows.sort(Comparator.comparingLong((long[] r) -> log.ts(r[0])).thenComparingLong(r -> r[0]));
  }

  /** The event-time range {min, max} of rows ({id, version}); empty rows give {MAX, MIN}. */
  static long[] range(List<long[]> rows, OpLog log) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (long[] r : rows) {
      long ts = log.ts(r[0]);
      min = Math.min(min, ts);
      max = Math.max(max, ts);
    }
    return new long[] {min, max};
  }

  /**
   * The row groups a region's rows are written as, each in event-time order (the rows are sorted
   * here): one, unless they pass {@code cap}. The newest is cut into full row groups of {@code cap}
   * rows and the rest, last; an older one, once it holds {@code 2 * cap} rows, into {@code rows /
   * cap} equal row groups. A cut never falls between two rows of one event time, so the pieces'
   * ranges are disjoint too.
   */
  List<List<long[]>> pieces(List<long[]> rows, OpLog log, boolean newest) {
    sort(rows, log);
    int n = rows.size();
    List<Integer> cuts = new ArrayList<>();
    if (newest) {
      int start = 0;
      while (n - start > cap) {
        int c = past(rows, log, start + (int) cap);
        if (c >= n) {
          break;
        }
        cuts.add(c);
        start = c;
      }
    } else if (n >= 2 * cap) {
      int m = (int) (n / cap);
      int last = 0;
      for (int i = 1; i < m; i++) {
        int c = past(rows, log, Math.max(last + 1, (int) ((long) i * n / m)));
        if (c >= n) {
          break;
        }
        cuts.add(c);
        last = c;
      }
    }
    List<List<long[]>> out = new ArrayList<>(cuts.size() + 1);
    int from = 0;
    for (int c : cuts) {
      out.add(new ArrayList<>(rows.subList(from, c)));
      from = c;
    }
    out.add(from == 0 ? rows : new ArrayList<>(rows.subList(from, n)));
    return out;
  }

  /** The first position at or after {@code c} that does not split rows of one event time. */
  private static int past(List<long[]> rows, OpLog log, int c) {
    while (c > 0 && c < rows.size() && log.ts(rows.get(c)[0]) == log.ts(rows.get(c - 1)[0])) {
      c++;
    }
    return c;
  }

  /** A data file's event-time bounds {lower, upper} from its metrics; null if it has none. */
  static long[] bounds(DataFile df) {
    ByteBuffer lo = df.lowerBounds() == null ? null : df.lowerBounds().get(TS_FIELD);
    ByteBuffer hi = df.upperBounds() == null ? null : df.upperBounds().get(TS_FIELD);
    if (lo == null || hi == null) {
      return null;
    }
    return new long[] {
      Conversions.<Long>fromByteBuffer(Types.LongType.get(), lo),
      Conversions.<Long>fromByteBuffer(Types.LongType.get(), hi)
    };
  }

  /**
   * The event times a write searches to place {@code sortedTs}, the batch's inserts' event times in
   * order: {lower, upper}, from the start of the earliest row group they join to the latest of
   * their event times and of those row groups' starts. A file whose event-time bounds (its manifest
   * entry) overlap it may hold one of those row groups, and every file that holds one does: its
   * bounds hold the row group's range. A file's bounds are those it was written with, which rows
   * dead since (a DV, or a footer that dropped them) do not narrow, so the writer narrows by its
   * index rather than by the bounds alone.
   */
  long[] searchRange(long[] sortedTs) {
    long lower = Long.MAX_VALUE;
    long upper = sortedTs[sortedTs.length - 1];
    for (long ts : sortedTs) {
      Region r = of(ts);
      lower = Math.min(lower, r.minTs());
      upper = Math.max(upper, r.minTs());
    }
    return new long[] {lower, upper};
  }

}
