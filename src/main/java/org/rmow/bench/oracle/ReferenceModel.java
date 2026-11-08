package org.rmow.bench.oracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.rmow.bench.workload.OpLog;

/**
 * The table every arm must read back: each id's live version (or none), applied batch by batch
 * from the op log. Keeps the digest after each step, so time-travel reads can be checked too.
 */
public final class ReferenceModel {

  private final OpLog log;
  private final int[] version; // -1: not inserted yet or deleted
  private final TableHash current = new TableHash();
  private final List<TableHash> history = new ArrayList<>(); // index 0: the initial load

  public ReferenceModel(OpLog log) {
    this.log = log;
    this.version = new int[(int) log.totalIds()];
    Arrays.fill(version, -1);
    for (int id = 0; id < log.initialRows(); id++) {
      version[id] = 0;
      current.add(id, 0);
    }
    history.add(current.copy());
  }

  public void apply(OpLog.Batch b) {
    for (int i = 0; i < b.updateIds().length; i++) {
      int id = (int) b.updateIds()[i];
      current.remove(id, version[id]);
      version[id] = b.updateVersions()[i];
      current.add(id, version[id]);
    }
    for (long d : b.deleteIds()) {
      int id = (int) d;
      current.remove(id, version[id]);
      version[id] = -1;
    }
    for (long ins : b.insertIds()) {
      int id = (int) ins;
      version[id] = 0;
      current.add(id, 0);
    }
    history.add(current.copy());
  }

  public TableHash expected() {
    return current.copy();
  }

  /** The digest after {@code step} batches (0: the initial load alone). */
  public TableHash expectedAfter(int steps) {
    return history.get(steps).copy();
  }

  /** Rows of the current table with {@code lo <= ts}. */
  public TableHash expectedTsAtLeast(long lo) {
    TableHash h = new TableHash();
    for (int id = 0; id < version.length; id++) {
      if (version[id] >= 0 && log.ts(id) >= lo) {
        h.add(id, version[id]);
      }
    }
    return h;
  }

  /** Rows of the current table with {@code lo <= ts < hi} or, by id, {@code lo <= id < hi}. */
  public TableHash expectedIn(boolean byTs, long lo, long hi) {
    TableHash h = new TableHash();
    int from = byTs ? 0 : (int) Math.max(0, lo);
    int to = byTs ? version.length : (int) Math.min(version.length, Math.max(0, hi));
    for (int id = from; id < to; id++) {
      if (version[id] >= 0 && (!byTs || log.ts(id) >= lo && log.ts(id) < hi)) {
        h.add(id, version[id]);
      }
    }
    return h;
  }

  public int version(long id) {
    return version[(int) id];
  }
}
