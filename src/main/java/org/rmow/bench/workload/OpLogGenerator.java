package org.rmow.bench.workload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Generates an {@link OpLog} from a {@link WorkloadSpec}. Deterministic: the same spec (including
 * its seed) always yields the same log, byte for byte. A spec that leaves regions out (recency or
 * uniform keys, one row an update) yields the log it did before regions existed: the regions'
 * heat comes from the seed apart from the generator's stream, and nothing else draws from it.
 */
public final class OpLogGenerator {

  private static final int MAX_TRIES = 64;
  // the 99th percentile of the standard normal distribution
  private static final double Z99 = 2.3263478740408408;
  private static final long HEAT_SALT = 0x3C6EF372FE94F82BL;

  private final WorkloadSpec spec;
  private final SplittableRandom rnd;
  private final double meanRows;
  private long[] ts;
  private int[] version;
  private final BitSet deleted = new BitSet();
  private long nextId;
  // per region: its heat, and its live rows
  private double[] heat;
  private int[] live;
  // KeyDist.REGIONS: the regions' cumulative weights (heat times live rows) at the batch's start
  private double[] weights;

  private OpLogGenerator(WorkloadSpec spec) {
    spec.validate();
    this.spec = spec;
    this.rnd = new SplittableRandom(spec.seed);
    this.meanRows = spec.recencyMeanBatches * spec.inserts();
  }

  public static OpLog generate(WorkloadSpec spec) {
    return new OpLogGenerator(spec).run();
  }

  private OpLog run() {
    long total = spec.initialRows + (long) spec.batches * spec.inserts();
    if (total > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too many ids for dense arrays: " + total);
    }
    ts = new long[(int) total];
    version = new int[(int) total];
    heat = new double[region(Math.max(0, total - 1)) + 1];
    live = new int[heat.length];
    for (int r = 0; r < heat.length; r++) {
      heat[r] = heat(r);
    }

    for (int i = 0; i < spec.initialRows; i++) {
      double jitter = rnd.nextDouble() * spec.initialJitterRows;
      ts[i] = (long) ((i + jitter) * spec.tsStepMicros);
      live[region(i)] += 1;
    }
    nextId = spec.initialRows;

    double mu = Math.log(spec.lateMedianBatches);
    double sigma = (Math.log(spec.lateP99Batches) - mu) / Z99;
    long batchSpan = (long) spec.inserts() * spec.tsStepMicros;

    List<OpLog.Batch> batches = new ArrayList<>(spec.batches);
    for (int b = 0; b < spec.batches; b++) {
      BitSet touched = new BitSet();
      if (spec.keyDist == WorkloadSpec.KeyDist.REGIONS) {
        weigh();
      }
      long[] updates;
      long[] deletes;
      if (spec.updateShare > 0) {
        // statements change the batch's rows, and its deletes are drawn from all they took
        long[] changed = statements(spec.updates() + spec.deletes(), touched);
        deletes = new long[spec.deletes()];
        for (int i = 0; i < deletes.length; i++) {
          int j = i + rnd.nextInt(changed.length - i);
          deletes[i] = changed[j];
          changed[j] = changed[i];
        }
        updates = Arrays.copyOfRange(changed, deletes.length, changed.length);
      } else {
        updates = choose(spec.updates(), touched);
        deletes = choose(spec.deletes(), touched);
      }
      Arrays.sort(updates);
      int[] versions = new int[updates.length];
      for (int i = 0; i < updates.length; i++) {
        int id = (int) updates[i];
        version[id] += 1;
        versions[i] = version[id];
      }
      for (long id : deletes) {
        deleted.set((int) id);
        live[region(id)] -= 1;
      }

      long[] inserts = new long[spec.inserts()];
      for (int i = 0; i < inserts.length; i++) {
        long id = nextId++;
        long now = id * spec.tsStepMicros;
        double late = Math.exp(mu + sigma * gaussian());
        ts[(int) id] = Math.max(0, now - (long) (late * batchSpan));
        live[region(id)] += 1;
        inserts[i] = id;
      }
      batches.add(new OpLog.Batch(b, inserts, updates, versions, deletes));
    }
    return new OpLog(spec.initialRows, ts, batches);
  }

  private double gaussian() {
    // Box-Muller from the splittable generator, so the log does not depend on java.util.Random
    double u1 = 1.0 - rnd.nextDouble();
    double u2 = rnd.nextDouble();
    return Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  }

  private int region(long id) {
    return (int) (id / spec.regionRows);
  }

  /** A region's heat: {@code U^-regionSkew}, U in (0, 1] from the seed and the region alone. */
  private double heat(int r) {
    long h = Rows.mix64(spec.seed * 0x9E3779B97F4A7C15L + r * 0xC2B2AE3D27D4EB4FL + HEAT_SALT);
    double u = ((h >>> 11) + 1) * 0x1.0p-53;
    return Math.pow(u, -spec.regionSkew);
  }

  /** Weighs the regions that have rows, by heat times live rows. */
  private void weigh() {
    weights = new double[region(nextId - 1) + 1];
    double sum = 0;
    for (int r = 0; r < weights.length; r++) {
      sum += heat[r] * live[r];
      weights[r] = sum;
    }
  }

  private int pickRegion() {
    double u = rnd.nextDouble() * weights[weights.length - 1];
    int lo = 0;
    int hi = weights.length - 1;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (weights[mid] > u) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }
    return lo;
  }

  /** Chooses {@code n} distinct live ids not yet touched in this batch. */
  private long[] choose(int n, BitSet touched) {
    long[] ids = new long[n];
    for (int i = 0; i < n; i++) {
      long id = pick(touched);
      touched.set((int) id);
      ids[i] = id;
    }
    return ids;
  }

  /** A live id not yet touched in this batch, by the key distribution (not marked touched). */
  private long pick(BitSet touched) {
    for (int t = 0; t < MAX_TRIES; t++) {
      long c =
          switch (spec.keyDist) {
            case RECENCY_EXP -> nextId - 1 - (long) (-Math.log(1.0 - rnd.nextDouble()) * meanRows);
            case UNIFORM -> rnd.nextLong(nextId);
            case REGIONS -> {
              long lo = (long) pickRegion() * spec.regionRows;
              yield lo + rnd.nextLong(Math.min(lo + spec.regionRows, nextId) - lo);
            }
          };
      if (c >= 0 && usable((int) c, touched)) {
        return c;
      }
    }
    return nearestUsable(rnd.nextLong(nextId), touched);
  }

  /**
   * Chooses {@code n} ids to change as statements ({@link WorkloadSpec#updateShare}): each takes
   * the region of an id the key distribution picks, and that id and others of the region, until it
   * holds the share of the region's live rows or the region has no usable row left.
   */
  private long[] statements(int n, BitSet touched) {
    long[] ids = new long[n];
    int at = 0;
    while (at < n) {
      long anchor = pick(touched);
      int r = region(anchor);
      long share = Math.max(1, Math.round(spec.updateShare * live[r]));
      at = statement(r, anchor, (int) Math.min(n - at, share), touched, ids, at);
    }
    return ids;
  }

  /** Takes {@code anchor} and up to {@code k - 1} other usable ids of its region, uniformly. */
  private int statement(int r, long anchor, int k, BitSet touched, long[] ids, int at) {
    touched.set((int) anchor);
    ids[at++] = anchor;
    int more = k - 1;
    long lo = (long) r * spec.regionRows;
    long hi = Math.min(lo + spec.regionRows, nextId);
    if (more > 0 && 4L * more <= hi - lo) {
      // a few of many: draw ids of the region until enough are usable
      for (int t = 0; more > 0 && t < MAX_TRIES * k; t++) {
        long c = lo + rnd.nextLong(hi - lo);
        if (usable((int) c, touched)) {
          touched.set((int) c);
          ids[at++] = c;
          more -= 1;
        }
      }
    }
    if (more > 0) {
      // the rest from the region's usable ids, by a partial shuffle
      long[] usable = new long[(int) (hi - lo)];
      int m = 0;
      for (long c = lo; c < hi; c++) {
        if (usable((int) c, touched)) {
          usable[m++] = c;
        }
      }
      for (int i = 0; i < Math.min(more, m); i++) {
        int j = i + rnd.nextInt(m - i);
        long c = usable[j];
        usable[j] = usable[i];
        touched.set((int) c);
        ids[at++] = c;
      }
    }
    return at;
  }

  private boolean usable(int id, BitSet touched) {
    return !deleted.get(id) && !touched.get(id);
  }

  private long nearestUsable(long from, BitSet touched) {
    for (long d = 0; d < nextId; d++) {
      long up = from + d;
      if (up < nextId && usable((int) up, touched)) {
        return up;
      }
      long down = from - d;
      if (down >= 0 && usable((int) down, touched)) {
        return down;
      }
    }
    throw new IllegalStateException("No live rows left to update or delete");
  }
}
