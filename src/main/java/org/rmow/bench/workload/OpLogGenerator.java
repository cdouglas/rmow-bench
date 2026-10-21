package org.rmow.bench.workload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Generates an {@link OpLog} from a {@link WorkloadSpec}. Deterministic: the same spec (including
 * its seed) always yields the same log, byte for byte.
 */
public final class OpLogGenerator {

  private static final int MAX_TRIES = 64;
  // the 99th percentile of the standard normal distribution
  private static final double Z99 = 2.3263478740408408;

  private final WorkloadSpec spec;
  private final SplittableRandom rnd;
  private long[] ts;
  private int[] version;
  private final BitSet deleted = new BitSet();
  private long nextId;

  private OpLogGenerator(WorkloadSpec spec) {
    spec.validate();
    this.spec = spec;
    this.rnd = new SplittableRandom(spec.seed);
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

    for (int i = 0; i < spec.initialRows; i++) {
      double jitter = rnd.nextDouble() * spec.initialJitterRows;
      ts[i] = (long) ((i + jitter) * spec.tsStepMicros);
    }
    nextId = spec.initialRows;

    double mu = Math.log(spec.lateMedianBatches);
    double sigma = (Math.log(spec.lateP99Batches) - mu) / Z99;
    long batchSpan = (long) spec.inserts() * spec.tsStepMicros;

    List<OpLog.Batch> batches = new ArrayList<>(spec.batches);
    for (int b = 0; b < spec.batches; b++) {
      BitSet touched = new BitSet();
      long[] updates = choose(spec.updates(), touched);
      Arrays.sort(updates);
      int[] versions = new int[updates.length];
      for (int i = 0; i < updates.length; i++) {
        int id = (int) updates[i];
        version[id] += 1;
        versions[i] = version[id];
      }
      long[] deletes = choose(spec.deletes(), touched);
      for (long id : deletes) {
        deleted.set((int) id);
      }

      long[] inserts = new long[spec.inserts()];
      for (int i = 0; i < inserts.length; i++) {
        long id = nextId++;
        long now = id * spec.tsStepMicros;
        double late = Math.exp(mu + sigma * gaussian());
        ts[(int) id] = Math.max(0, now - (long) (late * batchSpan));
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

  /** Chooses {@code n} distinct live ids not yet touched in this batch. */
  private long[] choose(int n, BitSet touched) {
    long[] ids = new long[n];
    double meanRows = spec.recencyMeanBatches * spec.inserts();
    for (int i = 0; i < n; i++) {
      long id = -1;
      for (int t = 0; t < MAX_TRIES && id < 0; t++) {
        long c =
            switch (spec.keyDist) {
              case RECENCY_EXP -> nextId - 1 - (long) (-Math.log(1.0 - rnd.nextDouble()) * meanRows);
              case UNIFORM -> rnd.nextLong(nextId);
            };
        if (c >= 0 && usable((int) c, touched)) {
          id = c;
        }
      }
      if (id < 0) {
        id = nearestUsable(rnd.nextLong(nextId), touched);
      }
      touched.set((int) id);
      ids[i] = id;
    }
    return ids;
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
