package org.rmow.bench.workload;

/**
 * What the op log contains. Bound from the benchmark config (public fields with defaults).
 *
 * <p>Event time: rows arrive in rough {@code ts} order. The initial load's {@code ts} grows with
 * its id plus a small jitter; each batch's inserts are stamped at the batch's clock minus a
 * lognormal lateness, so most land near "now" and a few arrive late.
 *
 * <p>Regions: ids fall into regions of {@code regionRows} consecutive ids, about a row group's
 * rows (ids grow with insertion, and writes keep rows in roughly that order). The key distribution
 * {@link KeyDist#REGIONS} picks rows by region, and an update share above 0 makes the updates and
 * deletes statements, each over one region.
 */
public final class WorkloadSpec {

  /** How updates and deletes choose existing rows. */
  public enum KeyDist {
    /** Age in rows (newest first) is exponential with mean {@code recencyMeanBatches} batches. */
    RECENCY_EXP,
    /** Uniform over every row ever inserted (rejecting deleted ones). */
    UNIFORM,
    /**
     * By region: each region's heat is drawn once, from the seed and the region alone, as {@code
     * U^-regionSkew} for a uniform U in (0, 1], so regions ranked by heat follow Zipf's law with
     * exponent {@code regionSkew}. A row is picked by picking a region with weight heat times live
     * rows (as of the batch's start), then an id in it uniformly, rejecting deleted ones. Hot
     * regions can be anywhere and stay hot; at a skew of 0 every live row is equally likely.
     */
    REGIONS
  }

  public long seed = 1;
  public long initialRows = 200_000;
  public int batches = 20;
  public int opsPerBatch = 2_000;
  public double insertFrac = 0.7;
  public double updateFrac = 0.25;
  public double deleteFrac = 0.05;
  public KeyDist keyDist = KeyDist.RECENCY_EXP;
  public double recencyMeanBatches = 2.0;
  /** Event-time spacing between consecutive inserts, in microseconds. */
  public long tsStepMicros = 1_000;
  /** Initial-load jitter, in rows of event time. */
  public double initialJitterRows = 100;
  /** Insert lateness, in batches of event time: lognormal by its median and 99th percentile. */
  public double lateMedianBatches = 0.05;

  public double lateP99Batches = 5;

  /**
   * Consecutive ids per region. A region stands for a row group: the default is about the rows of
   * a 4 MiB one (98,000 at zstd; a 1 MiB one holds 10,521).
   */
  public long regionRows = 100_000;
  /** {@link KeyDist#REGIONS}: the Zipf exponent of the regions' heat; 0 is uniform. */
  public double regionSkew = 1.0;
  /**
   * The share p of a region's live rows each statement changes (at least one row); 0 makes each
   * update and delete a row of its own. A statement's region is that of a row the key distribution
   * picks (by {@link KeyDist#REGIONS}, a region by its weight), and it changes rows of the region
   * not yet touched in the batch, picked uniformly. The batch's deletes are drawn uniformly from
   * the rows its statements changed, and the others are updated, as a MERGE updates some of the
   * rows it matches and deletes others. The rows changed per batch stay {@link #updates()} plus
   * {@link #deletes()}: the batch's last statement takes what is left, so p counts only up to
   * those over {@code regionRows}.
   */
  public double updateShare = 0;

  public int inserts() {
    return (int) Math.round(opsPerBatch * insertFrac);
  }

  public int updates() {
    return (int) Math.round(opsPerBatch * updateFrac);
  }

  public int deletes() {
    return opsPerBatch - inserts() - updates();
  }

  public void validate() {
    double sum = insertFrac + updateFrac + deleteFrac;
    if (Math.abs(sum - 1.0) > 1e-9) {
      throw new IllegalArgumentException("Op fractions must sum to 1: " + sum);
    }
    if (deletes() < 0) {
      throw new IllegalArgumentException("Negative delete count");
    }
    if (regionRows < 1) {
      throw new IllegalArgumentException("regionRows must be at least 1: " + regionRows);
    }
    if (!(regionSkew >= 0)) {
      throw new IllegalArgumentException("regionSkew must be at least 0: " + regionSkew);
    }
    if (!(updateShare >= 0 && updateShare <= 1)) {
      throw new IllegalArgumentException("updateShare must be in [0, 1]: " + updateShare);
    }
  }
}
