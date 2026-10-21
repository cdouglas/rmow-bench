package org.rmow.bench.workload;

/**
 * What the op log contains. Bound from the benchmark config (public fields with defaults).
 *
 * <p>Event time: rows arrive in rough {@code ts} order. The initial load's {@code ts} grows with
 * its id plus a small jitter; each batch's inserts are stamped at the batch's clock minus a
 * lognormal lateness, so most land near "now" and a few arrive late.
 */
public final class WorkloadSpec {

  /** How updates and deletes choose existing rows. */
  public enum KeyDist {
    /** Age in rows (newest first) is exponential with mean {@code recencyMeanBatches} batches. */
    RECENCY_EXP,
    /** Uniform over every row ever inserted (rejecting deleted ones). */
    UNIFORM
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
  }
}
