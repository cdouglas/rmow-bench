package org.rmow.bench.bench;

import java.util.List;
import org.rmow.bench.arms.LayoutSpec;
import org.rmow.bench.workload.WorkloadSpec;

/** A benchmark run's configuration, bound from JSON (public fields with defaults). */
public final class BenchConfig {

  /** Where objects live (see {@link Store}). */
  public static final class StoreSpec {
    /** {@code mem} (in-memory), {@code local} (a local filesystem) or {@code s3}. */
    public String type = "mem";
    /**
     * Location prefix: for {@code local}, an absolute directory; for {@code s3}, {@code
     * s3://bucket/prefix}. Each run puts its objects under its own prefix here.
     */
    public String root = "mem://bench";
    /** For {@code local}: fsync each written object and append. */
    public boolean fsync = false;
    /**
     * For {@code s3}: how RMoW appends. {@code express}: S3 Express One Zone, a PutObject at the
     * object's length. {@code emulated}: read the object and write it back longer, for emulators
     * only (unmeasured). {@code none}: standard S3, where RMoW arms cannot run.
     */
    public String append = "none";
    /** For {@code s3}: an endpoint to use instead of AWS's (an emulator's). */
    public String endpoint = "";
    /** For {@code s3}: the region; empty means {@code AWS_REGION}, or else us-east-1. */
    public String region = "";
    /** For {@code s3}: path-style requests, as emulators need. */
    public boolean pathStyle = false;
    /** For {@code s3}: the HTTP client's connection pool. */
    public int maxConnections = 64;
  }

  /** Which reads probe the arms, and how often. */
  public static final class ReadSpec {
    /** Probe after every {@code every} batches (and after the last). */
    public int every = 5;
    /** {@code read_recent} reads rows with {@code ts} in the newest fraction of the event time. */
    public double recentFraction = 0.01;
    /** {@code read_old} reads the table as it was this many batches earlier. */
    public int travelBack = 10;
    /**
     * The reader models every probe runs under (see {@link org.rmow.bench.arms.Reader}); the
     * first is the primary one.
     */
    public List<ReaderSpec> readers = List.of(ReaderSpec.oneFooter(), ReaderSpec.stock());
  }

  /** A reader model, by name. */
  public static final class ReaderSpec {
    public String name = "stock";
    /** Fetch each file's footer once per read, for all its splits. */
    public boolean footerCache = false;
    /** Prefetch this tail on each footer read (0: stock parquet-java's two requests). */
    public int footerPrefetchBytes = 0;
    /** Split files into scan tasks of about this size (Iceberg's read.split.target-size). */
    public long splitTargetBytes = 128L << 20;
    /** Run a read's scan tasks on this many threads. */
    public int parallelism = 1;

    /**
     * Stock Iceberg readers today: one split per row group, each re-reading the file's footer
     * (two requests, as parquet-java reads it).
     */
    public static ReaderSpec stock() {
      return new ReaderSpec();
    }

    /**
     * The primary model: a read opens each file at one version and reads that version's one
     * footer once, in one request (a 64 KiB tail prefetch, as Arrow and DuckDB read footers),
     * then only the row groups it needs.
     */
    public static ReaderSpec oneFooter() {
      ReaderSpec r = new ReaderSpec();
      r.name = "one-footer";
      r.footerCache = true;
      r.footerPrefetchBytes = 64 << 10;
      return r;
    }
  }

  /**
   * A warmup before the measured run, so the JIT has compiled the write and read paths before
   * latency counts: the same arms, layout and readers on a small op log of the same mix, on the
   * in-memory store, with every probe (time travel included) each batch. Nothing it does is
   * recorded, and it changes no request or byte of the measured run.
   */
  public static final class WarmupSpec {
    /** Batches of the warmup's op log (0: no warmup). */
    public int batches = 0;
    /** Its initial load. */
    public long initialRows = 100_000;
  }

  public String label = "smoke";
  public StoreSpec store = new StoreSpec();
  public WarmupSpec warmup = new WarmupSpec();
  public WorkloadSpec workload = new WorkloadSpec();
  public LayoutSpec layout = new LayoutSpec();
  public List<String> arms = List.of("mor-v3", "rmow-a");
  public ReadSpec reads = new ReadSpec();
  /** Check each decoded row's payload; costs the same CPU in every arm. */
  public boolean verifyRows = true;
}
