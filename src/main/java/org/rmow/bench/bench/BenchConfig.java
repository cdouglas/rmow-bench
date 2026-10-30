package org.rmow.bench.bench;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import org.rmow.bench.arms.LayoutSpec;
import org.rmow.bench.workload.WorkloadSpec;

/** A benchmark run's configuration, bound from JSON (public fields with defaults). */
public final class BenchConfig {

  /** Where objects live (see {@link Store}). */
  public static final class StoreSpec {
    /** {@code mem} (in-memory), {@code local} (a local filesystem), {@code s3} or {@code azure}. */
    public String type = "mem";
    /**
     * Location prefix: for {@code local}, an absolute directory; for {@code s3}, {@code
     * s3://bucket/prefix}; for {@code azure}, {@code
     * abfss://container@account.dfs.core.windows.net/prefix}. Each run puts its objects under its
     * own prefix here.
     */
    public String root = "mem://bench";
    /** For {@code local}: fsync each written object and append. */
    public boolean fsync = false;
    /**
     * For {@code s3} and {@code azure}: an emulator's endpoint, to use instead of the cloud's. An
     * emulator lacks part of the fork's atomic operations, so RMoW's appendable arms run there on
     * stand-ins ({@link org.rmow.bench.append.EmulatorAppenders}). For Azurite, {@code
     * http://127.0.0.1:10000/devstoreaccount1}, with the account's key in {@code
     * AZURE_STORAGE_KEY}; against Azure, the default credential chain applies.
     */
    public String endpoint = "";
    /** For {@code s3}: the region; empty means {@code AWS_REGION}, or else us-east-1. */
    public String region = "";
    /** For {@code s3}: path-style requests, as emulators need. */
    public boolean pathStyle = false;
    /** For {@code s3}: the HTTP client's connection pool. */
    public int maxConnections = 64;
    /**
     * A simulated store latency injected into every measured request (null: none), so a phase's
     * wall time becomes what it would take on such a store ({@link
     * org.rmow.bench.io.StoreLatency}). Meant for the local and in-memory stores.
     */
    public LatencySpec latency = null;
  }

  /**
   * A simulated store: median latencies by request kind with their lognormal sigmas, an extra GET
   * latency per appended part, transfer times per MiB, and the store's cap on an append, as {@code
   * lab/views/40_latency.sql}'s {@code store_profiles} lists them.
   */
  public static final class LatencySpec {
    /** A name for the profile, for the record. */
    public String profile = "";
    public double getMs = 0;
    public double getSigma = 0;
    public double putMs = 0;
    public double putSigma = 0;
    public double appendMs = 0;
    public double appendSigma = 0;
    public double partMs = 0;
    public double readMsPerMib = 0;
    public double writeMsPerMib = 0;
    /**
     * The most one append may write, or 0 for no cap: 100 for Azure, whose Append Block the fork
     * splits a larger append into.
     */
    public double maxAppendMib = 0;
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
    /**
     * Load each data file's deletes once per read, for all its splits, as an engine with a delete
     * cache would. Iceberg's readers load a file's deletion vector again for every split.
     */
    public boolean deleteCache = false;
    /** Prefetch this tail on each footer read (0: stock parquet-java's two requests). */
    public int footerPrefetchBytes = 0;
    /** Split files into scan tasks of about this size (Iceberg's read.split.target-size). */
    public long splitTargetBytes = 128L << 20;
    /** Run a read's scan tasks on this many threads. */
    public int parallelism = 1;
    /**
     * Plan no split for a row group the file's deletes cover whole, nor any for a file whose DV
     * covers every row, as an engine could from the footer it reads (needs {@code footerCache};
     * {@link org.rmow.bench.arms.Reader}). Iceberg's reader fetches such row groups and drops
     * their rows one by one.
     */
    public boolean skipDeadRowGroups = false;
    /**
     * Also read the table in event-time order at each probe ({@code read_sorted}): a merge of its
     * row groups, each a sorted run, opened as the merge reaches them, so the read holds every
     * row group whose event times overlap at once (needs {@code footerCache}, sorted writes and an
     * initial load in event-time order, {@code initialJitterRows} 0). Left out of the
     * configuration's hash when off, so configurations from before it keep theirs.
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean sortedRead = false;

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

    /**
     * An engine-like model: as {@link #oneFooter}, and each data file's deletes loaded once per
     * read too, so a read costs one footer and one delete load per file, then its row groups.
     */
    public static ReaderSpec cached() {
      ReaderSpec r = oneFooter();
      r.name = "cached";
      r.deleteCache = true;
      return r;
    }

    /** As {@link #cached}, and no row group read that the file's deletes cover whole. */
    public static ReaderSpec skipping() {
      ReaderSpec r = cached();
      r.name = "cached-skip";
      r.skipDeadRowGroups = true;
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
