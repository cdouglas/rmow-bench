package org.rmow.bench.arms;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.TableProperties;

/**
 * Physical layout knobs shared by every arm, so that all of them write the same encoding. Bound
 * from the benchmark config (public fields with defaults).
 */
public final class LayoutSpec {

  /** Parquet codec. Iceberg tables created since 1.4 default to zstd (gzip without a table). */
  public String codec = "zstd";
  /**
   * Row-group target of data files and of the initial load: Parquet's, which counts buffered
   * bytes (compressed pages plus the open, uncompressed ones), so row groups come out smaller on
   * disk (about a third of a 1 MiB target for the bench schema at zstd, three quarters of a 4 MiB
   * one). RMoW compaction packs row groups up to the size the writer's come out at on disk.
   */
  public long rowGroupBytes = 16L << 20;
  /** Data files (and appendable files' live bytes) roll over at this size. */
  public long targetFileBytes = 128L << 20;
  /** Sort the rows each write produces by {@code ts} (data files, merged regions). */
  public boolean sortWrites = true;
  /**
   * What every arm's writer holds of the footers it reads ({@code WriterFooters}, I/O parity rule
   * 6). A batch or a compaction is a transaction, and nothing carries over from one to the next:
   * an Iceberg writer starts each from storage. {@code transaction}: within one, a writer reads a
   * file version's footer once, on its first use, and reuses it (RMoW's locate, row-group reads
   * and new footer; MoR-family locate scans and merge reads). {@code open}: every open reads it
   * again, as parquet-java does. A file of at most 1 MiB is fetched whole on every open either
   * way. Readers (the probes) follow their reader model.
   */
  public String writerFooters = "transaction";
  /**
   * Tail merging (RMoW policies B and C, TailCoW): an open tail unit is sealed at this many
   * compressed bytes (under C, once {@code tierFanout} units of its size would pass it). Units
   * are row groups (RMoW, inserts only) or whole data files (TailCoW, with the batch's updated
   * versions), so one seal size makes TailCoW units seal sooner.
   */
  public long tailSealBytes = 16L << 20;
  /** Tiered tail merging (policy C): this many units of one level merge into one a level up. */
  public int tierFanout = 4;
  /**
   * RMoW: the tail file rolls over at this many row groups. Every version rewrites the whole
   * footer and every open decodes it, at about 1.2 KB and 30 us per row group for this schema
   * ({@code FooterBench}).
   */
  public int maxRowGroupsPerFile = 128;
  /**
   * RMoW: where a row group that replaces another is listed in the new footer. {@code logical}:
   * in the replaced row group's place, so footer order, which is row order, stays the order rows
   * were inserted in (id order) and a file written sorted stays sorted. {@code offset}: last,
   * which keeps the list in ascending file-offset order. Readers read either (the Parquet spec
   * requires neither; see {@code docs/DESIGN.md}), and the I/O is the same.
   */
  public String footerOrder = "logical";
  /**
   * Writers (every arm's locate scans, merge reads and compaction, data and delete files alike)
   * prefetch this many trailing bytes on a file's first footer probe, so a footer that fits costs
   * one request. 0: stock parquet-java, which reads the 8-byte tail and then the footer, two
   * requests. Each reader model sets its own ({@code reads.readers}).
   */
  public int footerPrefetchBytes = 0;

  /**
   * How writers find the rows a batch updates or deletes. {@code oracle}: from an in-memory index,
   * at no cost (isolates write I/O). {@code scan}: as a MERGE would, timed as the {@code locate}
   * phase and checked against the index; MoR-family arms scan {@code id} and {@code _pos} of every
   * live data file whose {@code id} bounds may hold a target, RMoW reads row-group {@code id}
   * statistics from its footers.
   */
  public String locate = "oracle";

  /**
   * What a write reads of the rows it updates (docs/HYPOTHESES.md, 2026-11-06). A new version is
   * the old row with some columns set, so a writer reads every column of it.
   *
   * <ul>
   *   <li>null or {@code id}: nothing beyond the locate's {@code id} and {@code _pos}; the new
   *       version is made from its id and version alone (blind), as before. RMoW and the region
   *       rewrites read the row groups they rewrite whole either way.
   *   <li>{@code row}: every column of each row group that holds a row the write updates, deletes
   *       applied, once. Row groups and files the write rewrites are read whole anyway (its
   *       merge-read); the others are read in phase {@code update-read}, after the locate. This is
   *       a plan in two passes, as Spark plans a copy-on-write MERGE: find the rows by key, then
   *       read what the write needs.
   *   <li>{@code scan}: MoR (v2 and v3) reads every column in its locate scan, one pass, as Spark
   *       plans a MERGE on a merge-on-read table. The arms that rewrite regions plan as under
   *       {@code row}.
   * </ul>
   *
   * Left out of the configuration's hash when null, so configurations from before it keep theirs.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String updateRead = null;

  /** Compact every this many batches (0: never), with the triggers below. */
  public int compactEvery = 0;
  /** MoR family: rewrite files with at least this fraction of their rows deleted. */
  public double compactDeleteRatio = 0.3;
  /** MoR family: bin-pack files smaller than this fraction of the target file size. */
  public double compactSmallFileFraction = 0.75;
  /**
   * RMoW, for space: rewrite an appendable file whose dead bytes are at least this fraction of its
   * length; in a split file, move the live row groups out of each object whose dead bytes are at
   * least this fraction of its length. Above 1: never.
   */
  public double compactGarbageRatio = 0.5;
  /**
   * RMoW, for reads: once a file holds this many sealed row groups under half the size of the
   * writer's row groups on disk, pack runs of them adjacent in id order into row groups of up to
   * that size (0: never).
   */
  public int compactSmallRowGroups = 8;
  /**
   * RMoR ({@code rmor-c}): a batch rewrites a region it changes (a sealed file's row group, or an
   * open tail file) when it updates or deletes at least this share of the region's live rows, and
   * otherwise puts DVs on the rows it changes. 0: every region it changes; above 1: none, which is
   * TailCoW-C.
   */
  public double rmorRewriteShare = 0;
  /**
   * The MoR family's writers (their scans to locate rows, merge reads and compaction) read no row
   * group the file's deletes cover whole, nor any file whose DV covers every row, as the {@code
   * skipDeadRowGroups} reader does. Off: as Iceberg's MERGE scans, which fetch such row groups and
   * drop their rows one by one.
   */
  public boolean writerSkipsDeadRowGroups = false;

  /**
   * RMoR with a cleaner ({@code rmor-c-clean}; docs/drafts/strides-and-cleaner-2026-11-04.md): its settings, or
   * null for the defaults. Left out of the configuration's hash when null, so configurations from
   * before it keep theirs.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Clean clean = null;

  /** The cleaner's settings, and its threshold for rewriting a row group. */
  public static final class Clean {
    /** A write rewrites a row group when it changes at least this share of its live rows. */
    public double rewriteShare = 0.25;
    /** A row group with fewer live rows than this share of its rows is sparse. */
    public double low = 0.5;
    /** A unit is cold once no write has changed it for this many writes. */
    public int age = 2;
    /** The cleaner carries at most this share of the write's own rows, drains apart. */
    public double budget = 0.5;
    /** A cold file with fewer live rows than this share of its rows has them all carried. */
    public double drain = 0.5;
    /** Files with at least this share of their rows dead have their row groups judged. */
    public double examine = 0.1;
  }

  /** The cleaner's settings in effect. */
  public Clean cleanOrDefault() {
    return clean != null ? clean : new Clean();
  }

  /**
   * RMoR with deferred merges ({@code rmor-density}; docs/HYPOTHESES.md, 2026-11-06): its
   * settings, or null for the defaults. Left out of the configuration's hash when null.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Defer defer = null;

  /** Deferred merges: when a row group is merged, and where the rows waiting for it go. */
  public static final class Defer {
    /**
     * θ: a write merges a row group it changes once the row group's dead rows and the write's
     * changes to it reach this share of its rows.
     */
    public double share = 0.06;
    /**
     * Where the new versions of the rows a DV hides wait for their region's merge: {@code region},
     * a row group per region (the base row group they left, which keeps its region through its
     * merges); {@code write}, one row group of the write's.
     */
    public String deltas = "region";
  }

  /** The deferred merges' settings in effect. */
  public Defer deferOrDefault() {
    return defer != null ? defer : new Defer();
  }

  /**
   * The arms that keep row groups whole ({@code rmor-c-kept}, {@code rmor-c-footer-kept}, {@code
   * rmor-c-clean}, {@code rmor-density}): a row group a write rewrites is written as several row
   * groups, cut where the write changes it ({@link Cut}; docs/HYPOTHESES.md, 2026-11-09). Null: as
   * one row group, as before. Left out of the configuration's hash when null.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public CutSpec cut = null;

  /** Where a rewritten row group is cut. */
  public static final class CutSpec {
    /** {@code changed}: at the runs of strides the write changes or that hold dead rows. */
    public String at = "changed";
    /** A run of fewer than this many unflagged strides joins the flagged run beside it. */
    public int minStrides = 4;
    /**
     * Rows a stride: Iceberg's page row limit, where every column of the bench's rows starts a
     * page. Smaller only for the fuzzer, whose row groups hold fewer rows than a page limit.
     */
    public int strideRows = Cut.STRIDE_ROWS;
  }

  /** {@link #updateRead}, with null as {@code id}. */
  public String updateReadOrDefault() {
    return updateRead != null ? updateRead : "id";
  }

  /** The Parquet writer properties for data files. */
  public Map<String, String> writerProperties() {
    Map<String, String> p = new LinkedHashMap<>();
    p.put(TableProperties.PARQUET_COMPRESSION, codec);
    p.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(rowGroupBytes));
    return p;
  }
}
