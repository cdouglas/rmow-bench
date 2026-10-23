package org.rmow.bench.arms;

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
   * Writers keep the footers they wrote: a writer's own reads (RMoW reading back the row groups a
   * batch touches) take the footer from memory instead of two GETs. MoR's writers read no data
   * files, so this changes only RMoW; readers (the probes) always read footers from storage.
   */
  public boolean writerCachesFooters = true;
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
   * Readers (every arm, data and delete files alike) prefetch this many trailing bytes on a
   * file's first footer probe, so a footer that fits costs one request. 0: stock parquet-java,
   * which reads the 8-byte tail and then the footer, two requests.
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

  /** The Parquet writer properties for data files. */
  public Map<String, String> writerProperties() {
    Map<String, String> p = new LinkedHashMap<>();
    p.put(TableProperties.PARQUET_COMPRESSION, codec);
    p.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(rowGroupBytes));
    return p;
  }
}
