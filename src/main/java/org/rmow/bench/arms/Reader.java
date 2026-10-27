package org.rmow.bench.arms;

import java.util.concurrent.ExecutorService;

/**
 * A reader model: how a read plans and opens files. The runner probes each table state with every
 * configured model, so they see identical tables.
 *
 * @param footerCache a read fetches each file version's one footer once, for all its splits (a
 *     version is summarized in exactly one footer); without it, as with stock Iceberg readers
 *     today, every split (one per row group) opens the file and reads the footer again
 * @param deleteCache a read loads each data file's deletes once, for all its splits; without it,
 *     as Iceberg's readers do ({@code BaseDeleteLoader} never caches a deletion vector), every
 *     split loads its file's DV again
 * @param footerPrefetchBytes the tail each footer read prefetches (0: stock parquet-java, which
 *     takes two requests, the 8-byte tail and then the footer)
 * @param splitTargetBytes files are split into scan tasks of about this size
 * @param skipDeadRowGroups before planning a file's splits, check its deletes against the row
 *     groups of the footer it reads, and plan none for a row group they cover whole, nor for a file
 *     whose DV covers every row. Iceberg's reader skips row groups only on statistics, dictionaries
 *     and bloom filters, and applies deletes row by row, so it fetches a dead row group and drops
 *     every row. Needs {@code footerCache}; a file of at most 1 MiB, fetched whole, is skipped only
 *     whole
 * @param pool runs a read's scan tasks; null runs them one after another
 */
public record Reader(
    String name,
    boolean footerCache,
    boolean deleteCache,
    int footerPrefetchBytes,
    long splitTargetBytes,
    boolean skipDeadRowGroups,
    ExecutorService pool) {

  public Reader {
    if (skipDeadRowGroups && !footerCache) {
      throw new IllegalArgumentException(
          "Reader " + name + ": skipDeadRowGroups needs footerCache, the footer it plans from");
    }
  }

  /** How writers read (locate scans, merges, compaction): sequential, with the layout's prefetch. */
  static Reader writer(LayoutSpec layout) {
    return new Reader(
        "writer", false, false, layout.footerPrefetchBytes, Long.MAX_VALUE, false, null);
  }
}
