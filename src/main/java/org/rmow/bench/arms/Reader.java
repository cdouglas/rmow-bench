package org.rmow.bench.arms;

import java.util.concurrent.ExecutorService;

/**
 * A reader model: how a read plans and opens files. The runner probes each table state with every
 * configured model, so they see identical tables.
 *
 * @param footerCache a read fetches each file version's one footer once, for all its splits (a
 *     version is summarized in exactly one footer); without it, as with stock Iceberg readers
 *     today, every split (one per row group) opens the file and reads the footer again
 * @param footerPrefetchBytes the tail each footer read prefetches (0: stock parquet-java, which
 *     takes two requests, the 8-byte tail and then the footer)
 * @param splitTargetBytes files are split into scan tasks of about this size
 * @param pool runs a read's scan tasks; null runs them one after another
 */
public record Reader(
    String name,
    boolean footerCache,
    int footerPrefetchBytes,
    long splitTargetBytes,
    ExecutorService pool) {

  /** How writers read (locate scans, merges, compaction): sequential, with the layout's prefetch. */
  static Reader writer(LayoutSpec layout) {
    return new Reader("writer", false, layout.footerPrefetchBytes, Long.MAX_VALUE, null);
  }
}
