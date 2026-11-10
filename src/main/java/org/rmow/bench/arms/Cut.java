package org.rmow.bench.arms;

import org.apache.iceberg.TableProperties;

/**
 * The cut ({@link LayoutSpec#cut}): how a row group that a kept arm rewrites is written as several
 * row groups. Its rows are counted in strides from its first row; a stride is flagged when the write
 * changes a row in it or the row group's DV holds a dead row there. Each run of flagged strides is
 * one piece, and so is each run of unflagged strides between them; an unflagged run shorter than
 * {@code minStrides} joins the flagged run beside it. A row group with no flagged stride is one
 * piece (docs/drafts/region-cut-2026-11-09.md).
 */
final class Cut {

  /**
   * A stride: the rows at which every column starts a new page, Iceberg's page row limit
   * ({@code write.parquet.page-row-limit}), which the arms' writers leave at its default.
   */
  static final int STRIDE_ROWS = TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT;

  private Cut() {}

  /** The strides of {@code strideRows} rows in a row group of {@code rows} (the last partial). */
  static int strides(long rows, int strideRows) {
    return (int) ((rows + strideRows - 1) / strideRows);
  }

  /** Each stride's piece, numbered from 0 in row order. */
  static int[] pieces(boolean[] flagged, int minStrides) {
    boolean[] f = flagged.clone();
    boolean any = false;
    for (boolean b : flagged) {
      any |= b;
    }
    if (any) {
      int i = 0;
      while (i < f.length) {
        if (flagged[i]) {
          i++;
          continue;
        }
        int j = i;
        while (j < f.length && !flagged[j]) {
          j++;
        }
        if (j - i < minStrides) {
          for (int s = i; s < j; s++) {
            f[s] = true;
          }
        }
        i = j;
      }
    }
    int[] out = new int[f.length];
    int piece = 0;
    for (int s = 0; s < f.length; s++) {
      if (s > 0 && f[s] != f[s - 1]) {
        piece++;
      }
      out[s] = piece;
    }
    return out;
  }
}
