package org.rmow.bench.arms;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The footers one arm's writer holds ({@link LayoutSpec#writerFooters}, I/O parity rule 6). A
 * writer works in transactions, a batch or a compaction, and each runs on its own: Iceberg keeps
 * no footer across them, and neither does this. Within a transaction:
 *
 * <ul>
 *   <li>{@code transaction}: the writer reads a file version's footer on its first use, as an open
 *       reads it, and reuses it after that; a version it wrote in the transaction it knows;
 *   <li>{@code open}: every use reads it again, as every parquet-java open does.
 * </ul>
 *
 * Either way a file of at most 1 MiB is fetched whole on every open, as Iceberg fetches small
 * files, so holding its footer saves nothing.
 */
final class WriterFooters {

  private final ArmContext ctx;
  private final boolean transaction;
  private final Set<String> known = new HashSet<>();
  private final Map<String, byte[]> tails = new HashMap<>();

  WriterFooters(ArmContext ctx) {
    this.ctx = ctx;
    this.transaction =
        switch (ctx.layout().writerFooters) {
          case "transaction" -> true;
          case "open" -> false;
          default ->
              throw new IllegalArgumentException(
                  "layout.writerFooters: transaction or open, not " + ctx.layout().writerFooters);
        };
  }

  /** Starts a transaction: nothing read before it carries over. */
  void begin() {
    known.clear();
    tails.clear();
  }

  /**
   * Whether a use of this version's footer reads it from storage: every use under {@code open};
   * under {@code transaction}, the first in the transaction (it is known from then on).
   */
  boolean mustRead(String location, long length) {
    return !transaction || known.add(location + "@" + length);
  }

  /** Notes a version this transaction wrote: its writer knows its footer. */
  void wrote(String location, long length) {
    if (transaction) {
      known.add(location + "@" + length);
    }
  }

  /**
   * The tail to open a version the arm holds no footer of (a MoR-family data file) with: under
   * {@code transaction}, read on the first use in the transaction, as an open reads it, and held
   * after that; null where the open reads it itself (under {@code open}, and for a file of at
   * most 1 MiB, which every open fetches whole).
   */
  byte[] tail(String location, long length) throws IOException {
    if (!transaction || length <= Scans.eagerBytes(ctx)) {
      return null;
    }
    String key = location + "@" + length;
    byte[] t = tails.get(key);
    if (t == null) {
      t = Scans.readTail(ctx, Reader.writer(ctx.layout()), location, length);
      tails.put(key, t);
      known.add(key);
    }
    return t;
  }
}
