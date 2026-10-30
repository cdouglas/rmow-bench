package org.rmow.bench.arms;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.data.Record;
import org.rmow.bench.io.Spans;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * A writer's rows, made from {id, version} pairs as the encoder takes them: a row is a function of
 * its id, event time and version ({@link Rows#record}), so a writer keeps the pairs, not the rows.
 * They are made a chunk at a time in a {@link Spans.Kind#ROWS} span, so the work stays out of the
 * encoder's span it runs in.
 */
final class MadeRows implements Iterable<Record> {

  private static final int CHUNK = 4096;

  private final List<long[]> rows; // {id, version}, in the order written
  private final OpLog log;

  MadeRows(List<long[]> rows, OpLog log) {
    this.rows = rows;
    this.log = log;
  }

  @Override
  public Iterator<Record> iterator() {
    return new Iterator<>() {
      private int next = 0; // rows handed out
      private Record[] chunk = new Record[0];
      private int at = 0;

      @Override
      public boolean hasNext() {
        return next < rows.size();
      }

      @Override
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        if (at == chunk.length) {
          chunk = new Record[Math.min(CHUNK, rows.size() - next)];
          at = 0;
          try (Spans.Span s = Spans.open(Spans.Kind.ROWS)) {
            for (int i = 0; i < chunk.length; i++) {
              long[] r = rows.get(next + i);
              chunk[i] = Rows.record(r[0], log.ts(r[0]), (int) r[1]);
            }
          }
        }
        next++;
        return chunk[at++];
      }
    };
  }
}
