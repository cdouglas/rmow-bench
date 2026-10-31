package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Files;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.rmow.ParquetBatches;
import org.rmow.bench.workload.Rows;

/** A batch's live rows: its deletes by position, and its residual compiled or evaluated. */
class RowBatchTest {

  private static final int ROWS = 12_000; // three batches of one row group

  @TempDir Path dir;

  @Test
  void residualsKeepWhatIcebergsEvaluatorKeeps() throws IOException {
    List<Record> rows = new ArrayList<>();
    for (long id = 0; id < ROWS; id++) {
      rows.add(Rows.record(id, 1_000_000 - id * 3, (int) (id % 3)));
    }
    File f = write(rows);
    List<Expression> filters =
        List.of(
            Expressions.and(
                Expressions.greaterThanOrEqual("id", 100L), Expressions.lessThanOrEqual("id", 7000L)),
            Expressions.greaterThanOrEqual("ts", 990_000L),
            Expressions.or(Expressions.lessThan("id", 50L), Expressions.greaterThan("id", 11_000L)),
            Expressions.not(Expressions.equal("version", 0)),
            Expressions.notEqual("version", 2),
            // a string predicate is not compiled: Iceberg's evaluator decides each row
            Expressions.and(
                Expressions.lessThan("id", 5000L), Expressions.startsWith("s1", "category-00")));
    for (Expression filter : filters) {
      Evaluator want = new Evaluator(Rows.SCHEMA.asStruct(), filter);
      List<Long> expected = new ArrayList<>();
      for (Record r : rows) {
        if (want.eval(withStrings(r))) {
          expected.add((Long) r.get(Rows.ID));
        }
      }
      assertThat(read(f, filter, null)).as(filter.toString()).isEqualTo(expected);
    }
  }

  @Test
  void deletesDropRowsByTheirPositionInTheFile() throws IOException {
    List<Record> rows = new ArrayList<>();
    for (long id = 0; id < ROWS; id++) {
      rows.add(Rows.record(id, id, 0));
    }
    File f = write(rows);
    Set<Long> positions = new HashSet<>();
    List<Long> expected = new ArrayList<>();
    for (long pos = 0; pos < ROWS; pos++) {
      if (pos % 7 == 0 || (pos >= 4990 && pos < 5010)) { // across a batch boundary
        positions.add(pos);
      } else {
        expected.add(pos); // id = position here
      }
    }
    PositionDeleteIndex deleted =
        new PositionDeleteIndex() {
          @Override
          public void delete(long position) {
            positions.add(position);
          }

          @Override
          public void delete(long posStart, long posEnd) {
            for (long p = posStart; p < posEnd; p++) {
              positions.add(p);
            }
          }

          @Override
          public boolean isDeleted(long position) {
            return positions.contains(position);
          }

          @Override
          public boolean isEmpty() {
            return positions.isEmpty();
          }
        };
    assertThat(read(f, Expressions.alwaysTrue(), deleted)).isEqualTo(expected);
  }

  /** A generated row with its strings as {@code String}s, as Iceberg's evaluator casts them. */
  private static Record withStrings(Record r) {
    Record copy = r.copy();
    for (int c = 0; c < copy.size(); c++) {
      if (copy.get(c) instanceof CharSequence cs) {
        copy.set(c, cs.toString());
      }
    }
    return copy;
  }

  private File write(List<Record> rows) throws IOException {
    File f = dir.resolve("data-" + rows.size() + "-" + System.nanoTime() + ".parquet").toFile();
    try (FileAppender<Record> a =
        Parquet.write(Files.localOutput(f))
            .schema(Rows.SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      a.addAll(rows);
    }
    return f;
  }

  private static List<Long> read(File f, Expression filter, PositionDeleteIndex deleted)
      throws IOException {
    RowBatch.Columns columns = new RowBatch.Columns(Rows.SCHEMA);
    RowBatch.Residual residual = RowBatch.Residual.of(filter, columns);
    List<Long> ids = Lists.newArrayList();
    try (CloseableIterable<ParquetBatches.Batch> batches =
        ParquetBatches.open(Files.localInput(f), columns.data, Expressions.alwaysTrue())) {
      for (ParquetBatches.Batch b : batches) {
        RowBatch live = RowBatch.select(b, columns, deleted, residual);
        for (int k = 0; k < live.size(); k++) {
          ids.add(live.id(k));
        }
      }
    }
    return ids;
  }
}
