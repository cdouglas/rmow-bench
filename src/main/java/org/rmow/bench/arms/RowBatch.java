package org.rmow.bench.arms;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.rmow.bench.rmow.ParquetBatches;
import org.rmow.bench.workload.Rows;

/**
 * The live rows of one decoded batch ({@link ParquetBatches.Batch}): those its file's deletes do
 * not cover and the scan's residual keeps, as Spark's batch reader selects them (a row's position
 * against the file's position deletes, then the residual on the row). Rows are read by their index
 * among the live ones. A batch is valid only until its scan decodes the next.
 */
final class RowBatch {

  private final ParquetBatches.Batch batch;
  private final Columns columns;
  private final int[] live;
  private final int size;

  private RowBatch(ParquetBatches.Batch batch, Columns columns, int[] live, int size) {
    this.batch = batch;
    this.columns = columns;
    this.live = live;
    this.size = size;
  }

  /** The rows of {@code batch} that {@code deleted} (null: none) and {@code residual} keep. */
  static RowBatch select(
      ParquetBatches.Batch batch, Columns columns, PositionDeleteIndex deleted, Residual residual) {
    int[] live = new int[batch.rows()];
    int n = 0;
    Residual.Test keep = residual == null ? null : residual.on(batch, columns);
    for (int i = 0; i < batch.rows(); i++) {
      if (deleted != null && deleted.isDeleted(batch.position(i))) {
        continue;
      }
      if (keep != null && !keep.test(batch, i)) {
        continue;
      }
      live[n++] = i;
    }
    return new RowBatch(batch, columns, live, n);
  }

  /**
   * A scan's residual on its rows. Compiled to comparisons on the batch's columns where it can be,
   * as Spark compiles a filter to code over its batches: {@code and}, {@code or}, {@code not}, and
   * an int or long column (or {@code _pos}) compared with a literal, which covers every filter the
   * arms use. Otherwise Iceberg's evaluator on each row, as a struct of boxed values.
   */
  static final class Residual {
    private final Test compiled;
    private final Expression unbound;

    private Residual(Test compiled, Expression unbound) {
      this.compiled = compiled;
      this.unbound = unbound;
    }

    /** {@code filter} as a residual on {@code columns}' rows; null if it keeps every row. */
    static Residual of(Expression filter, Columns columns) {
      if (filter == null || filter == Expressions.alwaysTrue()) {
        return null;
      }
      Test compiled = compile(Binder.bind(columns.projection.asStruct(), filter, true), columns);
      return new Residual(compiled, compiled == null ? filter : null);
    }

    /** The test to run on one batch's rows. */
    Test on(ParquetBatches.Batch batch, Columns columns) {
      if (compiled != null) {
        return compiled;
      }
      Evaluator evaluator = new Evaluator(columns.projection.asStruct(), unbound);
      View view = new View(batch, columns);
      return (b, row) -> evaluator.eval(view.at(row));
    }

    interface Test {
      boolean test(ParquetBatches.Batch batch, int row);
    }

    private interface LongValue {
      long get(ParquetBatches.Batch batch, int row);
    }

    private static Test compile(Expression e, Columns columns) {
      switch (e.op()) {
        case TRUE:
          return (b, row) -> true;
        case FALSE:
          return (b, row) -> false;
        case AND:
          {
            Test left = compile(((And) e).left(), columns);
            Test right = compile(((And) e).right(), columns);
            return left == null || right == null
                ? null
                : (b, row) -> left.test(b, row) && right.test(b, row);
          }
        case OR:
          {
            Test left = compile(((Or) e).left(), columns);
            Test right = compile(((Or) e).right(), columns);
            return left == null || right == null
                ? null
                : (b, row) -> left.test(b, row) || right.test(b, row);
          }
        case NOT:
          {
            Test child = compile(((Not) e).child(), columns);
            return child == null ? null : (b, row) -> !child.test(b, row);
          }
        default:
          return comparison(e, columns);
      }
    }

    private static Test comparison(Expression e, Columns columns) {
      if (!(e instanceof BoundPredicate<?> p)
          || !p.isLiteralPredicate()
          || !(p.term() instanceof BoundReference<?> ref)) {
        return null;
      }
      int f = -1;
      for (int i = 0; i < columns.fields.size(); i++) {
        if (columns.fields.get(i).fieldId() == ref.fieldId()) {
          f = i;
        }
      }
      if (f < 0) {
        return null;
      }
      int c = columns.dataIndex[f];
      Type.TypeID type = columns.fields.get(f).type().typeId();
      LongValue value;
      if (c < 0) {
        value = (b, row) -> b.position(row);
      } else if (type == Type.TypeID.LONG) {
        value = (b, row) -> b.getLong(c, row);
      } else if (type == Type.TypeID.INTEGER) {
        value = (b, row) -> b.getInt(c, row);
      } else {
        return null;
      }
      Object literal = p.asLiteralPredicate().literal().value();
      if (!(literal instanceof Long || literal instanceof Integer)) {
        return null;
      }
      long v = ((Number) literal).longValue();
      return switch (p.op()) {
        case LT -> (b, row) -> value.get(b, row) < v;
        case LT_EQ -> (b, row) -> value.get(b, row) <= v;
        case GT -> (b, row) -> value.get(b, row) > v;
        case GT_EQ -> (b, row) -> value.get(b, row) >= v;
        case EQ -> (b, row) -> value.get(b, row) == v;
        case NOT_EQ -> (b, row) -> value.get(b, row) != v;
        default -> null;
      };
    }
  }

  int size() {
    return size;
  }

  long id(int k) {
    return batch.getLong(columns.id, live[k]);
  }

  long ts(int k) {
    return batch.getLong(columns.ts, live[k]);
  }

  int version(int k) {
    return batch.getInt(columns.version, live[k]);
  }

  /**
   * Adds each live row to {@code out} as {id, version}, as writers keep the rows they read back: a
   * row is a function of those ({@link MadeRows} makes it again for the encoder).
   */
  void idVersions(List<long[]> out) {
    for (int k = 0; k < size; k++) {
      out.add(new long[] {id(k), version(k)});
    }
  }

  /** Live row {@code k}'s position in its file (Iceberg's {@code _pos}). */
  long position(int k) {
    return batch.position(live[k]);
  }

  /** Live row {@code k} as a record of the projection, for checks that read every column. */
  Record record(int k) {
    GenericRecord r = GenericRecord.create(columns.projection);
    View view = new View(batch, columns).at(live[k]);
    for (int f = 0; f < columns.fields.size(); f++) {
      r.set(f, view.get(f, Object.class));
    }
    return r;
  }

  /**
   * A scan's projection as its batches hold it: the data columns read from the file, in order,
   * and the position of the columns callers read by name ({@code id}, {@code ts}, {@code version};
   * -1 where the projection has none). {@code _pos} comes from the batch, not the file.
   */
  static final class Columns {
    final Schema projection;
    final Schema data;
    final List<Types.NestedField> fields;
    final int[] dataIndex; // per projection field: its column in the batch, or -1 for _pos
    final int id;
    final int ts;
    final int version;

    Columns(Schema projection) {
      this.projection = projection;
      this.fields = projection.columns();
      List<Types.NestedField> data = new ArrayList<>();
      this.dataIndex = new int[fields.size()];
      for (int f = 0; f < fields.size(); f++) {
        Types.NestedField field = fields.get(f);
        if (field.fieldId() == MetadataColumns.ROW_POSITION.fieldId()) {
          dataIndex[f] = -1;
        } else if (MetadataColumns.isMetadataColumn(field.fieldId())) {
          throw new IllegalArgumentException("A scan reads no metadata column but _pos: " + field);
        } else {
          dataIndex[f] = data.size();
          data.add(field);
        }
      }
      this.data = new Schema(data);
      this.id = column(Rows.ID);
      this.ts = column(Rows.TS);
      this.version = column(Rows.VERSION);
    }

    private int column(int tableColumn) {
      Types.NestedField want = Rows.SCHEMA.columns().get(tableColumn);
      for (int f = 0; f < fields.size(); f++) {
        if (fields.get(f).fieldId() == want.fieldId()) {
          return dataIndex[f];
        }
      }
      return -1;
    }
  }

  /** One row of a batch as a {@link StructLike} of the projection, for the residual. */
  private static final class View implements StructLike {
    private final ParquetBatches.Batch batch;
    private final Columns columns;
    private int row;

    View(ParquetBatches.Batch batch, Columns columns) {
      this.batch = batch;
      this.columns = columns;
    }

    View at(int row) {
      this.row = row;
      return this;
    }

    @Override
    public int size() {
      return columns.fields.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      int c = columns.dataIndex[pos];
      return javaClass.cast(c < 0 ? batch.position(row) : batch.get(c, row));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("A batch's rows are read-only");
    }
  }
}
