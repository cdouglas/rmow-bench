package org.rmow.bench.rmow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.vectorized.ArrowVectorAccessor;
import org.apache.iceberg.arrow.vectorized.BaseBatchReader;
import org.apache.iceberg.arrow.vectorized.GenericArrowVectorAccessorFactory;
import org.apache.iceberg.arrow.vectorized.VectorizedReaderBuilder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.types.Type;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * The one Parquet read path every arm uses: Iceberg's vectorized reader (iceberg-arrow), as Spark
 * reads an Iceberg table. A file decodes into batches of Arrow vectors, one per projected column,
 * of at most {@link #BATCH_ROWS} rows of one row group, and values are read through the accessors
 * Spark's Iceberg integration reads them with ({@code GenericArrowVectorAccessorFactory}), which
 * decode a dictionary-encoded vector's values as they are read; a string becomes a {@code String}
 * only when one is asked for. The filter only skips row groups (by stats, dictionaries and bloom
 * filters); callers evaluate it on rows, as a residual. Deletes are the caller's too: a batch
 * knows its rows' positions in the file.
 */
public final class ParquetBatches {

  /** Rows per batch: Iceberg's default ({@code read.parquet.vectorization.batch-size}). */
  public static final int BATCH_ROWS = 5000;

  static {
    // as Iceberg's Spark reader sets them when it loads (VectorizedSparkParquetReaders), unless
    // they are set: no bounds check and no null check on every value Arrow gets
    setIfAbsent("arrow.enable_unsafe_memory_access", "true");
    setIfAbsent("arrow.enable_null_check_for_get", "false");
  }

  private static void setIfAbsent(String key, String value) {
    if (System.getProperty(key) == null) {
      System.setProperty(key, value);
    }
  }

  private ParquetBatches() {}

  /**
   * Opens {@code in}'s rows of {@code projection} (data columns only) as batches, the split
   * {@code [start, start + length)} or the whole file ({@code start} -1). A batch is valid until
   * the next is read: the reader reuses its vectors, as Spark's does.
   */
  public static CloseableIterable<Batch> open(
      InputFile in, Schema projection, Expression filter, long start, long length) {
    Parquet.ReadBuilder b =
        Parquet.read(in)
            .project(projection)
            .createBatchedReaderFunc(
                fileSchema ->
                    TypeWithSchemaVisitor.visit(
                        projection.asStruct(),
                        fileSchema,
                        new VectorizedReaderBuilder(
                            projection, fileSchema, false, Map.of(), BatchReader::new)))
            .recordsPerBatch(BATCH_ROWS)
            .reuseContainers();
    if (filter != null && filter != Expressions.alwaysTrue()) {
      b = b.filter(filter);
    }
    if (start >= 0) {
      b = b.split(start, length);
    }
    return b.build();
  }

  public static CloseableIterable<Batch> open(InputFile in, Schema projection, Expression filter) {
    return open(in, projection, filter, -1, -1);
  }

  /** One decoded batch: a column per projected field, its rows at consecutive file positions. */
  public static final class Batch {
    private final ArrowVectorAccessor<BigDecimal, String, Object, AutoCloseable>[] columns;
    private final Type.TypeID[] types;
    private final int rows;
    private final long first;

    private Batch(
        ArrowVectorAccessor<BigDecimal, String, Object, AutoCloseable>[] columns,
        Type.TypeID[] types,
        int rows,
        long first) {
      this.columns = columns;
      this.types = types;
      this.rows = rows;
      this.first = first;
    }

    public int rows() {
      return rows;
    }

    /** Row {@code row}'s position in the file (Iceberg's {@code _pos}). */
    public long position(int row) {
      return first + row;
    }

    public long getLong(int column, int row) {
      return columns[column].getLong(row);
    }

    public int getInt(int column, int row) {
      return columns[column].getInt(row);
    }

    public double getDouble(int column, int row) {
      return columns[column].getDouble(row);
    }

    public String getString(int column, int row) {
      return columns[column].getUTF8String(row);
    }

    /** A value as Iceberg's generic records hold it (boxed; a string as {@code String}). */
    public Object get(int column, int row) {
      return switch (types[column]) {
        case LONG -> getLong(column, row);
        case INTEGER -> getInt(column, row);
        case DOUBLE -> getDouble(column, row);
        case STRING -> getString(column, row);
        default ->
            throw new UnsupportedOperationException(
                "Not a type of the benchmark's schema: " + types[column]);
      };
    }
  }

  /**
   * Iceberg's Arrow batch reader ({@code ArrowBatchReader}), but for the position of each batch's
   * first row: its row group's (parquet-java's row index offset, as Spark's batch reader takes it
   * to apply deletes) plus the rows read before it in the row group.
   */
  private static final class BatchReader extends BaseBatchReader<Batch> {
    private static final Accessors ACCESSORS = new Accessors();

    private long rowGroupStart = 0;
    private long readInRowGroup = 0;

    BatchReader(List<VectorizedReader<?>> readers) {
      super(readers);
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      super.setRowGroupInfo(pages, metadata);
      this.rowGroupStart =
          pages
              .getRowIndexOffset()
              .orElseThrow(() -> new IllegalStateException("A row group without its first row"));
      this.readInRowGroup = 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Batch read(Batch reuse, int rows) {
      if (reuse == null) {
        closeVectors();
      }
      ArrowVectorAccessor<BigDecimal, String, Object, AutoCloseable>[] columns =
          new ArrowVectorAccessor[readers.length];
      Type.TypeID[] types = new Type.TypeID[readers.length];
      for (int i = 0; i < readers.length; i++) {
        vectorHolders[i] = readers[i].read(vectorHolders[i], rows);
        if (vectorHolders[i].numValues() != rows) {
          throw new IllegalStateException(
              "Read " + vectorHolders[i].numValues() + " values of a column, expected " + rows);
        }
        columns[i] = ACCESSORS.getVectorAccessor(vectorHolders[i]);
        types[i] = vectorHolders[i].icebergType().typeId();
      }
      Batch b = new Batch(columns, types, rows, rowGroupStart + readInRowGroup);
      readInRowGroup += rows;
      return b;
    }
  }

  /** Accessors for flat schemas: strings as {@code String}, decimals as {@code BigDecimal}. */
  private static final class Accessors
      extends GenericArrowVectorAccessorFactory<BigDecimal, String, Object, AutoCloseable> {

    Accessors() {
      super(Decimals::new, Strings::new, StructChildren::new, Arrays::new);
    }

    private static final class Strings implements StringFactory<String> {
      @Override
      public Class<String> getGenericClass() {
        return String.class;
      }

      @Override
      public String ofRow(VarCharVector vector, int rowId) {
        return new String(vector.get(rowId), StandardCharsets.UTF_8);
      }

      @Override
      public String ofBytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
      }

      @Override
      public String ofByteBuffer(ByteBuffer buffer) {
        return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
      }
    }

    private static final class Decimals implements DecimalFactory<BigDecimal> {
      @Override
      public Class<BigDecimal> getGenericClass() {
        return BigDecimal.class;
      }

      @Override
      public BigDecimal ofLong(long value, int precision, int scale) {
        return BigDecimal.valueOf(value, scale);
      }

      @Override
      public BigDecimal ofBigDecimal(BigDecimal value, int precision, int scale) {
        return value.setScale(scale);
      }
    }

    private static final class StructChildren implements StructChildFactory<AutoCloseable> {
      @Override
      public Class<AutoCloseable> getGenericClass() {
        return AutoCloseable.class;
      }

      @Override
      public AutoCloseable of(ValueVector childVector) {
        throw new UnsupportedOperationException("The benchmark's schema has no structs");
      }
    }

    private static final class Arrays implements ArrayFactory<AutoCloseable, Object> {
      @Override
      public AutoCloseable ofChild(ValueVector childVector) {
        throw new UnsupportedOperationException("The benchmark's schema has no lists");
      }

      @Override
      public Object ofRow(ValueVector vector, AutoCloseable childData, int rowId) {
        throw new UnsupportedOperationException("The benchmark's schema has no lists");
      }
    }
  }
}
