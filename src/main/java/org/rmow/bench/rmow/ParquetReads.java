package org.rmow.bench.rmow;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;

/**
 * The one Parquet read path every arm uses: Iceberg's {@code Parquet.read} with the generic record
 * reader, as {@code GenericReader} opens data files. The filter only skips row groups (by stats,
 * dictionaries and bloom filters); callers evaluate it on rows, as a residual.
 */
public final class ParquetReads {

  private ParquetReads() {}

  public static CloseableIterable<Record> open(
      InputFile in, Schema projection, Expression filter, long start, long length) {
    Parquet.ReadBuilder b =
        Parquet.read(in)
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema, Map.of()));
    if (filter != null && filter != Expressions.alwaysTrue()) {
      b = b.filter(filter);
    }
    if (start >= 0) {
      b = b.split(start, length);
    }
    return b.build();
  }

  public static CloseableIterable<Record> open(InputFile in, Schema projection, Expression filter) {
    return open(in, projection, filter, -1, -1);
  }
}
