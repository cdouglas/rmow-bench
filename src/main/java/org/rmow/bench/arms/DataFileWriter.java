package org.rmow.bench.arms;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.rmow.bench.io.Spans;
import org.rmow.bench.workload.Rows;

/**
 * Writes an ordinary Iceberg data file with the shared writer settings, as {@code
 * GenericFileWriterFactory} would, and describes it as a {@link DataFile} (metrics for pruning,
 * split offsets).
 */
public final class DataFileWriter {

  public static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private DataFileWriter() {}

  /** Writes the file: an {@link Spans.Kind#ENCODE} span, with its PUT a request span. */
  public static DataFile write(
      FileIO io, String location, Iterable<Record> rows, Map<String, String> writerProperties)
      throws IOException {
    try (Spans.Span s = Spans.open(Spans.Kind.ENCODE)) {
      return encode(io, location, rows, writerProperties);
    }
  }

  private static DataFile encode(
      FileIO io, String location, Iterable<Record> rows, Map<String, String> writerProperties)
      throws IOException {
    FileAppender<Record> appender =
        Parquet.write(io.newOutputFile(location))
            .schema(Rows.SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .setAll(writerProperties)
            .overwrite() // as Iceberg's writer factories do: no existence check
            .build();
    try (FileAppender<Record> a = appender) {
      a.addAll(rows);
    }
    return describe(location, appender);
  }

  /** Describes a closed appender's file. */
  public static DataFile describe(String location, FileAppender<?> appender) {
    return DataFiles.builder(SPEC)
        .withPath(location)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(appender.length())
        .withMetrics(appender.metrics())
        .withSplitOffsets(appender.splitOffsets())
        .build();
  }

  /** The same file's description at another location (the arms' copies of the initial load). */
  public static DataFile relocate(DataFile file, String location) {
    return DataFiles.builder(SPEC).copy(file).withPath(location).build();
  }
}
