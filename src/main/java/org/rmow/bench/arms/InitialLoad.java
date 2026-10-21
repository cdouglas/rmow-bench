package org.rmow.bench.arms;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.Parquet;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * The initial table, written once with the shared writer settings and copied byte for byte to
 * every arm: an ordinary Parquet file is already the first version of an appendable one, so every
 * arm starts from identical bytes.
 *
 * <p>Rows are written in id order (event time grows with the id, up to the load's jitter), so
 * file {@code k} holds ids {@code [firstId, firstId + rows)} at positions {@code id - firstId}.
 */
public final class InitialLoad {

  public record InitialFile(String location, long length, long firstId, long rows, DataFile file) {}

  private static final int SIZE_CHECK_ROWS = 1024;

  private InitialLoad() {}

  public static List<InitialFile> write(FileIO io, String prefix, OpLog log, LayoutSpec layout)
      throws IOException {
    List<InitialFile> files = new ArrayList<>();
    long id = 0;
    while (id < log.initialRows()) {
      String location = String.format("%s/initial-%05d.parquet", prefix, files.size());
      long first = id;
      FileAppender<Record> appender =
          Parquet.write(io.newOutputFile(location))
              .schema(Rows.SCHEMA)
              .createWriterFunc(GenericParquetWriter::create)
              .setAll(layout.writerProperties())
              .overwrite()
              .build();
      try (FileAppender<Record> a = appender) {
        while (id < log.initialRows()) {
          a.add(Rows.record(id, log.ts(id), 0));
          id++;
          if ((id - first) % SIZE_CHECK_ROWS == 0 && a.length() >= layout.targetFileBytes) {
            break;
          }
        }
      }
      files.add(
          new InitialFile(
              location,
              appender.length(),
              first,
              id - first,
              DataFileWriter.describe(location, appender)));
    }
    return files;
  }

  /** Copies the initial files under {@code prefix} (setup I/O: untraced). */
  public static List<InitialFile> copy(FileIO io, List<InitialFile> files, String prefix)
      throws IOException {
    List<InitialFile> copies = new ArrayList<>(files.size());
    for (InitialFile f : files) {
      String name = f.location().substring(f.location().lastIndexOf('/') + 1);
      String location = prefix + "/" + name;
      byte[] bytes = new byte[(int) f.length()];
      try (SeekableInputStream in = io.newInputFile(f.location(), f.length()).newStream()) {
        IOUtil.readFully(in, bytes, 0, bytes.length);
      }
      try (OutputStream out = io.newOutputFile(location).createOrOverwrite()) {
        out.write(bytes);
      }
      copies.add(
          new InitialFile(
              location,
              f.length(),
              f.firstId(),
              f.rows(),
              DataFileWriter.relocate(f.file(), location)));
    }
    return copies;
  }
}
