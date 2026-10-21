package org.rmow.bench.arms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.ParquetReads;
import org.rmow.bench.rmow.VersionedInputFile;
import org.rmow.bench.workload.Rows;

/**
 * The one file-scan path every arm uses, as Iceberg's generic reader opens a scan task: a {@code
 * FileScanTask} for the data file and its delete files, Iceberg's delete filter (opening delete
 * files with their lengths), the {@link VersionedInputFile} view (the file's length, the optional
 * cached tail and footer prefetch), the stock Parquet reader, and the filter as a residual.
 */
final class Scans {

  private static final String SCHEMA_JSON = SchemaParser.toJson(Rows.SCHEMA);
  private static final String SPEC_JSON = PartitionSpecParser.toJson(DataFileWriter.SPEC);

  private Scans() {}

  /** A data file description of one version of an object: its path and that version's length. */
  static DataFile version(String location, long length, long rows) {
    return DataFiles.builder(DataFileWriter.SPEC)
        .withPath(location)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(length)
        .withRecordCount(rows)
        .build();
  }

  /**
   * Reads {@code df} with {@code deletes} applied and {@code filter} as a residual; rows have the
   * delete filter's required schema ({@code projection}, plus {@code _pos} if there are deletes).
   *
   * @param tail the file's cached tail, or null to read its footer from storage
   * @param start the split's start, or -1 for the whole file
   */
  static void scan(
      ArmContext ctx,
      Reader reader,
      DataFile df,
      DeleteFile[] deletes,
      Schema projection,
      Expression filter,
      byte[] tail,
      long start,
      long length,
      Consumer<Record> rows)
      throws IOException {
    FileScanTask task =
        new BaseFileScanTask(
            df, deletes, SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
    LengthAwareDeleteFilter deleteFilter =
        new LengthAwareDeleteFilter(
            ctx.io(), task, Rows.SCHEMA, projection, reader.footerPrefetchBytes());
    Schema required = deleteFilter.requiredSchema();
    Evaluator residual =
        filter == Expressions.alwaysTrue() ? null : new Evaluator(required.asStruct(), filter);
    InternalRecordWrapper wrapper = new InternalRecordWrapper(required.asStruct());
    VersionedInputFile in =
        new VersionedInputFile(
            ctx.io().newInputFile(df.location(), df.fileSizeInBytes()),
            df.fileSizeInBytes(),
            tail,
            reader.footerPrefetchBytes());
    try (CloseableIterable<Record> rs =
        deleteFilter.filter(ParquetReads.open(in, required, filter, start, length))) {
      for (Record r : rs) {
        if (residual == null || residual.eval(wrapper.wrap(r))) {
          rows.accept(r);
        }
      }
    }
  }


  /** Iceberg fetches Parquet files up to this size whole ({@code Parquet.ReadBuilder}). */
  private static final long EAGER_BYTES = 1L << 20;

  /**
   * A file's tail, read the way parquet-java reads a footer: the last 8 bytes, then the footer
   * (one request, if the footer-prefetch knob covers it).
   */
  static byte[] readTail(ArmContext ctx, Reader reader, String location, long length)
      throws IOException {
    try (SeekableInputStream in =
        new VersionedInputFile(
                ctx.io().newInputFile(location, length),
                length,
                null,
                reader.footerPrefetchBytes())
            .newStream()) {
      byte[] probe = new byte[8];
      in.seek(length - 8);
      IOUtil.readFully(in, probe, 0, 8);
      int footerLength = ByteBuffer.wrap(probe, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      byte[] tail = new byte[footerLength + 8];
      in.seek(length - tail.length);
      IOUtil.readFully(in, tail, 0, tail.length);
      return tail;
    }
  }

  /** A file a read opens, with the delete files that apply to it. */
  record ReadFile(DataFile file, DeleteFile[] deletes) {}

  /**
   * Reads the files as an engine would: each is planned into split tasks as Iceberg plans them
   * ({@code FileScanTask.split} on the file's split offsets, at the configured target size),
   * and the tasks run on the read pool, or one after another without one. Every task opens its
   * file and reads its footer, as Iceberg's readers do.
   */
  static TableHash readAll(ArmContext ctx, Reader reader, List<ReadFile> files, Expression filter)
      throws IOException {
    List<Callable<TableHash>> tasks = new ArrayList<>();
    for (ReadFile f : files) {
      FileScanTask task =
          new BaseFileScanTask(
              f.file(), f.deletes(), SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
      List<FileScanTask> splits = new ArrayList<>();
      task.split(reader.splitTargetBytes()).forEach(splits::add);
      // with a footer cache, the file's footer is fetched once for all its splits; files up to
      // 1 MiB are fetched whole by each split anyway (Iceberg's eager read), so they skip it
      byte[] tail =
          reader.footerCache() && splits.size() > 1 && f.file().fileSizeInBytes() > EAGER_BYTES
              ? readTail(ctx, reader, f.file().location(), f.file().fileSizeInBytes())
              : null;
      for (FileScanTask split : splits) {
        long start = split.start();
        long length = split.length();
        tasks.add(
            () -> {
              TableHash h = new TableHash();
              scan(ctx, reader, f.file(), f.deletes(), Rows.SCHEMA, filter, tail, start, length,
                  r -> h.add(r, ctx.verifyRows()));
              return h;
            });
      }
    }
    TableHash hash = new TableHash();
    try {
      if (reader.pool() == null) {
        for (Callable<TableHash> t : tasks) {
          hash.addAll(t.call());
        }
      } else {
        for (Future<TableHash> f : reader.pool().invokeAll(tasks)) {
          hash.addAll(f.get());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    return hash;
  }
}
