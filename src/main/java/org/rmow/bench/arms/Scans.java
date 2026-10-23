package org.rmow.bench.arms;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.parquet.format.FileMetaData;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.ParquetReads;
import org.rmow.bench.rmow.SplitFile;
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
  private static final ThreadMXBean MX = ManagementFactory.getThreadMXBean();
  private static final String SPEC_JSON = PartitionSpecParser.toJson(DataFileWriter.SPEC);

  private Scans() {}

  /** A data file description of one version of an object: its path and that version's length. */
  static DataFile version(String location, long length, long rows) {
    return version(location, length, rows, null);
  }

  /** The same, with the version's split offsets (null: none, as for a split file's objects). */
  static DataFile version(String location, long length, long rows, List<Long> splitOffsets) {
    return DataFiles.builder(DataFileWriter.SPEC)
        .withPath(location)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(length)
        .withRecordCount(rows)
        .withSplitOffsets(splitOffsets)
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
    scan(ctx, reader, df, deletes, projection, filter, tail, start, length, null, rows);
  }

  /** @param deleteCache the read's loaded deletes ({@link Reader#deleteCache}); null: none */
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
      Map<String, PositionDeleteIndex> deleteCache,
      Consumer<Record> rows)
      throws IOException {
    FileScanTask task =
        new BaseFileScanTask(
            df, deletes, SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
    LengthAwareDeleteFilter deleteFilter =
        new LengthAwareDeleteFilter(
            ctx.io(), task, Rows.SCHEMA, projection, reader.footerPrefetchBytes(), deleteCache);
    Schema required = deleteFilter.requiredSchema();
    Evaluator residual =
        filter == Expressions.alwaysTrue() ? null : new Evaluator(required.asStruct(), filter);
    InternalRecordWrapper wrapper = new InternalRecordWrapper(required.asStruct());
    // storage holds what is below a cached tail (for a split file's object, the tail is not in
    // storage at all, and the virtual file may be longer than the object)
    long stored = df.fileSizeInBytes() - (tail == null ? 0 : tail.length);
    VersionedInputFile in =
        new VersionedInputFile(
            ctx.io().newInputFile(df.location(), stored),
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
   * ({@code FileScanTask.split}: one per row group, on the file's split offsets), and the tasks
   * run on the read pool, or one after another without one. A {@code footerCache} reader reads
   * each file version's one footer once and shares it with the file's splits; a stock reader's
   * splits each open the file and read the footer, as Iceberg's readers do today.
   */
  static TableHash readAll(ArmContext ctx, Reader reader, List<ReadFile> files, Expression filter)
      throws IOException {
    List<Callable<TableHash>> tasks = new ArrayList<>();
    Map<String, PositionDeleteIndex> deletes = reader.deleteCache() ? new ConcurrentHashMap<>() : null;
    for (ReadFile f : files) {
      FileScanTask task =
          new BaseFileScanTask(
              f.file(), f.deletes(), SCHEMA_JSON, SPEC_JSON, ResidualEvaluator.unpartitioned(filter));
      List<FileScanTask> splits = new ArrayList<>();
      byte[] tail = null;
      if (reader.footerCache() && f.file().fileSizeInBytes() <= EAGER_BYTES) {
        // one fetch of the whole version (Iceberg reads files up to 1 MiB whole), not one per
        // split
        splits.add(task);
      } else {
        task.split(reader.splitTargetBytes()).forEach(splits::add);
        if (reader.footerCache() && splits.size() > 1) {
          // the version's one footer, read once and shared by every split of the file
          tail = readTail(ctx, reader, f.file().location(), f.file().fileSizeInBytes());
        }
      }
      byte[] footer = tail;
      for (FileScanTask split : splits) {
        long start = split.start();
        long length = split.length();
        tasks.add(
            () -> {
              TableHash h = new TableHash();
              scan(ctx, reader, f.file(), f.deletes(), Rows.SCHEMA, filter, footer, start, length,
                  deletes, r -> h.add(r, ctx.verifyRows()));
              return h;
            });
      }
    }
    return run(ctx, reader, tasks);
  }

  /**
   * Reads footer-only versions ({@link SplitFile}): each version's one footer, then every object
   * it names as a virtual Parquet file whose footer comes from memory ({@link SplitFile#views}),
   * so an object costs only its row groups' reads. A footer-cache reader reads the version's
   * footer once; a stock one reads it again for every object it opens, as stock readers re-read a
   * file's footer for every split. No stock reader reads these files at all: this is the reader
   * an engine would need.
   */
  static TableHash readSplit(
      ArmContext ctx, Reader reader, List<ReadFile> versions, Expression filter)
      throws IOException {
    List<Callable<TableHash>> tasks = new ArrayList<>();
    for (ReadFile f : versions) {
      String location = f.file().location();
      long length = f.file().fileSizeInBytes();
      byte[] tail = readFooterObject(ctx, reader, location, length);
      FileMetaData footer = FooterCodec.read(tail, 0, tail.length - 8);
      for (SplitFile.ObjectView v : SplitFile.views(location, footer)) {
        tasks.add(
            () -> {
              if (!reader.footerCache()) {
                readFooterObject(ctx, reader, location, length);
              }
              TableHash h = new TableHash();
              scan(ctx, reader, version(v.location(), v.length(), v.rows()), f.deletes(),
                  Rows.SCHEMA, filter, v.tail(), -1, -1, r -> h.add(r, ctx.verifyRows()));
              return h;
            });
      }
    }
    return run(ctx, reader, tasks);
  }

  /**
   * A footer object's footer, length and magic: fetched whole when it is at most 1 MiB, as
   * Iceberg fetches small files (one request); otherwise read as a footer.
   */
  static byte[] readFooterObject(ArmContext ctx, Reader reader, String location, long length)
      throws IOException {
    if (length > EAGER_BYTES) {
      return readTail(ctx, reader, location, length);
    }
    byte[] all = new byte[(int) length];
    try (SeekableInputStream in =
        new VersionedInputFile(ctx.io().newInputFile(location, length), length).newStream()) {
      ((RangeReadable) in).readFully(0, all, 0, all.length);
    }
    return Arrays.copyOfRange(all, FooterCodec.MAGIC.length, all.length);
  }

  /**
   * Runs a read's tasks on the reader's pool, or one after another without one. When the trace
   * logs requests, each task's requests carry its number, and its end its thread CPU.
   */
  private static TableHash run(ArmContext ctx, Reader reader, List<Callable<TableHash>> tasks)
      throws IOException {
    IoTrace trace = ctx.io().trace();
    if (trace.logging()) {
      boolean inline = reader.pool() == null;
      int group = trace.nextTasks(tasks.size());
      List<Callable<TableHash>> tagged = new ArrayList<>(tasks.size());
      for (int i = 0; i < tasks.size(); i++) {
        int id = group + i;
        Callable<TableHash> t = tasks.get(i);
        tagged.add(
            () -> {
              IoTrace.enterTask(id);
              long cpu0 = MX.getCurrentThreadCpuTime();
              try {
                return t.call();
              } finally {
                trace.taskDone(id, group, MX.getCurrentThreadCpuTime() - cpu0, inline);
                IoTrace.exitTask();
              }
            });
      }
      tasks = tagged;
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
