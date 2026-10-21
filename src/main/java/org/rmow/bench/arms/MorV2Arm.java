package org.rmow.bench.arms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.ContentFileUtil;
import org.rmow.bench.rmow.VersionedInputFile;

/**
 * Merge-on-read on a v2 table: each batch's deletes are Parquet position-delete files, sorted by
 * (file, position), written as Spark's position-delta writer does.
 *
 * <ul>
 *   <li>{@code PARTITION} granularity (Iceberg's default): one delete file per batch for every
 *       data file it deletes from (this table has one partition). Earlier delete files are kept,
 *       so every data file is read with every partition delete file written since it.
 *   <li>{@code FILE} granularity: one delete file per data file a batch deletes from, merged with
 *       that data file's previous file-scoped deletes (read from storage), which it replaces; so
 *       a data file has at most one live delete file, as with DVs.
 * </ul>
 */
public final class MorV2Arm extends MorArm {

  private final DeleteGranularity granularity;

  public MorV2Arm(ArmContext ctx, DeleteGranularity granularity) {
    super(ctx);
    this.granularity = granularity;
  }

  @Override
  protected void writeDeletes(int batch, long seq, Map<Integer, List<Integer>> deleted)
      throws IOException {
    int[] next = {0};
    Supplier<FileWriter<PositionDelete<Record>, DeleteWriteResult>> writers =
        () -> {
          String location =
              String.format("%s/deletes/pos-%05d-%04d.parquet", ctx.root(), batch, next[0]++);
          try {
            // full file_path bounds, as Spark's writers keep them: a delete file whose bounds
            // match is file-scoped (Iceberg derives its referenced data file from them)
            return Parquet.writeDeletes(ctx.io().newOutputFile(location))
                .setAll(ctx.layout().writerProperties())
                .metricsConfig(MetricsConfig.forPositionDelete())
                .withSpec(DataFileWriter.SPEC)
                .overwrite()
                .buildPositionWriter();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
    DeleteLoader loader =
        new BaseDeleteLoader(
            df -> new VersionedInputFile(ctx.io().newInputFile(df), df.fileSizeInBytes()));
    SortingPositionOnlyDeleteWriter<Record> writer =
        granularity == DeleteGranularity.FILE
            ? new SortingPositionOnlyDeleteWriter<>(
                writers, granularity, path -> previous(loader, path.toString()))
            : new SortingPositionOnlyDeleteWriter<>(writers, granularity);
    PositionDelete<Record> delete = PositionDelete.create();
    try (SortingPositionOnlyDeleteWriter<Record> w = writer) {
      for (Map.Entry<Integer, List<Integer>> e : deleted.entrySet()) {
        String path = files.get(e.getKey()).location();
        for (int pos : e.getValue()) {
          w.write(delete.set(path, pos));
        }
      }
    }
    Set<String> rewritten = new HashSet<>();
    for (DeleteFile f : writer.result().rewrittenDeleteFiles()) {
      rewritten.add(f.location());
    }
    deletes.removeIf(d -> rewritten.contains(d.file().location()));
    for (DeleteFile f : writer.result().deleteFiles()) {
      deletes.add(new Delete(f, seq));
      deleteObjects.put(f.location(), f.fileSizeInBytes());
    }
  }

  /** A data file's live file-scoped deletes, loaded from storage; null if it has none. */
  private PositionDeleteIndex previous(DeleteLoader loader, String path) {
    List<DeleteFile> previous = new ArrayList<>();
    for (Delete d : deletes) {
      if (path.equals(ContentFileUtil.referencedDataFileLocation(d.file()))) {
        previous.add(d.file());
      }
    }
    return previous.isEmpty() ? null : loader.loadPositionDeletes(previous, path);
  }

  @Override
  protected long liveBytes(DeleteFile d) {
    return d.fileSizeInBytes();
  }
}
