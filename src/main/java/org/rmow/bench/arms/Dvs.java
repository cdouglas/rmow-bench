package org.rmow.bench.arms;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.DeleteWriteResult;

/** Writes one batch's deletion vectors, as Spark's DV writer does, for every arm that uses DVs. */
final class Dvs {

  private Dvs() {}

  /**
   * Writes a Puffin file at {@code location} with one DV per data file in {@code positions}. A
   * data file whose {@code previous} DV is not null gets a DV merged with it (read from storage);
   * the caller replaces the previous DV with the result's.
   */
  static DeleteWriteResult write(
      ArmContext ctx,
      String location,
      Map<String, List<Long>> positions,
      Function<String, DeleteFile> previous)
      throws IOException {
    DeleteLoader loader = new BaseDeleteLoader(df -> ctx.io().newInputFile(df));
    @SuppressWarnings("deprecation") // the non-deprecated constructor needs a Table
    BaseDVFileWriter writer =
        new BaseDVFileWriter(
            () -> ctx.io().newOutputFile(location),
            path -> {
              DeleteFile dv = previous.apply(path);
              return dv == null ? null : loader.loadPositionDeletes(List.of(dv), path);
            });
    try (BaseDVFileWriter w = writer) {
      for (Map.Entry<String, List<Long>> e : positions.entrySet()) {
        for (long pos : e.getValue()) {
          w.delete(e.getKey(), pos, DataFileWriter.SPEC, null);
        }
      }
    }
    return writer.result();
  }
}
