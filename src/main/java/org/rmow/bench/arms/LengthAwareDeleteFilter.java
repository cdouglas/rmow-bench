package org.rmow.bench.arms;

import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.StructLikeSet;
import org.rmow.bench.io.Spans;
import org.rmow.bench.rmow.VersionedInputFile;

/**
 * Iceberg's generic delete filter, opening delete files with their known length. The stock
 * filter opens them by location alone, so each one costs a HEAD on an object store before its
 * GET, a cost Spark's readers do not pay; without this the benchmark would charge it to MoR alone
 * (I/O parity rule 2). Delete files are read through the same {@link VersionedInputFile} view as
 * data files, so the footer-prefetch knob applies to them too.
 *
 * <p>Given a cache (a reader model's {@code deleteCache}), position deletes are loaded once per
 * read for each data file and shared by its splits; without one, every split loads its file's
 * deletes again, as Iceberg's {@code BaseDeleteLoader} does for deletion vectors. Each load is a
 * {@link Spans.Kind#DELETES} span; applying them to rows is the scan's decoding.
 */
public final class LengthAwareDeleteFilter extends GenericDeleteFilter {

  private final FileIO io;
  private final int footerPrefetch;
  private final Map<String, PositionDeleteIndex> cache;

  public LengthAwareDeleteFilter(
      FileIO io, FileScanTask task, Schema tableSchema, Schema requestedSchema, int footerPrefetch) {
    this(io, task, tableSchema, requestedSchema, footerPrefetch, null);
  }

  /** @param cache one read's position deletes, by data file and delete files; null: none */
  public LengthAwareDeleteFilter(
      FileIO io,
      FileScanTask task,
      Schema tableSchema,
      Schema requestedSchema,
      int footerPrefetch,
      Map<String, PositionDeleteIndex> cache) {
    super(io, task, tableSchema, requestedSchema);
    this.io = io;
    this.footerPrefetch = footerPrefetch;
    this.cache = cache;
  }

  @Override
  protected DeleteLoader newDeleteLoader() {
    DeleteLoader loader = super.newDeleteLoader();
    return new DeleteLoader() {
      @Override
      public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> files, Schema projection) {
        try (Spans.Span s = Spans.open(Spans.Kind.DELETES)) {
          return loader.loadEqualityDeletes(files, projection);
        }
      }

      @Override
      public PositionDeleteIndex loadPositionDeletes(
          Iterable<DeleteFile> files, CharSequence dataFile) {
        try (Spans.Span s = Spans.open(Spans.Kind.DELETES)) {
          return load(files, dataFile);
        }
      }

      private PositionDeleteIndex load(Iterable<DeleteFile> files, CharSequence dataFile) {
        if (cache == null) {
          return loader.loadPositionDeletes(files, dataFile);
        }
        StringBuilder key = new StringBuilder(dataFile);
        for (DeleteFile f : files) {
          key.append('|').append(f.location()).append('@').append(f.contentOffset());
        }
        return cache.computeIfAbsent(
            key.toString(), k -> loader.loadPositionDeletes(files, dataFile));
      }
    };
  }

  @Override
  protected InputFile loadInputFile(DeleteFile deleteFile) {
    return new VersionedInputFile(
        io.newInputFile(deleteFile), deleteFile.fileSizeInBytes(), null, footerPrefetch);
  }
}
