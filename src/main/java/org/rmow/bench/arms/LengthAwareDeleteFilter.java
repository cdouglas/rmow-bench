package org.rmow.bench.arms;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.rmow.bench.rmow.VersionedInputFile;

/**
 * Iceberg's generic delete filter, opening delete files with their known length. The stock
 * filter opens them by location alone, so each one costs a HEAD on an object store before its
 * GET, a cost Spark's readers do not pay; without this the benchmark would charge it to MoR alone
 * (I/O parity rule 2). Delete files are read through the same {@link VersionedInputFile} view as
 * data files, so the footer-prefetch knob applies to them too.
 */
public final class LengthAwareDeleteFilter extends GenericDeleteFilter {

  private final FileIO io;
  private final int footerPrefetch;

  public LengthAwareDeleteFilter(
      FileIO io, FileScanTask task, Schema tableSchema, Schema requestedSchema, int footerPrefetch) {
    super(io, task, tableSchema, requestedSchema);
    this.io = io;
    this.footerPrefetch = footerPrefetch;
  }

  @Override
  protected InputFile loadInputFile(DeleteFile deleteFile) {
    return new VersionedInputFile(
        io.newInputFile(deleteFile), deleteFile.fileSizeInBytes(), null, footerPrefetch);
  }
}
