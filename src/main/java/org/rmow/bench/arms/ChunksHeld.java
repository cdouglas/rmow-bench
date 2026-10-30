package org.rmow.bench.arms;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.rmow.bench.io.Held;
import org.rmow.bench.rmow.VersionedInputFile;

/**
 * The column chunks one scan's reader has fetched for the row group it is decoding
 * ({@link Held.Kind#CHUNKS}): every data read the scan's stream serves from storage, by the row
 * group its offset falls in (the file's split offsets), held until the reader fetches from the
 * next row group, as parquet-java decodes a row group before it reads the next. A file Iceberg
 * fetches whole is held whole.
 */
final class ChunksHeld implements VersionedInputFile.DataReads, AutoCloseable {

  private final long[] starts; // the row groups' start offsets, ascending
  private final String name;
  private final Held.Scope held;
  private int rowGroup = -1;
  private long bytes = 0;

  ChunksHeld(DataFile df) {
    List<Long> offsets = df.splitOffsets();
    this.starts =
        offsets == null || offsets.isEmpty()
            ? new long[] {0}
            : offsets.stream().mapToLong(Long::longValue).sorted().toArray();
    String location = df.location();
    this.name = location.substring(location.lastIndexOf('/') + 1);
    this.held = Held.scope("scan of " + name + " done");
  }

  @Override
  public void read(long position, long n) {
    int i = Arrays.binarySearch(starts, position);
    int rg = i >= 0 ? i : Math.max(0, -i - 2);
    if (rg != rowGroup) {
      if (bytes > 0) {
        held.release(Held.Kind.CHUNKS, bytes, "row group " + rowGroup + " of " + name + " done");
      }
      rowGroup = rg;
      bytes = 0;
    }
    bytes += n;
    held.hold(Held.Kind.CHUNKS, n, "fetch row group " + rg + " of " + name);
  }

  @Override
  public void close() {
    held.close();
  }
}
