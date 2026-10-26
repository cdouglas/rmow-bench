package org.rmow.bench.fork;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.rmow.bench.append.RegionAppender;

/**
 * RMoW's appends through the atomicio fork's {@link SupportsAtomicOperations} ({@code
 * store.append} {@code fork}), the API the fork's catalog commits through: one appender for every
 * store the fork implements it on (S3 Express, ADLS). Compiled only with {@code -Pfork}.
 *
 * <p>An append is the fork's APPEND to a snapshot of the object: written at the snapshot's length
 * and conditioned on its ETag, so of several writers appending to one version exactly one lands.
 * Each write returns the next snapshot, which the appender keeps per object, so appends chain
 * without looking anything up. An append at a length the appender did not write looks the
 * object's ETag up first. A new object is a CAS against what the lookup finds there, an ETag or
 * nothing, so it replaces an object as {@code createOrOverwrite} does, atomically; on ADLS it makes
 * the Append Blob later appends need.
 */
public final class ForkAppenders {

  private ForkAppenders() {}

  public static RegionAppender atomic(FileIO io) {
    if (!(io instanceof SupportsAtomicOperations atomic)) {
      throw new IllegalArgumentException(
          io.getClass().getName() + " does not support atomic operations");
    }
    return new RegionAppender() {
      // the snapshot each object's last write here returned
      private final Map<String, InputFile> snapshots = new ConcurrentHashMap<>();

      @Override
      public long append(String location, long expectedLength, byte[] bytes) throws IOException {
        InputFile snapshot = snapshots.get(location);
        if (snapshot == null || snapshot.getLength() != expectedLength) {
          snapshot = atomic.newInputFile(location, expectedLength);
        }
        InputFile next =
            write(atomic.newOutputFile(snapshot), AtomicOutputFile.Strategy.APPEND, location, bytes);
        snapshots.put(location, next);
        return next.getLength();
      }

      @Override
      public void create(FileIO unused, String location, byte[] bytes) throws IOException {
        AtomicOutputFile out = atomic.newOutputFile(atomic.newInputFile(location));
        snapshots.put(location, write(out, AtomicOutputFile.Strategy.CAS, location, bytes));
      }
    };
  }

  private static InputFile write(
      AtomicOutputFile out, AtomicOutputFile.Strategy strategy, String location, byte[] bytes)
      throws IOException {
    try {
      CAS token = out.prepare(() -> new ByteArrayInputStream(bytes), strategy);
      return out.writeAtomic(token, () -> new ByteArrayInputStream(bytes));
    } catch (SupportsAtomicOperations.AtomicOperationException e) {
      throw new IOException(strategy + " to " + location + " failed: " + e.getMessage(), e);
    }
  }
}
