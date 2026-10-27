package org.rmow.bench.append;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;

/**
 * RMoW's appends through the atomicio fork's {@link SupportsAtomicOperations}, the API the fork's
 * catalog commits through: one appender for every store. S3FileIO (S3 Express) and ADLSFileIO
 * implement it on the fork, and the local stores ({@code MemFileIO}, {@code PreadFileIO}) here.
 *
 * <p>An append is the fork's APPEND to a snapshot of the object: written at the snapshot's length
 * and conditioned on its ETag, so of several writers appending to one version exactly one lands.
 * Each write returns the next snapshot, which the appender keeps per object, so appends chain
 * without looking anything up. An append at a length the appender did not write looks the
 * object's ETag up when it writes. A new object is a CAS from a snapshot that has seen nothing,
 * which assumes the object is absent, so it sends no lookup and fails if an object is there; RMoW's
 * files, each under a name of its own, never meet one. On ADLS it makes the Append Blob later
 * appends need.
 */
public final class AtomicAppender implements RegionAppender {

  private final SupportsAtomicOperations io;
  // the snapshot each object's last write here returned
  private final Map<String, InputFile> snapshots = new ConcurrentHashMap<>();

  public AtomicAppender(SupportsAtomicOperations io) {
    this.io = io;
  }

  /**
   * One APPEND, or, where the store caps an append ({@link
   * SupportsAtomicOperations#maxAppendBytes}: Azure's Append Block), several in order, each to the
   * snapshot the last returned, so the footer lands last and no reader sees it before its regions.
   */
  @Override
  public long append(String location, long expectedLength, byte[] bytes) throws IOException {
    InputFile snapshot = snapshots.get(location);
    if (snapshot == null || snapshot.getLength() != expectedLength) {
      snapshot = io.newInputFile(location, expectedLength);
    }
    long max = io.maxAppendBytes(location);
    int part = max > 0 && max < bytes.length ? (int) max : bytes.length;
    int off = 0;
    do {
      int n = Math.min(part, bytes.length - off);
      byte[] b = n == bytes.length ? bytes : Arrays.copyOfRange(bytes, off, off + n);
      snapshot = write(io.newOutputFile(snapshot), AtomicOutputFile.Strategy.APPEND, location, b);
      snapshots.put(location, snapshot);
      off += n;
    } while (off < bytes.length);
    return snapshot.getLength();
  }

  @Override
  public void create(String location, byte[] bytes) throws IOException {
    AtomicOutputFile out = io.newOutputFile(io.newInputFile(location));
    snapshots.put(location, write(out, AtomicOutputFile.Strategy.CAS, location, bytes));
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
