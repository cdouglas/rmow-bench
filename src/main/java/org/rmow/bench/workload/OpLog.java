package org.rmow.bench.workload;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * The operations every arm replays: the initial load (ids {@code [0, initialRows)}) and the
 * batches. Ids are dense and grow with insertion order; each id's event time is fixed at insert
 * and kept by updates, so {@link #ts} is all a writer needs to rebuild any row version.
 */
public final class OpLog {

  /** One batch; its inserts, updates and deletes touch disjoint ids. */
  public record Batch(
      int index, long[] insertIds, long[] updateIds, int[] updateVersions, long[] deleteIds) {}

  private final long initialRows;
  private final long[] tsById;
  private final List<Batch> batches;
  private final String sha256;

  OpLog(long initialRows, long[] tsById, List<Batch> batches) {
    this.initialRows = initialRows;
    this.tsById = tsById;
    this.batches = List.copyOf(batches);
    this.sha256 = digest();
  }

  public long initialRows() {
    return initialRows;
  }

  public long totalIds() {
    return tsById.length;
  }

  public long ts(long id) {
    return tsById[(int) id];
  }

  public List<Batch> batches() {
    return batches;
  }

  /** SHA-256 of the serialized log: two runs replayed the same operations iff their hashes match. */
  public String sha256() {
    return sha256;
  }

  /**
   * Streams the serialized log to {@code sink}. Never held whole: at 128 MiB row groups the
   * inserts' log is about 3 GB, past the 2 GiB a byte array holds.
   */
  private void serialize(OutputStream sink) throws IOException {
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(sink, 1 << 16));
    out.writeLong(initialRows);
    out.writeInt(tsById.length);
    for (long t : tsById) {
      out.writeLong(t);
    }
    out.writeInt(batches.size());
    for (Batch b : batches) {
      out.writeInt(b.index());
      writeLongs(out, b.insertIds());
      writeLongs(out, b.updateIds());
      out.writeInt(b.updateVersions().length);
      for (int v : b.updateVersions()) {
        out.writeInt(v);
      }
      writeLongs(out, b.deleteIds());
    }
    out.flush();
  }

  private static void writeLongs(DataOutputStream out, long[] a) throws IOException {
    out.writeInt(a.length);
    for (long x : a) {
      out.writeLong(x);
    }
  }

  private String digest() {
    MessageDigest sha;
    try {
      sha = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    try {
      serialize(new DigestOutputStream(OutputStream.nullOutputStream(), sha));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return HexFormat.of().formatHex(sha.digest());
  }

  /** Writes the serialized log, so a run's exact operations are kept with its results. */
  public void write(Path file) throws IOException {
    try (OutputStream out = Files.newOutputStream(file)) {
      serialize(out);
    }
  }
}
