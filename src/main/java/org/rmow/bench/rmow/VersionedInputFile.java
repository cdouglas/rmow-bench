package org.rmow.bench.rmow;

import java.io.EOFException;
import java.io.IOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * A view of the first {@code length} bytes of an object: one version of an appendable Parquet
 * file, whose footer ends at {@code length}.
 *
 * <ul>
 *   <li>{@code getLength()} is the version's length, so Parquet finds that version's footer;
 *   <li>streams return EOF at the length ({@code EagerInputFile}, used for files up to 1 MiB,
 *       checks that a stream ends exactly there);
 *   <li>{@code readTail} is relative to the length: a cloud stream's suffix-range GET would read
 *       the newest bytes of the object instead;
 *   <li>optionally, the version's tail (footer, length, magic) is served from memory: the "warm
 *       footer" knob, applied to every arm alike (I/O parity rule 3);
 *   <li>optionally, the first read of the last 8 bytes prefetches the last {@code footerPrefetch}
 *       bytes in one bounded request, as Arrow and DuckDB read footers, so a footer that fits
 *       costs one request instead of stock parquet-java's two. Also applied to every arm alike.
 * </ul>
 *
 * The delegate's stream must be {@link RangeReadable} (I/O parity rule 1).
 *
 * <p>A {@link DataReads} listener hears of every read served from storage below the footer: the
 * row groups' bytes a reader fetched, for what it holds ({@code Held}). The footer prefetch, the
 * footer once the probe has located it, and a cached tail are not data.
 */
public final class VersionedInputFile implements InputFile {

  /** Hears of the data a stream reads from storage: {@code n} bytes at {@code position}. */
  @FunctionalInterface
  public interface DataReads {
    void read(long position, long n);
  }

  private static final int FOOTER_PROBE = 8; // footer length and magic

  private final InputFile delegate;
  private final long length;
  private final byte[] tail;
  private final int footerPrefetch;
  private final DataReads dataReads; // null: none

  public VersionedInputFile(InputFile delegate, long length) {
    this(delegate, length, null, 0);
  }

  public VersionedInputFile(InputFile delegate, long length, byte[] tail) {
    this(delegate, length, tail, 0);
  }

  /**
   * @param tail the last {@code tail.length} bytes of this version, or null to read them from
   *     storage
   * @param footerPrefetch bytes to prefetch on the first footer probe (0: stock behaviour)
   */
  public VersionedInputFile(InputFile delegate, long length, byte[] tail, int footerPrefetch) {
    this(delegate, length, tail, footerPrefetch, null);
  }

  /** @param dataReads hears of the data each stream reads from storage; null: none */
  public VersionedInputFile(
      InputFile delegate, long length, byte[] tail, int footerPrefetch, DataReads dataReads) {
    if (tail != null && tail.length > length) {
      throw new IllegalArgumentException("Tail longer than the version: " + tail.length);
    }
    this.delegate = delegate;
    this.length = length;
    this.tail = tail;
    this.footerPrefetch = footerPrefetch;
    this.dataReads = dataReads;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new BoundedStream();
  }

  @Override
  public String location() {
    return delegate.location();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  /** Opens the delegate's stream only when a read falls outside the cached tail. */
  private final class BoundedStream extends SeekableInputStream implements RangeReadable {
    private SeekableInputStream in = null;
    private long pos = 0;
    private final byte[] one = new byte[1];
    private byte[] cached = tail; // the cached tail: given, or prefetched by this stream
    private long tailStart = tail == null ? length : length - tail.length;
    private long footerStart = tail == null ? length : footerStart(tail, tail.length - FOOTER_PROBE);

    /** Where the footer starts, from the probe at {@code b[off]} (footer length, then magic). */
    private long footerStart(byte[] b, int off) {
      if (off < 0) {
        return length;
      }
      int footerLength =
          (b[off] & 0xFF) | (b[off + 1] & 0xFF) << 8 | (b[off + 2] & 0xFF) << 16
              | (b[off + 3] & 0xFF) << 24;
      return Math.max(0, length - FOOTER_PROBE - footerLength);
    }

    /** Tells the listener of data read from storage, and locates the footer from its probe. */
    private void served(long position, byte[] b, int off, int n) {
      if (position == length - FOOTER_PROBE && n == FOOTER_PROBE) {
        footerStart = footerStart(b, off);
        return;
      }
      if (dataReads != null && position < footerStart) {
        dataReads.read(position, Math.min(n, footerStart - position));
      }
    }

    /** Prefetches the tail on the first footer probe, if prefetching is on. */
    private void maybePrefetch(long position) throws IOException {
      if (cached == null && footerPrefetch > 0 && position >= length - FOOTER_PROBE) {
        int n = (int) Math.min(length, Math.max(footerPrefetch, FOOTER_PROBE));
        byte[] t = new byte[n];
        ((RangeReadable) in()).readFully(length - n, t, 0, n);
        cached = t;
        tailStart = length - n;
        footerStart = footerStart(t, n - FOOTER_PROBE);
      }
    }

    private SeekableInputStream in() throws IOException {
      if (in == null) {
        in = delegate.newStream();
        if (!(in instanceof RangeReadable)) {
          throw new IOException("Stream of " + delegate.location() + " is not RangeReadable");
        }
      }
      return in;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0) {
        throw new EOFException("Negative seek: " + newPos);
      }
      this.pos = newPos;
    }

    @Override
    public int read() throws IOException {
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (pos >= length) {
        return -1;
      }
      maybePrefetch(pos);
      if (pos >= tailStart) {
        int n = (int) Math.min(len, length - pos);
        System.arraycopy(cached, (int) (pos - tailStart), b, off, n);
        pos += n;
        return n;
      }
      // stop at the cached tail, so the stream request never reads bytes the cache holds
      int n = (int) Math.min(len, tailStart - pos);
      SeekableInputStream s = in();
      if (s.getPos() != pos) {
        s.seek(pos);
      }
      int r = s.read(b, off, n);
      if (r < 0) {
        throw new EOFException("Object ended before its version length " + length + " at " + pos);
      }
      served(pos, b, off, r);
      pos += r;
      return r;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int len) throws IOException {
      if (position < 0 || position + len > length) {
        throw new EOFException(
            "Range [" + position + ", " + (position + len) + ") outside length " + length);
      }
      maybePrefetch(position);
      if (position >= tailStart) {
        System.arraycopy(cached, (int) (position - tailStart), buffer, offset, len);
        return;
      }
      int fromStorage = (int) Math.min(len, tailStart - position);
      ((RangeReadable) in()).readFully(position, buffer, offset, fromStorage);
      served(position, buffer, offset, fromStorage);
      if (fromStorage < len) {
        System.arraycopy(cached, 0, buffer, offset + fromStorage, len - fromStorage);
      }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int len) throws IOException {
      int n = (int) Math.min(len, length);
      readFully(length - n, buffer, offset, n);
      return n;
    }

    @Override
    public void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }
  }
}
