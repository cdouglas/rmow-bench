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
 */
public final class VersionedInputFile implements InputFile {

  private static final int FOOTER_PROBE = 8; // footer length and magic

  private final InputFile delegate;
  private final long length;
  private final byte[] tail;
  private final int footerPrefetch;

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
    if (tail != null && tail.length > length) {
      throw new IllegalArgumentException("Tail longer than the version: " + tail.length);
    }
    this.delegate = delegate;
    this.length = length;
    this.tail = tail;
    this.footerPrefetch = footerPrefetch;
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

    /** Prefetches the tail on the first footer probe, if prefetching is on. */
    private void maybePrefetch(long position) throws IOException {
      if (cached == null && footerPrefetch > 0 && position >= length - FOOTER_PROBE) {
        int n = (int) Math.min(length, Math.max(footerPrefetch, FOOTER_PROBE));
        byte[] t = new byte[n];
        ((RangeReadable) in()).readFully(length - n, t, 0, n);
        cached = t;
        tailStart = length - n;
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
