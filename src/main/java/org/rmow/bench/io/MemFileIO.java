package org.rmow.bench.io;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;

/**
 * An in-memory object store. Objects are append-only lists of segments, so an append costs only
 * the appended bytes and readers of an older length keep seeing the older bytes. Every stream is
 * {@link RangeReadable}, as the cloud streams are (I/O parity rule 1).
 *
 * <p>It has the atomicio fork's atomic operations, as S3FileIO and ADLSFileIO implement them. Every
 * write makes a new version of its object, and a snapshot ({@link InputFile}) pins the version it
 * has seen: one a write returned, or one a lookup through it found ({@code getLength()} of a file
 * opened without a length, {@code exists()}; a cloud store answers each with a HEAD, and {@link
 * #lookups} counts them). A CAS lands only on that version, or, from a snapshot that has seen none,
 * only if no object exists. An APPEND lands only on that version at the snapshot's length, and
 * from a snapshot that has seen none looks the object up when it writes. {@code prepare} computes
 * the CRC32C the cloud stores' does.
 */
public final class MemFileIO implements SupportsAtomicOperations {

  private final ConcurrentMap<String, MemObject> objects = new ConcurrentHashMap<>();
  private final AtomicLong versions = new AtomicLong();
  private final LongAdder lookups = new LongAdder();

  @Override
  public InputFile newInputFile(String path) {
    return new MemInputFile(path, -1, -1);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new MemInputFile(path, length, -1);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new MemOutputFile(path);
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    if (!(replace instanceof MemInputFile snapshot) || snapshot.io() != this) {
      throw new IllegalArgumentException("Not a snapshot of this store: " + replace);
    }
    return new MemAtomicOutputFile(snapshot.location(), snapshot.length, snapshot.version);
  }

  @Override
  public void deleteFile(String path) {
    if (objects.remove(path) == null) {
      throw new NotFoundException("No such object: %s", path);
    }
  }

  /** Lookups of an object's version so far: what a cloud store would answer with a HEAD. */
  public long lookups() {
    return lookups.sum();
  }

  public long length(String path) {
    return object(path).length();
  }

  /** Total bytes held, for space accounting. */
  public long totalBytes() {
    return objects.values().stream().mapToLong(MemObject::length).sum();
  }

  private MemObject object(String path) {
    MemObject obj = objects.get(path);
    if (obj == null) {
      throw new NotFoundException("No such object: %s", path);
    }
    return obj;
  }

  /**
   * Append-only segments; positions map to segments by binary search over their starts. {@code
   * version} names the bytes it holds, and changes with every append.
   */
  private static final class MemObject {
    private final List<byte[]> segments = new ArrayList<>();
    private long[] starts = new long[4];
    private long length = 0;
    private long version;

    MemObject(byte[] first, long version) {
      add(first);
      this.version = version;
    }

    synchronized long length() {
      return length;
    }

    synchronized long version() {
      return version;
    }

    /** Appends at {@code at} if the object is version {@code base} (-1: any) and that long. */
    synchronized long append(long base, long at, byte[] bytes, long next) {
      if ((base >= 0 && version != base) || (at >= 0 && length != at)) {
        throw new AppendException(
            "Not the pinned version at its length: the object is version " + version + ", "
                + length + " bytes",
            null);
      }
      add(bytes);
      version = next;
      return length;
    }

    private void add(byte[] bytes) {
      if (segments.size() == starts.length) {
        starts = Arrays.copyOf(starts, starts.length * 2);
      }
      starts[segments.size()] = length;
      segments.add(bytes);
      length += bytes.length;
    }

    synchronized void read(long pos, byte[] buf, int off, int len) {
      int seg = Arrays.binarySearch(starts, 0, segments.size(), pos);
      if (seg < 0) {
        seg = -seg - 2;
      }
      long p = pos;
      int o = off;
      int remaining = len;
      while (remaining > 0) {
        byte[] s = segments.get(seg);
        int inSeg = (int) (p - starts[seg]);
        int n = Math.min(remaining, s.length - inSeg);
        System.arraycopy(s, inSeg, buf, o, n);
        p += n;
        o += n;
        remaining -= n;
        seg += 1;
      }
    }
  }

  /** A snapshot: a length (-1 until known) and the version it has seen (-1: none). */
  private final class MemInputFile implements InputFile {
    private final String path;
    private long length;
    private long version;

    MemInputFile(String path, long length, long version) {
      this.path = path;
      this.length = length;
      this.version = version;
    }

    MemFileIO io() {
      return MemFileIO.this;
    }

    /** A lookup: the version found is the one the snapshot pins, unless it has seen one. */
    private synchronized MemObject lookUp() {
      lookups.increment();
      MemObject obj = objects.get(path);
      if (obj != null && version < 0) {
        synchronized (obj) {
          version = obj.version();
          if (length < 0) {
            length = obj.length();
          }
        }
      }
      return obj;
    }

    @Override
    public synchronized long getLength() {
      if (length < 0 && lookUp() == null) {
        throw new NotFoundException("No such object: %s", path);
      }
      return length;
    }

    @Override
    public SeekableInputStream newStream() {
      MemObject obj = object(path);
      long limit;
      synchronized (this) {
        limit = length >= 0 ? length : obj.length();
      }
      if (limit > obj.length()) {
        throw new NotFoundException(
            "Object %s is %s bytes, shorter than the requested length %s",
            path, obj.length(), limit);
      }
      return new MemStream(obj, limit);
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public boolean exists() {
      return lookUp() != null;
    }
  }

  /** A stream over the first {@code limit} bytes of an object; EOF at the limit. */
  private static final class MemStream extends SeekableInputStream implements RangeReadable {
    private final MemObject obj;
    private final long limit;
    private long pos = 0;
    private final byte[] one = new byte[1];

    MemStream(MemObject obj, long limit) {
      this.obj = obj;
      this.limit = limit;
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
    public int read() {
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      if (pos >= limit) {
        return -1;
      }
      int n = (int) Math.min(len, limit - pos);
      obj.read(pos, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (position < 0 || position + length > limit) {
        throw new EOFException(
            "Range [" + position + ", " + (position + length) + ") outside length " + limit);
      }
      obj.read(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      int n = (int) Math.min(length, limit);
      readFully(limit - n, buffer, offset, n);
      return n;
    }
  }

  private final class MemOutputFile implements OutputFile {
    private final String path;

    MemOutputFile(String path) {
      this.path = path;
    }

    @Override
    public PositionOutputStream create() {
      if (objects.containsKey(path)) {
        throw new AlreadyExistsException("Object already exists: %s", path);
      }
      return new MemOutputStream(path, false);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new MemOutputStream(path, true);
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      return new MemInputFile(path, -1, -1);
    }
  }

  /** Buffers the object and publishes it whole on close, like a single PUT. */
  private final class MemOutputStream extends PositionOutputStream {
    private final String path;
    private final boolean overwrite;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private boolean closed = false;

    MemOutputStream(String path, boolean overwrite) {
      this.path = path;
      this.overwrite = overwrite;
    }

    @Override
    public long getPos() {
      return buf.size();
    }

    @Override
    public void write(int b) {
      buf.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      buf.write(b, off, len);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      MemObject obj = new MemObject(buf.toByteArray(), versions.incrementAndGet());
      if (overwrite) {
        objects.put(path, obj);
      } else if (objects.putIfAbsent(path, obj) != null) {
        throw new AlreadyExistsException("Object already exists: %s", path);
      }
    }
  }

  /** A write to the version a snapshot saw: {@code version} -1 if it saw none. */
  private final class MemAtomicOutputFile implements AtomicOutputFile {
    private final String path;
    private final long length;
    private final long version;

    MemAtomicOutputFile(String path, long length, long version) {
      this.path = path;
      this.length = length;
      this.version = version;
    }

    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
      return LocalCas.prepare(source, howto);
    }

    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
      byte[] bytes = LocalCas.content(token, source);
      long next = versions.incrementAndGet();
      if (LocalCas.strategy(token) == Strategy.APPEND) {
        if (version < 0) {
          lookups.increment(); // the version to extend: the object's current one
        }
        long[] newLength = new long[1];
        objects.compute(
            path,
            (k, cur) -> {
              if (cur == null) {
                throw new AppendException("Cannot append to " + path + ": no object", null);
              }
              newLength[0] = cur.append(version, length, bytes, next);
              return cur;
            });
        return new MemInputFile(path, newLength[0], next);
      }
      boolean[] landed = new boolean[1];
      objects.compute(
          path,
          (k, cur) -> {
            if (version < 0 ? cur != null : cur == null || cur.version() != version) {
              return cur;
            }
            landed[0] = true;
            return new MemObject(bytes, next);
          });
      if (!landed[0]) {
        throw new StorageInvariantException(
            version < 0
                ? "Object exists: " + path
                : "Not the pinned version " + version + ": " + path,
            null);
      }
      return new MemInputFile(path, bytes.length, next);
    }

    @Override
    public PositionOutputStream create() {
      return new MemOutputFile(path).create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new MemOutputFile(path).createOrOverwrite();
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      return new MemInputFile(path, length, version);
    }
  }
}
