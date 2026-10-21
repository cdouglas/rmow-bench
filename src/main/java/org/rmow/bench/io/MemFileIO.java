package org.rmow.bench.io;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.append.RegionAppender;

/**
 * An in-memory object store. Objects are append-only lists of segments, so an append costs only
 * the appended bytes and readers of an older length keep seeing the older bytes. Every stream is
 * {@link RangeReadable}, as the cloud streams are (I/O parity rule 1).
 */
public final class MemFileIO implements FileIO, RegionAppender {

  private final ConcurrentMap<String, MemObject> objects = new ConcurrentHashMap<>();

  @Override
  public InputFile newInputFile(String path) {
    return new MemInputFile(path, -1);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new MemInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new MemOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    if (objects.remove(path) == null) {
      throw new NotFoundException("No such object: %s", path);
    }
  }

  @Override
  public long append(String location, long expectedLength, byte[] bytes) throws IOException {
    MemObject obj = objects.get(location);
    if (obj == null) {
      throw new IOException("Cannot append to missing object: " + location);
    }
    return obj.append(expectedLength, bytes);
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

  /** Append-only segments; positions map to segments by binary search over their starts. */
  private static final class MemObject {
    private final List<byte[]> segments = new ArrayList<>();
    private long[] starts = new long[4];
    private long length = 0;

    MemObject(byte[] first) {
      add(first);
    }

    synchronized long length() {
      return length;
    }

    synchronized long append(long expectedLength, byte[] bytes) throws IOException {
      if (length != expectedLength) {
        throw new IOException(
            "Append position mismatch: object is " + length + " bytes, expected " + expectedLength);
      }
      add(bytes);
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

  private final class MemInputFile implements InputFile {
    private final String path;
    private final long length;

    MemInputFile(String path, long length) {
      this.path = path;
      this.length = length;
    }

    @Override
    public long getLength() {
      return length >= 0 ? length : object(path).length();
    }

    @Override
    public SeekableInputStream newStream() {
      MemObject obj = object(path);
      long limit = getLength();
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
      return objects.containsKey(path);
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
      return new MemInputFile(path, -1);
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
      MemObject obj = new MemObject(buf.toByteArray());
      if (overwrite) {
        objects.put(path, obj);
      } else if (objects.putIfAbsent(path, obj) != null) {
        throw new AlreadyExistsException("Object already exists: %s", path);
      }
    }
  }
}
