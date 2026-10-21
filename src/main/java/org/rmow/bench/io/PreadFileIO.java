package org.rmow.bench.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
 * A local-filesystem FileIO with positional reads. Iceberg's local and Hadoop FileIOs either lack
 * {@link RangeReadable} (so Parquet falls back to seek-and-read) or ignore a supplied length; this
 * one keeps both, so local runs take the same Parquet read path as the cloud stores.
 *
 * <p>Locations are absolute paths, optionally prefixed with {@code file:}.
 */
public final class PreadFileIO implements FileIO, RegionAppender {

  private final boolean fsync;

  public PreadFileIO(boolean fsync) {
    this.fsync = fsync;
  }

  static Path path(String location) {
    return Paths.get(location.startsWith("file:") ? location.substring(5) : location);
  }

  @Override
  public InputFile newInputFile(String path) {
    return new LocalInputFile(path, -1);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new LocalInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new LocalOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    try {
      Files.delete(path(path));
    } catch (NoSuchFileException e) {
      throw new NotFoundException(e, "No such file: %s", path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long append(String location, long expectedLength, byte[] bytes) throws IOException {
    try (FileChannel ch = FileChannel.open(path(location), StandardOpenOption.WRITE)) {
      long size = ch.size();
      if (size != expectedLength) {
        throw new IOException(
            "Append position mismatch: " + location + " is " + size + " bytes, expected "
                + expectedLength);
      }
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      long pos = size;
      while (buf.hasRemaining()) {
        pos += ch.write(buf, pos);
      }
      if (fsync) {
        ch.force(false);
      }
      return pos;
    }
  }

  private static final class LocalInputFile implements InputFile {
    private final String location;
    private final long length;

    LocalInputFile(String location, long length) {
      this.location = location;
      this.length = length;
    }

    @Override
    public long getLength() {
      if (length >= 0) {
        return length;
      }
      try {
        return Files.size(path(location));
      } catch (NoSuchFileException e) {
        throw new NotFoundException(e, "No such file: %s", location);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        FileChannel ch = FileChannel.open(path(location), StandardOpenOption.READ);
        return new PreadStream(ch, getLength());
      } catch (NoSuchFileException e) {
        throw new NotFoundException(e, "No such file: %s", location);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return Files.exists(path(location));
    }
  }

  /** A stream over the first {@code limit} bytes of a file; EOF at the limit. */
  private static final class PreadStream extends SeekableInputStream implements RangeReadable {
    private final FileChannel ch;
    private final long limit;
    private long pos = 0;
    private final byte[] one = new byte[1];

    PreadStream(FileChannel ch, long limit) {
      this.ch = ch;
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
    public int read() throws IOException {
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (pos >= limit) {
        return -1;
      }
      int n = ch.read(ByteBuffer.wrap(b, off, (int) Math.min(len, limit - pos)), pos);
      if (n < 0) {
        throw new EOFException("File ended before its length " + limit + " at " + pos);
      }
      pos += n;
      return n;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (position < 0 || position + length > limit) {
        throw new EOFException(
            "Range [" + position + ", " + (position + length) + ") outside length " + limit);
      }
      ByteBuffer buf = ByteBuffer.wrap(buffer, offset, length);
      long p = position;
      while (buf.hasRemaining()) {
        int n = ch.read(buf, p);
        if (n < 0) {
          throw new EOFException("File ended before " + (position + length));
        }
        p += n;
      }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      int n = (int) Math.min(length, limit);
      readFully(limit - n, buffer, offset, n);
      return n;
    }

    @Override
    public void close() throws IOException {
      ch.close();
    }
  }

  private final class LocalOutputFile implements OutputFile {
    private final String location;

    LocalOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      return open(StandardOpenOption.CREATE_NEW);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return open(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private PositionOutputStream open(StandardOpenOption... opts) {
      Path p = path(location);
      try {
        Files.createDirectories(p.getParent());
        StandardOpenOption[] all = new StandardOpenOption[opts.length + 1];
        System.arraycopy(opts, 0, all, 0, opts.length);
        all[opts.length] = StandardOpenOption.WRITE;
        return new LocalOutputStream(FileChannel.open(p, all), fsync);
      } catch (FileAlreadyExistsException e) {
        throw new AlreadyExistsException(e, "File already exists: %s", location);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new LocalInputFile(location, -1);
    }
  }

  private static final class LocalOutputStream extends PositionOutputStream {
    private static final int BUFFER = 1 << 16;
    private final FileChannel ch;
    private final boolean fsync;
    private final ByteBuffer buf = ByteBuffer.allocate(BUFFER);
    private long pos = 0;
    private boolean closed = false;

    LocalOutputStream(FileChannel ch, boolean fsync) {
      this.ch = ch;
      this.fsync = fsync;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int b) throws IOException {
      if (!buf.hasRemaining()) {
        flushBuffer();
      }
      buf.put((byte) b);
      pos += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (len > buf.remaining()) {
        flushBuffer();
        if (len > BUFFER) {
          ByteBuffer direct = ByteBuffer.wrap(b, off, len);
          while (direct.hasRemaining()) {
            ch.write(direct);
          }
          pos += len;
          return;
        }
      }
      buf.put(b, off, len);
      pos += len;
    }

    private void flushBuffer() throws IOException {
      buf.flip();
      while (buf.hasRemaining()) {
        ch.write(buf);
      }
      buf.clear();
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      flushBuffer();
      if (fsync) {
        ch.force(false);
      }
      ch.close();
    }
  }
}
