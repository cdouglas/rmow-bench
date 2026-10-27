package org.rmow.bench.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
 * A local-filesystem FileIO with positional reads. Iceberg's local and Hadoop FileIOs either lack
 * {@link RangeReadable} (so Parquet falls back to seek-and-read) or ignore a supplied length; this
 * one keeps both, so local runs take the same Parquet read path as the cloud stores.
 *
 * <p>It has the atomicio fork's atomic operations, as {@link MemFileIO} does. A file's version is
 * its file key (the inode, which a replace changes), length and modification time. A CAS writes a
 * temporary sibling and renames it onto the file; an APPEND writes at the snapshot's length. Each
 * checks the version under a lock that every instance in the process shares, so they are atomic
 * among this process's writers, which are all a run has; another process writing the same files is
 * not excluded.
 *
 * <p>Locations are absolute paths, optionally prefixed with {@code file:}.
 */
public final class PreadFileIO implements SupportsAtomicOperations {

  /** A version of a file. */
  private record Version(Object fileKey, long length, long modifiedNs) {
    static Version of(BasicFileAttributes a) {
      return new Version(a.fileKey(), a.size(), a.lastModifiedTime().to(TimeUnit.NANOSECONDS));
    }
  }

  // the atomic writes' locks, striped by path, shared by every instance in the process
  private static final Object[] LOCKS = new Object[64];

  static {
    for (int i = 0; i < LOCKS.length; i++) {
      LOCKS[i] = new Object();
    }
  }

  private final boolean fsync;

  public PreadFileIO(boolean fsync) {
    this.fsync = fsync;
  }

  private static Object lock(Path p) {
    return LOCKS[Math.floorMod(p.toAbsolutePath().normalize().hashCode(), LOCKS.length)];
  }

  /** The file's attributes, or null if there is no file. */
  private static BasicFileAttributes attributes(Path p) {
    try {
      return Files.readAttributes(p, BasicFileAttributes.class);
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Path path(String location) {
    return Paths.get(location.startsWith("file:") ? location.substring(5) : location);
  }

  @Override
  public InputFile newInputFile(String path) {
    return new LocalInputFile(path, -1, null);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new LocalInputFile(path, length, null);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new LocalOutputFile(path);
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    if (!(replace instanceof LocalInputFile snapshot) || snapshot.io() != this) {
      throw new IllegalArgumentException("Not a snapshot of this store: " + replace);
    }
    synchronized (snapshot) {
      return new LocalAtomicOutputFile(snapshot.location, snapshot.length, snapshot.version);
    }
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

  /** A snapshot: a length (-1 until known) and the version it has seen (null: none). */
  private final class LocalInputFile implements InputFile {
    private final String location;
    private long length;
    private Version version;

    LocalInputFile(String location, long length, Version version) {
      this.location = location;
      this.length = length;
      this.version = version;
    }

    PreadFileIO io() {
      return PreadFileIO.this;
    }

    /** A lookup: the version found is the one the snapshot pins, unless it has seen one. */
    private synchronized BasicFileAttributes lookUp() {
      BasicFileAttributes a = attributes(path(location));
      if (a != null && version == null) {
        version = Version.of(a);
        if (length < 0) {
          length = a.size();
        }
      }
      return a;
    }

    @Override
    public synchronized long getLength() {
      if (length < 0 && lookUp() == null) {
        throw new NotFoundException("No such file: %s", location);
      }
      return length;
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
      return lookUp() != null;
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
      return new LocalInputFile(location, -1, null);
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

  /** A write to the version a snapshot saw: {@code version} null if it saw none. */
  private final class LocalAtomicOutputFile implements AtomicOutputFile {
    private final String location;
    private final long length;
    private final Version version;

    LocalAtomicOutputFile(String location, long length, Version version) {
      this.location = location;
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
      Path p = path(location);
      synchronized (lock(p)) {
        BasicFileAttributes current = attributes(p);
        if (LocalCas.strategy(token) == Strategy.APPEND) {
          return append(p, current, bytes);
        }
        boolean pinned = current != null && version != null && version.equals(Version.of(current));
        if (version == null ? current != null : !pinned) {
          throw new StorageInvariantException(
              version == null ? "File exists: " + location : "Not the pinned version: " + location,
              null);
        }
        Files.createDirectories(p.getParent());
        Path temp = p.resolveSibling("." + p.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
          try (FileChannel ch =
              FileChannel.open(temp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            write(ch, 0, bytes);
          }
          Files.move(temp, p, StandardCopyOption.ATOMIC_MOVE);
        } finally {
          Files.deleteIfExists(temp);
        }
        return new LocalInputFile(location, bytes.length, Version.of(attributes(p)));
      }
    }

    /** At the snapshot's length, of the version it saw or, if it saw none, the file's current. */
    private InputFile append(Path p, BasicFileAttributes current, byte[] bytes) throws IOException {
      if (current == null) {
        throw new AppendException("Cannot append to " + location + ": no file", null);
      }
      long at = length >= 0 ? length : current.size();
      if ((version != null && !version.equals(Version.of(current))) || current.size() != at) {
        throw new AppendException(
            "Not the pinned version at its length: " + location + " is " + current.size()
                + " bytes",
            null);
      }
      try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
        write(ch, at, bytes);
      }
      return new LocalInputFile(location, at + bytes.length, Version.of(attributes(p)));
    }

    private void write(FileChannel ch, long at, byte[] bytes) throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      long pos = at;
      while (buf.hasRemaining()) {
        pos += ch.write(buf, pos);
      }
      if (fsync) {
        ch.force(false);
      }
    }

    @Override
    public PositionOutputStream create() {
      return new LocalOutputFile(location).create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new LocalOutputFile(location).createOrOverwrite();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new LocalInputFile(location, length, version);
    }
  }
}
