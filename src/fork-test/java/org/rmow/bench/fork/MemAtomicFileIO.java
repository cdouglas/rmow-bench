package org.rmow.bench.fork;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;

/**
 * An in-memory store with the fork's atomic operations, as S3FileIO and ADLSFileIO implement them:
 * a snapshot pins the ETag of the version it has seen (one a write returned, or a lookup found). A
 * CAS lands only on that version, or, from a snapshot that has seen none, only if no object exists;
 * an APPEND lands only at the pinned object's length, and from a snapshot that has seen no version
 * looks the object up when it writes. It counts {@link #lookups}, what a store answers with a HEAD.
 */
final class MemAtomicFileIO implements SupportsAtomicOperations {

  private record Obj(byte[] bytes, String etag) {}

  private final Map<String, Obj> objects = new ConcurrentHashMap<>();
  private int versions = 0;
  int lookups = 0;

  byte[] bytes(String location) {
    return objects.get(location).bytes();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new In(path, null, null);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new In(path, length, null);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new Out(path, null, null);
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    In snapshot = (In) replace;
    return new Out(snapshot.location(), snapshot.etag, snapshot.length);
  }

  @Override
  public void deleteFile(String path) {
    objects.remove(path);
  }

  private final class In implements InputFile {
    private final String location;
    private Long length;
    private String etag;

    In(String location, Long length, String etag) {
      this.location = location;
      this.length = length;
      this.etag = etag;
    }

    /** A lookup the snapshot sees: the version found is the one it pins. */
    private Obj lookUp() {
      lookups++;
      Obj o = objects.get(location);
      if (o != null && etag == null) {
        etag = o.etag();
        if (length == null) {
          length = (long) o.bytes().length;
        }
      }
      return o;
    }

    @Override
    public long getLength() {
      if (length == null) {
        if (lookUp() == null) {
          throw new NotFoundException("No object at %s", location);
        }
      }
      return length;
    }

    @Override
    public SeekableInputStream newStream() {
      throw new UnsupportedOperationException("not needed by the appender's tests");
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

  private record Token(AtomicOutputFile.Strategy strategy, long contentLength) implements CAS {
    @Override
    public byte[] contentChecksumBytes() {
      return new byte[0];
    }

    @Override
    public String contentHeaderString() {
      return "";
    }
  }

  private final class Out implements AtomicOutputFile {
    private final String location;
    // the version the snapshot saw; null if it saw none
    private final String etag;
    private final Long length;

    Out(String location, String etag, Long length) {
      this.location = location;
      this.etag = etag;
      this.length = length;
    }

    private byte[] read(Supplier<InputStream> source) {
      try (InputStream in = source.get()) {
        return in.readAllBytes();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) {
      return new Token(howto, read(source).length);
    }

    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) {
      byte[] bytes = read(source);
      synchronized (objects) {
        Obj current = objects.get(location);
        String next = "\"v" + ++versions + "\"";
        if (((Token) token).strategy() == Strategy.APPEND) {
          // from a snapshot that saw no version, the object's current one, looked up now
          String base = etag;
          if (base == null) {
            lookups++;
            base = current == null ? null : current.etag();
          }
          long at = length != null ? length : current == null ? -1 : current.bytes().length;
          boolean pinned = current != null && current.etag().equals(base);
          if (!pinned || current.bytes().length != at) {
            throw new AppendException("Not the pinned object at its length: " + location, null);
          }
          byte[] joined = Arrays.copyOf(current.bytes(), current.bytes().length + bytes.length);
          System.arraycopy(bytes, 0, joined, current.bytes().length, bytes.length);
          objects.put(location, new Obj(joined, next));
          return new In(location, (long) joined.length, next);
        }
        boolean pinned = current != null && current.etag().equals(etag);
        if (etag != null ? !pinned : current != null) {
          throw new CASException("Not the pinned object: " + location, null);
        }
        objects.put(location, new Obj(bytes, next));
        return new In(location, (long) bytes.length, next);
      }
    }

    @Override
    public PositionOutputStream create() {
      throw new UnsupportedOperationException("not needed by the appender's tests");
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      throw new UnsupportedOperationException("not needed by the appender's tests");
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new In(location, length, etag);
    }
  }
}
