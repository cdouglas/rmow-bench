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
 * a snapshot pins an object's ETag (looked up if it does not carry one), a CAS lands only on the
 * pinned object (or, pinned absent, on none), and an APPEND only at the pinned object's length.
 * It counts {@link #lookups}, what a store answers with a HEAD.
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
    return new Out(path, null, false, null);
  }

  @Override
  public AtomicOutputFile newOutputFile(InputFile replace) {
    In snapshot = (In) replace;
    String etag = snapshot.etag();
    return new Out(snapshot.location(), etag, etag == null, etag == null ? null : snapshot.length);
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

    String etag() {
      if (etag == null) {
        lookups++;
        Obj o = objects.get(location);
        if (o != null) {
          etag = o.etag();
          if (length == null) {
            length = (long) o.bytes().length;
          }
        }
      }
      return etag;
    }

    @Override
    public long getLength() {
      if (length == null) {
        lookups++;
        Obj o = objects.get(location);
        if (o == null) {
          throw new NotFoundException("No object at %s", location);
        }
        length = (long) o.bytes().length;
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
      lookups++;
      return objects.containsKey(location);
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
    private final String etag;
    private final boolean absent;
    private final Long length;

    Out(String location, String etag, boolean absent, Long length) {
      this.location = location;
      this.etag = etag;
      this.absent = absent;
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
        boolean pinned = current != null && current.etag().equals(etag);
        String next = "\"v" + ++versions + "\"";
        if (((Token) token).strategy() == Strategy.APPEND) {
          if (!pinned || length == null || current.bytes().length != length) {
            throw new AppendException("Not the pinned object at its length: " + location, null);
          }
          byte[] joined = Arrays.copyOf(current.bytes(), current.bytes().length + bytes.length);
          System.arraycopy(bytes, 0, joined, current.bytes().length, bytes.length);
          objects.put(location, new Obj(joined, next));
          return new In(location, (long) joined.length, next);
        }
        if (etag != null ? !pinned : absent && current != null) {
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
