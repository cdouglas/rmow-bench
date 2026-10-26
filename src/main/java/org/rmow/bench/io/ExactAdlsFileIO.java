package org.rmow.bench.io;

import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.FileReadAsyncResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * Iceberg's ADLSFileIO with reads that send what {@link TracingFileIO} models and Iceberg's
 * S3InputStream sends. Writes, deletes and everything else are ADLSFileIO's.
 *
 * <p>Iceberg 1.12's {@code ADLSInputStream} does not: its constructor downloads the file's first
 * chunk (up to 4 MiB) before any read, and its {@code readFully(position, ...)} asks for {@code
 * position + length} bytes where it needs {@code length}, so a ranged read at an offset fetches up
 * to a 4 MiB chunk. Here:
 *
 * <ul>
 *   <li>opening a stream sends nothing;
 *   <li>each {@link RangeReadable} call is one GET of exactly its range ({@code readTail} needs the
 *       length, which Iceberg passes; without it, one HEAD per file first);
 *   <li>a sequential read opens one open-ended GET and streams it; a forward seek of at most
 *       {@link TracingFileIO#SKIP_SIZE} reads through, and any other seek abandons the request and
 *       opens a new one on the next read.
 * </ul>
 */
public final class ExactAdlsFileIO implements FileIO {

  private final ADLSFileIO delegate;
  private final AdlsClients clients;

  public ExactAdlsFileIO(ADLSFileIO delegate, AdlsClients clients) {
    this.delegate = delegate;
    this.clients = clients;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new In(path, null);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new In(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void close() {
    delegate.close();
  }

  private static RuntimeException notFound(RuntimeException e, String location) {
    boolean missing =
        (e instanceof BlobStorageException b && b.getStatusCode() == 404)
            || (e instanceof DataLakeStorageException d && d.getStatusCode() == 404);
    return missing ? new NotFoundException(e, "Location does not exist: %s", location) : e;
  }

  private final class In implements InputFile {
    private final String location;
    private final DataLakeFileClient file;
    private DataLakeFileAsyncClient async; // for streamed reads, built on the first
    private Long length;

    In(String location, Long length) {
      this.location = location;
      this.file = clients.file(location);
      this.length = length;
    }

    DataLakeFileAsyncClient async() {
      if (async == null) {
        async = clients.fileAsync(location);
      }
      return async;
    }

    @Override
    public long getLength() {
      if (length == null) {
        try {
          length = file.getProperties().getFileSize(); // a HEAD
        } catch (RuntimeException e) {
          throw notFound(e, location);
        }
      }
      return length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new Stream(this);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return file.exists();
    }
  }

  private static final class Stream extends SeekableInputStream implements RangeReadable {
    private final In in;
    private long pos = 0; // the next byte a read returns
    private Body body; // the open request, if any
    private long bodyPos; // the next byte it delivers

    Stream(In in) {
      this.in = in;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      pos = newPos;
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      return read(b, 0, 1) < 0 ? -1 : b[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (!position()) {
        return -1;
      }
      int n = body.read(b, off, len);
      if (n > 0) {
        pos += n;
        bodyPos += n;
      }
      return n;
    }

    /** Positions the open request at {@code pos}, or opens one; false at the end of the file. */
    private boolean position() throws IOException {
      if (body != null && pos == bodyPos) {
        return true;
      }
      if (body != null && pos > bodyPos && pos - bodyPos <= TracingFileIO.SKIP_SIZE) {
        body.skipNBytes(pos - bodyPos);
        bodyPos = pos;
        return true;
      }
      abandon();
      if (in.length != null && pos >= in.length) {
        return false;
      }
      try {
        FileReadAsyncResponse r =
            in.async().readWithResponse(new FileRange(pos), null, null, false).block();
        body = new Body(r.getValue());
      } catch (BlobStorageException e) {
        if (e.getStatusCode() == 416) {
          return false; // at or past the end of a file whose length was not given
        }
        throw notFound(e, in.location);
      } catch (RuntimeException e) {
        throw notFound(e, in.location);
      }
      bodyPos = pos;
      return true;
    }

    private void abandon() {
      if (body != null) {
        body.close();
        body = null;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (length == 0) {
        return;
      }
      get(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      long size = in.getLength();
      int n = (int) Math.min(length, size);
      if (n > 0) {
        get(size - n, buffer, offset, n);
      }
      return n;
    }

    /** One GET of exactly {@code [position, position + length)}. */
    private void get(long position, byte[] buffer, int offset, int length) throws IOException {
      Into into = new Into(buffer, offset, length);
      try {
        in.file.readWithResponse(
            into, new FileRange(position, (long) length), null, null, false, null, Context.NONE);
      } catch (UncheckedIOException e) {
        throw e.getCause();
      } catch (RuntimeException e) {
        throw notFound(e, in.location);
      }
      if (into.written != length) {
        throw new IOException(
            "Read " + into.written + " of " + length + " bytes at " + position + ": " + in.location);
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      abandon();
    }
  }

  /** Writes a download into a slice of an array. */
  private static final class Into extends OutputStream {
    private final byte[] buffer;
    private final int offset;
    private final int length;
    private int written = 0;

    Into(byte[] buffer, int offset, int length) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (written + len > length) {
        throw new IOException("The store sent more than the " + length + " bytes asked for");
      }
      System.arraycopy(b, off, buffer, offset + written, len);
      written += len;
    }
  }

  /**
   * A streamed response body, read as it arrives: one buffer requested at a time, so abandoning
   * the stream (closing it) stops the transfer, as aborting S3InputStream's request does.
   */
  private static final class Body extends InputStream {
    private static final Object END = new Object();
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final BaseSubscriber<ByteBuffer> subscriber;
    private ByteBuffer current;
    private boolean done = false;

    Body(Flux<ByteBuffer> flux) {
      this.subscriber =
          new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription s) {
              s.request(1);
            }

            @Override
            protected void hookOnNext(ByteBuffer b) {
              ByteBuffer copy = ByteBuffer.allocate(b.remaining());
              copy.put(b).flip();
              queue.add(copy);
            }

            @Override
            protected void hookOnComplete() {
              queue.add(END);
            }

            @Override
            protected void hookOnError(Throwable t) {
              queue.add(t);
            }
          };
      flux.subscribe(subscriber);
    }

    private boolean fill() throws IOException {
      while (current == null || !current.hasRemaining()) {
        if (done) {
          return false;
        }
        Object next;
        try {
          next = queue.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
        if (next == END) {
          done = true;
          return false;
        }
        if (next instanceof Throwable t) {
          done = true;
          throw new IOException(t);
        }
        current = (ByteBuffer) next;
        subscriber.request(1);
      }
      return true;
    }

    @Override
    public int read() throws IOException {
      return fill() ? current.get() & 0xff : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (!fill()) {
        return -1;
      }
      int n = Math.min(len, current.remaining());
      current.get(b, off, n);
      return n;
    }

    @Override
    public void close() {
      done = true;
      subscriber.dispose(); // cancels the request
    }
  }
}
