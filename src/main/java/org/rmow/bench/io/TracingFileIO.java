package org.rmow.bench.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.append.RegionAppender;

/**
 * Wraps a FileIO (and the store's appender) and counts requests in an {@link IoTrace}.
 *
 * <p>Requests are modelled on Iceberg's object-store streams ({@code S3InputStream}, {@code
 * ADLSInputStream}):
 *
 * <ul>
 *   <li>each {@link RangeReadable} call is one bounded ranged GET;
 *   <li>a sequential stream opens an open-ended GET lazily, on its first read; a forward seek of
 *       at most {@link #SKIP_SIZE} reads through the gap on the same request, and any other seek
 *       abandons the request (an abort) and opens a new one on the next read;
 *   <li>{@code getLength()} on an InputFile created without a length is a HEAD (once per file);
 *   <li>{@code OutputFile.create()} is a HEAD, the existence check Iceberg's S3 and ADLS output
 *       files make ({@code createOrOverwrite()} makes none);
 *   <li>an output stream is one PUT on close; an append is one APPEND.
 * </ul>
 *
 * The wrapper keeps the delegate's {@link RangeReadable} capability: dropping it would silently
 * switch Parquet to seek-and-read (I/O parity rule 1).
 *
 * <p>Given a {@link StoreLatency}, each request also sleeps its simulated latency, so wall time
 * becomes what the phase would take on that store.
 */
public final class TracingFileIO implements FileIO, RegionAppender {

  /** Iceberg's S3InputStream/ADLSInputStream read through forward seeks up to this many bytes. */
  public static final long SKIP_SIZE = 1024 * 1024;

  /** Stream transfer time is slept in chunks of at least this much, not per small read. */
  private static final long MIN_SLEEP_NS = 100_000;

  private final FileIO delegate;
  private final RegionAppender appender;
  private final IoTrace trace;
  private final StoreLatency latency; // null: no simulated latency

  public TracingFileIO(FileIO delegate, RegionAppender appender, IoTrace trace) {
    this(delegate, appender, trace, null);
  }

  public TracingFileIO(
      FileIO delegate, RegionAppender appender, IoTrace trace, StoreLatency latency) {
    this.delegate = delegate;
    this.appender = appender;
    this.trace = trace;
    this.latency = latency;
  }

  public IoTrace trace() {
    return trace;
  }

  public FileIO delegate() {
    return delegate;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new TracingInputFile(delegate.newInputFile(path), false);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new TracingInputFile(delegate.newInputFile(path, length), true);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new TracingOutputFile(delegate.newOutputFile(path));
  }

  @Override
  public void deleteFile(String path) {
    trace.record(IoTrace.Kind.DELETE, path, 0, 0);
    if (latency != null) {
      latency.delete(path);
    }
    delegate.deleteFile(path);
  }

  @Override
  public long append(String location, long expectedLength, byte[] bytes) throws IOException {
    if (appender == null) {
      throw new UnsupportedOperationException("This store has no append: " + location);
    }
    trace.record(IoTrace.Kind.APPEND, location, expectedLength, bytes.length);
    if (latency != null) {
      latency.append(location, bytes.length);
    }
    return appender.append(location, expectedLength, bytes);
  }

  private final class TracingInputFile implements InputFile {
    private final InputFile file;
    private final boolean knownLength;
    private boolean headDone = false;

    TracingInputFile(InputFile file, boolean knownLength) {
      this.file = file;
      this.knownLength = knownLength;
    }

    @Override
    public long getLength() {
      if (!knownLength && !headDone) {
        headDone = true;
        trace.record(IoTrace.Kind.HEAD, file.location(), 0, 0);
        if (latency != null) {
          latency.head();
        }
      }
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      // The cloud streams learn the length lazily too; a stream over a file of unknown length
      // reads to EOF without a HEAD, so no request is counted here.
      SeekableInputStream s = file.newStream();
      return s instanceof RangeReadable
          ? new TracingRangeStream(s, file.location())
          : new TracingStream(s, file.location());
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public boolean exists() {
      trace.record(IoTrace.Kind.HEAD, file.location(), 0, 0);
      if (latency != null) {
        latency.head();
      }
      return file.exists();
    }
  }

  private class TracingStream extends SeekableInputStream {
    protected final SeekableInputStream in;
    private final String location;
    private boolean requestOpen = false;
    private long requestPos = 0; // the next byte the open request would deliver
    private long transferNs = 0; // simulated transfer time not slept yet

    TracingStream(SeekableInputStream in, String location) {
      this.in = in;
      this.location = location;
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      in.seek(newPos);
    }

    private void beforeRead() throws IOException {
      long pos = in.getPos();
      if (requestOpen && pos == requestPos) {
        return;
      }
      if (requestOpen && pos > requestPos && pos - requestPos <= SKIP_SIZE) {
        trace.record(IoTrace.Kind.SKIP_THROUGH, location, requestPos, pos - requestPos);
        delivered(pos - requestPos);
        requestPos = pos;
        return;
      }
      if (requestOpen) {
        trace.record(IoTrace.Kind.ABORT, location, requestPos, 0);
      }
      trace.record(IoTrace.Kind.GET_STREAM, location, pos, -1);
      if (latency != null) {
        sleepTransfer();
        latency.open(location);
      }
      requestOpen = true;
      requestPos = pos;
    }

    /** Charges the transfer of {@code n} streamed bytes, slept once enough has built up. */
    private void delivered(long n) {
      if (latency != null) {
        transferNs += latency.transfer(n);
        if (transferNs >= MIN_SLEEP_NS) {
          sleepTransfer();
        }
      }
    }

    private void sleepTransfer() {
      StoreLatency.sleep(transferNs);
      transferNs = 0;
    }

    @Override
    public int read() throws IOException {
      beforeRead();
      int b = in.read();
      if (b >= 0) {
        trace.streamBytes(location, 1);
        delivered(1);
        requestPos += 1;
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      beforeRead();
      int n = in.read(b, off, len);
      if (n > 0) {
        trace.streamBytes(location, n);
        delivered(n);
        requestPos += n;
      }
      return n;
    }

    @Override
    public void close() throws IOException {
      // A request closed before EOF is aborted by the cloud streams. The model does not know the
      // object's length here, so closes are not counted as aborts; seeks away are.
      if (latency != null) {
        sleepTransfer();
      }
      in.close();
    }

    protected String location() {
      return location;
    }
  }

  private final class TracingRangeStream extends TracingStream implements RangeReadable {
    private final RangeReadable range;

    TracingRangeStream(SeekableInputStream in, String location) {
      super(in, location);
      this.range = (RangeReadable) in;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      trace.record(IoTrace.Kind.GET_RANGE, location(), position, length);
      if (latency != null) {
        latency.get(location(), length);
      }
      range.readFully(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      int n = range.readTail(buffer, offset, length);
      trace.record(IoTrace.Kind.GET_RANGE, location(), -1, n);
      if (latency != null) {
        latency.get(location(), n);
      }
      return n;
    }

    @Override
    public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {
      // Count each range as one GET; a delegate that coalesces ranges issues fewer, which the
      // SDK-level recorders would show.
      for (FileRange r : ranges) {
        trace.record(IoTrace.Kind.GET_RANGE, location(), r.offset(), r.length());
        if (latency != null) {
          latency.get(location(), r.length()); // one request after another
        }
      }
      range.readVectored(ranges, allocate);
    }
  }

  private final class TracingOutputFile implements OutputFile {
    private final OutputFile file;

    TracingOutputFile(OutputFile file) {
      this.file = file;
    }

    /**
     * Iceberg's S3 and ADLS output files check that the object does not exist first (a HEAD);
     * writers that may replace a file call {@link #createOrOverwrite} instead. Of stock Iceberg's
     * writers only Puffin's ({@code PuffinWriter}, so every DV file) calls this.
     */
    @Override
    public PositionOutputStream create() {
      trace.record(IoTrace.Kind.HEAD, file.location(), 0, 0);
      if (latency != null) {
        latency.head();
      }
      return new TracingOutputStream(file.create(), file.location());
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new TracingOutputStream(file.createOrOverwrite(), file.location());
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public InputFile toInputFile() {
      return new TracingInputFile(file.toInputFile(), false);
    }
  }

  private final class TracingOutputStream extends PositionOutputStream {
    private final PositionOutputStream out;
    private final String location;
    private boolean closed = false;

    TracingOutputStream(PositionOutputStream out, String location) {
      this.out = out;
      this.location = location;
    }

    @Override
    public long getPos() throws IOException {
      return out.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        long len = out.getPos();
        out.close();
        trace.record(IoTrace.Kind.PUT, location, 0, len);
        if (latency != null) {
          latency.put(location, len);
        }
      }
    }
  }
}
