package org.rmow.bench.rmow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * An in-memory OutputFile whose stream reports positions as if its bytes were written at offset
 * {@code baseLength} of an existing object. Parquet records every offset it writes (pages, column
 * and offset indexes, bloom filters, row-group starts) from {@code getPos()}, so the stock writer
 * produces a region whose offsets are already absolute in the target object.
 *
 * <p>When {@code baseLength > 0}, the leading {@code PAR1} magic that {@code
 * ParquetFileWriter.start()} writes is dropped: the object already starts with one.
 */
public final class OffsetOutputFile implements OutputFile {

  private static final byte[] MAGIC = {'P', 'A', 'R', '1'};

  private final String location;
  private final long baseLength;
  private Stream stream = null;

  public OffsetOutputFile(String location, long baseLength) {
    if (baseLength != 0 && baseLength < MAGIC.length) {
      throw new IllegalArgumentException("Base length too short for a Parquet file: " + baseLength);
    }
    this.location = location;
    this.baseLength = baseLength;
  }

  @Override
  public PositionOutputStream create() {
    if (stream != null) {
      throw new IllegalStateException("Already created: " + location);
    }
    this.stream = new Stream();
    return stream;
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    this.stream = new Stream();
    return stream;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    throw new UnsupportedOperationException("A region is not readable on its own: " + location);
  }

  public long baseLength() {
    return baseLength;
  }

  /** The bytes to append at {@code baseLength} (or, for a new object, the whole object). */
  public byte[] bytes() {
    if (stream == null || !stream.closed) {
      throw new IllegalStateException("Not written and closed: " + location);
    }
    return stream.buf.toByteArray();
  }

  private final class Stream extends PositionOutputStream {
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream(1 << 20);
    private final boolean dropMagic = baseLength > 0;
    private final byte[] magic = new byte[MAGIC.length];
    private long written = 0; // bytes handed to this stream, including a dropped magic
    private boolean closed = false;

    @Override
    public long getPos() {
      return dropMagic ? baseLength - MAGIC.length + written : written;
    }

    @Override
    public void write(int b) {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      int o = off;
      int n = len;
      while (dropMagic && written < MAGIC.length && n > 0) {
        magic[(int) written] = b[o];
        written += 1;
        o += 1;
        n -= 1;
        if (written == MAGIC.length && !Arrays.equals(magic, MAGIC)) {
          throw new IllegalStateException("Region does not start with the Parquet magic");
        }
      }
      buf.write(b, o, n);
      written += n;
    }

    @Override
    public void close() throws IOException {
      this.closed = true;
    }
  }
}
