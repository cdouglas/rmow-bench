package org.rmow.bench.io;

import java.io.IOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.append.RegionAppender;

/**
 * Maps a fixed logical root onto a run's physical prefix, so every run of every configuration
 * names its files alike while each keeps its own objects.
 *
 * <p>Paths end up inside what is written: Puffin footers name the data files their DVs apply to,
 * and Iceberg's DV writer orders a file's blobs by a hash map of those paths. A run id in the
 * paths would change the blob order, and with it the width of the footer's offsets, by a byte or
 * two from run to run. Locations, as files and the arms see them, stay logical; only the delegate
 * sees physical ones. Streams are the delegate's, so their capabilities ({@code RangeReadable})
 * are kept.
 */
public final class RelocatedFileIO implements FileIO, RegionAppender {

  private final FileIO delegate;
  private final RegionAppender appender;
  private final String logicalRoot;
  private final String physicalRoot;

  public RelocatedFileIO(
      FileIO delegate, RegionAppender appender, String logicalRoot, String physicalRoot) {
    this.delegate = delegate;
    this.appender = appender;
    this.logicalRoot = logicalRoot;
    this.physicalRoot = physicalRoot;
  }

  /** The physical location of a logical one (others pass through). */
  public String physical(String location) {
    return location.startsWith(logicalRoot)
        ? physicalRoot + location.substring(logicalRoot.length())
        : location;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new In(delegate.newInputFile(physical(path)), path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new In(delegate.newInputFile(physical(path), length), path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new Out(delegate.newOutputFile(physical(path)), path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(physical(path));
  }

  @Override
  public long append(String location, long expectedLength, byte[] bytes) throws IOException {
    if (appender == null) {
      throw new UnsupportedOperationException("This store has no append: " + location);
    }
    return appender.append(physical(location), expectedLength, bytes);
  }

  private record In(InputFile file, String location) implements InputFile {
    @Override
    public long getLength() {
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return file.newStream();
    }

    @Override
    public boolean exists() {
      return file.exists();
    }
  }

  private record Out(OutputFile file, String location) implements OutputFile {
    @Override
    public PositionOutputStream create() {
      return file.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return file.createOrOverwrite();
    }

    @Override
    public InputFile toInputFile() {
      return new In(file.toInputFile(), location);
    }
  }
}
