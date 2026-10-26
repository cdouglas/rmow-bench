package org.rmow.bench.append;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.iceberg.io.FileIO;

/**
 * Appends bytes to the end of an existing object, conditioned on its length: the append succeeds
 * only if the object is exactly {@code expectedLength} bytes long, so a region and its footer land
 * where the footer's offsets say they do. This is the one primitive region-merge-on-write needs
 * beyond an ordinary whole-object write.
 */
public interface RegionAppender {

  /**
   * Appends {@code bytes} to {@code location}.
   *
   * @return the object's new length
   * @throws IOException if the object's length is not {@code expectedLength} or the write fails
   */
  long append(String location, long expectedLength, byte[] bytes) throws IOException;

  /**
   * Writes a new object holding {@code bytes}, which {@link #append} can then extend. Most stores
   * append to any object, so this writes through {@code io}; Azure appends only to an Append Blob,
   * which a FileIO's output stream does not make.
   */
  default void create(FileIO io, String location, byte[] bytes) throws IOException {
    try (OutputStream out = io.newOutputFile(location).createOrOverwrite()) {
      out.write(bytes);
    }
  }
}
