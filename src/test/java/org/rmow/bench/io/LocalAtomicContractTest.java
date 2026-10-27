package org.rmow.bench.io;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;

/**
 * The atomicio fork's contract for {@link SupportsAtomicOperations}, which S3FileIO, ADLSFileIO
 * and GCSFileIO pass on the fork, on the local stores: creates, replaces and appends, each pinned
 * to the version a snapshot saw, and races of several writers, serial and concurrent (each with
 * a FileIO of its own), with exactly one winner.
 */
class LocalAtomicContractTest {

  /** One store per test: the instance is the store, so every client of it shares it. */
  @Nested
  class InMemory extends SupportsAtomicOperationsContractTest {
    private final MemFileIO store = new MemFileIO();

    @Override
    protected SupportsAtomicOperations newFileIO() {
      return store;
    }

    @Override
    protected String randomLocation(String slug) {
      return "mem://contract/" + slug + "-" + UUID.randomUUID();
    }
  }

  @Nested
  class LocalFilesystem extends SupportsAtomicOperationsContractTest {
    @TempDir Path dir;

    @Override
    protected SupportsAtomicOperations newFileIO() {
      return new PreadFileIO(false);
    }

    @Override
    protected String randomLocation(String slug) {
      return dir.resolve(slug + "-" + UUID.randomUUID()).toString();
    }
  }
}
