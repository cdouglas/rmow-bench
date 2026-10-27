package org.rmow.bench.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;

/**
 * The local stores' atomic-write tokens: {@code prepare} reads the content once and computes its
 * CRC32C, as S3FileIO's and ADLSFileIO's do, so a local write costs the client what a cloud one
 * does; the store checks only its length.
 */
final class LocalCas {

  private LocalCas() {}

  private record Token(AtomicOutputFile.Strategy strategy, long contentLength, long crc32c)
      implements CAS {
    @Override
    public byte[] contentChecksumBytes() {
      return new byte[] {
        (byte) (crc32c >>> 24), (byte) (crc32c >>> 16), (byte) (crc32c >>> 8), (byte) crc32c
      };
    }

    @Override
    public String contentHeaderString() {
      return Base64.getEncoder().encodeToString(contentChecksumBytes());
    }
  }

  static CAS prepare(Supplier<InputStream> source, AtomicOutputFile.Strategy strategy)
      throws IOException {
    byte[] bytes = readAll(source);
    CRC32C crc = new CRC32C();
    crc.update(bytes);
    return new Token(strategy, bytes.length, crc.getValue());
  }

  static AtomicOutputFile.Strategy strategy(CAS token) {
    if (!(token instanceof Token t)) {
      throw new IllegalArgumentException("Not a token of a local store: " + token);
    }
    return t.strategy();
  }

  /** The content to write: {@code source} read whole, which must be as long as it was prepared. */
  static byte[] content(CAS token, Supplier<InputStream> source) throws IOException {
    byte[] bytes = readAll(source);
    if (bytes.length != token.contentLength()) {
      throw new IOException(
          "Content is " + bytes.length + " bytes, prepared as " + token.contentLength());
    }
    return bytes;
  }

  private static byte[] readAll(Supplier<InputStream> source) throws IOException {
    try (InputStream in = source.get()) {
      return in.readAllBytes();
    }
  }
}
