package org.rmow.bench.append;

import com.azure.storage.blob.models.BlobStorageException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.rmow.bench.io.AdlsClients;

/**
 * Stand-ins for the part of {@link SupportsAtomicOperations} an emulator lacks, so the tests and
 * the emulator twins can run RMoW's appendable arms there. Everything else is {@link
 * AtomicAppender}'s. A store on a cloud endpoint never uses them ({@code Store}).
 */
public final class EmulatorAppenders {

  private EmulatorAppenders() {}

  /**
   * For S3 emulators, which ignore write offsets (versitygw): an append reads the object and CASes
   * it back longer, on {@code unmeasured}, a FileIO over a client nothing records, since these
   * requests are not what an append costs. A create is the fork's, on {@code measured}.
   */
  public static RegionAppender rewriting(
      SupportsAtomicOperations measured, SupportsAtomicOperations unmeasured) {
    RegionAppender creates = new AtomicAppender(measured);
    return new RegionAppender() {
      @Override
      public long append(String location, long expectedLength, byte[] bytes) throws IOException {
        InputFile current = unmeasured.newInputFile(location);
        long length = current.getLength(); // pins the version the CAS replaces
        if (length != expectedLength) {
          throw new IOException(
              "Append position mismatch: " + location + " is " + length + " bytes, expected "
                  + expectedLength);
        }
        byte[] all = new byte[(int) length + bytes.length];
        try (SeekableInputStream in = current.newStream()) {
          IOUtil.readFully(in, all, 0, (int) length);
        }
        System.arraycopy(bytes, 0, all, (int) length, bytes.length);
        AtomicOutputFile out = unmeasured.newOutputFile(current);
        try {
          CAS token = out.prepare(() -> new ByteArrayInputStream(all), AtomicOutputFile.Strategy.CAS);
          out.writeAtomic(token, () -> new ByteArrayInputStream(all));
        } catch (SupportsAtomicOperations.AtomicOperationException e) {
          throw new IOException("CAS to " + location + " failed: " + e.getMessage(), e);
        }
        return all.length;
      }

      @Override
      public void create(String location, byte[] bytes) throws IOException {
        creates.create(location, bytes);
      }
    };
  }

  /**
   * For Azure emulators, which cannot rename (Azurite), as the fork's atomic create on ADLS does: a
   * create makes the Append Blob in place, empty and only if none is there, then appends its bytes
   * through the fork, whose APPEND from a snapshot that has seen nothing looks the blob up first. So
   * a create is an empty Append Blob, a HEAD and an Append Block, where Azure's is a staged Append
   * Blob, an Append Block and a rename. Appends are the fork's.
   */
  public static RegionAppender appendBlobs(AdlsClients clients, SupportsAtomicOperations adls) {
    RegionAppender atomic = new AtomicAppender(adls);
    return new RegionAppender() {
      @Override
      public long append(String location, long expectedLength, byte[] bytes) throws IOException {
        return atomic.append(location, expectedLength, bytes);
      }

      @Override
      public void create(String location, byte[] bytes) throws IOException {
        try {
          clients.appendBlob(location).create();
        } catch (BlobStorageException e) {
          throw new IOException("Create " + location + " failed: " + e.getErrorCode(), e);
        }
        atomic.append(location, 0, bytes);
      }
    };
  }
}
