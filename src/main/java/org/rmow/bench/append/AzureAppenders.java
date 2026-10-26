package org.rmow.bench.append;

import com.azure.core.util.Context;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.iceberg.io.FileIO;
import org.rmow.bench.io.AdlsClients;

/** Appenders for Azure locations ({@code abfss://container@account.dfs.core.windows.net/path}). */
public final class AzureAppenders {

  private AzureAppenders() {}

  /**
   * Append Blobs. An append is one {@code AppendBlock} conditioned on the blob's committed length
   * ({@code x-ms-blob-condition-appendpos}): the service rejects it ({@code
   * AppendPositionConditionNotMet}) unless the blob is exactly that long. A region and footer
   * larger than a block (100 MiB on current service versions) take several blocks, in order, so the
   * footer lands last and no reader sees it before its regions.
   *
   * <p>{@code AppendBlock} only extends an Append Blob ({@code InvalidBlobType} otherwise), and a
   * FileIO's output stream writes a block blob. So RMoW creates its files here: an empty Append
   * Blob, then its first version appended.
   */
  public static RegionAppender appendBlob(AdlsClients clients) {
    return new RegionAppender() {
      @Override
      public long append(String location, long expectedLength, byte[] bytes) throws IOException {
        return appendBlocks(clients.appendBlob(location), location, expectedLength, bytes);
      }

      @Override
      public void create(FileIO io, String location, byte[] bytes) throws IOException {
        AppendBlobClient blob = clients.appendBlob(location);
        try {
          blob.create(true);
        } catch (BlobStorageException e) {
          throw new IOException("Create " + location + " failed: " + e.getErrorCode(), e);
        }
        appendBlocks(blob, location, 0, bytes);
      }
    };
  }

  private static long appendBlocks(AppendBlobClient blob, String location, long at, byte[] bytes)
      throws IOException {
    int max = (int) Math.min(Integer.MAX_VALUE, blob.getMaxAppendBlockBytes());
    long pos = at;
    for (int off = 0; off < bytes.length; off += max) {
      int n = Math.min(max, bytes.length - off);
      try {
        blob.appendBlockWithResponse(
            new ByteArrayInputStream(bytes, off, n),
            n,
            null,
            new AppendBlobRequestConditions().setAppendPosition(pos),
            null,
            Context.NONE);
      } catch (BlobStorageException e) {
        throw new IOException(
            "Append to " + location + " at " + pos + " failed: " + e.getErrorCode(), e);
      }
      pos += n;
    }
    return pos;
  }
}
