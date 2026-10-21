package org.rmow.bench.append;

import java.io.IOException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Appenders for S3 locations ({@code s3://bucket/key}). */
public final class S3Appenders {

  private S3Appenders() {}

  private record Location(String bucket, String key) {
    static Location of(String location) {
      int scheme = location.indexOf("://");
      int slash = location.indexOf('/', scheme + 3);
      if (scheme < 0 || slash < 0) {
        throw new IllegalArgumentException("Not an S3 location: " + location);
      }
      return new Location(location.substring(scheme + 3, slash), location.substring(slash + 1));
    }
  }

  /**
   * S3 Express One Zone: a PutObject at a write offset, the object's committed length. One request,
   * no HEAD: the offset is the condition, and S3 rejects it ({@code InvalidWriteOffset}) unless it
   * is the object's current size. Appends to an object are limited to 10,000 parts.
   */
  public static RegionAppender express(S3Client s3) {
    return (location, expectedLength, bytes) -> {
      Location l = Location.of(location);
      try {
        s3.putObject(
            PutObjectRequest.builder()
                .bucket(l.bucket())
                .key(l.key())
                .writeOffsetBytes(expectedLength)
                .build(),
            RequestBody.fromBytes(bytes));
      } catch (S3Exception e) {
        throw new IOException(
            "Append to " + location + " at " + expectedLength + " failed: "
                + e.awsErrorDetails().errorCode(),
            e);
      }
      return expectedLength + bytes.length;
    };
  }

  /**
   * For emulators without append (versitygw accepts {@code writeOffsetBytes} and ignores it): reads
   * the whole object and writes it back with the bytes appended. Only for testing the rest of the
   * S3 path; pass a client nothing measures, since these requests are not what an append costs.
   */
  public static RegionAppender emulated(S3Client s3) {
    return (location, expectedLength, bytes) -> {
      Location l = Location.of(location);
      byte[] old =
          s3.getObjectAsBytes(GetObjectRequest.builder().bucket(l.bucket()).key(l.key()).build())
              .asByteArray();
      if (old.length != expectedLength) {
        throw new IOException(
            "Append position mismatch: " + location + " is " + old.length + " bytes, expected "
                + expectedLength);
      }
      byte[] all = new byte[old.length + bytes.length];
      System.arraycopy(old, 0, all, 0, old.length);
      System.arraycopy(bytes, 0, all, old.length, bytes.length);
      s3.putObject(
          PutObjectRequest.builder().bucket(l.bucket()).key(l.key()).build(),
          RequestBody.fromBytes(all));
      return all.length;
    };
  }
}
