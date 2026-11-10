package org.rmow.bench.micro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/** The append spike's check of a blob's frames: it must fail every way a blob can be wrong. */
class AzureAppendSpikeTest {

  private static final int LEVEL = 7;
  private static final int BYTES = 64;

  private static byte[] frame(int writer, long seq) {
    byte[] b = new byte[BYTES];
    AzureAppendSpike.frame(b, LEVEL, writer, seq, new SplittableRandom(writer * 1000L + seq));
    return b;
  }

  /** Frames for (writer, seq) pairs, in order, and the offsets their replies would name. */
  private static Map<String, Object> check(byte[] blob, Map<Long, Long> acked, int blocks) {
    AzureAppendSpike.FrameCheck check = new AzureAppendSpike.FrameCheck(LEVEL, BYTES, acked);
    // in uneven pieces, as a download delivers them
    for (int off = 0; off < blob.length; off += 37) {
      check.write(blob, off, Math.min(37, blob.length - off));
    }
    return check.result(blob.length, blocks);
  }

  private static byte[] concat(byte[]... frames) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] f : frames) {
      out.writeBytes(f);
    }
    return out.toByteArray();
  }

  @Test
  void aBlobOfAcknowledgedFramesPasses() {
    byte[] blob = concat(frame(0, 0), frame(1, 0), frame(0, 1));
    Map<Long, Long> acked = new HashMap<>();
    acked.put(AzureAppendSpike.key(0, 0), 0L);
    acked.put(AzureAppendSpike.key(1, 0), (long) BYTES);
    acked.put(AzureAppendSpike.key(0, 1), 2L * BYTES);
    Map<String, Object> r = check(blob, acked, 3);
    assertThat(r).containsEntry("ok", true).containsEntry("matched", 3L).containsEntry("frames", 3L);
  }

  @Test
  void aTornFrameFails() {
    byte[] blob = concat(frame(0, 0), frame(1, 0));
    blob[BYTES + 30] ^= 1; // one bit of the second frame's filler
    Map<Long, Long> acked = new HashMap<>();
    acked.put(AzureAppendSpike.key(0, 0), 0L);
    acked.put(AzureAppendSpike.key(1, 0), (long) BYTES);
    Map<String, Object> r = check(blob, acked, 2);
    assertThat(r).containsEntry("ok", false).containsEntry("bad", 1L).containsEntry("missing", 1L);
  }

  @Test
  void aDuplicateFails() {
    byte[] blob = concat(frame(0, 0), frame(0, 0));
    Map<Long, Long> acked = Map.of(AzureAppendSpike.key(0, 0), 0L);
    Map<String, Object> r = check(blob, acked, 2);
    assertThat(r).containsEntry("ok", false).containsEntry("duplicates", 1L);
  }

  @Test
  void aFrameAwayFromItsAcknowledgedOffsetFails() {
    byte[] blob = concat(frame(1, 0), frame(0, 0));
    Map<Long, Long> acked = new HashMap<>();
    acked.put(AzureAppendSpike.key(0, 0), 0L); // its reply said 0; it is at 64
    acked.put(AzureAppendSpike.key(1, 0), (long) BYTES);
    Map<String, Object> r = check(blob, acked, 2);
    assertThat(r).containsEntry("ok", false).containsEntry("misplaced", 2L);
  }

  @Test
  void anAcknowledgedAppendThatIsAbsentFails() {
    byte[] blob = frame(0, 0);
    Map<Long, Long> acked = new HashMap<>();
    acked.put(AzureAppendSpike.key(0, 0), 0L);
    acked.put(AzureAppendSpike.key(0, 1), (long) BYTES);
    Map<String, Object> r = check(blob, acked, 1);
    assertThat(r).containsEntry("ok", false).containsEntry("missing", 1L);
  }

  @Test
  void anUnacknowledgedLandingIsCountedNotFailed() {
    // a request that failed ambiguously, but landed
    byte[] blob = concat(frame(0, 0), frame(0, 1));
    Map<Long, Long> acked = Map.of(AzureAppendSpike.key(0, 1), (long) BYTES);
    Map<String, Object> r = check(blob, acked, 2);
    assertThat(r).containsEntry("ok", true).containsEntry("unacked_landed", 1L);
  }

  @Test
  void blocksThatAreNotFramesFail() {
    // the service's count of blocks disagrees with the frames read: a block held two frames
    byte[] blob = concat(frame(0, 0), frame(0, 1));
    Map<Long, Long> acked = new HashMap<>();
    acked.put(AzureAppendSpike.key(0, 0), 0L);
    acked.put(AzureAppendSpike.key(0, 1), (long) BYTES);
    assertThat(check(blob, acked, 1)).containsEntry("ok", false);
    // and a trailing partial frame
    byte[] torn = concat(blob, new byte[10]);
    assertThat(check(torn, acked, 2)).containsEntry("ok", false).containsEntry("tail_bytes", 10);
  }
}
