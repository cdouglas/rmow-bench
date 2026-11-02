package org.rmow.bench.arms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.roaringbitmap.RoaringBitmap;

/**
 * Whether a file's deletes cover a range of its positions whole, as a skipping reader or writer
 * asks of each row group: at the cost of the bitmap's containers in the range, not of its
 * positions. A row group RMoR rewrote is one run in its file's deletion vector, which Iceberg
 * writes run-length encoded, so the check costs a few container lookups where testing each
 * position cost one lookup per row (about 514,000 for a 16 MiB row group).
 *
 * <p>Iceberg's {@link PositionDeleteIndex} answers only position by position. Its bitmaps are
 * private, so they are read back from the index's {@link PositionDeleteIndex#serialize()} (a
 * deletion vector's own blob), once per index and only when a row group's two ends are deleted:
 * a row group with a live end is decided by those two lookups, so a file whose deletes are
 * scattered rows (MoR's) is never serialized. An index that cannot be serialized is checked
 * position by position.
 */
final class DeletedRanges {
  private final PositionDeleteIndex deleted;
  private RoaringBitmap[] bitmaps; // by a position's high 32 bits; null until a range needs them
  private boolean unserializable;

  DeletedRanges(PositionDeleteIndex deleted) {
    this.deleted = deleted;
  }

  /** Whether every position in {@code [first, first + count)} is deleted. */
  boolean coversAll(long first, long count) {
    if (count <= 0) {
      return true;
    }
    long last = first + count - 1;
    if (!deleted.isDeleted(first) || !deleted.isDeleted(last)) {
      return false;
    }
    RoaringBitmap[] bs = bitmaps();
    if (bs == null) {
      for (long p = first + 1; p < last; p++) {
        if (!deleted.isDeleted(p)) {
          return false;
        }
      }
      return true;
    }
    for (long key = first >>> 32; key <= last >>> 32; key++) {
      if (key >= bs.length || bs[(int) key] == null) {
        return false;
      }
      long lo = key == first >>> 32 ? first & 0xFFFFFFFFL : 0;
      long hi = key == last >>> 32 ? (last & 0xFFFFFFFFL) + 1 : 1L << 32;
      if (!containsRange(bs[(int) key], lo, hi)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether {@code bitmap} holds every value in {@code [lo, hi)}, 0 <= lo < hi <= 2^32. RoaringBitmap
   * 1.6's range check wraps when the range ends at 2^32 (the end's container key overflows), so
   * that last value is checked on its own.
   */
  private static boolean containsRange(RoaringBitmap bitmap, long lo, long hi) {
    if (hi < 1L << 32) {
      return bitmap.contains(lo, hi);
    }
    return bitmap.contains((int) (hi - 1)) && (lo == hi - 1 || bitmap.contains(lo, hi - 1));
  }

  private RoaringBitmap[] bitmaps() {
    if (bitmaps == null && !unserializable) {
      try {
        bitmaps = parse(deleted.serialize());
      } catch (UnsupportedOperationException e) {
        unserializable = true;
      }
    }
    return bitmaps;
  }

  /**
   * The bitmaps of a serialized index, by key: Iceberg's {@code BitmapPositionDeleteIndex} format
   * (a 4-byte length, a 4-byte magic number, the bitmaps, a CRC), whose bitmaps are {@code
   * RoaringPositionBitmap}'s portable form (an 8-byte count, then each bitmap's 4-byte key and its
   * Roaring serialization, little-endian, keys ascending).
   */
  static RoaringBitmap[] parse(ByteBuffer serialized) {
    ByteBuffer b = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    b.position(b.position() + 8); // the length and the magic number
    long count = b.getLong();
    List<RoaringBitmap> byKey = new ArrayList<>();
    for (long i = 0; i < count; i++) {
      int key = b.getInt();
      RoaringBitmap bitmap = new RoaringBitmap();
      try {
        bitmap.deserialize(b);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      b.position(b.position() + bitmap.serializedSizeInBytes());
      while (byKey.size() < key) {
        byKey.add(null);
      }
      byKey.add(bitmap);
    }
    return byKey.toArray(new RoaringBitmap[0]);
  }
}
