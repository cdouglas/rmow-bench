package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

class DeletedRangesTest {

  /** An index as Iceberg loads a deletion vector: a bitmap of the positions. */
  private static PositionDeleteIndex index(LongStream positions) {
    List<Long> ps = positions.boxed().toList();
    return Deletes.toPositionIndex(CloseableIterable.withNoopClose(ps));
  }

  /** The check as it was: every position looked up. */
  private static boolean byPosition(PositionDeleteIndex deleted, long first, long count) {
    for (long p = first; p < first + count; p++) {
      if (!deleted.isDeleted(p)) {
        return false;
      }
    }
    return true;
  }

  @Test
  void aRowGroupIsDeadOnlyWhenEveryRowIs() {
    // row groups of 1,000 rows: the first deleted whole, the second but one row, the third at its
    // two ends only, the fourth not at all
    PositionDeleteIndex deleted =
        index(
            Stream.of(
                    LongStream.range(0, 1000),
                    LongStream.range(1000, 1500),
                    LongStream.range(1501, 2000),
                    LongStream.of(2000, 2999))
                .flatMapToLong(s -> s));
    DeletedRanges ranges = new DeletedRanges(deleted);
    assertThat(ranges.coversAll(0, 1000)).isTrue();
    assertThat(ranges.coversAll(1000, 1000)).isFalse();
    assertThat(ranges.coversAll(2000, 1000)).isFalse();
    assertThat(ranges.coversAll(3000, 1000)).isFalse();
    assertThat(ranges.coversAll(0, 0)).isTrue();
  }

  @Test
  void rangesAcrossTheBitmapsKeys() {
    long key = 1L << 32;
    DeletedRanges ranges = new DeletedRanges(index(LongStream.range(key - 5, key + 5)));
    assertThat(ranges.coversAll(key - 5, 10)).isTrue();
    assertThat(ranges.coversAll(key - 6, 11)).isFalse();
    assertThat(ranges.coversAll(key - 5, 11)).isFalse();
    assertThat(ranges.coversAll(2 * key, 1)).isFalse();
  }

  @Test
  void agreesWithEveryPositionLookedUp() {
    Random random = new Random(20261102);
    for (int t = 0; t < 200; t++) {
      // runs, some as long as a row group, and scattered rows, over 20,000 rows
      Set<Long> positions = new TreeSet<>();
      for (int run = random.nextInt(6); run > 0; run--) {
        long start = random.nextInt(20_000);
        long length = random.nextBoolean() ? random.nextInt(5_000) : random.nextInt(50);
        for (long p = start; p < start + length; p++) {
          positions.add(p);
        }
      }
      for (int row = random.nextInt(300); row > 0; row--) {
        positions.add((long) random.nextInt(20_000));
      }
      PositionDeleteIndex deleted = index(positions.stream().mapToLong(Long::longValue));
      DeletedRanges ranges = new DeletedRanges(deleted);
      for (int q = 0; q < 100; q++) {
        long first = random.nextInt(20_000);
        long count = 1 + random.nextInt(random.nextBoolean() ? 5_000 : 20);
        assertThat(ranges.coversAll(first, count))
            .as("[%d, %d) of %s", first, first + count, positions)
            .isEqualTo(byPosition(deleted, first, count));
      }
    }
  }

  @Test
  void anIndexThatCannotSerializeIsCheckedRowByRow() {
    Set<Long> positions = new HashSet<>();
    LongStream.range(100, 200).forEach(positions::add);
    positions.remove(150L);
    PositionDeleteIndex deleted =
        new PositionDeleteIndex() {
          @Override
          public void delete(long position) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void delete(long posStart, long posEnd) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isDeleted(long position) {
            return positions.contains(position);
          }

          @Override
          public boolean isEmpty() {
            return positions.isEmpty();
          }
        };
    DeletedRanges ranges = new DeletedRanges(deleted);
    assertThat(ranges.coversAll(100, 50)).isTrue();
    assertThat(ranges.coversAll(100, 100)).isFalse();
    assertThat(ranges.coversAll(151, 49)).isTrue();
  }
}
