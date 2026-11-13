package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.OpLogGenerator;
import org.rmow.bench.workload.WorkloadSpec;

/** The event-time merge's index: where an insert goes, and how a grown row group is cut. */
class EventTimeRegionsTest {

  /** An op log whose initial rows have event times 0, 10, 20, ... (id times the step). */
  private static OpLog log() {
    WorkloadSpec w = new WorkloadSpec();
    w.initialRows = 1_000;
    w.batches = 1;
    w.opsPerBatch = 10;
    w.insertFrac = 1;
    w.updateFrac = 0;
    w.deleteFrac = 0;
    w.initialJitterRows = 0;
    w.tsStepMicros = 10;
    return OpLogGenerator.generate(w);
  }

  private static List<long[]> rows(long from, long to) {
    List<long[]> out = new ArrayList<>();
    for (long id = from; id < to; id++) {
      out.add(new long[] {id, 0});
    }
    return out;
  }

  @Test
  void anInsertJoinsTheRowGroupThatHoldsItsEventTimeOrTheOneBefore() {
    OpLog log = log();
    EventTimeRegions index = new EventTimeRegions(100);
    index.add(1, rows(0, 100), log); // 0..990
    index.add(2, rows(200, 300), log); // 2000..2990
    assertThat(index.of(500).key()).isEqualTo(1);
    assertThat(index.of(1500).key()).isEqualTo(1); // the gap: the row group before
    assertThat(index.of(2000).key()).isEqualTo(2);
    assertThat(index.of(9000).key()).isEqualTo(2); // the newest
    assertThat(index.newest().key()).isEqualTo(2);
    index.remove(1);
    assertThat(index.of(500).key()).isEqualTo(2); // before the first: the first
    assertThat(new EventTimeRegions(100).of(5)).isNull();
    assertThat(index.assign(new long[] {}, log)).isEmpty();
  }

  @Test
  void overlappingRowGroupsAreRefused() {
    OpLog log = log();
    EventTimeRegions index = new EventTimeRegions(100);
    index.add(1, rows(100, 200), log);
    assertThatThrownBy(() -> index.add(2, rows(150, 250), log))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> index.add(3, rows(50, 101), log))
        .isInstanceOf(IllegalStateException.class);
    index.add(4, rows(200, 210), log); // next to it
    index.add(5, rows(0, 100), log);
    assertThat(index.regions()).extracting(EventTimeRegions.Region::key).containsExactly(5L, 1L, 4L);
  }

  @Test
  void theNewestIsCutIntoFullRowGroupsAndTheRest() {
    OpLog log = log();
    EventTimeRegions index = new EventTimeRegions(100);
    List<List<long[]>> pieces = index.pieces(rows(0, 250), log, true);
    assertThat(pieces).extracting(List::size).containsExactly(100, 100, 50);
    assertThat(index.pieces(rows(0, 100), log, true)).extracting(List::size).containsExactly(100);
  }

  @Test
  void anOlderRowGroupIsCutOnlyOnceItHoldsTwoFullOnesInEqualParts() {
    OpLog log = log();
    EventTimeRegions index = new EventTimeRegions(100);
    assertThat(index.pieces(rows(0, 199), log, false)).extracting(List::size).containsExactly(199);
    assertThat(index.pieces(rows(0, 200), log, false)).extracting(List::size).containsExactly(100, 100);
    assertThat(index.pieces(rows(0, 350), log, false)).extracting(List::size)
        .containsExactly(116, 117, 117);
  }

  @Test
  void piecesAreInEventTimeOrderAndApart() {
    OpLog log = log();
    EventTimeRegions index = new EventTimeRegions(100);
    List<long[]> shuffled = rows(0, 300);
    java.util.Collections.reverse(shuffled);
    long last = Long.MIN_VALUE;
    for (List<long[]> piece : index.pieces(shuffled, log, true)) {
      long[] range = EventTimeRegions.range(piece, log);
      assertThat(range[0]).isGreaterThan(last);
      last = range[1];
    }
    // every insert lands somewhere, by key
    Map<Long, List<Long>> placed = new EventTimeRegions(100).assign(new long[] {1, 2}, log);
    assertThat(placed).containsOnlyKeys(-1L);
  }
}
