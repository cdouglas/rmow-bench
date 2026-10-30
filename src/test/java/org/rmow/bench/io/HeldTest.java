package org.rmow.bench.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/** What is held: levels by kind, each phase's peaks, and nothing left at an outermost phase's end. */
class HeldTest {

  @Test
  void aPhaseSeesThePeaksOfWhatIsHeldWhileItRuns() {
    Held.Peak outer = Held.open("batch");
    Held.Peak inner = Held.open("write");
    try (Held.Scope rows = Held.scope("rows released");
        Held.Scope chunks = Held.scope("chunks released")) {
      rows.hold(Held.Kind.ROWS, 1_000, "read rows");
      chunks.hold(Held.Kind.CHUNKS, 300, "fetch a row group");
      chunks.release(Held.Kind.CHUNKS, 300, "row group done");
      chunks.hold(Held.Kind.CHUNKS, 200, "fetch the next");
      Held.pulse(Held.Kind.WRITER, 500, "encode");
    }
    Held.close(inner);
    Held.closeOutermost(outer, "batch");

    assertThat(inner.max(Held.Kind.ROWS)).isEqualTo(1_000);
    assertThat(inner.max(Held.Kind.CHUNKS)).isEqualTo(300);
    assertThat(inner.max(Held.Kind.WRITER)).isEqualTo(500);
    // bytes together: the writer's pulse came while the second row group was held
    assertThat(inner.maxBytes()).isEqualTo(700);
    assertThat(outer.maxBytes()).isEqualTo(700);
  }

  @Test
  void partsOfAPhaseTakeTheLargerPeaks() {
    Held.Peak a = Held.open("write");
    Held.pulse(Held.Kind.PENDING, 40, "a region");
    Held.close(a);
    Held.Peak b = Held.open("write");
    Held.pulse(Held.Kind.PENDING, 10, "another");
    Held.pulse(Held.Kind.ROWS, 7, "rows");
    Held.close(b);

    Held.Peak both = a.plus(b);
    assertThat(both.max(Held.Kind.PENDING)).isEqualTo(40);
    assertThat(both.max(Held.Kind.ROWS)).isEqualTo(7);
    assertThat(both.maxBytes()).isEqualTo(40);
  }

  @Test
  void anOutermostPhaseThatLeavesSomethingHeldFails() {
    Held.Peak p = Held.open("batch");
    Held.scope("never closed").hold(Held.Kind.PENDING, 1, "a region");
    assertThatThrownBy(() -> Held.closeOutermost(p, "batch"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("left held");
    // the check reset the levels, so the next phase starts from nothing
    Held.Peak next = Held.open("batch");
    Held.closeOutermost(next, "batch");
    assertThat(next.maxBytes()).isZero();
  }

  @Test
  void theLogRecordsEveryChangeInOrder() {
    Held.logEvents(true);
    try {
      Held.Peak p = Held.open("read_sorted");
      try (Held.Scope s = Held.scope("done")) {
        s.hold(Held.Kind.CHUNKS, 5, "open a run");
        s.hold(Held.Kind.CHUNKS, 3, "open another");
      }
      Held.closeOutermost(p, "read_sorted");
      List<Held.Event> events = Held.drainEvents();
      assertThat(events).extracting(Held.Event::bytes).containsExactly(5L, 8L, 0L);
      assertThat(events).extracting(Held.Event::phase).containsOnly("read_sorted");
    } finally {
      Held.logEvents(false);
    }
  }
}
