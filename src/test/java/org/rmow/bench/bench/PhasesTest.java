package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.rmow.bench.io.Held;
import org.rmow.bench.io.IoTrace;

/** A phase run in parts is one phase: measurements added, peaks the larger, at its first part. */
class PhasesTest {

  @Test
  void aPhaseRunInPartsIsRecordedOnce() throws IOException {
    Phases phases = new Phases(new IoTrace());
    phases.run(
        "batch",
        () -> {
          phases.part("merge-read", () -> Held.pulse(Held.Kind.CHUNKS, 30, "a row group"));
          phases.part("write", () -> Held.pulse(Held.Kind.WRITER, 50, "its region"));
          phases.part("merge-read", () -> Held.pulse(Held.Kind.CHUNKS, 20, "another"));
          phases.part("write", () -> Held.pulse(Held.Kind.WRITER, 10, "its region"));
        });
    List<Phases.Measured> m = phases.drain();

    assertThat(m).extracting(Phases.Measured::phase)
        .containsExactly("merge-read", "write", "batch");
    assertThat(m.get(0).held().max(Held.Kind.CHUNKS)).isEqualTo(30);
    assertThat(m.get(1).held().max(Held.Kind.WRITER)).isEqualTo(50);
    assertThat(m.get(2).held().maxBytes()).isEqualTo(50);
    assertThat(m.get(0).wallNs() + m.get(1).wallNs()).isLessThanOrEqualTo(m.get(2).wallNs());
  }
}
