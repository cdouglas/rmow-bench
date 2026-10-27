package org.rmow.bench.io;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Spans count exclusive time, and only on a measured thread. */
class SpansTest {

  private static void spin(long ns) {
    long end = System.nanoTime() + ns;
    while (System.nanoTime() < end) {
      Thread.onSpinWait();
    }
  }

  @Test
  void aNestedSpanCountsOnceAndNotInItsParent() {
    Spans.measure();
    try {
      Spans.Totals before = Spans.totals();
      long outerWall;
      Spans.Span inner;
      try (Spans.Span outer = Spans.open(Spans.Kind.ENCODE)) {
        spin(2_000_000);
        inner = Spans.open(Spans.Kind.REQUEST);
        try (inner) {
          spin(3_000_000);
        }
        spin(1_000_000);
        outer.close();
        outerWall = outer.wallNs();
      }
      Spans.Totals d = Spans.totals().minus(before);
      assertThat(inner.wallNs()).isGreaterThanOrEqualTo(3_000_000);
      assertThat(d.wallNs(Spans.Kind.REQUEST)).isEqualTo(inner.wallNs());
      assertThat(d.wallNs(Spans.Kind.ENCODE)).isEqualTo(outerWall - inner.wallNs());
      assertThat(d.wallNs(Spans.Kind.ENCODE)).isGreaterThanOrEqualTo(3_000_000);
      // spinning is on the CPU
      assertThat(d.cpuNs(Spans.Kind.REQUEST)).isPositive();
      assertThat(d.cpuNs(Spans.Kind.ENCODE) + d.cpuNs(Spans.Kind.REQUEST))
          .isLessThanOrEqualTo(outerWall + 1_000_000);
    } finally {
      Spans.unmeasure();
    }
  }

  @Test
  void aSpanOfTheSameKindInsideAnotherCountsOnce() {
    Spans.measure();
    try {
      Spans.Totals before = Spans.totals();
      long wall;
      try (Spans.Span outer = Spans.open(Spans.Kind.FOOTER)) {
        try (Spans.Span inner = Spans.open(Spans.Kind.FOOTER)) {
          spin(1_000_000);
        }
        outer.close();
        wall = outer.wallNs();
      }
      assertThat(Spans.totals().minus(before).wallNs(Spans.Kind.FOOTER)).isEqualTo(wall);
    } finally {
      Spans.unmeasure();
    }
  }

  @Test
  void anUnmeasuredThreadCountsNothing() {
    Spans.Totals before = Spans.totals();
    Spans.Span s = Spans.open(Spans.Kind.DECODE);
    try (s) {
      spin(1_000_000);
    }
    assertThat(s.wallNs()).isZero();
    assertThat(Spans.totals().minus(before).wallNs(Spans.Kind.DECODE)).isZero();
  }
}
