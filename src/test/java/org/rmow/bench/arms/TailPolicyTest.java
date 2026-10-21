package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TailPolicyTest {

  /** Runs {@code batches} batches of {@code bytes} each; returns the bytes each batch writes. */
  private static long[] simulate(TailPolicy p, int batches, long bytes) {
    long[] written = new long[batches];
    for (int b = 0; b < batches; b++) {
      TailPolicy.Plan plan = p.plan(bytes);
      long merged = bytes + plan.merge().stream().mapToLong(TailPolicy.Unit::bytes).sum();
      p.commit(plan, b, merged);
      written[b] = merged;
    }
    return written;
  }

  @Test
  void appendNeverMerges() {
    TailPolicy p = new TailPolicy(TailPolicy.Kind.A, 1000, 2);
    assertThat(simulate(p, 5, 100)).containsOnly(100);
    assertThat(p.open()).isEmpty();
  }

  @Test
  void trailingMergeRewritesTheTailUntilSealed() {
    TailPolicy p = new TailPolicy(TailPolicy.Kind.B, 400, 2);
    // the tail grows 100, 200, 300, 400 (sealed), then starts again
    assertThat(simulate(p, 6, 100)).containsExactly(100, 200, 300, 400, 100, 200);
    assertThat(p.open()).extracting(TailPolicy.Unit::bytes).containsExactly(200L);
  }

  @Test
  void tieredMergesLikeABinaryCounter() {
    TailPolicy p = new TailPolicy(TailPolicy.Kind.C, 1 << 20, 2);
    // fanout 2: 1, 2, 1, 4, 1, 2, 1, 8 units of 100 bytes
    assertThat(simulate(p, 8, 100)).containsExactly(100, 200, 100, 400, 100, 200, 100, 800);
    assertThat(p.open()).hasSize(1);
    assertThat(p.open().get(0).level()).isEqualTo(3);
  }

  @Test
  void tieredSealsUnitsThatCannotMergeAgain() {
    TailPolicy p = new TailPolicy(TailPolicy.Kind.C, 250, 2);
    // two 200-byte units would make 400, past the seal size: each 200 is sealed as it is written
    long[] w = simulate(p, 6, 100);
    assertThat(w).containsExactly(100, 200, 100, 200, 100, 200);
    assertThat(p.open()).isEmpty();
  }

  @Test
  void replaceKeepsPlaceAndLevel() {
    TailPolicy p = new TailPolicy(TailPolicy.Kind.C, 1 << 20, 4);
    // fanout 4: three level-0 units stay open
    simulate(p, 3, 100);
    p.replace(1, 99, 50);
    assertThat(p.open())
        .containsExactly(
            new TailPolicy.Unit(0, 0, 100),
            new TailPolicy.Unit(99, 0, 50),
            new TailPolicy.Unit(2, 0, 100));
    p.replace(99, 100, 0);
    assertThat(p.open()).extracting(TailPolicy.Unit::id).isEqualTo(List.of(0L, 2L));
  }
}
