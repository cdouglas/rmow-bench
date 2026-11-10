package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** The cut's pieces ({@link Cut}), against the layout model's {@code fill_gaps} and runs. */
class CutTest {

  private static boolean[] flags(String s) {
    boolean[] f = new boolean[s.length()];
    for (int i = 0; i < f.length; i++) {
      f[i] = s.charAt(i) == 'x';
    }
    return f;
  }

  @Test
  void strideIsIcebergsPageRowLimit() {
    assertThat(Cut.STRIDE_ROWS).isEqualTo(20_000);
    assertThat(Cut.strides(1, Cut.STRIDE_ROWS)).isEqualTo(1);
    assertThat(Cut.strides(20_000, Cut.STRIDE_ROWS)).isEqualTo(1);
    assertThat(Cut.strides(20_001, Cut.STRIDE_ROWS)).isEqualTo(2);
    // a 128 MiB row group of the bench's rows
    assertThat(Cut.strides(4_293_518, Cut.STRIDE_ROWS)).isEqualTo(215);
  }

  @Test
  void noFlaggedStrideIsOnePiece() {
    assertThat(Cut.pieces(flags("......"), 4)).containsExactly(0, 0, 0, 0, 0, 0);
  }

  @Test
  void everyStrideFlaggedIsOnePiece() {
    assertThat(Cut.pieces(flags("xxxx"), 4)).containsExactly(0, 0, 0, 0);
  }

  @Test
  void aShortRowGroupIsOnePiece() {
    assertThat(Cut.pieces(flags("x"), 4)).containsExactly(0);
    assertThat(Cut.pieces(flags("."), 4)).containsExactly(0);
  }

  @Test
  void runsOfFlaggedAndUnflaggedStridesArePieces() {
    // ....xxx.....xx...... : cold, hot, cold, hot, cold
    assertThat(Cut.pieces(flags("....xxx.....xx......"), 4))
        .containsExactly(0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4);
  }

  @Test
  void shortUnflaggedRunsJoinTheFlaggedRunBesideThem() {
    // the gap of 3 between two hot runs joins them; the leading run of 2 joins the first
    assertThat(Cut.pieces(flags("..xx...x......"), 4))
        .containsExactly(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1);
    // with a minimum of 1 nothing joins
    assertThat(Cut.pieces(flags("..xx...x......"), 1))
        .containsExactly(0, 0, 1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 4, 4);
  }

  @Test
  void aShortTrailingRunJoinsTooAsTheModelDoes() {
    // fill_gaps flags every unflagged run shorter than m, at the edges too
    assertThat(Cut.pieces(flags("......x.."), 4)).containsExactly(0, 0, 0, 0, 0, 0, 1, 1, 1);
  }
}
