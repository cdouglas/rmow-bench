package org.rmow.bench.fuzz;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.bench.BenchConfig;

/**
 * The fuzzer's draws are configurations the runner accepts, so a failing seed is a finding about
 * the arms, not about the draw; and a few seeds pass end to end, twins included.
 */
class FuzzTest {

  @Test
  void drawsAreValidConfigurations() {
    Set<String> armsSeen = new HashSet<>();
    int twins = 0;
    int sorted = 0;
    for (long seed = 1; seed <= 2_000; seed++) {
      Fuzz.Trial t = Fuzz.draw(seed);
      BenchConfig c = t.cfg();
      c.workload.validate();
      assertThat(c.arms).isNotEmpty();
      armsSeen.addAll(c.arms);
      if (c.layout.rmorRewriteShare != 0) {
        assertThat(c.arms).doesNotContainAnyElementsOf(Fuzz.SHARE_ZERO);
      }
      Set<String> names = new HashSet<>();
      boolean anySorted = false;
      for (BenchConfig.ReaderSpec r : c.reads.readers) {
        assertThat(names.add(r.name)).as("reader names are unique").isTrue();
        if (r.skipDeadRowGroups || r.sortedRead) {
          assertThat(r.footerCache).isTrue();
        }
        anySorted |= r.sortedRead;
      }
      if (anySorted) {
        sorted++;
        assertThat(c.layout.sortWrites).isTrue();
        assertThat(c.workload.initialJitterRows).isZero();
        assertThat(c.arms).noneMatch(a -> a.endsWith("-split"));
      }
      if (c.reads.windows != null) {
        assertThat(c.reads.windows.fraction * c.reads.windows.count).isLessThanOrEqualTo(1);
      }
      // no batch changes more rows than a quarter of the table, nor deletes half of it
      assertThat(c.workload.updates() + c.workload.deletes())
          .isLessThanOrEqualTo((int) Math.max(1, c.workload.initialRows / 4));
      assertThat((long) c.workload.deletes() * c.workload.batches)
          .isLessThanOrEqualTo(c.workload.initialRows / 2);
      twins += t.twin() ? 1 : 0;
    }
    assertThat(armsSeen).containsExactlyInAnyOrderElementsOf(Fuzz.ARMS);
    assertThat(twins).isBetween(300, 500);
    assertThat(sorted).isGreaterThan(200);
  }

  @Test
  void consecutiveSeedsDrawUnrelatedConfigurations() {
    // a seed scaled by SplittableRandom's own gamma made seed s + 1 draw seed s's stream shifted
    Set<String> armSets = new HashSet<>();
    for (long seed = 1; seed <= 20; seed++) {
      armSets.add(Fuzz.draw(seed).cfg().arms.toString());
    }
    assertThat(armSets).hasSizeGreaterThan(15);
  }

  @Test
  void seedsPassEndToEnd(@TempDir Path dir) throws IOException {
    // small draws: a twin on the other store (17, 20), the local store's footprint check (24)
    for (long seed : new long[] {17, 20, 24}) {
      Fuzz.Outcome o = Fuzz.trial(seed, dir.resolve("scratch"), dir.resolve("out"));
      assertThat(o.ok()).as("seed %d: %s %s", seed, o.kind(), o.message()).isTrue();
    }
  }
}
