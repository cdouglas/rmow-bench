package org.rmow.bench.arms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.api.Test;
import org.rmow.bench.append.RegionAppender;
import org.rmow.bench.append.AtomicAppender;
import org.rmow.bench.bench.Phases;
import org.rmow.bench.io.IoTrace;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.io.TracingFileIO;
import org.rmow.bench.oracle.ReferenceModel;
import org.rmow.bench.rmow.FooterCodec;
import org.rmow.bench.rmow.LogicalFile;
import org.rmow.bench.rmow.SplitFile;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.OpLogGenerator;
import org.rmow.bench.workload.WorkloadSpec;

/**
 * RMoW compaction's two actions, each with the other's trigger off, on a recency-skewed run under
 * policy A (every batch's inserts are a new sealed row group): evacuating a split file's mostly
 * dead objects (space) and packing small sealed row groups (reads). The batches after compaction
 * locate their rows by scanning, checked against the arm's index, and every step's read is
 * checked against the reference model.
 */
class RmowCompactionTest {

  private static final Reader READER = new Reader("one-footer", true, false, 64 << 10, 128L << 20, null);

  private static final class Run {
    final OpLog log;
    final ReferenceModel model;
    final RmowArm arm;
    int next = 0;

    Run(LayoutSpec layout, boolean split) throws IOException {
      WorkloadSpec w = new WorkloadSpec();
      w.initialRows = 30_000;
      w.batches = 16;
      w.opsPerBatch = 1_000;
      log = OpLogGenerator.generate(w);
      model = new ReferenceModel(log);
      MemFileIO mem = new MemFileIO();
      RegionAppender appender = new AtomicAppender(mem);
      IoTrace trace = new IoTrace();
      ArmContext ctx =
          new ArmContext(
              "t",
              "mem://t/arm",
              new TracingFileIO(mem, appender, trace),
              mem,
              new Phases(trace),
              layout,
              log,
              true);
      arm = new RmowArm(ctx, TailPolicy.Kind.A, split);
      arm.load(
          InitialLoad.copy(
              mem, InitialLoad.write(mem, "mem://t/base", log, layout), ctx.root(), appender));
    }

    void batches(int n) throws IOException {
      for (int end = next + n; next < end; next++) {
        OpLog.Batch b = log.batches().get(next);
        arm.apply(b);
        model.apply(b);
        assertThat(arm.read(Expressions.alwaysTrue(), READER)).isEqualTo(model.expected());
      }
    }
  }

  private static LayoutSpec layout() {
    LayoutSpec layout = new LayoutSpec();
    layout.rowGroupBytes = 128 << 10;
    layout.targetFileBytes = 1 << 20;
    layout.locate = "scan";
    return layout;
  }

  /** Each object's dead fraction of its length, by object, over the objects a version reads. */
  private static Map<String, Double> deadFractions(SplitFile f) {
    Map<String, Long> live = new HashMap<>();
    for (long rg : f.rgIds()) {
      live.merge(f.objectOf(rg), FooterCodec.liveBytes(f.rowGroup(rg)), Long::sum);
    }
    Map<String, Double> dead = new HashMap<>();
    live.forEach((o, l) -> dead.put(o, 1 - l / (double) f.objectLength(o)));
    return dead;
  }

  @Test
  void splitFilesEvacuateMostlyDeadObjects() throws IOException {
    LayoutSpec layout = layout();
    layout.compactGarbageRatio = 0.3;
    layout.compactSmallRowGroups = 0;
    Run run = new Run(layout, true);
    run.batches(8);
    List<LogicalFile> before = run.arm.files();
    long objectsBefore = before.stream().mapToLong(f -> f.objects().size()).sum();
    assertThat(before)
        .as("updates left some object at least 30% dead")
        .anyMatch(f -> deadFractions((SplitFile) f).values().stream().anyMatch(d -> d >= 0.3));
    Map<LogicalFile, Long> garbage = new HashMap<>();
    before.forEach(f -> garbage.put(f, f.garbageBytes()));

    run.arm.compact();

    // same files, each at most one version later; no object it reads is 30% dead any more
    List<LogicalFile> after = run.arm.files();
    assertThat(after).containsExactlyElementsOf(before);
    long written = 0;
    for (LogicalFile f : after) {
      assertThat(deadFractions((SplitFile) f).values()).allMatch(d -> d < 0.3);
      assertThat(f.garbageBytes()).isLessThanOrEqualTo(garbage.get(f));
      written += f.objects().size();
    }
    assertThat(written - objectsBefore).as("a region and a footer object per moved file").isEven();
    assertThat(run.arm.read(Expressions.alwaysTrue(), READER)).isEqualTo(run.model.expected());
    run.batches(8);
  }

  @Test
  void packingMergesSmallSealedRowGroupsUpToTheWritersSize() throws IOException {
    for (boolean split : new boolean[] {false, true}) {
      LayoutSpec layout = layout();
      layout.compactGarbageRatio = 2; // never
      layout.compactSmallRowGroups = 4;
      Run run = new Run(layout, split);
      run.batches(10);
      long target = run.arm.packBytes();
      assertThat(target).isLessThan(layout.rowGroupBytes); // the writer's target counts buffers
      int rowGroups = rowGroups(run.arm);
      long smallBefore = small(run.arm, target);
      assertThat(smallBefore).as("one small row group of inserts per batch").isGreaterThan(4);

      run.arm.compact();

      assertThat(rowGroups(run.arm)).isLessThan(rowGroups);
      assertThat(small(run.arm, target)).isLessThan(smallBefore);
      for (LogicalFile f : run.arm.files()) {
        for (RowGroup rg : f.footer().getRow_groups()) {
          // packed ones within re-encoding noise of the sum of theirs
          assertThat(FooterCodec.compressedSize(rg)).isLessThan(target * 11 / 10);
        }
      }
      assertThat(run.arm.read(Expressions.alwaysTrue(), READER)).isEqualTo(run.model.expected());
      run.batches(6);
    }
  }

  private static int rowGroups(RmowArm arm) {
    return arm.files().stream().mapToInt(f -> f.footer().getRow_groupsSize()).sum();
  }

  private static long small(RmowArm arm, long target) {
    return arm.files().stream()
        .flatMap(f -> f.footer().getRow_groups().stream())
        .filter(rg -> FooterCodec.compressedSize(rg) < target / 2)
        .count();
  }
}
