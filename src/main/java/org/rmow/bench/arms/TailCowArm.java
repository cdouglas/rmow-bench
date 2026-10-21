package org.rmow.bench.arms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.Record;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * Plain Iceberg (v3) with tail merging: the baseline that separates what merging the tail buys
 * from what a single appendable object buys. The same {@link TailPolicy} as RMoW picks the open
 * tail units to merge, but here a unit is a small data file: the batch reads the picked files back
 * (their DVs applied) and writes them, with its inserts and updated rows, as one new data file
 * that replaces them. Updates and deletes of rows in any other file get DVs, as in {@link
 * MorV3Arm}.
 */
public final class TailCowArm extends MorArm {

  private final TailPolicy policy;
  private double bytesPerRow = 40; // running estimate, for planning merges

  public TailCowArm(ArmContext ctx, TailPolicy.Kind kind) {
    super(ctx);
    this.policy = new TailPolicy(kind, ctx.layout().tailSealBytes, ctx.layout().tierFanout);
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    long seq = batchSeq(b);
    locate(b);
    TailPolicy.Plan plan =
        b.insertIds().length > 0
            ? policy.plan((long) (b.insertIds().length * bytesPerRow))
            : new TailPolicy.Plan(List.of(), 0);
    Set<Integer> merge = new HashSet<>();
    for (TailPolicy.Unit u : plan.merge()) {
      merge.add((int) u.id());
    }

    // rows of merged files are dropped by the merge; rows elsewhere get DVs
    Set<Long> replaced = new HashSet<>();
    Map<Integer, List<Integer>> dvPositions = new HashMap<>();
    for (long id : b.updateIds()) {
      mark(id, merge, replaced, dvPositions);
    }
    for (long id : b.deleteIds()) {
      mark(id, merge, replaced, dvPositions);
      fileOfId[(int) id] = -1;
    }

    Map<Integer, List<Record>> mergedRows = new HashMap<>();
    if (!merge.isEmpty()) {
      ctx.phases()
          .run(
              "merge-read",
              () -> {
                for (int k : merge) {
                  mergedRows.put(k, readLive(k));
                }
              });
    }
    ctx.phases()
        .run(
            "write",
            () -> {
              List<long[]> rows = batchRows(b);
              for (List<Record> rs : mergedRows.values()) {
                for (Record r : rs) {
                  long id = (Long) r.get(Rows.ID);
                  if (!replaced.contains(id)) {
                    rows.add(new long[] {id, (Integer) r.get(Rows.VERSION)});
                  }
                }
              }
              int k = writeDataFile(batchName(b), rows, seq);
              for (int m : merge) {
                retire(m);
              }
              if (k >= 0 && b.insertIds().length > 0) {
                long bytes = files.get(k).fileSizeInBytes();
                bytesPerRow = bytes / (double) rows.size();
                policy.commit(plan, k, bytes);
              }
            });
    if (!dvPositions.isEmpty()) {
      ctx.phases().run("delete-write", () -> writeDVs(b.index(), seq, dvPositions));
    }
    endBatch();
  }

  private void mark(
      long id, Set<Integer> merge, Set<Long> replaced, Map<Integer, List<Integer>> dvPositions) {
    int k = fileOfId[(int) id];
    removed(k);
    if (merge.contains(k)) {
      replaced.add(id);
    } else {
      dvPositions.computeIfAbsent(k, x -> new ArrayList<>()).add(posOfId[(int) id]);
    }
  }

  /** The open tail files are the policy's to merge; compaction leaves them alone. */
  @Override
  protected boolean compactable(int k) {
    return !policy.isOpen(k);
  }

  @Override
  protected void writeDeletes(int batch, long seq, Map<Integer, List<Integer>> deleted) {
    throw new UnsupportedOperationException("TailCoW writes its deletes in apply");
  }

  @Override
  protected long liveBytes(DeleteFile d) {
    return d.contentSizeInBytes();
  }
}
