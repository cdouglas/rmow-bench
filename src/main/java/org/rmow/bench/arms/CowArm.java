package org.rmow.bench.arms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.Record;
import org.rmow.bench.workload.OpLog;
import org.rmow.bench.workload.Rows;

/**
 * Iceberg copy-on-write: every data file a batch updates or deletes from is rewritten whole, as
 * one new file of its surviving rows and the new versions of its updated rows (as a CoW MERGE
 * writes them); the batch's inserts go to a new data file. No delete files. Against RMoW-A this is
 * the same policy at file granularity instead of row-group granularity.
 */
public final class CowArm extends MorArm {

  public CowArm(ArmContext ctx) {
    super(ctx);
  }

  @Override
  public void apply(OpLog.Batch b) throws IOException {
    long seq = batchSeq(b);
    locate(b);
    Map<Integer, Map<Long, Integer>> touched = new TreeMap<>(); // file -> id -> new version, or -1
    for (int i = 0; i < b.updateIds().length; i++) {
      long id = b.updateIds()[i];
      touched.computeIfAbsent(fileOfId[(int) id], k -> new HashMap<>()).put(id, b.updateVersions()[i]);
      removed(fileOfId[(int) id]);
    }
    for (long id : b.deleteIds()) {
      touched.computeIfAbsent(fileOfId[(int) id], k -> new HashMap<>()).put(id, -1);
      removed(fileOfId[(int) id]);
      fileOfId[(int) id] = -1;
    }

    Map<Integer, List<Record>> rows = new HashMap<>();
    if (!touched.isEmpty()) {
      ctx.phases()
          .run(
              "merge-read",
              () -> {
                for (int k : touched.keySet()) {
                  rows.put(k, readLive(k));
                }
              });
    }
    ctx.phases()
        .run(
            "write",
            () -> {
              int n = 0;
              for (Map.Entry<Integer, Map<Long, Integer>> e : touched.entrySet()) {
                List<long[]> merged = new ArrayList<>();
                for (Record r : rows.get(e.getKey())) {
                  long id = (Long) r.get(Rows.ID);
                  Integer v = e.getValue().get(id);
                  if (v == null) {
                    merged.add(new long[] {id, (Integer) r.get(Rows.VERSION)});
                  } else if (v >= 0) {
                    merged.add(new long[] {id, v});
                  }
                }
                writeDataFile(String.format("%s-%03d", batchName(b), n++), merged, seq);
                retire(e.getKey());
              }
              List<long[]> inserts = new ArrayList<>();
              for (long id : b.insertIds()) {
                inserts.add(new long[] {id, 0});
              }
              writeDataFile(String.format("%s-%03d", batchName(b), n), inserts, seq);
            });
    endBatch();
  }

  @Override
  protected void writeDeletes(int batch, long seq, Map<Integer, List<Integer>> deleted) {
    throw new UnsupportedOperationException("CoW writes no delete files");
  }

  @Override
  protected long liveBytes(DeleteFile d) {
    return 0;
  }
}
