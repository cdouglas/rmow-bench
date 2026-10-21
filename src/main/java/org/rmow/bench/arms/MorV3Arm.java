package org.rmow.bench.arms;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;

/**
 * Merge-on-read on a v3 table: each batch's deletes are deletion vectors in one Puffin file, one
 * DV per data file it deletes from. A data file that already has a DV gets a new one merged with
 * the old (read from storage, as Spark's writers do), so every data file has at most one live DV.
 */
public final class MorV3Arm extends MorArm {

  public MorV3Arm(ArmContext ctx) {
    super(ctx);
  }

  @Override
  protected void writeDeletes(int batch, long seq, Map<Integer, List<Integer>> deleted)
      throws IOException {
    writeDVs(batch, seq, deleted);
  }

  @Override
  protected long liveBytes(DeleteFile d) {
    return d.contentSizeInBytes();
  }
}
