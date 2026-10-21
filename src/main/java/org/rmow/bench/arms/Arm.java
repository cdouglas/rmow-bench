package org.rmow.bench.arms;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.workload.OpLog;

/**
 * One way of absorbing the op log into storage. Every arm starts from the same initial-load bytes
 * (copied to its own prefix), replays the same batches, and must read back what the {@link
 * org.rmow.bench.oracle.ReferenceModel} says.
 */
public interface Arm {

  String name();

  /** Adopts this arm's copies of the initial-load files. Setup: not measured. */
  void load(List<InitialLoad.InitialFile> files) throws IOException;

  /** Applies one batch; times its parts through the context's {@code Phases}. */
  void apply(OpLog.Batch batch) throws IOException;

  /**
   * Compacts what the layout's compaction triggers select (the runner times it as {@code
   * compact}). Rewritten objects stay stored, for time travel, and count as reclaimable.
   */
  void compact() throws IOException;

  /** Reads the current table; {@code filter} skips files and row groups and is applied to rows. */
  TableHash read(Expression filter, Reader reader) throws IOException;

  /** Reads the table as it was after {@code steps} batches (0: the initial load alone). */
  TableHash readAfter(int steps, Reader reader) throws IOException;

  ArmStats stats();
}
