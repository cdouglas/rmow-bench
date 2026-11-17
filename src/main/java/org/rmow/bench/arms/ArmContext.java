package org.rmow.bench.arms;

import org.apache.iceberg.io.FileIO;
import org.rmow.bench.bench.Phases;
import org.rmow.bench.io.TracingFileIO;
import org.rmow.bench.workload.OpLog;

/**
 * What an arm works with.
 *
 * @param root the arm's own location prefix
 * @param io the traced store; every measured request goes through it
 * @param setupIo the same store untraced, for setup (adopting the initial load)
 * @param verifyRows in a verification run, check every decoded row's payload too
 * @param oracle a verification run: reads hash each row they return for the reference model's
 *     check; otherwise they only count rows, so the oracle costs a measured read nothing a row
 */
public record ArmContext(
    String name,
    String root,
    TracingFileIO io,
    FileIO setupIo,
    Phases phases,
    LayoutSpec layout,
    OpLog log,
    boolean verifyRows,
    boolean oracle) {

  /** A context for a verification run. */
  public ArmContext(
      String name,
      String root,
      TracingFileIO io,
      FileIO setupIo,
      Phases phases,
      LayoutSpec layout,
      OpLog log,
      boolean verifyRows) {
    this(name, root, io, setupIo, phases, layout, log, verifyRows, true);
  }

  /** Whether reads check each row's payload: a verification run whose config asks for it. */
  public boolean checkPayloads() {
    return oracle && verifyRows;
  }
}
