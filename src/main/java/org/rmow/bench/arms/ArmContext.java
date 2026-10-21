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
 * @param verifyRows check every decoded row's payload (costs CPU equally in every arm)
 */
public record ArmContext(
    String name,
    String root,
    TracingFileIO io,
    FileIO setupIo,
    Phases phases,
    LayoutSpec layout,
    OpLog log,
    boolean verifyRows) {}
