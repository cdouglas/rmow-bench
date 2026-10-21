package org.rmow.bench.arms;

/**
 * An arm's storage footprint after a step.
 *
 * @param objects objects written and still stored (nothing is deleted: history stays readable)
 * @param storedBytes bytes of those objects
 * @param liveBytes bytes the current version references
 * @param reclaimableBytes bytes no current object references: deletable once old versions expire
 *     (MoR's superseded files); bytes dead inside live objects are {@code storedBytes - liveBytes -
 *     reclaimableBytes} and need a rewrite to reclaim (RMoW's replaced regions)
 * @param rowGroups live row groups across the current version's data files
 * @param dataFiles live data files (or appendable objects)
 * @param deleteFiles live delete files (DVs count once each)
 * @param footerBytes bytes of the current version's footers, read on every open
 * @param rowGroupsOutOfOrder RMoW: row groups listed after a row group holding larger ids, so
 *     footer order (row order) is not the order rows were inserted in; -1 for other arms, whose
 *     data files are each written once, in one sort
 */
public record ArmStats(
    long objects,
    long storedBytes,
    long liveBytes,
    long reclaimableBytes,
    long rowGroups,
    long dataFiles,
    long deleteFiles,
    long footerBytes,
    long rowGroupsOutOfOrder) {}
