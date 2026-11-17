---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: rmow-bench results
---

```{code-cell} ipython3
:tags: [remove-cell]

from labbook import q, table, meta, data_version
```

What it costs a writer, and saves its readers, to treat a Parquet row group as the unit a write
rewrites, against Iceberg's merge-on-read. Five designs (the glossary below), two workloads
(updates only, and inserts only), on S3 Express and Azure Blob.

## Glossary of the designs

Every page names a design the same way. A *row group* is the part of a Parquet file a reader
reads or skips as a whole; the file's *footer*, at its end, lists its row groups. A *deletion
vector* (DV) is Iceberg's list of a data file's dead rows, in a file of its own.

| Design | What a write does with a row group whose rows it changes | What hides the old copy | Arm |
|---|---|---|---|
| **MoR** | Leaves it; the changed rows' new versions go to a new file, with the write's new rows | a DV over the old rows | `mor-v3` |
| **MoR (compacted)** | As MoR; a compaction rewrites the small files and those with many dead rows, as Iceberg's bin-pack does (at 128 MiB planned after every write, by Iceberg's rule; every 10 writes in the 16 MiB runs) | the compaction's files replace them | `mor-v3` with `compactEvery` |
| **RMoR** | Copies it whole, with the changes, into the write's new file | a DV over the whole old row group | `rmor-c-kept` |
| **RMoW-external** | As RMoR | a new footer appended to the old file, which no longer lists it | `rmor-c-footer-kept` |
| **RMoW-append** | Appends the rewritten row group to its own file | the new footer appended after it | `rmow-c` |
| **RMoR-density** | As RMoR once its dead and changed rows reach a share θ of it (6%); until then as MoR, the new versions waiting in small *delta* row groups for that merge | a DV | `rmor-density` |

RMoR, RMoW-external, RMoW-append and RMoR-density are the *region designs*: a write's unit is the
row group. In their names, R or W says when a row group's old and new rows are reconciled: on read
(RMoR: readers apply the DV, which covers the old row group whole, so a reader can skip it) or on
write (RMoW: the old copy is gone from the footer readers use). Where a write's new rows go differs by workload:
- **Updates:** rows keep their place, so a write rewrites the row groups that hold them.
- **Inserts:** at 128 MiB the *event-time merge* puts each new row into the row group its event
  time falls in, so no two row groups overlap in event time (`layout.inserts` `event-time`). It ran
  for RMoR-density, beside MoR and MoR (compacted). Merging every late row at once would write
  2.3 GB a write, so RMoR, RMoW-external and RMoW-append are projected there, not run ([the inserts
  at 128 MiB, projected](pages/inserts-128.md)). In the 16 MiB runs the region designs put new
  rows in row groups of their own, merged four writes' worth at a time (RMoR there is `rmor-c`).

**Results**
- [Updates and inserts at 128 MiB: writes against reads](pages/workloads-128.md): every design at
  Iceberg's 128 MiB row groups as snapshots are added, where the time goes, and compaction,
  simulated from local runs.
- [Appendix A: changes to the readers](pages/reader-changes.md): what our readers and writers do
  that stock Iceberg does not, for every design.

**Decisions:** [copies cut where a write changes](pages/region-cut.md), at Iceberg's 128 MiB row
groups; [the inserts at 128 MiB, projected](pages/inserts-128.md): which designs to run, and
RMoR-density against compaction; [the 128 MiB suite](pages/suite-128.md): its hypotheses judged.

**Calibration:** [the simulation against the cloud](pages/simulation-cloud.md) (the simulated
times beside the measured ones at 16 MiB, and whether the cloud runs did the same work), [the
inserts model's check](pages/model-128.md) at 128 MiB,
[repetitions](pages/compare.md) (two runs of a configuration must count the same requests and
bytes) and the [footer micro-benchmarks](pages/micro.md).

**Browsing:** [one run, step by step](pages/run.md). The runs and what each campaign varied are
described in the repository's `docs/EXPERIMENTS.md`; what earlier, retired experiments taught is in
`docs/LESSONS.md`.

## Campaigns

```{code-cell} ipython3
:label: campaign-table
:tags: [remove-input]
:class: col-page-right

table(q("""
    SELECT c.campaign, count(r.run_id) AS runs, c.title
    FROM campaigns c
    LEFT JOIN campaign_runs cr USING (campaign)
    LEFT JOIN runs r USING (run_id)
    GROUP BY ALL ORDER BY min(r.started_at)"""))
```

## Every run

Newest first. Superseded and excluded runs, when there are any, are listed here and left out of
the other pages, which read `canonical_runs`.

```{code-cell} ipython3
:label: run-catalog
:tags: [remove-input]
:class: col-page-right

table(q("""
    SELECT run_id, campaigns, host, status, coalesce(mark, '') AS mark,
           strftime(started_at, '%Y-%m-%d %H:%M') AS started,
           left(git_sha, 10) || CASE WHEN git_dirty THEN '*' ELSE '' END AS git
    FROM runs ORDER BY started_at DESC NULLS LAST"""))
```

```{code-cell} ipython3
:tags: [remove-input]

print(f"data version {data_version()}, ingested {meta()['ingested_at']}")
```
