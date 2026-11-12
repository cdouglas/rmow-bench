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
rewrites, against Iceberg's merge-on-read. Four designs: **MoR** (Iceberg's merge-on-read:
deletion vectors, new rows in new files), **RMoR** (each changed row group copied into the write's
new file, a deletion vector over the old one), **RMoW-external** (the same, the old row group
dropped by a footer appended to its file) and **RMoW-append** (each changed row group appended to
its own file, with a new footer), with RMoR's **deferred merges** beside them. Two workloads,
updates only and inserts only, on S3 Express and Azure Blob.

**Results**
- [Updates and inserts: writes against reads](pages/workloads.md): every design as snapshots are
  added, compaction, and each knob, simulated from local runs.
- [Updates and inserts in the cloud](pages/workloads-cloud.md): the main points measured on both
  stores, beside the simulation.
- [Appendix A: changes to the readers](pages/reader-changes.md): what our readers and writers do
  that stock Iceberg does not, for every design.

**Decisions:** [copies cut where a write changes](pages/region-cut.md), at Iceberg's 128 MiB row
groups.

**Calibration:** [repetitions](pages/compare.md) (two runs of a configuration must count the same
requests and bytes) and the [footer micro-benchmarks](pages/micro.md).

**Browsing:** [one run, step by step](pages/run.md). The runs and what each campaign varied are
described in the repository's `docs/EXPERIMENTS.md`; what earlier, retired experiments taught is in
`docs/LESSONS.md`.

## Campaigns

```{code-cell} ipython3
:label: campaigns
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
