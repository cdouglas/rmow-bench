---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The inserts at 128 MiB, projected
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, table, theme
theme()
fig = figures_module()

proj = fig.et_projection()
args = proj["args"]
wr = fig.et_written(proj)
rd = fig.et_reads(proj)
NAMES = list(fig.ET_COLOR)
COLOR = alt.Scale(domain=NAMES, range=[fig.ET_COLOR[n] for n in NAMES])
# the theta 0 designs dashed: where they read as RMoR-density does, both lines show
DASH = alt.StrokeDash("name:N", sort=NAMES, legend=None,
                      scale=alt.Scale(domain=NAMES, range=[[1, 0], [1, 0], [1, 0], [6, 4]]))
WRITES = args["writes"]
LAST = int(rd.step.max())

mb = {fig.ET_NAMES[k]: sum(v) / len(v) / 1e6 for k, v in proj["written"].items()}
gb = {fig.ET_NAMES[k]: sum(v) / 1e9 for k, v in proj["written"].items()}
groups = {fig.ET_NAMES[k]: n for k, n in proj["row_groups"].items()}
at_last = rd[rd.step == LAST]
window = at_last.groupby("name").ratio.agg(["min", "max", "mean"])
held = at_last.groupby("name").held_gb.first()
M, C, D, X = NAMES  # MoR, MoR (compacted), RMoR-density, the theta 0 designs
```

What the inserts workload would write and read at Iceberg's 128 MiB row groups, by design, as
`analysis/event_time_model.py` projects it, to decide which designs the 128 MiB suite runs
(docs/HYPOTHESES.md, 2026-11-12, W1–W3, revised the same day). Nothing here is measured. The model
was checked against the harness only at a small scale, on its writes.

## Setup

- **The workload,** as `configs/w128-ing-*.json`: an initial load of
  {eval}`f"{args['initial']:,}"` rows in event-time order, {eval}`args['row_groups']` row groups of
  128 MiB, then {eval}`WRITES` writes of {eval}`f"{args['per_write']:,}"` inserts, late by a
  lognormal of median {eval}`args['late_median']` writes and 99th percentile
  {eval}`f"{args['late_p99']:g}"`; {eval}`args['bpr']` bytes a row.
- **The designs** (the glossary on the front page):
  - **MoR** writes each write's inserts as a file of their own, one row group at this size.
  - **MoR (compacted)** compacts as Iceberg plans its bin-pack, checked after every write: the files
    under 0.75 of the 512 MiB target, rewritten together, sorted, once there are 5 of them.
  - **RMoR-density** places each insert by the event-time merge, and merges a row group once the rows
    joining it, those waiting included, reach θ = {eval}`args['theta']` of it. Until then they wait in
    delta row groups, one a row group a write.
  - **RMoR, RMoW-external, RMoW-append** (θ 0) merge every late row into its row group at once. They
    write the same bytes and keep the same row groups, so the model draws them as one line.
- **What the model counts:** a row group is an event-time range and its rows. A 1% range read reads
  every row group whose range overlaps it, whole; the sorted read holds every row group whose range
  spans the event time it has reached. Requests, footers and pages are left out.
- **Its check:** at 400,000 rows and 1 MiB row groups over 12 writes it wrote within 10% of what the
  harness wrote, at θ 0 and 0.06 (the model's docstring). At this size, against the three designs
  that ran, it is checked on a calibration page, [](model-128.md).

## Data written as snapshots are added

```{code-cell} ipython3
:label: inserts-128-written
:tags: [remove-input]
:class: col-page-right

w = wr[wr.step > 0]
alt.Chart(w).mark_line(strokeWidth=2).encode(
    x=alt.X("step:Q", title="snapshot", scale=alt.Scale(domain=[0, WRITES], nice=False)),
    y=alt.Y("gb:Q", title="data written (GB, log scale)", scale=alt.Scale(type="log")),
    color=alt.Color("name:N", sort=NAMES, scale=COLOR,
                    legend=alt.Legend(orient="bottom", direction="horizontal", title=None, labelLimit=320)),
    strokeDash=DASH,
    tooltip=["name", "step", alt.Tooltip("gb:Q", format=".2f")],
).properties(width=460, height=220)
```

```{code-cell} ipython3
:tags: [remove-input]

table(w[w.step % 40 == 0].pivot(index="step", columns="name", values="gb")[NAMES].round(2)
      .reset_index().rename(columns={"step": "snapshot"}),
      caption="Data written (GB), projected, every 40th snapshot")
```

Merging every late row at once writes {eval}`f"{mb[X]:,.0f}"` MB a write,
{eval}`f"{mb[X] / mb[M]:.0f}"` times MoR's {eval}`f"{mb[M]:.1f}"`: {eval}`f"{gb[X]:.0f}"` GB over the
{eval}`WRITES` writes. A write's few hundred rows that come tens of writes late each reach back to
another old row group, which the write rewrites whole. RMoR-density writes
{eval}`f"{mb[D]:.0f}"` MB a write and MoR (compacted) {eval}`f"{mb[C]:.0f}"`:
{eval}`f"{mb[D] / mb[C]:.2f}"` of compaction's bytes.

## What a read touches after the {eval}`LAST`th write

```{code-cell} ipython3
:label: inserts-128-windows
:tags: [remove-input]
:class: col-page-right

alt.Chart(at_last).mark_line(point=True, strokeWidth=2).encode(
    x=alt.X("window:Q", title="where the 1% range falls in the event-time span",
            scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(values=[0.1, 0.3, 0.5, 0.7, 0.9], format=".1f")),
    y=alt.Y("ratio:Q", title="bytes read over live bytes", scale=alt.Scale(zero=True)),
    color=alt.Color("name:N", sort=NAMES, scale=COLOR,
                    legend=alt.Legend(orient="bottom", direction="horizontal", title=None, labelLimit=320)),
    strokeDash=DASH,
    tooltip=["name", "window", alt.Tooltip("ratio:Q", format=".2f")],
).properties(width=460, height=200)
```

```{code-cell} ipython3
:tags: [remove-input]

t = at_last.pivot(index="window", columns="name", values="ratio")[NAMES].round(2).reset_index()
table(t, caption=f"A 1% range read by event time: bytes read over live bytes, projected, after write {LAST}")
```

```{code-cell} ipython3
:label: inserts-128-sorted
:tags: [remove-input]
:class: col-page-right

h = rd.groupby(["name", "step"]).held_gb.first().reset_index()
alt.Chart(h).mark_line(point=True, strokeWidth=2).encode(
    x=alt.X("step:Q", title="snapshot", scale=alt.Scale(domain=[0, WRITES], nice=False)),
    y=alt.Y("held_gb:Q", title="sorted read: bytes held at once (GB)", scale=alt.Scale(zero=True)),
    color=alt.Color("name:N", sort=NAMES, scale=COLOR,
                    legend=alt.Legend(orient="bottom", direction="horizontal", title=None, labelLimit=320)),
    strokeDash=DASH,
    tooltip=["name", "step", alt.Tooltip("held_gb:Q", format=".2f")],
).properties(width=460, height=200)
```

```{code-cell} ipython3
:tags: [remove-input]

t = h.pivot(index="step", columns="name", values="held_gb")[NAMES].round(2).reset_index()
table(pd.concat([t.rename(columns={"step": "snapshot"}).astype({"snapshot": str}),
                 pd.DataFrame([{"snapshot": "row groups at the end", **groups}])]),
      caption="The sorted read's peak bytes held (GB), projected, and each design's row groups after the last write")
```

- **MoR's** batch files each hold a few rows that come so late they reach back to the start of the
  table, so every file spans almost all of event time. A range read reads every file
  ({eval}`f"{window.loc[M, 'min']:.0f}"` to {eval}`f"{window.loc[M, 'max']:.0f}"` times its live
  bytes), and the sorted read holds them all at once ({eval}`f"{held[M]:.1f}"` GB).
- **MoR (compacted)** merges five files at a time into one sorted file, but each compacted file's
  first row group holds its belated rows and spans back as far. Its range reads are
  {eval}`f"{window.loc[C, 'min']:.0f}"` to {eval}`f"{window.loc[C, 'max']:.0f}"` times live, and its
  sorted read holds {eval}`f"{held[C]:.2f}"` GB.
- **The event-time merge** keeps row groups apart, so a range reads the one or two row groups that
  hold it ({eval}`f"{window.loc[D, 'min']:.1f}"` to {eval}`f"{window.loc[D, 'max']:.1f}"` times live)
  and the sorted read holds {eval}`f"{held[D]:.2f}"` GB, at θ 0 and under RMoR-density alike: a
  delta row group's rows wait inside the range of the row group they wait for.
- **RMoR-density's price** is in what the model leaves out: {eval}`f"{groups[D]:,}"` row groups after
  the last write, against {eval}`f"{groups[C]:,}"` for MoR (compacted), nearly all of them small
  delta row groups. Each a range read touches is a request, and every footer lists them.

## Decision

The 128 MiB suite runs MoR, MoR (compacted) and RMoR-density on the inserts
(`configs/w128-ing-{mor,compact,density}.json`), and not RMoR, RMoW-external or RMoW-append, whose
{eval}`f"{gb[X]:.0f}"` GB no run can store. Their data written is projected, and plotted beside the
runs that measure the others. The runs also check the model at this size (docs/HYPOTHESES.md,
revised 2026-11-12): only if they match does the projection stand.

RMoR-density is, by this projection, an alternative to compaction on these inserts. It writes about
compaction's bytes, and a range read or a sorted read touches a fraction of what compaction leaves.
What it costs in requests, for its delta row groups, the runs measure.

## Limits

- **A model:** it counts bytes of whole row groups, not requests, footers or pages, and splits a
  cut row group's range in proportion to its rows. At this size it is checked against the three
  designs that ran on [](model-128.md).
- **One draw of the lateness:** median 0.05 writes, p99 5, with no watermark. The θ 0 cost comes from
  its tail; a stream processor that dropped rows past an allowed lateness would cut it.
- **One θ:** RMoR-density at 0.06, its value on the updates.
