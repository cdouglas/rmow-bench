---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Row reader or vectorized reader
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()
OLD, NEW, BUILDS = fig.BP_OLD, fig.BP_NEW, fig.BP_BUILDS
LAPTOP, CLOUD, SPIKE = fig.BP_LAPTOP, fig.BP_CLOUD, fig.BP_SPIKE

# V1: the laptop's runs on the two builds, record by record
same = q("""SELECT count(*) AS records, count(*) FILTER (WHERE len(differs) > 0) AS differing
            FROM steps_diff($a, $b)""", a=LAPTOP[OLD], b=LAPTOP[NEW]).iloc[0]

# V2: the laptop's instructions per mean phase, on each build
laptop = fig.bp_times(LAPTOP)
work = laptop.pivot_table(index=["arm", "arm_label", "ord", "what"], columns="build",
                          values="g_instructions").reset_index()
work["new_over_old"] = work[NEW] / work[OLD]
full = work[work.what == "a full read"].new_over_old

# V3: the laptop's runs simulated at S3 Express prices (the work at its arm's pace plus its
# requests priced with the first S3 Express VM's spike), each arm over MoR-v3
sim = pd.concat([q(f"""SELECT arm, {fig.BP_WHAT} AS what, avg(work_ms + request_ms) / 1e3 AS s
                       FROM sized_latency($run, $spike) WHERE {fig.BP_JUDGED} GROUP BY ALL""",
                   run=LAPTOP[b], spike=SPIKE[OLD]).assign(build=b) for b in BUILDS])
SIM = fig.bp_over_mor(sim).set_index(["build", "arm", "what"]).over_mor_v3.sort_index()

# On S3 Express: each build's measured times, and each arm over MoR-v3
cloud = fig.bp_times()
C = cloud.set_index(["build", "arm", "what"]).s.sort_index()
over = fig.bp_over_mor(cloud)
OV = over.set_index(["build", "arm", "what"]).over_mor_v3.sort_index()
parts = fig.bp_parts()
P = parts.set_index(["build", "arm", "what", "part"]).s.sort_index()
elapsed = q("SELECT run_id, elapsed_s / 60 AS min FROM runs WHERE list_contains($r, run_id)",
            r=list(LAPTOP.values()) + list(CLOUD.values())).set_index("run_id")["min"]
```

Whether the write and read trade-off depends on the Parquet code the designs are read and written
with, and so which reader the benchmark uses. **Decided 2026-10-31** (6f24575): every design reads
through Iceberg's vectorized reader, as Spark reads an Iceberg table, and the writer takes strings
as UTF-8 bytes, as Spark's rows hold them. The results are in
[The base point on S3 Express](./base-point.md).

## Why

Until 2026-10-31 every design read through Iceberg's generic row reader, which builds a Java object
per row and per value. On the first S3 Express run of the base point the store took only a third of
a full read, and a profile put about 85% of a read's CPU in that reader's per-value and per-row
work, and 8.5% in decompression (docs/HYPOTHESES.md, The base point on a vectorized reader). Applications read Iceberg tables through vectorized readers, so the
costs the benchmark attributed to each design were mostly the harness's own. The new reader
(README, What is measured) decodes batches of 5,000 rows through iceberg-arrow, applies deletes by
position, and runs a scan's filter as comparisons on the batch's columns, as Spark does. The writer
stays Iceberg's over parquet-java, which Spark's and Flink's are too.

## The same work

Requests, bytes, table state and held memory follow from the op log, the configuration and the
code, so the reader may change none of them (V1): the two builds' laptop runs differ in
{eval}`f"{same.differing:,}"` of {eval}`f"{same.records:,}"` records.

## The work per phase

Instructions per mean phase on the laptop, the vectorized reader's over the row reader's.

```{code-cell} ipython3
:label: vectorized-work
:tags: [remove-input]

ARMS = list(work.sort_values("ord").arm_label.unique())
alt.Chart(work).mark_bar().encode(
    x=alt.X("new_over_old:Q", title="instructions, vectorized over row reader", scale=alt.Scale(domain=[0, 1])),
    y=alt.Y("arm_label:N", title=None, sort=ARMS),
    color=alt.Color("what:N", title=None, sort=fig.BP_WHATS),
    yOffset=alt.YOffset("what:N", sort=fig.BP_WHATS),
    tooltip=["arm_label", "what", alt.Tooltip("new_over_old:Q", format=".2f")],
).properties(width=560, height=280)
```

```{code-cell} ipython3
:tags: [remove-input]

table(work.sort_values(["what", "ord"])[["what", "arm_label", OLD, NEW, "new_over_old"]]
      .rename(columns={"arm_label": "arm", OLD: "row reader (G instr.)",
                       NEW: "vectorized reader (G instr.)"}).round(2))
```

A full read takes {eval}`f"{full.min():.2f}"`–{eval}`f"{full.max():.2f}"` of the instructions it took
on the row reader. The laptop's whole run took {eval}`f"{elapsed[LAPTOP[NEW]]:.0f}"` minutes instead
of {eval}`f"{elapsed[LAPTOP[OLD]]:.0f}"`, and the S3 Express run {eval}`f"{elapsed[CLOUD[NEW]]:.0f}"`
instead of {eval}`f"{elapsed[CLOUD[OLD]]:.0f}"`.

## What it changed on S3 Express

Each design's mean batch and full read on S3 Express on each build, split by what its own thread
did.

```{code-cell} ipython3
:label: vectorized-parts
:tags: [remove-input]

fig.bp_parts_chart(parts, rows="build")
```

```{code-cell} ipython3
:tags: [remove-input]

o = over.pivot_table(index=["ord", "arm_label", "what"], columns="build", values="over_mor_v3").reset_index()
t = cloud.pivot_table(index=["ord", "arm_label", "what"], columns="build", values="s").reset_index()
t = t.merge(o, on=["ord", "arm_label", "what"], how="left", suffixes=(" (s)", " over MoR-v3's"))
table(t.sort_values(["what", "ord"]).drop(columns="ord").rename(columns={"arm_label": "arm"}).round(2))
```

- **Reads got faster for every design alike:** MoR-v3's full read went from
  {eval}`f"{C[(OLD, 'mor-v3', 'a full read')]:.1f}"` s to {eval}`f"{C[(NEW, 'mor-v3', 'a full read')]:.1f}"` s,
  RMoW-C's from {eval}`f"{C[(OLD, 'rmow-c', 'a full read')]:.1f}"` s to
  {eval}`f"{C[(NEW, 'rmow-c', 'a full read')]:.1f}"` s, so RMoW-C's read stays
  {eval}`f"{OV[(NEW, 'rmow-c', 'a full read')]:.2f}"` of MoR-v3's
  ({eval}`f"{OV[(OLD, 'rmow-c', 'a full read')]:.2f}"` before). Decoding no longer dominates a
  read; the store does.
- **MoR-v3's batch got faster than RMoW-C's:** its largest part is decoding the rows it scans for
  those it deletes, which went from {eval}`f"{P[(OLD, 'mor-v3', 'a batch', 'decoding')]:.1f}"` s to
  {eval}`f"{P[(NEW, 'mor-v3', 'a batch', 'decoding')]:.1f}"` s; RMoW-C's largest part is encoding the
  regions it rewrites, {eval}`f"{P[(OLD, 'rmow-c', 'a batch', 'encoding')]:.1f}"` s then and
  {eval}`f"{P[(NEW, 'rmow-c', 'a batch', 'encoding')]:.1f}"` s now. So RMoW-C's batch went from
  {eval}`f"{OV[(OLD, 'rmow-c', 'a batch')]:.2f}"` to {eval}`f"{OV[(NEW, 'rmow-c', 'a batch')]:.2f}"`
  times MoR-v3's.

With the row reader the benchmark understated what RMoW-C pays to write, against MoR-v3, by
{eval}`f"{100 * (1 - OV[(OLD, 'rmow-c', 'a batch')] / OV[(NEW, 'rmow-c', 'a batch')]):.0f}"`%.

## Verdicts

From docs/HYPOTHESES.md, The base point on a vectorized reader (registered 2026-10-31, before the
laptop run):
- **V1, the reader changes no request, byte or held peak:** {eval}`"held" if same.differing == 0 else "contradicted"`.
- **V2, a full read takes at most a third of its instructions:**
  {eval}`"held" if full.max() <= 1 / 3 else "contradicted"` ({eval}`f"{full.min():.2f}"`–{eval}`f"{full.max():.2f}"`).
- **V3, at S3 Express prices RMoW-C's batch grows past 3.05 times MoR-v3's and its full read stays
  between 0.80 and 0.95 of it:** {eval}`"held" if SIM[(NEW, 'rmow-c', 'a batch')] > 3.05 and 0.80 <= SIM[(NEW, 'rmow-c', 'a full read')] <= 0.95 else "contradicted"`,
  {eval}`f"{SIM[(NEW, 'rmow-c', 'a batch')]:.2f}"` and {eval}`f"{SIM[(NEW, 'rmow-c', 'a full read')]:.2f}"`
  simulated from the laptop; S3 Express then measured {eval}`f"{OV[(NEW, 'rmow-c', 'a batch')]:.2f}"`
  and {eval}`f"{OV[(NEW, 'rmow-c', 'a full read')]:.2f}"` ([The model on the cloud base point](./base-point-model.md)).

## Limits

- One setting, the base point; the sweeps run on the vectorized reader only.
- The writer is unchanged but for its strings: parquet-java's encoding is now the largest part of a
  region design's batch. A faster encoder (an engine's own) would narrow the gap this change
  widened.
