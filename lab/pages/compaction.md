---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: When to compact
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# The compaction sweep: configs/local-compact-*.json, from one build
BUILD = "b6c2605"
ARMS = ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
READERS = ["one-footer", "cached"]  # one-footer decides H6 (as registered); cached is the post's
LEVELS = {0.25: "aggressive", 0.5: "default", 0.75: "lazy"}  # by RMoW's garbage ratio
# the analysis is figures.compaction_analysis, shared with the summary page; its names are this
# page's
globals().update(vars(fig.compaction_analysis(BUILD, ARMS, READERS)))
```

When should each arm compact? This page reads the compaction sweep: eleven policies of one
workload, from no compaction to every batch at the most aggressive thresholds. It decides H6
(docs/HYPOTHESES.md) as registered, on the `one-footer` reader, and gives the costs under the
`cached` reader too.

## Setup

**Configurations.** `configs/local-compact-*.json` share one workload: the m1 recency workload,
1M initial rows and 60 batches of 10k operations with updates and deletes skewed to recent
rows. They differ only in the compaction policy:
- the check period `P` (`compactEvery`): never, or every 1, 5, 10 or 20 batches;
- the thresholds, at three levels. The P = 10 policy runs at the default level only.

```{code-cell} ipython3
:tags: [remove-input]

table(pd.DataFrame({"level": ["aggressive", "default", "lazy"],
                    "RMoW garbage ratio": [0.25, 0.5, 0.75],
                    "MoR delete ratio": [0.1, 0.3, 0.5],
                    "RMoW small row groups": [4, 8, 16]}))
```

MoR's small-file fraction stays at Iceberg's 0.75. RMoW compacts for space, rewriting an
appendable file or moving the live row groups out of a split file's mostly dead objects, and for
reads, packing small sealed row groups (docs/DESIGN.md, Compaction).

**Arms:** MoR-v3, TailCoW-C, and RMoW-B and RMoW-C in both layouts.

**Measures.**
- **Writes** are per batch: means over the 60 batches, compaction included.
- **Reads** are one full read: the mean over the probes after the initial load. Probes come
  every 3 batches, so they sample every phase of a 1-, 5-, 10- or 20-batch cycle.
- **Kept/live** is the mean over steps of what remains once every older version expires,
  against live bytes.
- **CPU** is the phases' modeled time (lab/pages/cpu-measure.md).

```{code-cell} ipython3
:tags: [remove-input]

table(runs.sort_values("key")[["policy", "label", "run_id"]].reset_index(drop=True))
```

## What compaction costs and buys

```{code-cell} ipython3
:label: compaction-frontier
:tags: [remove-input]
:class: col-page-right

color, shape = fig.arm_encoding(ARMS)
fr = T["one-footer"][["arm_label", "policy", "write_mb", "kept_over_live", "read_gets"]]
base = alt.Chart(fr).encode(color=color, shape=shape,
                            tooltip=["arm_label", "policy", alt.Tooltip("write_mb:Q", format=".2f"),
                                     alt.Tooltip("kept_over_live:Q", format=".2f"),
                                     alt.Tooltip("read_gets:Q", format=".1f")])
left = base.mark_point(filled=True, size=70).encode(
    x=alt.X("write_mb:Q", title="MB written per batch, compaction included", scale=alt.Scale(type="log")),
    y=alt.Y("kept_over_live:Q", title="kept ÷ live (mean)", scale=alt.Scale(type="log")))
right = base.mark_point(filled=True, size=70).encode(
    x=alt.X("write_mb:Q", title="MB written per batch, compaction included", scale=alt.Scale(type="log")),
    y=alt.Y("read_gets:Q", title="GETs per full read (one-footer, mean)"))
(left.properties(width=260, height=200) | right.properties(width=260, height=200))
```

Each point is one arm under one policy. On the left, what each policy writes against the space
it keeps; on the right, against what a full read costs in requests.

```{code-cell} ipython3
:tags: [remove-input]

cols = ["arm_label", "policy", "write_mb", "compact_mb", "puts", "write_gets", "read_gets", "read_mb",
        "recent_gets", "kept_over_live", "kept_over_live_end"]
table(T["one-footer"].sort_values(["arm", "key"])[cols].round(3).reset_index(drop=True))
```

## The cheapest policy

The cost per batch is:
- **writes:** PUTs × put + GETs × get + GB written × upload + GB read × retrieval + CPU × compute;
- **reads:** q × the same terms for one full read;
- **storage:** kept GB × storage per GB-month × the batch interval.

The prices are US East list prices. S3 Standard has free in-region transfer; S3 Express One Zone
has the prices after its April 2025 cut. Compute is assumed at \$0.04 per vCPU-hour. The batch
interval weighs storage against the rest, so a longer one stands in for a larger table.

Each cell is an arm's cheapest policy, its cost per batch in µ\$, and its saving against no
compaction. Policies of an arm that did the same I/O count as one. A saving within the noise
band, the largest cost difference between runs that did the same I/O, counts as none.

Under `cached`:

```{code-cell} ipython3
:tags: [remove-input]

table(CHEAPEST["cached"])
```

Under `one-footer`, which decides H6:

```{code-cell} ipython3
:tags: [remove-input]

table(CHEAPEST["one-footer"])
```

## Findings

- **The store decides whether MoR should compact.**
  - On S3 Standard, a GET costs 13 times what it costs on S3 Express, and bytes are free. There,
    MoR-v3's cheapest policy at one full read per batch and 5-minute batches is
    {eval}`N["std_best"]`, saving {eval}`N["std_saving"]`.
  - On S3 Express, reads are charged by the byte, and compaction leaves those nearly unchanged,
    so it saves MoR-v3 at most {eval}`N["mor_saving"]` there ({eval}`N["mor_best"]`).
- **MoR-v3 and TailCoW-C trade writes for reads.** As the period falls from none through 20, 10
  and 5 to every batch, MoR-v3 writes {eval}`N["mor_w"]` MB per batch while its full read falls
  {eval}`N["mor_r"]` GETs. TailCoW-C's tail merge already does much of compaction's work: it
  writes {eval}`N["tc_w"]` MB, and reads {eval}`N["tc_r"]` GETs.
- **Space is the only reason to compact RMoW, and it pays only once storage weighs enough.** At
  5-minute batches on this table, storage is at most {eval}`N["share5"]` of any arm's cost, and no
  RMoW compaction pays on S3 Express.
- **At a day's storage weight, appendable files need the most aggressive policy tested.** Their
  cheapest is garbage 0.25 at P = 1, which cuts their cost by {eval}`N["day_app"]` on S3 Express at
  up to one full read per batch. Uncompacted they keep {eval}`N["kept_end_app"]`× their live
  bytes at batch 60. The optimum is at the edge of the sweep, so a lower threshold may do
  better.
- **Split files need little compaction, and it is cheap.**
  - Moving row groups out of objects at least 25% dead holds their kept/live at
    {eval}`N["aggr_kept"]`, against {eval}`N["none_split_kept"]` uncompacted, for
    {eval}`N["aggr_mb"]` MB of compaction writes per batch.
  - It saves {eval}`N["day_split"]` at a day's weight.
  - At each layout's cheapest policy, split files cost {eval}`N["split_vs_app"]` less than
    appendable ones at a day's weight, with up to one full read per batch.
- **RMoW reads at least as well uncompacted as MoR does at its most frequent compaction.**
  RMoW-B's full read takes {eval}`N["b_rng"]` GETs at every policy, against MoR-v3's
  {eval}`N["mor_r"]` from no compaction to every batch.

## H6 (docs/HYPOTHESES.md)

Decided on the `one-footer` reader, as registered. Savings within the noise band
({eval}`N["band"]`, from CPU alone) count as none.

- **RMoW reads barely depend on compaction: {eval}`N["v_reads"]`.** RMoW-B's full read moves by
  at most {eval}`N["b_dev"]` GETs from its uncompacted {eval}`N["b_none"]`, in either layout.
  RMoW-C's drops by up to {eval}`N["c_dev"]`, from {eval}`N["c_none"]` to {eval}`N["c_min"]`. When
  its tail file rolls over, tiered merging leaves small row groups sealed, and rewrites and
  packing merge them.
- **Appendable files need compaction for space: {eval}`N["v_space_app"]`.** Uncompacted, kept/live
  reaches {eval}`N["kept_end_app"]`× at batch 60. At P = 1 the mean kept/live is
  {eval}`N["kept_p1"]` for garbage ratios 0.25, 0.5 and 0.75, under the bounds 1.33, 2 and 4, and
  compaction writes fall ({eval}`N["comp_p1"]` MB per batch) as the ratio rises.
- **Split files need little: {eval}`N["v_space_split"]`.** At garbage 0.25 the mean kept/live is
  {eval}`N["aggr_kept"]`, for {eval}`N["aggr_mb"]` MB of compaction writes per batch; at 0.75
  nothing moves.
- **MoR-v3 and TailCoW trade writes for reads: {eval}`N["v_trade"]`.** Writes rise and full-read
  GETs fall as the period falls, monotonically.
- **Costs (S3 Express, 5-minute batches, one full read per batch).**
  - **Split RMoW's cheapest policy is within 5% of none: {eval}`N["v_cost_split"]`.**
  - **MoR-v3's cheapest has P ≤ 5: {eval}`N["v_cost_mor"]`.** It is {eval}`N["mor_best"]`,
    saving {eval}`N["mor_saving"]`.

## Limits

- **Scale.** The table is about 55 MB. The batch interval stands in for table size only as far
  as storage goes; a larger table also has more files to read and larger files to rewrite (see
  At Iceberg's defaults).
- **Retention.** Kept bytes assume every older version expires at once. With a retention window,
  the objects a rewrite replaces, and a split file's reclaimable objects, stay stored until they
  leave it, which raises storage most for aggressive policies.
- **CPU** is modeled time from instruction counts, at each arm's pace over the sweep. Compute is
  priced by assumption.
- **Not modeled:** metadata and commit costs (manifests, the catalog), for every arm.
- **Grid.** MoR's small-file fraction, the only MoR threshold that fired, was not varied. For
  RMoW at a day's weight, the cheapest policy is the grid's most aggressive one.
