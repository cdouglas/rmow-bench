---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The sweeps on S3 Express and Azure
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

STORES = fig.CS_STORES
S3X, AZ = STORES
RUNS = fig.CS_RUNS
times = fig.cs_frame(fig.bp_times, RUNS)
over = fig.cs_over_mor(times)
REGION = ["rmow-c", "rmor-c", "rmor-c-footer"]
T = times.set_index(["store", "config", "arm", "what"]).s.sort_index()
O = over.set_index(["store", "config", "arm", "what"]).over_mor_v3.sort_index()


def o(config, arm, what="a write", store=S3X):
    return O[(store, config, arm, what)]


# full reads per write that repay a design's write against MoR-v3's: its extra time per write over
# the time it saves per full read (none where it saves nothing)
be = times[times.what != "a full read, skipping dead row groups"].pivot_table(
    index=["store", "config", "arm"], columns="what", values="s").reset_index()
mor = be[be.arm == "mor-v3"].set_index(["store", "config"])
be = be[be.arm.isin(REGION)].copy()
saved = [mor.loc[(st, c), "a full read"] - r for st, c, r in zip(be.store, be.config, be["a full read"])]
extra = [w - mor.loc[(st, c), "a write"] for st, c, w in zip(be.store, be.config, be["a write"])]
be["reads_per_write"] = [e / sv if sv > 0 else np.inf for e, sv in zip(extra, saved)]
BE = be.set_index(["store", "config", "arm"]).reads_per_write.sort_index()
labels = q("SELECT arm, arm_label, ord FROM arm_meta")
be = be.merge(labels, on="arm")

# the full read's GETs along the row group's size (the same on both stores)
gets = times[(times.what == "a full read") & times.arm.isin(["mor-v3", "rmow-c"])
             & (times.store == S3X)].set_index(["config", "arm"]).gets

# the store's share of each write on Azure (phase_breakdown: waiting on it and its client's CPU)
az_parts = fig.cs_frame(fig.bp_parts, {AZ: RUNS[AZ]})
az_w = az_parts[az_parts.what == "a write"].pivot_table(index=["config", "arm"], columns="part",
                                                         values="s")
az_share = (az_w["the store"] / az_w.sum(axis=1)).unstack()

# MB a write carries (the same on both stores)
mbw = times[(times.what == "a write") & (times.store == S3X)].set_index(["config", "arm"]).mb_written

rmow_w = over[(over.arm == "rmow-c") & (over.what == "a write")]
rmow_r = over[(over.arm == "rmow-c") & (over.what == "a full read")]
footer_r = over[(over.arm == "rmor-c-footer") & (over.what == "a full read")]
rmor_r = over[(over.arm == "rmor-c") & (over.what == "a full read")].groupby("store").over_mor_v3.max()
skip_r = over[over.what == "a full read, skipping dead row groups"].groupby("store").over_mor_v3
region_w = over[over.arm.isin(REGION) & (over.what == "a write")]
```

How each design's trade-off moves as the workload and the layout change, measured on S3 Express and
Azure Blob. From the base point, one setting changes at a time: how much of a region an update
statement changes (the update share p), how the updates spread over regions (their skew), the row
group's size, how late inserts arrive, and a workload of recent keys instead of regions.

- **No region design's write catches MoR-v3's at any setting.** RMoW-C's write takes
  {eval}`f"{rmow_w[rmow_w.store == S3X].over_mor_v3.min():.1f}"`–{eval}`f"{rmow_w[rmow_w.store == S3X].over_mor_v3.max():.1f}"`
  times as long as MoR-v3's on S3 Express and
  {eval}`f"{rmow_w[rmow_w.store == AZ].over_mor_v3.min():.1f}"`–{eval}`f"{rmow_w[rmow_w.store == AZ].over_mor_v3.max():.1f}"`
  on Azure; its full read takes {eval}`f"{rmow_r.over_mor_v3.min():.2f}"`–{eval}`f"{rmow_r.over_mor_v3.max():.2f}"`
  of MoR-v3's everywhere.
- **When an update changes more of a region, RMoW-C pays for itself after about one full read
  per write.** From p = 0.03 to 0.3, RMoW-C's write over MoR-v3's falls from
  {eval}`f"{o('sweep-base', 'rmow-c'):.1f}"` to {eval}`f"{o('sweep-p30', 'rmow-c'):.1f}"` on S3
  Express and from {eval}`f"{o('sweep-base', 'rmow-c', store=AZ):.1f}"` to
  {eval}`f"{o('sweep-p30', 'rmow-c', store=AZ):.1f}"` on Azure, and the full reads per write that
  repay its write fall from {eval}`f"{BE[(S3X, 'sweep-base', 'rmow-c')]:.1f}"` to
  {eval}`f"{BE[(S3X, 'sweep-p30', 'rmow-c')]:.1f}"` and from
  {eval}`f"{BE[(AZ, 'sweep-base', 'rmow-c')]:.1f}"` to {eval}`f"{BE[(AZ, 'sweep-p30', 'rmow-c')]:.1f}"`.
- **Bigger row groups widen it.** At 64 MiB RMoW-C's write takes
  {eval}`f"{o('sweep-rg64m', 'rmow-c'):.1f}"` and {eval}`f"{o('sweep-rg64m', 'rmow-c', store=AZ):.1f}"`
  times MoR-v3's.
- **RMoW-C writes fastest of the region designs at every setting,** but its lead over RMoR-C narrows
  as p rises: its write takes {eval}`f"{o('sweep-base', 'rmow-c') / o('sweep-base', 'rmor-c'):.2f}"`
  of RMoR-C's at p = 0.03 and {eval}`f"{o('sweep-p30', 'rmow-c') / o('sweep-p30', 'rmor-c'):.2f}"`
  at 0.3 on S3 Express.

## Setup

**The table and its updates** are the base point's ([The base point on S3 Express and
Azure](./base-point.md): 16 MiB row groups, 50 regions growing to 71, 30 writes of 500,000
operations, statements changing p = 0.03 of a region, a region skew of 1, inserts up to 5 writes
late at the 99th percentile), each run changing one setting:

```{code-cell} ipython3
:tags: [remove-input]

pts = pd.DataFrame(fig.CS_POINTS, columns=["config", "axis", "value"])
pts = pts[pts.config != "sweep-base"]
w = pd.DataFrame([(c, "workload", d) for c, d in fig.CS_WORKLOADS.items() if c != "sweep-base"],
                 columns=["config", "axis", "value"])
table(pd.concat([pts, w]).rename(columns={"config": "run", "axis": "varies", "value": "to"}))
```

The designs, the write and the full read are the base point's. Times are means over the 30 writes
and the 6 full reads after the initial load.

**Where it ran** (build 6f24575, every run once):
- **S3 Express:** one `c7a.4xlarge` per configuration beside the directory bucket in usw2-az3,
  nine at once on 2026-11-01 (`sweep-rg64m` on `sweep-p30`'s VM after it), so each measured the
  store under the others' load;
- **Azure Blob:** one `D8as_v6` in westus, the configurations one after another from 2026-11-01.

## Writes

Each region design's mean write over MoR-v3's, along each setting (a log scale; 1 is MoR-v3's).

```{code-cell} ipython3
:label: cloud-sweep-writes
:tags: [remove-input]

color, shape = fig.arm_encoding(REGION, title="design")


def along(d, y, title, domain, ticks):
    """d along each axis: a column per axis, a row per store, a line per design"""
    d = fig.cs_axes(d)
    dash = alt.StrokeDash("dash:N", legend=None,
                          scale=alt.Scale(domain=["solid", "footer", "skipping"],
                                          range=[[1, 0], [5, 2], [1, 2]]))
    d = d.assign(dash=["footer" if "footer" in l else "skipping" if "skipping" in l else "solid"
                       for l in d.arm_label])
    cols = []
    for axis in fig.CS_AXES:
        a = d[d.axis == axis]
        log = axis != "region skew"
        x = alt.X("value:Q", title=axis, scale=alt.Scale(type="log" if log else "linear",
                                                           nice=False, zero=False),
                  axis=alt.Axis(values=sorted(a.value.unique())))
        y_ = alt.Y(f"{y}:Q", title=title, scale=alt.Scale(type="log", domain=domain, nice=False),
                   axis=alt.Axis(values=ticks))
        lines = alt.Chart().mark_line(point=True).encode(
            x=x, y=y_, color=color, shape=shape, strokeDash=dash, detail="arm_label:N",
            tooltip=["store", "config", "arm_label", alt.Tooltip(f"{y}:Q", format=".2f")])
        one = alt.Chart().mark_rule(strokeDash=[4, 3], color="gray").encode(y=alt.datum(1.0))
        cols.append(alt.layer(one, lines, data=a).properties(width=170, height=170)
                    .facet(row=alt.Row("store:N", title=None, sort=STORES)))
    return alt.hconcat(*cols).resolve_scale(x="independent")


along(region_w, "over_mor_v3", "a write over MoR-v3's", [1, 16], [1, 2, 4, 8, 16]).configure_legend(orient="bottom")
```

- **The update share and the skew move every region design the same way:** the fewer regions a
  write changes (more of each changed at p = 0.3, or updates piled onto hot regions at a skew of
  2), the less a region design rewrites. At a skew of 2 RMoR-C's write comes closest to RMoW-C's
  ({eval}`f"{o('sweep-s2', 'rmor-c'):.1f}"` against {eval}`f"{o('sweep-s2', 'rmow-c'):.1f}"` times
  MoR-v3's on S3 Express).
- **Lateness moves the RMoR-C designs, not RMoW-C.** With a p99 of 20 writes instead of 1, RMoR-C's
  write goes from {eval}`f"{o('sweep-late99-1', 'rmor-c'):.1f}"` to
  {eval}`f"{o('sweep-late99-20', 'rmor-c'):.1f}"` times MoR-v3's on S3 Express, RMoW-C's from
  {eval}`f"{o('sweep-late99-1', 'rmow-c'):.1f}"` to {eval}`f"{o('sweep-late99-20', 'rmow-c'):.1f}"`.
  RMoR-C's write carries more bytes the later inserts arrive
  ({eval}`f"{mbw[('sweep-late99-1', 'rmor-c')]:.0f}"`, {eval}`f"{mbw[('sweep-base', 'rmor-c')]:.0f}"`
  and {eval}`f"{mbw[('sweep-late99-20', 'rmor-c')]:.0f}"` MB at a p99 of 1, 5 and 20 writes), and
  RMoW-C's the same {eval}`f"{mbw[('sweep-base', 'rmow-c')]:.0f}"` MB at each. The likely cause, not
  checked here: RMoR-C sorts each write's new file by event time, which mixes late inserts among the
  rows of the regions it rewrote, while RMoW-C keeps inserts in its tail.
- **Azure's gaps are narrower at every setting:** its slower requests make MoR-v3's write, of which
  the store takes {eval}`f"{100 * az_share['mor-v3'].min():.0f}"`–{eval}`f"{100 * az_share['mor-v3'].max():.0f}"`%
  there, cost more, while the store takes at most
  {eval}`f"{100 * az_share[REGION].max().max():.0f}"`% of a region design's write, whose largest
  part is encoding the regions it rewrites.

## Full reads

Each region design's mean full read over MoR-v3's (RMoR-C also skipping the row groups its
deletes cover whole). RMoW-C's and RMoR-C with footers' lie on top of each other.

```{code-cell} ipython3
:label: cloud-sweep-reads
:tags: [remove-input]

r = over[over.arm.isin(REGION) & (over.what != "a write")].copy()
skip = r.what == "a full read, skipping dead row groups"
r.loc[skip, "arm_label"] = r.loc[skip, "arm_label"] + " (skipping)"
# the designs' own colors and shapes, RMoR-C's skipping read in RMoR-C's color
c0, s0 = fig.arm_encoding(REGION, title="design")
c0, s0 = c0.to_dict()["scale"], s0.to_dict()["scale"]
dom, rng, shp = list(c0["domain"]), list(c0["range"]), list(s0["range"])
i = dom.index(labels.set_index("arm").arm_label["rmor-c"])
dom.insert(i + 1, dom[i] + " (skipping)")
rng.insert(i + 1, rng[i])
shp.insert(i + 1, "diamond")
color = alt.Color("arm_label:N", title="design", sort=dom, scale=alt.Scale(domain=dom, range=rng))
shape = alt.Shape("arm_label:N", title="design", sort=dom, scale=alt.Scale(domain=dom, range=shp))
along(r, "over_mor_v3", "a full read over MoR-v3's", [0.6, 6], [0.6, 0.8, 1, 1.5, 2, 3, 4, 6]).configure_legend(orient="bottom")
```

- **RMoW-C and RMoR-C with footers read a little faster than MoR-v3 everywhere,** at
  {eval}`f"{rmow_r.over_mor_v3.min():.2f}"`–{eval}`f"{rmow_r.over_mor_v3.max():.2f}"` and
  {eval}`f"{footer_r.over_mor_v3.min():.2f}"`–{eval}`f"{footer_r.over_mor_v3.max():.2f}"` of its
  time: they read only live row groups, from fewer files.
- **On Azure bigger row groups make RMoW-C's read faster still** (to
  {eval}`f"{o('sweep-rg64m', 'rmow-c', 'a full read', AZ):.2f}"` of MoR-v3's at 64 MiB), and not on
  S3 Express ({eval}`f"{o('sweep-rg64m', 'rmow-c', 'a full read'):.2f}"`). A full read of RMoW-C's
  table takes {eval}`f"{gets[('sweep-base', 'rmow-c')]:.0f}"`, {eval}`f"{gets[('sweep-rg32m', 'rmow-c')]:.0f}"`
  and {eval}`f"{gets[('sweep-rg64m', 'rmow-c')]:.0f}"` GETs at 16, 32 and 64 MiB, a row group each,
  where MoR-v3's write files stay about 15 MiB ({eval}`f"{gets[('sweep-base', 'mor-v3')]:.0f}"` to
  {eval}`f"{gets[('sweep-rg64m', 'mor-v3')]:.0f}"` GETs). A request costs more on Azure, so the
  fewer, larger GETs save RMoW-C more there.
- **RMoR-C reads its old regions too,** under deletion vectors, so its full read takes up to
  {eval}`f"{rmor_r[S3X]:.1f}"` times MoR-v3's on S3 Express and {eval}`f"{rmor_r[AZ]:.1f}"` on
  Azure. Skipping the row groups a deletion vector covers whole brings it to
  {eval}`f"{skip_r.min()[S3X]:.2f}"`–{eval}`f"{skip_r.max()[S3X]:.2f}"` of MoR-v3's on S3 Express,
  but only to {eval}`f"{skip_r.min()[AZ]:.2f}"`–{eval}`f"{skip_r.max()[AZ]:.2f}"` on Azure, where
  its write files past 256 MiB, uploaded in blocks, read slower ([The base point](./base-point.md)).

## Reads per write that repay a write

How many full reads per write repay a design's write against MoR-v3's: its extra time per write
over the time it saves per full read ("never" where it saves none).

```{code-cell} ipython3
:tags: [remove-input]

b = be.pivot_table(index=["config"], columns=["store", "arm_label"], values="reads_per_write")
b = b.reindex([c for c in fig.CS_CONFIGS if c in b.index])
b = b.reindex(columns=[(st, a) for st in STORES
                       for a in labels.set_index("arm").arm_label[REGION] if (st, a) in b.columns])
table(b.map(lambda v: "never" if np.isinf(v) else f"{v:.1f}").reset_index())
```

## Times

Each run's mean write and full read, in seconds.

```{code-cell} ipython3
:tags: [remove-input]

t = times[times.what != "a full read, skipping dead row groups"].pivot_table(
    index=["config", "arm_label", "ord"], columns=["store", "what"], values="s").reset_index()
t["config"] = pd.Categorical(t.config, fig.CS_CONFIGS)
t = t.sort_values(["config", "ord"]).drop(columns="ord")
t.columns = [" ".join(c).strip() if isinstance(c, tuple) else c for c in t.columns]
table(t.rename(columns={"config": "run", "arm_label": "design"}).round(2))
```

## Limits

- One run of each configuration, on one seed, and one setting changed at a time.
- The S3 Express runs loaded one bucket at once; the Azure runs came one after another over about a
  day, at different hours.
- One kind of read: a single thread's full scan.
- No compaction.

How the model priced these runs, the checks that each did the work its twin counts, and the
hypotheses' verdicts are in [The model on the cloud sweeps](./cloud-sweep-model.md).
