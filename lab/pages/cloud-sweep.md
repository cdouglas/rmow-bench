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
# RMoR's from its own run of each setting, with its check by range (figures.py, CS_RMOR_RUNS)
RMOR = fig.CS_RMOR_RUNS
KEYS = ["store", "config"]
MAIN, REGION = fig.MAIN, fig.REGION_DESIGNS
WRITE, READ = fig.WRITE, fig.READ



def spliced(fn, **kw):
    """fn over every pinned run, with RMoR's rows from its own runs"""
    return fig.rmor_spliced(fig.cs_frame(fn, RUNS, **kw), fig.cs_frame(fn, RMOR, **kw))


# MB a write MoR reads at each setting once it reads the rows its updates replace (`updateRead`
# `row`; campaign mor-row, havoc), which these runs' MoR does not
mor_row_mb = q("""SELECT r.label, avg(s.bytes_read) / 1e6 AS mb
                  FROM steps s JOIN canonical_runs r USING (run_id)
                  WHERE list_contains(r.campaigns, 'mor-row') AND s.arm = 'mor-v3'
                    AND s.phase = 'batch' AND s.step >= 1
                  GROUP BY 1""").set_index("label").mb

# each design's mean write and full read (s), and what it sent and moved, per store and setting;
# `every` has the appendix's two variants too. RMoR's are over the MoR of its own run (`own`)
every = fig.designs(spliced(fig.bp_times), main=False)
times = every[every.design.isin(MAIN)]
own = fig.designs(fig.cs_frame(fig.bp_times, RMOR))
over = fig.over_mor(times, KEYS, own)
# MoR's time in RMoR's run of a setting over its time in the others'
_mor = times[times.design == "MoR"].set_index(KEYS + ["what"]).s
mor_drift = (own[own.design == "MoR"].set_index(KEYS + ["what"]).s / _mor - 1).groupby(
    ["store", "what"])
O = over.set_index(KEYS + ["design", "what"]).over_mor.sort_index()


def o(config, design, what=WRITE, store=S3X):
    """a design's mean write (or full read) over MoR's"""
    return O[(store, config, design, what)]


def rng(g, key=None, pct=False, f=".2f"):
    """'lo–hi' of a series, or of a grouped one at key; as percentages with pct"""
    lo, hi = (g.min(), g.max()) if key is None else (g.min()[key], g.max()[key])
    lo, hi = (f"{100 * lo:.0f}", f"{100 * hi:.0f}") if pct else (f"{lo:{f}}", f"{hi:{f}}")
    return (lo if lo == hi else f"{lo}–{hi}") + ("%" if pct else "")


def ratio(design, what, store=None):
    """a design's mean write (or full read) over MoR's at every setting (on one store)"""
    d = over[(over.design == design) & (over.what == what)]
    return d[d.store == store].over_mor if store else d.over_mor


# full reads per write that repay a design's write against MoR's: its extra time per write over
# the time it saves per full read (never, where it saves none)
be = over.pivot_table(index=KEYS + ["design"], columns="what", values=["s", "mor_s"])
saved = be[("mor_s", READ)] - be[("s", READ)]
BE = ((be[("s", WRITE)] - be[("mor_s", WRITE)]) / saved).where(saved > 0, np.inf).sort_index()

# the full read's GETs and a write's MB along the row group's size (the same on both stores)
s3 = times[times.store == S3X].set_index(["config", "design", "what"])
gets = s3.gets.xs(READ, level="what")
mbw = s3.mb_written.xs(WRITE, level="what")

# each design's writes and full reads over its run: the mean, the writes' 10th and 90th
# percentiles, and the full reads after 5 and after 30 writes (1. Writing against reading)
spread = spliced(fig.bp_spread)
trade = fig.tradeoff(spread)
trade["grow"] = trade.read_last / trade.read_first
grow = trade.groupby("design").grow
_m, _r = (trade[trade.design == d].set_index(KEYS) for d in ("MoR", "RMoW-append"))
rw_first = (_r.read_first / _m.read_first).groupby("store")
rw_last = (_r.read_last / _m.read_last).groupby("store")

# where each write's and full read's time went (phase_breakdown), by part and by the write's steps
_parts_main, _parts_rmor = (fig.cs_frame(fig.bp_parts, r, fine=True) for r in (RUNS, RMOR))
parts = fig.designs(fig.rmor_spliced(_parts_main, _parts_rmor))
parts["setting"] = parts.config.map(fig.CS_SETTING)
share = parts.pivot_table(index=KEYS + ["design", "what"], columns="part", values="s")
share = share.div(share.sum(axis=1), axis=0)
read_store = share.xs(READ, level="what")["the store"].groupby("store")
write_share = share.xs(WRITE, level="what").groupby(["store", "design"])
store_w = write_share["the store"].max()
steps = fig.designs(spliced(fig.bp_steps))
step_share = steps.groupby(KEYS + ["design", "write_step"]).s.sum().unstack()
step_share = step_share.div(step_share.sum(axis=1), axis=0).groupby(["store", "design"])
# rows and planning in each design's read and write
RP = "rows and planning"
rp_read = parts[(parts.what == READ) & (parts.part == RP)].set_index(KEYS + ["design"]).s
rp_write = parts[(parts.what == WRITE) & (parts.part == RP)].set_index(KEYS + ["design"]).s

# the time a full read waits on the store in RMoR's run of a setting against the others' run
# (Limits): MoR's and RMoR's, the designs RMoR's runs hold
def waits(p):
    p = fig.designs(p)
    return p[(p.what == READ) & (p.part == "the store")].set_index(KEYS + ["design"]).s


wait_d = (waits(_parts_rmor) - waits(_parts_main)).dropna().groupby(["store", "design"])

# the settings in prose
PROSE = {"sweep-base": "the base point", "sweep-p10": "p = 0.1", "sweep-p30": "p = 0.3",
         "sweep-s0": "a skew of 0", "sweep-s2": "a skew of 2", "sweep-rg32m": "32 MiB row groups",
         "sweep-rg64m": "64 MiB row groups", "sweep-late99-1": "a p99 lateness of 1 write",
         "sweep-late99-20": "a p99 lateness of 20 writes",
         "sweep-ingest": "the ingest window with statements",
         "sweep-ingest-rows": "the ingest window with a row a change"}


def listed(configs):
    v = [PROSE[c] for c in fig.CS_CONFIGS if c in set(configs)]
    return v[0] if len(v) == 1 else ", ".join(v[:-1]) + " and " + v[-1]


# where RMoR reads faster than MoR on Azure, and the reads per write that repay its write
r_az = over[(over.design == "RMoR") & (over.what == READ) & (over.store == AZ)]
az_fast = listed(r_az[r_az.over_mor < 1].config) if (r_az.over_mor < 1).any() else None
be_r = BE.xs("RMoR", level="design")
be_r_s3 = be_r[S3X]
be_r_az = be_r[AZ][np.isfinite(be_r[AZ])]
az_pays = " and ".join(f"{PROSE[c]} (past {be_r_az[c]:.0f})" for c in fig.CS_CONFIGS
                       if c in be_r_az.index) or "nowhere"

# each design's latency at every write and full read of its run (Latency as snapshots are added)
series = fig.designs(spliced(fig.bp_series))

# Appendix B: RMoR read without skipping, against RMoR's read; TailCoW against MoR
noskip = every[(every.design == "RMoR without skipping") & (every.what == READ)].set_index(KEYS).s
noskip_ratio = noskip / times[(times.design == "RMoR") & (times.what == READ)].set_index(KEYS).s
sp_all = fig.designs(spread, main=False)
_n = sp_all[(sp_all.design == "RMoR without skipping") & (sp_all.what == READ)]
noskip_grow = (_n.last_s / _n.first_s).max()
tail = fig.over_mor(every[every.design.isin(["MoR", "TailCoW"])], KEYS)
tail_g = tail.groupby(["store", "what"]).over_mor
```

:::{note} Calibration: the sweeps' first cloud measurement, superseded as results
These are the sweeps as first measured in the cloud (2026-11-01), kept for the record and for the
model's check ([the model on the sweeps](./cloud-sweep-model.md)). Their workload mixes inserts and
updates, and their MoR does not read the rows its updates replace, as later runs do: with that
read, MoR's write reads {eval}`f"{mor_row_mb.min():.0f}"`–{eval}`f"{mor_row_mb.max():.0f}"` MB at
these settings (havoc's runs of MoR alone, campaign `mor-row`), so the ratios below overstate what
the region designs' writes cost against MoR's. The results are now
[Updates and inserts: writes against reads](./workloads.md), simulated at the current build, until
a cloud run of those workloads measures them.
:::

How each design's trade-off between writing and reading moves as the workload and the layout
change, measured on S3 Express and Azure Blob. From the base point, one setting changes at a time:
how much of a region an update statement changes (the update share p), how the updates spread over
regions (their skew), the row group's size, how late inserts arrive, and a workload of recent keys
instead of regions.

- **No region design's write catches MoR's at any setting.** RMoW-append's write takes
  {eval}`rng(ratio('RMoW-append', WRITE, S3X), f='.1f')` times as long as MoR's on S3 Express and
  {eval}`rng(ratio('RMoW-append', WRITE, AZ), f='.1f')` on Azure; its full read takes
  {eval}`rng(ratio('RMoW-append', READ))` of MoR's everywhere.
- **When an update changes more of a region, RMoW-append pays for itself after about one full read
  per write.** From p = 0.03 to 0.3, RMoW-append's write over MoR's falls from
  {eval}`f"{o('sweep-base', 'RMoW-append'):.1f}"` to {eval}`f"{o('sweep-p30', 'RMoW-append'):.1f}"`
  on S3 Express and from {eval}`f"{o('sweep-base', 'RMoW-append', store=AZ):.1f}"` to
  {eval}`f"{o('sweep-p30', 'RMoW-append', store=AZ):.1f}"` on Azure, and the full reads per write
  that repay its write fall from {eval}`f"{BE[(S3X, 'sweep-base', 'RMoW-append')]:.1f}"` to
  {eval}`f"{BE[(S3X, 'sweep-p30', 'RMoW-append')]:.1f}"` and from
  {eval}`f"{BE[(AZ, 'sweep-base', 'RMoW-append')]:.1f}"` to
  {eval}`f"{BE[(AZ, 'sweep-p30', 'RMoW-append')]:.1f}"`.
- **Bigger row groups widen the gap.** At 64 MiB RMoW-append's write takes
  {eval}`f"{o('sweep-rg64m', 'RMoW-append'):.1f}"` and
  {eval}`f"{o('sweep-rg64m', 'RMoW-append', store=AZ):.1f}"` times MoR's.
- **RMoW-append writes fastest of the region designs at every setting,** but its lead over RMoR
  narrows as p rises: its write takes
  {eval}`f"{o('sweep-base', 'RMoW-append') / o('sweep-base', 'RMoR'):.2f}"` of RMoR's at p = 0.03
  and {eval}`f"{o('sweep-p30', 'RMoW-append') / o('sweep-p30', 'RMoR'):.2f}"` at 0.3 on S3 Express.
- **RMoR reads a little faster than MoR on S3 Express, and slower on Azure at most settings**
  ({eval}`rng(ratio('RMoR', READ, S3X))` and {eval}`rng(ratio('RMoR', READ, AZ))` of MoR's time),
  while its write takes {eval}`rng(ratio('RMoR', WRITE, S3X), f='.1f')` and
  {eval}`rng(ratio('RMoR', WRITE, AZ), f='.1f')` times as long as MoR's.

## The designs

```{include} ./_designs.md
```

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

A **write** is one writer committing one batch, and its time is the **write latency**. A **full
read** is one thread reading the whole table, and its time is the **read latency**. Each write adds
a snapshot. Latencies are means over the 30 writes and the 6 full reads after the initial load, one
every 5 writes.

**Where it ran** (build 6f24575, every run once):
- **S3 Express:** one `c7a.4xlarge` per configuration beside the directory bucket in usw2-az3,
  nine at once on 2026-11-01 (`sweep-rg64m` on `sweep-p30`'s VM after it), so each measured the
  store under the others' load;
- **Azure Blob:** one `D8as_v6` in westus, the configurations one after another from 2026-11-01.

**RMoR's times are from runs of its own,** one per setting with MoR and RMoR alone, on one VM per
store (a `c7a.4xlarge` and a `D8as_v6`) running them one after another on 2026-11-03 and 04, at
d30544a. That build changes one thing a run does: RMoR's writer and reader check a row group
against its deletion vector by asking about the row group's whole range, where the runs above
tested each row position in turn and spent up to seconds of each write and read on it
([RMoR re-measured](./rmor-rerun.md)). RMoR's ratios are over the MoR of its own run.

(writing-against-reading)=
## 1. Writing against reading

Each design's mean write latency and mean read latency at every setting, a row per setting (RMoR's
from its own runs), and how many full reads per write repay a region design's write against MoR's:
its extra time per write over the time it saves per full read, or never where it saves none
([4. When a write pays for itself](#when-a-write-pays) has the numbers). Read a row
across: a region design writes slower than MoR (left) and mostly reads faster (middle), and the
right panel says how often the table must be read in full, per write, to make up for it. On a write
the whisker runs from the 10th to the 90th percentile of the design's 30 writes. On a read it runs
from the full read after 5 writes to the one after 30: each setting ran once, and with no compaction
a full read slows as updates pile up, so it shows the table aging, not noise. Each panel's scale
fits its store's values.

```{code-cell} ipython3
:label: cloud-sweep-tradeoff-s3x
:tags: [remove-input]
:class: col-page-right

color, shape = fig.design_encoding()
be_frame = BE.rename("reads").reset_index()
fig.cs_tradeoff_panels(trade, be_frame, S3X, color, shape).configure_legend(orient="bottom",
                                                                            symbolOpacity=1)
```

```{code-cell} ipython3
:label: cloud-sweep-tradeoff-azure
:tags: [remove-input]
:class: col-page-right

fig.cs_tradeoff_panels(trade, be_frame, AZ, color, shape).configure_legend(orient="bottom",
                                                                           symbolOpacity=1)
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

names = {"write_s": "write", "write_p10": "write p10", "write_p90": "write p90",
         "read_s": "full read", "read_first": "read after 5", "read_last": "read after 30"}
t = (trade.assign(setting=pd.Categorical(trade.setting, fig.CS_SETTINGS))
     .pivot_table(index=["setting", "ord", "design"], columns="store", values=list(names),
                  observed=True)
     .reindex(columns=[(v, st) for st in STORES for v in names]))
t.columns = [f"{st}: {names[v]}" for v, st in t.columns]
table(t.reset_index().drop(columns="ord").round(2),
      caption="Seconds: each design's mean write and its 10th and 90th percentiles, its mean full "
              "read, and its full reads after 5 and after 30 writes")
```

- **A full read slows as the updates pile up, MoR's more than RMoW-append's.** From the read after
  5 writes to the read after 30, MoR's grows {eval}`rng(grow, 'MoR')` times and RMoW-append's
  {eval}`rng(grow, 'RMoW-append')` times, so RMoW-append's full read goes from
  {eval}`rng(rw_first, S3X)` of MoR's to {eval}`rng(rw_last, S3X)` on S3 Express, and from
  {eval}`rng(rw_first, AZ)` to {eval}`rng(rw_last, AZ)` on Azure.
- **RMoR's full read grows {eval}`rng(grow, 'RMoR')` times** over a run, the widest range of the
  four, and RMoW-external's {eval}`rng(grow, 'RMoW-external')` times.

### Latency as snapshots are added

The same runs write by write: each design's write latency at every write, and its read latency
after the initial load and every 5 writes, for one setting at a time (chosen in the dropdown; the
base point at first). One latency axis for both stores.

```{code-cell} ipython3
:label: cloud-sweep-latency
:tags: [remove-input]
:class: col-page-right

fig.latency_chart(series).configure_legend(orient="bottom")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

def series_table(what):
    d = series[series.what == what].assign(
        setting=lambda x: pd.Categorical(x.config.map(fig.CS_SETTING), fig.CS_SETTINGS))
    t = (d.pivot_table(index=["setting", "step"], columns=["store", "design"], values="s",
                       observed=True)
         .reindex(columns=[(st, x) for st in STORES for x in MAIN]))
    t.columns = [f"{st}: {x}" for st, x in t.columns]
    return table(t.reset_index().round(2),
                 caption=f"{fig.TIME_TITLE[what].capitalize()} (s) at each snapshot")


series_table(WRITE)
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

series_table(READ)
```

## 2. What a write costs

Each region design's mean write latency over MoR's, along each setting (a log scale; 1 is
MoR's).

```{code-cell} ipython3
:label: cloud-sweep-writes
:tags: [remove-input]

rcolor, rshape = fig.design_encoding(REGION)


def along(d, title, domain, ticks):
    """d's over_mor along each axis: a column per axis, a row per store, a line per design"""
    d = fig.cs_axes(d)
    cols = []
    for axis in fig.CS_AXES:
        a = d[d.axis == axis]
        log = axis != "region skew"
        x = alt.X("value:Q", title=axis, scale=alt.Scale(type="log" if log else "linear",
                                                           nice=False, zero=False),
                  axis=alt.Axis(values=sorted(a.value.unique())))
        y = alt.Y("over_mor:Q", title=title, scale=alt.Scale(type="log", domain=domain, nice=False),
                  axis=alt.Axis(values=ticks))
        lines = alt.Chart().mark_line(point=True).encode(
            x=x, y=y, color=rcolor, shape=rshape, detail="design:N",
            tooltip=["store", "config", "design", alt.Tooltip("over_mor:Q", format=".2f")])
        one = alt.Chart().mark_rule(strokeDash=[4, 3], color="gray").encode(y=alt.datum(1.0))
        cols.append(alt.layer(one, lines, data=a).properties(width=170, height=170)
                    .facet(row=alt.Row("store:N", title=None, sort=STORES)))
    return alt.hconcat(*cols).resolve_scale(x="independent")


along(over[over.what == WRITE], "write latency over MoR's", [1, 16],
      [1, 2, 4, 8, 16]).configure_legend(orient="bottom")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

def over_table(what):
    d = over[over.what == what].assign(
        setting=lambda x: pd.Categorical(x.config.map(fig.CS_SETTING), fig.CS_SETTINGS))
    t = d.pivot_table(index="setting", columns=["store", "design"], values="over_mor",
                      observed=True).reindex(columns=[(st, x) for st in STORES for x in REGION])
    t.columns = [f"{st}: {x}" for st, x in t.columns]
    return table(t.reset_index().round(2),
                 caption=f"Each design's mean {what[2:]} over MoR's (RMoR's over the MoR of its "
                         "own run)")


over_table(WRITE)
```

- **The update share and the skew move every region design the same way:** the fewer regions a
  write changes (more of each changed at p = 0.3, or updates piled onto hot regions at a skew of
  2), the less a region design rewrites. At a skew of 2 RMoR's write comes closest to
  RMoW-append's ({eval}`f"{o('sweep-s2', 'RMoR'):.1f}"` against
  {eval}`f"{o('sweep-s2', 'RMoW-append'):.1f}"` times MoR's on S3 Express).
- **Lateness moves RMoR and RMoW-external, not RMoW-append.** With a p99 of 20 writes instead of 1,
  RMoR's write goes from {eval}`f"{o('sweep-late99-1', 'RMoR'):.1f}"` to
  {eval}`f"{o('sweep-late99-20', 'RMoR'):.1f}"` times MoR's on S3 Express, RMoW-append's from
  {eval}`f"{o('sweep-late99-1', 'RMoW-append'):.1f}"` to
  {eval}`f"{o('sweep-late99-20', 'RMoW-append'):.1f}"`. RMoR's write carries more bytes the later
  inserts arrive ({eval}`f"{mbw[('sweep-late99-1', 'RMoR')]:.0f}"`,
  {eval}`f"{mbw[('sweep-base', 'RMoR')]:.0f}"` and {eval}`f"{mbw[('sweep-late99-20', 'RMoR')]:.0f}"`
  MB at a p99 of 1, 5 and 20 writes), and RMoW-append's the same
  {eval}`f"{mbw[('sweep-base', 'RMoW-append')]:.0f}"` MB at each. The likely cause, not checked
  here: RMoR sorts each write's new file by event time, which mixes late inserts among the rows of
  the regions it rewrote, while RMoW-append keeps inserts in its tail.
- **Azure's gaps are narrower at every setting:** its slower requests make MoR's write, of which
  the store takes {eval}`rng(write_share['the store'], (AZ, 'MoR'), pct=True)` there, cost more,
  while the store takes at most {eval}`f"{100 * store_w[AZ][REGION].max():.0f}"`% of a region
  design's write.

### Where a write's time goes

Each design's mean write latency at every setting, split by what the write's thread did:
- **decoding** and **encoding** Parquet;
- **deletion vectors and footers:** building, loading and merging them;
- **rows and planning:** the rest of its CPU, making the rows it writes, merging the changes into
  the regions it rewrites and sorting them by event time, and planning its scans;
- **the store:** waiting on it, and its client's CPU;
- **paused:** neither running nor waiting on the store (garbage collection, the JIT, scheduling).

One axis for both stores.

```{code-cell} ipython3
:label: cloud-sweep-write-parts
:tags: [remove-input]
:class: col-page-right

fig.parts_chart(parts, WRITE, "setting", fig.CS_SETTINGS).configure_legend(orient="top")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

def parts_table(what):
    d = parts[parts.what == what].assign(
        setting=lambda x: pd.Categorical(x.setting, fig.CS_SETTINGS))
    t = (d.pivot_table(index=["setting", "ord", "design"], columns=["store", "part"], values="s",
                       observed=True)
         .reindex(columns=[(st, p) for st in STORES for p in fig.TIME_PARTS]))
    t.columns = [f"{st}: {p}" for st, p in t.columns]
    return table(t.reset_index().drop(columns="ord").round(2),
                 caption=f"Seconds of each design's mean {what[2:]}, by part")


parts_table(WRITE)
```

- **Rows and planning cost the region designs up to several seconds per write, and MoR tenths:**
  {eval}`rng(rp_write.xs('RMoR', level='design'), f='.1f')` s of RMoR's write,
  {eval}`rng(rp_write.xs('RMoW-external', level='design'), f='.1f')` s of RMoW-external's and
  {eval}`rng(rp_write.xs('RMoW-append', level='design'), f='.1f')` s of RMoW-append's, across the
  settings and stores, against {eval}`rng(rp_write.xs('MoR', level='design'), f='.2f')` s of MoR's.
  A region design makes and sorts every row of every region it rewrites.
- **RMoW-append's write is mostly CPU at every setting, encoding above all.** The store takes
  {eval}`rng(write_share['the store'], (S3X, 'RMoW-append'), pct=True)` of it on S3 Express and
  {eval}`rng(write_share['the store'], (AZ, 'RMoW-append'), pct=True)` on Azure, and encoding the
  regions it rewrites {eval}`rng(write_share['encoding'], (S3X, 'RMoW-append'), pct=True)` and
  {eval}`rng(write_share['encoding'], (AZ, 'RMoW-append'), pct=True)`.

A write's time by step, for one setting at a time (chosen in the dropdown; the base point at
first): finding the rows the write changes (locate), reading back what it rewrites (merge-read),
writing data files, regions and footers (write), writing deletion vectors and RMoW-external's
footers (delete-write), and the rest of the write. Each step is split as above, on one axis for
every setting.

```{code-cell} ipython3
:label: cloud-sweep-steps
:tags: [remove-input]
:class: col-page-right

fig.steps_chart(steps).configure_legend(orient="top")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

w = steps.assign(setting=pd.Categorical(steps.config.map(fig.CS_SETTING), fig.CS_SETTINGS),
                 step=pd.Categorical(steps.write_step.map(fig.CS_STEP), list(fig.CS_STEP.values())))
w = (w.pivot_table(index=["setting", "ord", "design", "step"], columns=["store", "part"],
                   values="s", observed=True)
     .reindex(columns=[(st, p) for st in STORES for p in fig.TIME_PARTS]))
w.columns = [f"{st}: {p}" for st, p in w.columns]
table(w.reset_index().drop(columns="ord").round(2),
      caption="Seconds of each design's mean write, by step and part")
```

- **MoR spends much of its write finding the rows it changes.** Its scan of the files that may
  hold them takes {eval}`rng(step_share['locate'], (S3X, 'MoR'), pct=True)` of its write on S3
  Express and {eval}`rng(step_share['locate'], (AZ, 'MoR'), pct=True)` on Azure, the least at
  p = 0.3 and on the ingest window with statements.
- **RMoW-append finds them almost for nothing,** in its footers' row-group statistics
  ({eval}`rng(step_share['locate'], (S3X, 'RMoW-append'), pct=True)` and
  {eval}`rng(step_share['locate'], (AZ, 'RMoW-append'), pct=True)` of its write), and spends
  {eval}`rng(step_share['write'], (S3X, 'RMoW-append'), pct=True)` and
  {eval}`rng(step_share['write'], (AZ, 'RMoW-append'), pct=True)` writing its regions and footers.

## 3. What a read costs

Each region design's mean read latency over MoR's, along each setting (a log scale; 1 is
MoR's).
RMoW-append's and RMoW-external's lie close together.

```{code-cell} ipython3
:label: cloud-sweep-reads
:tags: [remove-input]

along(over[over.what == READ], "read latency over MoR's", [0.6, 2],
      [0.6, 0.8, 1, 1.25, 1.5, 2]).configure_legend(orient="bottom")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

over_table(READ)
```

- **RMoW-append and RMoW-external read faster than MoR everywhere,** in
  {eval}`rng(ratio('RMoW-append', READ))` and {eval}`rng(ratio('RMoW-external', READ))` of its
  time: they read only live row groups, from fewer files.
- **On Azure bigger row groups make RMoW-append's read faster still** (to
  {eval}`f"{o('sweep-rg64m', 'RMoW-append', READ, AZ):.2f}"` of MoR's at 64 MiB), and not on
  S3 Express ({eval}`f"{o('sweep-rg64m', 'RMoW-append', READ):.2f}"`). A full read of
  RMoW-append's table takes {eval}`f"{gets[('sweep-base', 'RMoW-append')]:.0f}"`,
  {eval}`f"{gets[('sweep-rg32m', 'RMoW-append')]:.0f}"` and
  {eval}`f"{gets[('sweep-rg64m', 'RMoW-append')]:.0f}"` GETs at 16, 32 and 64 MiB, a row group
  each, where MoR's write files stay about 15 MiB ({eval}`f"{gets[('sweep-base', 'MoR')]:.0f}"` to
  {eval}`f"{gets[('sweep-rg64m', 'MoR')]:.0f}"` GETs). A request costs more on Azure, so the fewer,
  larger GETs save RMoW-append more there.
- **RMoR reads a little faster than MoR on S3 Express, and slower on Azure at most settings:**
  {eval}`rng(ratio('RMoR', READ, S3X))` of MoR's time on S3 Express and
  {eval}`rng(ratio('RMoR', READ, AZ))` on Azure, faster there only at {eval}`az_fast`. On Azure its
  write files, past 256 MiB, are uploaded in blocks that read slower
  ([The base point](./base-point.md)).

### Where a read's time goes

Each design's mean read latency at every setting, split as a write's above. In a read, rows and
planning is planning the scan. One axis for both stores.

```{code-cell} ipython3
:label: cloud-sweep-read-parts
:tags: [remove-input]
:class: col-page-right

fig.parts_chart(parts, READ, "setting", fig.CS_SETTINGS).configure_legend(orient="top")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

parts_table(READ)
```

- **A full read is mostly the store's at every setting:** it takes
  {eval}`rng(read_store, S3X, pct=True)` of every design's full read on S3 Express and
  {eval}`rng(read_store, AZ, pct=True)` on Azure.
- **Planning a read takes every design at most {eval}`f"{rp_read.max():.2f}"` s,** across the
  settings and stores, RMoR's included, whose reader checks each row group against its deletion
  vector.

(when-a-write-pays)=
## 4. When a write pays for itself

How many full reads per write repay a design's write against MoR's: its extra time per write over
the time it saves per full read ("never" where it saves none), as the right panels of
[1. Writing against reading](#writing-against-reading) draw it.

```{code-cell} ipython3
:tags: [remove-input]

b = BE.rename("reads").reset_index().assign(
    setting=lambda x: pd.Categorical(x.config.map(fig.CS_SETTING), fig.CS_SETTINGS))
b = b.pivot_table(index="setting", columns=["store", "design"], values="reads", observed=True)
b = b.reindex(columns=[(st, x) for st in STORES for x in REGION])
b.columns = [f"{st}: {x}" for st, x in b.columns]
table(b.map(lambda v: "never" if np.isinf(v) else f"{v:.1f}").reset_index())
```

RMoR repays its write on S3 Express past {eval}`rng(be_r_s3, f='.0f')` full reads per write, where
RMoW-append's repays past {eval}`rng(BE.xs('RMoW-append', level='design')[S3X], f='.1f')`. On Azure
it repays only where it reads faster than MoR: at {eval}`az_pays`.

## Limits

- One run of each configuration, on one seed, and one setting changed at a time.
- The S3 Express runs loaded one bucket at once; the Azure runs came one after another over about a
  day, at different hours.
- One kind of read: a single thread's full scan, through the changed reader
  ([Appendix A](./reader-changes.md)).
- RMoR's times are from runs of their own (Setup), compared over the MoR of the same run. Between
  the runs MoR's own write moved by {eval}`rng(mor_drift.get_group((S3X, WRITE)), f='+.1%')`
  and its full read by {eval}`rng(mor_drift.get_group((S3X, READ)), f='+.1%')` on S3
  Express, and by {eval}`rng(mor_drift.get_group((AZ, WRITE)), f='+.1%')` and
  {eval}`rng(mor_drift.get_group((AZ, READ)), f='+.1%')` on Azure, where the store takes
  most of MoR's write and the time a full read waits on it moves from run to run: RMoR's read
  waited {eval}`rng(wait_d.get_group((AZ, 'RMoR')), f='+.2f')` s against the others' run of its
  setting, MoR's {eval}`rng(wait_d.get_group((AZ, 'MoR')), f='+.2f')` s
  ([RMoR re-measured](./rmor-rerun.md)). A ratio over MoR's on Azure carries that.
- No compaction.

How the model priced these runs, the checks that each did the work its twin counts, and the
hypotheses' verdicts are in [The model on the cloud sweeps](./cloud-sweep-model.md), which prices
the runs above, RMoR's with the check by position among them, and names the designs as the code does: MoR-v3 for MoR, RMoR-C for RMoR, RMoR-C footer for RMoW-external and
RMoW-C for RMoW-append.

## Appendix B: the variants left out

**RMoR without skipping.** Read by a reader that fetches and decodes each row group its deletion
vectors cover whole, as stock Iceberg's does, RMoR's full read takes
{eval}`rng(noskip_ratio, f='.1f')` times as long as with skipping, on either store, and grows up to
{eval}`f"{noskip_grow:.1f}"` times over a run, as dead row groups pile up. Its write is RMoR's.

**TailCoW**, a control, is MoR with RMoR's tail of inserts: it isolates the tail's merging from how
RMoR handles updates. Its writes take {eval}`rng(tail_g, (S3X, WRITE))` of MoR's time on S3
Express and {eval}`rng(tail_g, (AZ, WRITE))` on Azure, and its full reads
{eval}`rng(tail_g, (S3X, READ))` and {eval}`rng(tail_g, (AZ, READ))`: each write's new file is
about the tail's target size, so the tail seldom has anything to merge.

```{code-cell} ipython3
:tags: [remove-input]

a = (noskip.rename("RMoR without skipping: read (s)").to_frame()
     .join(noskip_ratio.rename("RMoR without skipping: read over RMoR's")))
tw = tail.pivot_table(index=KEYS, columns="what", values="over_mor")
tw.columns = [f"TailCoW: {w[2:]} over MoR's" for w in tw.columns]
a = a.join(tw).reset_index()
a["setting"] = pd.Categorical(a.config.map(fig.CS_SETTING), fig.CS_SETTINGS)
a["store"] = pd.Categorical(a.store, STORES)
table(a.sort_values(["store", "setting"])[["store", "setting", *a.columns[2:-1]]].round(2))
```
