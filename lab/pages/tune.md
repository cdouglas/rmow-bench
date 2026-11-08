---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Tuning write policy
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
from labbook.db import connect
theme()
fig = figures_module()
tune = fig.tuner()
con = connect()

train = tune.records(con, tune.TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
formula = tune.Formula(train)
ns = tune.pace(con)
J = tune.judge(con, formula, ns)
pred = tune.name_policies(con, J["pred"])
ranks = tune.name_policies(con, J["ranks"])
fin = tune.name_policies(con, J["finalists"])
rec, loo_mean, loo_write = tune.loo(train)

WL = {"upd": "updates", "ing": "inserts"}
ST = {"s3x": "S3 Express", "azure": "Azure Blob"}
MIX = {"writes": "1 window and 0.1 full reads a write", "windows": "10 windows a write",
       "scans": "2 full reads a write", "sorted": "1 sorted read a write"}


def pct(x, d=0):
    return f"{100 * x:.{d}f}%"


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


runs = q("""SELECT label, CASE WHEN host = 'havoc' THEN 'havoc' ELSE 'laptop' END AS host, run_id,
                   status, coalesce(mark, '') AS mark, round(elapsed_s / 60) AS minutes
            FROM runs WHERE list_contains(campaigns, 'tune') ORDER BY label, host, run_id""")
n_havoc = int((runs.host == "havoc").sum())
n_laptop = int(((runs.host == "laptop") & (runs.status == "finished") & (runs.mark == "")).sum())
n_excluded = int((runs.mark == "excluded").sum())
havoc_min = runs[runs.host == "havoc"].minutes
```

Which write policy should a table use, given how it is read? This page ranks every policy of two
workloads under four read mixes on S3 Express and Azure Blob, from a grid of configurations run on
havoc (campaign `tune`, beside `windows`), and checks the ranking on the laptop, which counts each
phase's instructions. It judges T1–T4 (docs/HYPOTHESES.md, 2026-11-08).

## Setup

**The workloads** are the `windows` campaign's: the updates (150,000 changes a write, clustered by
key region, every update reading its old rows) and the inserts at lateness p99 5 (350,000 a write,
sorted by event time, some late), 30 writes each on a table of 25.7 million rows.

**A policy** is a configuration and an arm. The grid moves one knob at a time from the `windows`
configurations (`configs/tune-*.json`): MoR compacted every 3, 6 and 12 writes on both workloads;
on the updates the deferred merges at θ 0.12, 0.5 and 1, and RMoW-append and the deferred merges
with row groups of 4, 8 and 32 MiB; on the inserts RMoR's tail sealed at 256 MiB, and at 160 MiB
with fanouts of 2 and 8, and row groups of 4 MiB. {eval}`n_havoc` runs on havoc
({eval}`span(havoc_min, "{:.0f}")` minutes, 2 cores each, beside another user's load, which moves
no request or byte), and {eval}`n_laptop` on the laptop, for instructions. Three laptop runs
ran beside the lab's own ingest and build, which slowed the laptop's clock (to 2.5 GHz from 4.6
for the 32 MiB updates run) and so their work's pace; they are excluded
({eval}`n_excluded` marked) and were run again on a quiet laptop.

```{code-cell} ipython3
:tags: [remove-input]

table(runs.rename(columns={"minutes": "min"}), caption="The grid's runs")
```

**The tuner** (`analysis/tune.py`) prices each policy's time per write under a mix as

- its write, plus its compactions spread over the writes,
- plus, for each read, the reads a write times the read's time.

A phase's time is its work plus its requests, priced by size with the store's spike (the lab's
`sized_latency`; S3 Express `spike/s3x-spike-20261031T163533Z`, Azure Blob
`spike/azure-spike-20261031T234510Z`). The reads are those of the probes after the 5th to the 30th
write: a full read, a 1% window (the mean of each probe's five) and the newest 1% under the reader
that skips dead row groups, and the sorted read under `cached`.

**The work** comes from havoc's counts through a formula fitted on the laptop's `windows` runs. It
is linear in the bytes read and written and the requests, with the merge's rows times log2 of the
row groups it holds for the sorted read. It is fitted per kind of phase, and per arm and workload
where the training runs allow. The work is then priced at the laptop's pace for that arm and
phase. Held out one configuration at a time, its error on a policy's mean:

```{code-cell} ipython3
:tags: [remove-input]

err = loo_mean.groupby("kind").err.agg(lambda e: e.abs().median()).rename("median")
mx = loo_mean.groupby("kind").err.agg(lambda e: e.abs().max()).rename("max")
loo_t = pd.concat([err, mx], axis=1).reset_index()
loo_t = loo_t[loo_t.kind.isin(["read_full", "read_window", "read_recent", "read_sorted", "compact"])]
loo_t = pd.concat([pd.DataFrame([{"kind": "write", "median": loo_write.err.abs().median(),
                                  "max": loo_write.err.abs().max()}]), loo_t])
w_med, w_max = loo_write.err.abs().median(), loo_write.err.abs().max()
table(loo_t, fmt=".3f", caption="The formula's error held out by configuration (|predicted/measured − 1|)")
```

**The mixes**, as reads a write: `writes` (1 window and 0.1 full reads), `windows` (10 windows),
`scans` (2 full reads) and `sorted` (1 sorted read). The finalists are each mix's two cheapest
policies on each workload and store.

## What the knobs do (T3, T4)

```{code-cell} ipython3
:tags: [remove-cell]

counts = q("""
WITH r AS (SELECT run_id, label FROM runs WHERE host = 'havoc'
           AND (list_contains(campaigns, 'tune') OR list_contains(campaigns, 'windows'))),
w AS (SELECT r.label, s.arm, avg(s.bytes_written) / 1e6 AS write_mb FROM steps s JOIN r USING (run_id)
      WHERE s.phase = 'batch' AND s.step >= 1 GROUP BY ALL),
win AS (SELECT r.label, s.arm, avg(s.bytes_read) / 1e6 AS window_mb, avg(s.requests) AS window_requests
        FROM steps s JOIN r USING (run_id)
        WHERE s.phase = 'read_window' AND s.step = 30 AND s.reader = 'cached-skip' GROUP BY ALL),
so AS (SELECT r.label, s.arm, avg(s.held_bytes) / 1e6 AS sorted_held_mb FROM steps s JOIN r USING (run_id)
       WHERE s.phase = 'read_sorted' AND s.step = 30 GROUP BY ALL)
SELECT * FROM w JOIN win USING (label, arm) LEFT JOIN so USING (label, arm)""")
counts = tune.name_policies(con, counts.assign(design=counts.arm.map(tune.DESIGN)))
C = counts.set_index(["label", "arm"])


def ratio(a, b, col):
    return C.loc[a, col] / C.loc[b, col]


T3 = {
    "θ 0.12 write": (ratio(("tune-upd-defer-p12", "rmor-density"), ("win-upd-base", "rmor-density"), "write_mb"), "≤ 0.6", lambda v: v <= 0.6),
    "θ 0.5 write": (ratio(("tune-upd-defer-p50", "rmor-density"), ("win-upd-base", "rmor-density"), "write_mb"), "≤ 0.2", lambda v: v <= 0.2),
    "θ 1 write": (ratio(("tune-upd-defer-p100", "rmor-density"), ("win-upd-base", "rmor-density"), "write_mb"), "≤ 0.2", lambda v: v <= 0.2),
    "θ 0.12 window": (ratio(("tune-upd-defer-p12", "rmor-density"), ("win-upd-base", "rmor-density"), "window_mb"), "≤ 1.5", lambda v: v <= 1.5),
    "θ 0.5 window": (ratio(("tune-upd-defer-p50", "rmor-density"), ("win-upd-base", "rmor-density"), "window_mb"), "≤ 1.5", lambda v: v <= 1.5),
    "θ 1 window": (ratio(("tune-upd-defer-p100", "rmor-density"), ("win-upd-base", "rmor-density"), "window_mb"), "≤ 1.5", lambda v: v <= 1.5),
    "RMoW-append 4 MiB window": (ratio(("tune-upd-rg4m", "rmow-c"), ("win-upd-base", "rmow-c"), "window_mb"), "≤ 0.6", lambda v: v <= 0.6),
    "RMoW-append 8 MiB window": (ratio(("tune-upd-rg8m", "rmow-c"), ("win-upd-base", "rmow-c"), "window_mb"), "≤ 0.85", lambda v: v <= 0.85),
    "RMoW-append 4 MiB write": (ratio(("tune-upd-rg4m", "rmow-c"), ("win-upd-base", "rmow-c"), "write_mb"), "≤ 1.1", lambda v: v <= 1.1),
    "RMoW-append 8 MiB write": (ratio(("tune-upd-rg8m", "rmow-c"), ("win-upd-base", "rmow-c"), "write_mb"), "≤ 1.1", lambda v: v <= 1.1),
    "RMoW-append 32 MiB write": (ratio(("tune-upd-rg32m", "rmow-c"), ("win-upd-base", "rmow-c"), "write_mb"), "≥ 1.5", lambda v: v >= 1.5),
}
T4 = {
    "seal 256 MiB write": (ratio(("tune-ing-seal256", "rmor-c"), ("win-ing-seal160", "rmor-c"), "write_mb"), "≤ 1.5", lambda v: v <= 1.5),
    "seal 256 MiB window": (ratio(("tune-ing-seal256", "rmor-c"), ("win-ing-seal160", "rmor-c"), "window_mb"), "≤ 0.85", lambda v: v <= 0.85),
    "fanout 2 write": (ratio(("tune-ing-seal160-f2", "rmor-c"), ("win-ing-seal160", "rmor-c"), "write_mb"), "≤ 1.6", lambda v: v <= 1.6),
    "fanout 2 window": (ratio(("tune-ing-seal160-f2", "rmor-c"), ("win-ing-seal160", "rmor-c"), "window_mb"), "≤ 0.8", lambda v: v <= 0.8),
    "fanout 8 write": (ratio(("tune-ing-seal160-f8", "rmor-c"), ("win-ing-seal160", "rmor-c"), "write_mb"), "0.85–1.15", lambda v: abs(v - 1) <= 0.15),
    "fanout 8 window": (ratio(("tune-ing-seal160-f8", "rmor-c"), ("win-ing-seal160", "rmor-c"), "window_mb"), "0.85–1.15", lambda v: abs(v - 1) <= 0.15),
}
for arm, d in (("mor-v3", "MoR"), ("rmor-c-kept", "RMoR"), ("rmow-c", "RMoW-append")):
    T4[f"{d} 4 MiB window"] = (ratio(("tune-ing-rg4m", arm), ("win-ing-base", arm), "window_mb"), "≤ 0.5", lambda v: v <= 0.5)
    T4[f"{d} 4 MiB sorted held"] = (ratio(("tune-ing-rg4m", arm), ("win-ing-base", arm), "sorted_held_mb"), "≤ 0.5", lambda v: v <= 0.5)


def verdicts(T):
    return pd.DataFrame([{"claim": k, "measured": v, "predicted": p, "held": ok(v)} for k, (v, p, ok) in T.items()])


t3, t4 = verdicts(T3), verdicts(T4)
t3_bad, t4_bad = t3[~t3.held].claim.tolist(), t4[~t4.held].claim.tolist()
rg_rows = q("""SELECT r.label, s.arm, s.row_groups FROM steps s JOIN runs r USING (run_id)
               WHERE r.host = 'havoc' AND r.label IN ('tune-ing-rg4m', 'win-ing-base')
                 AND s.phase = 'state' AND s.step IN (0, 30)""")
added = (rg_rows.groupby(["label", "arm"]).row_groups.max() - rg_rows.groupby(["label", "arm"]).row_groups.min())
added_mor, added_app = int(added[("tune-ing-rg4m", "mor-v3")]), int(added[("tune-ing-rg4m", "rmow-c")])
```

Every count below is havoc's after the 30th write: a write's bytes as the mean of the 30, a
window's (under the reader that skips dead row groups) as the mean of the probe's five, and the
bytes the sorted read holds at its peak.

```{code-cell} ipython3
:tags: [remove-input]

show = counts[counts.label.str.startswith("tune-") | counts.label.isin(["win-upd-base", "win-ing-base", "win-ing-seal160"])]
table(show[["policy", "label", "write_mb", "window_mb", "window_requests", "sorted_held_mb"]]
      .sort_values(["label", "policy"]), fmt=".3g", caption="Bytes a write, a window and the sorted read's peak (MB), havoc")
```

**T3 (the updates):** the deferred merges write {eval}`f"{T3['θ 0.12 write'][0]:.2f}"` of θ
0.06's bytes at θ 0.12, {eval}`f"{T3['θ 0.5 write'][0]:.2f}"` at θ 0.5 and
{eval}`f"{T3['θ 1 write'][0]:.2f}"` at θ 1, where a write is MoR's
({eval}`f"{C.loc[('tune-upd-defer-p100', 'rmor-density'), 'write_mb']:.1f}"` MB against
{eval}`f"{C.loc[('win-upd-base', 'mor-v3'), 'write_mb']:.1f}"`), and their windows read
{eval}`span([T3[k][0] for k in ('θ 0.12 window', 'θ 0.5 window', 'θ 1 window')])` times θ 0.06's
bytes. With row groups of 4 and 8 MiB, RMoW-append's windows read
{eval}`f"{T3['RMoW-append 4 MiB window'][0]:.2f}"` and
{eval}`f"{T3['RMoW-append 8 MiB window'][0]:.2f}"` of its bytes at 16 MiB, and it writes less, not
the same: {eval}`f"{T3['RMoW-append 4 MiB write'][0]:.2f}"` and
{eval}`f"{T3['RMoW-append 8 MiB write'][0]:.2f}"` of the bytes. A 16 MiB row group seldom lines up
with a key region, so a statement rewrites about one and a half regions' worth at 16 MiB. With 4
MiB row groups it rewrites about its own region. At 32 MiB a write takes
{eval}`f"{T3['RMoW-append 32 MiB write'][0]:.2f}"` times the bytes, where the prediction, which
took 16 MiB to rewrite one region, was at least 1.5.

**T4 (the inserts):** RMoR sealed at 256 MiB writes
{eval}`f"{T4['seal 256 MiB write'][0]:.2f}"` times its bytes sealed at 160 MiB, and its windows read
{eval}`f"{T4['seal 256 MiB window'][0]:.2f}"` of theirs (the model said 1.24 and 0.74). A fanout of
2 writes {eval}`f"{T4['fanout 2 write'][0]:.2f}"` times and reads
{eval}`f"{T4['fanout 2 window'][0]:.2f}"`; a fanout of 8 does what a fanout of 4 does within 30
writes. With row groups of 4 MiB, MoR's windows read
{eval}`f"{T4['MoR 4 MiB window'][0]:.2f}"` of their bytes at 16 MiB and its sorted read holds
{eval}`f"{T4['MoR 4 MiB sorted held'][0]:.2f}"`, as predicted. RMoR's and RMoW-append's do not
move ({eval}`f"{T4['RMoR 4 MiB window'][0]:.2f}"` and
{eval}`f"{T4['RMoW-append 4 MiB window'][0]:.2f}"`). Their writers keep a write's inserts as one
row group whatever the target: over the 30 writes MoR added {eval}`added_mor` row groups, and
RMoW-append {eval}`added_app`. RMoR sealed at 256 MiB, whose merged units its writer cuts into row
groups, reads {eval}`f"{C.loc[('tune-ing-rg4m-seal256', 'rmor-c'), 'window_mb'] / C.loc[('tune-ing-seal256', 'rmor-c'), 'window_mb']:.2f}"`
of its windows' bytes at 16 MiB.

```{code-cell} ipython3
:tags: [remove-input]

table(pd.concat([t3.assign(h="T3"), t4.assign(h="T4")])[["h", "claim", "measured", "predicted", "held"]],
      fmt=".3f", caption="T3 and T4, each claim as registered")
```

## The picks

```{code-cell} ipython3
:tags: [remove-cell]

top = ranks[ranks["rank"] <= 3].copy()
top["s"] = top.cost_ms / 1e3


def pick(wl, st, mix, n=1):
    r = top[(top.workload == wl) & (top.store == st) & (top.mix == mix) & (top["rank"] == n)].iloc[0]
    return r


def pk(wl, st, mix):
    r = pick(wl, st, mix)
    return f"{r.policy} ({r.s:.1f} s)"


def margin(wl, st, mix):
    return pick(wl, st, mix, 2).cost_ms / pick(wl, st, mix).cost_ms


P = pred.set_index(["workload", "store", "label", "arm"])


def ph(wl, st, label, arm, col):
    return P.loc[(wl, st, label, arm), col] / 1e3


defer_win = [ph("upd", "s3x", l, "rmor-density", "window_ms") for l in ("win-upd-base", "tune-upd-defer-p12", "tune-upd-defer-p50", "tune-upd-defer-p100")]
morc_win = [ph("upd", "s3x", l, "mor-v3", "window_ms") for l in ("tune-upd-mor-c3", "tune-upd-mor-c6", "tune-upd-mor-c12", "win-upd-base-compact")]
defer_req = [C.loc[(l, "rmor-density"), "window_requests"] for l in ("win-upd-base", "tune-upd-defer-p12", "tune-upd-defer-p50", "tune-upd-defer-p100")]
defer_mb = [C.loc[(l, "rmor-density"), "window_mb"] for l in ("win-upd-base", "tune-upd-defer-p12", "tune-upd-defer-p50", "tune-upd-defer-p100")]
defer_w = [ph("upd", "s3x", l, "rmor-density", "write_ms") for l in ("win-upd-base", "tune-upd-defer-p12", "tune-upd-defer-p50", "tune-upd-defer-p100")]
mor_w = ph("upd", "s3x", "win-upd-base", "mor-v3", "write_ms")
app4 = {k: ph("upd", "s3x", "tune-upd-rg4m", "rmow-c", f"{k}_ms") for k in ("write", "window", "full")}
app16 = {k: ph("upd", "s3x", "win-upd-base", "rmow-c", f"{k}_ms") for k in ("write", "window", "full")}
morc = ranks[ranks.label.isin(["tune-upd-mor-c3", "tune-upd-mor-c6", "tune-upd-mor-c12", "win-upd-base-compact",
                                "tune-ing-mor-c3", "tune-ing-mor-c6", "tune-ing-mor-c12", "win-ing-base-compact"])]
morc_spread = morc.groupby(["workload", "store", "mix"]).cost_ms.agg(lambda c: c.max() / c.min())
morc_upd = morc_spread.loc["upd"].max()
morc_ing_win = morc_spread.loc[("ing", slice(None), "windows")].max()
morc_ing_rest = morc_spread.loc["ing"].drop("windows", level="mix").max()
app4_az_full = ph("upd", "azure", "tune-upd-rg4m", "rmow-c", "full_ms")
app16_az_full = ph("upd", "azure", "win-upd-base", "rmow-c", "full_ms")
```

Each mix's three cheapest policies, in simulated seconds a write at the laptop's pace:

```{code-cell} ipython3
:tags: [remove-input]

t = top.assign(workload=top.workload.map(WL), store=top.store.map(ST))
table(t[["workload", "store", "mix", "rank", "policy", "s", "over_best", "write_ms", "compact_ms",
         "window_ms", "full_ms", "sorted_ms"]], fmt=".3g",
      caption="The three cheapest policies per mix (s a write; phases in ms)")
```

On the updates, S3 Express:
- **Writes mostly** (1 window and 0.1 full reads a write): {eval}`pk("upd", "s3x", "writes")`.
  Compacted MoR's windows take {eval}`span(morc_win)` s, about what the deferred merges' take
  ({eval}`span(defer_win)` s). Those read only {eval}`span(defer_mb, "{:.0f}")` MB, but in
  {eval}`span(defer_req, "{:.0f}")` requests, and their writes take {eval}`span(defer_w, "{:.1f}")`
  s against MoR's {eval}`f"{mor_w:.1f}"`.
- **Windows** (10 a write): {eval}`pk("upd", "s3x", "windows")`, ahead by
  {eval}`pct(margin("upd", "s3x", "windows") - 1)`. With 4 MiB row groups a window takes
  RMoW-append {eval}`f"{app4['window']:.2f}"` s against {eval}`f"{app16['window']:.2f}"` at 16 MiB,
  and its write {eval}`f"{app4['write']:.1f}"` s against {eval}`f"{app16['write']:.1f}"`.
- **Scans** (2 full reads a write): {eval}`pk("upd", "s3x", "scans")`.
- **Sorted** (1 sorted read a write): {eval}`pk("upd", "s3x", "sorted")`, with the runner-up
  within {eval}`pct(margin("upd", "s3x", "sorted") - 1, 1)`.

On Azure Blob the picks are {eval}`pk("upd", "azure", "writes")` for writes mostly,
{eval}`pk("upd", "azure", "windows")` for windows, {eval}`pk("upd", "azure", "scans")` for scans
and {eval}`pk("upd", "azure", "sorted")` for sorted reads. Azure prices a request higher, so
RMoW-append's full read with 4 MiB row groups takes {eval}`f"{app4_az_full:.1f}"` s there against
{eval}`f"{app16_az_full:.1f}"` at 16 MiB (on S3 Express {eval}`f"{app4['full']:.1f}"` and
{eval}`f"{app16['full']:.1f}"`).

On the inserts the picks are {eval}`pk("ing", "s3x", "writes")` for writes mostly,
{eval}`pk("ing", "s3x", "windows")` for windows, {eval}`pk("ing", "s3x", "scans")` for scans and
{eval}`pk("ing", "s3x", "sorted")` for sorted reads on S3 Express. On Azure Blob they are
{eval}`pk("ing", "azure", "writes")`, {eval}`pk("ing", "azure", "windows")`,
{eval}`pk("ing", "azure", "scans")` and {eval}`pk("ing", "azure", "sorted")`.

Where full and sorted reads dominate, the designs differ little: every policy reads about the same
bytes, so the top three are within {eval}`pct(max(margin(w, s, m) for w in WL for s in ST for m in ("scans", "sorted")) - 1, 1)`
of one another at most.

**Which policy wins where.** The cheapest policy at each mix of windows and full reads a write
(no sorted reads):

```{code-cell} ipython3
:tags: [remove-cell]

W = np.geomspace(0.1, 100, 25)
F = np.geomspace(0.01, 10, 25)
pm = tune.policy_map(pred, W, F)
pm["facet"] = pm.workload.map(WL) + ", " + pm.store.map(ST)
tune_chart, comps = fig.tune_map(pm, W, F)
margin_s3x = pm[pm.store == "s3x"].margin.median()
margin_az = pm[pm.store == "azure"].margin.median()
regions = (pm.groupby(["facet", "policy"])
             .agg(cells=("cost_ms", "size"), windows_lo=("windows", "min"), windows_hi=("windows", "max"),
                  fulls_lo=("fulls", "min"), fulls_hi=("fulls", "max"), margin_median=("margin", "median"))
             .reset_index())
```

```{code-cell} ipython3
:label: tune-map
:tags: [remove-input]
:class: col-page-right

tune_chart
```

On S3 Express two policies split each workload's map: few reads favour MoR (compacted on the
updates, with 4 MiB row groups on the inserts), many windows or full reads the designs that merge
regions. On Azure Blob the map is a patchwork because its policies cost nearly the same: at the
median cell the runner-up costs {eval}`pct(margin_az - 1, 1)` more than the cheapest, against
{eval}`pct(margin_s3x - 1, 1)` on S3 Express.

```{code-cell} ipython3
:tags: [remove-input]

table(regions, fmt=".3g", caption="Where each policy is the cheapest: its cells (of 625), the windows and full reads a write it covers, and how far the runner-up is above it (median)")
```

## The check on the laptop (T1, T2)

```{code-cell} ipython3
:tags: [remove-cell]

t1, t1c, t2, missing = J["t1"], J["t1_cost"], J["t2"], J["missing"]
t1 = tune.name_policies(con, t1)
t1c = tune.name_policies(con, t1c)
small = t1[t1.kind != "sorted"]
t1_med = small.err.abs().median() if len(small) else np.nan
t1_bad = small[small.err.abs() > 0.15]
t1_sorted_bad = t1[(t1.kind == "sorted") & (t1.err.abs() > 0.30)]
t1c_max = t1c.err.abs().max() if len(t1c) else np.nan
t1_held = len(t1_bad) == 0 and len(t1_sorted_bad) == 0 and t1_med <= 0.08 and t1c_max <= 0.10
t2_max = t2.pick_over_best.max() if len(t2) else np.nan
t2_bad = t2[t2.pick_over_best > 1.05]
t2_held = len(t2_bad) == 0 and len(missing) == 0
```

The laptop ran every configuration of the grid with the same build, so each finalist's work is
measured; {eval}`len(missing)` finalists lack it. The formula's work against the measured, on the
finalists from the grid (the `windows` configurations trained it):

```{code-cell} ipython3
:tags: [remove-input]

table(t1[["policy", "label", "kind", "pred_ms", "meas_ms", "err"]], fmt=".3g",
      caption="T1: each priced phase's mean work, predicted from havoc's counts and measured on the laptop (ms)")
```

**T1:** the median error over the writes and the full, window and newest reads is
{eval}`pct(t1_med, 1)`; {eval}`len(t1_bad)` of {eval}`len(small)` are off by more than 15%
({eval}`", ".join(f"{r.policy} {r.kind} {r.err:+.0%}" for r in t1_bad.itertuples()) or "none"`). The
time per write under each finalist's mixes is within {eval}`pct(t1c_max, 1)`: requests, priced
exactly from the counts, are most of every phase's time.

```{code-cell} ipython3
:tags: [remove-input]

table(t1c[["store", "mix", "policy", "cost_ms_p", "cost_ms_m", "err"]], fmt=".4g",
      caption="T1: time per write (ms), predicted and with the measured work")
```

**T2:** priced with the measured work, the tuner's pick is the cheapest finalist, or within 5% of
it, in {eval}`len(t2) - len(t2_bad)` of {eval}`len(t2)` cases; at worst it costs
{eval}`pct(t2_max - 1, 1)` more.

```{code-cell} ipython3
:tags: [remove-input]

t2n = t2.copy()
if len(t2n):
    t2n["pick"] = tune.name_policies(con, t2n.assign(label=t2n.pick, arm=t2n.pick_arm, design=t2n.pick_design)).policy.values
    t2n["cheapest"] = tune.name_policies(con, t2n.assign(label=t2n.best, arm=t2n.best_arm, design=t2n.best_design)).policy.values
table(t2n.reindex(columns=["workload", "store", "mix", "pick", "cheapest", "pick_over_best"]), fmt=".3f",
      caption="T2: the pick against the cheapest finalist, both priced with the measured work")
```

## Verdicts

```{code-cell} ipython3
:tags: [remove-cell]

big = small.groupby("kind").err.agg(lambda e: e.abs().max())
fine = [k for k, v in big.items() if v <= 0.15]
off = [k for k, v in big.items() if v > 0.15]
V = {
    "T1": "held" if t1_held else "contradicted in part",
    "T2": "held" if t2_held else "contradicted in part",
    "T3": "held" if not t3_bad else "held but for " + ", ".join(t3_bad),
    "T4": "held" if not t4_bad else "held but for " + ", ".join(t4_bad),
}
```

- **T1 (the formula, held out): {eval}`V["T1"]`.** Within 15% on every finalist:
  {eval}`", ".join(fine) or "no kind of phase"`; beyond it on some: {eval}`", ".join(off) or "none"`.
  The median error is {eval}`pct(t1_med, 1)` (registered: at most 8%), and the time per write is
  within {eval}`pct(t1c_max, 1)` (at most 10%). The misses are small reads, most with row groups
  of 4 or 8 MiB, which the training runs never had: a read's work per row group is not one of
  the formula's counts.
- **T2 (the picks): {eval}`V["T2"]`.**
- **T3 (the updates' knobs): {eval}`V["T3"]`.**
- **T4 (the inserts' knobs): {eval}`V["T4"]`.**

## Measured, not judged

- **The deferred merges' windows are cheap in bytes, not in requests.** Each window's delta row
  groups are separate requests, so on S3 Express their windows cost what compacted MoR's do.
- **Smaller row groups pay off for RMoW-append on the updates:** with 4 MiB row groups its write
  takes {eval}`f"{app4['write']:.1f}"` s and a window {eval}`f"{app4['window']:.2f}"` s on S3 Express
  (16 MiB: {eval}`f"{app16['write']:.1f}"` and {eval}`f"{app16['window']:.2f}"`), while its full read
  takes {eval}`f"{app4_az_full:.1f}"` s on Azure Blob against {eval}`f"{app16_az_full:.1f}"`.
- **MoR's compaction interval** (every 3, 6, 10 or 12 writes) moves its time per write by at
  most {eval}`pct(morc_upd - 1, 1)` on the updates under any mix, and on the inserts by
  {eval}`pct(morc_ing_rest - 1, 1)` but for the windows mix, where it moves it by
  {eval}`pct(morc_ing_win - 1, 0)`: appended late rows spread a window across the files a
  compaction has not yet merged.

## Limits

- **One seed, 30 writes, one table size.** RMoW-append and the deferred merges never compact, so
  they keep dead bytes the 30 writes do not price; storage is not in the cost.
- **Time is simulated at the laptop's pace.** The base point's S3 Express VM did the same work in
  1.3–1.6 times the laptop's thread time; writes, more work than reads, would weigh more there.
- **The sorted read on the updates** runs under `cached`, which reads RMoR's and the deferred
  merges' dead row groups.
- **Azure Blob's block blobs** past 256 MiB read slower than their price; no policy here writes a
  file that large.
- **The tuner prices one read at a time on one thread.** Readers that fetch row groups in
  parallel hide request latency, which favours the policies that send more requests (the
  deferred merges, small row groups).
