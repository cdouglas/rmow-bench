---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The tuner's picks in the cloud
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, palette, q, table, theme
from labbook.db import connect
theme()
fig = figures_module()
tune = fig.tuner()
con = connect()

train = tune.records(con, tune.TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
formula = tune.Formula(train)
ns = tune.pace(con)
J, C = {}, {}
for st in ("s3x", "azure"):
    J[st], C[st] = tune.cloud_judge(con, formula, ns, st)
A = pd.concat(J.values())

WL = {"upd": "updates", "ing": "inserts"}
ST = {"s3x": "S3 Express", "azure": "Azure Blob"}
READS = {"writes": "few reads", "windows": "10 range reads", "scans": "2 full scans", "sorted": "1 sorted read"}
NAME = {("tune-upd-mor-c12", "mor-v3"): "MoR, compacted every 12 writes, 16 MiB row groups",
        ("tune-upd-rg4m", "rmow-c"): "RMoW-append, 4 MiB row groups",
        ("tune-upd-rg4m", "rmor-density"): "RMoR with deferred merges, 4 MiB row groups",
        ("tune-ing-rg4m", "mor-v3"): "MoR, 4 MiB row groups",
        ("tune-ing-rg4m-seal256", "rmor-c"): "RMoR sealed at 256 MiB, 4 MiB row groups",
        ("tune-ing-rg4m", "rmow-c"): "RMoW-append, 4 MiB row groups",
        ("tune-ing-rg4m", "rmor-c-kept"): "RMoR with row groups kept, 4 MiB row groups"}
DESIGN = {"mor-v3": "MoR", "rmow-c": "RMoW-append", "rmor-c": "RMoR, sealed at 256 MiB",
          "rmor-c-kept": "RMoR, row groups kept", "rmor-density": "RMoR, deferred merges"}
FAMILY = {"mor-v3": "mor", "rmow-c": "rmow", "rmor-density": "rmor", "rmor-c": "rmor", "rmor-c-kept": "rmor"}
SHAPE = {"mor-v3": "circle", "rmow-c": "triangle-up", "rmor-density": "triangle-down", "rmor-c": "square",
         "rmor-c-kept": "diamond"}
A["policy"] = [NAME[(l, a)] for l, a in zip(A.label, A.arm)]
A["dname"] = A.arm.map(DESIGN)
A["reads"] = A.mix.map(READS)
A["Store"] = A.store.map(ST)
A["Workload"] = A.workload.map(WL)
dorder = list(DESIGN.values())
fam = palette("family")
dcolor = alt.Color("dname:N", title="design", sort=dorder, legend=alt.Legend(labelLimit=320),
                   scale=alt.Scale(domain=dorder, range=[fam[FAMILY[a]] for a in DESIGN]))
dshape = alt.Shape("dname:N", title="design", sort=dorder, legend=alt.Legend(labelLimit=320),
                   scale=alt.Scale(domain=dorder, range=[SHAPE[a] for a in DESIGN]))


def pct(x, d=0):
    return f"{100 * x:.{d}f}%"


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


def row(st, wl, mix, label, arm):
    return A[(A.store == st) & (A.workload == wl) & (A.mix == mix) & (A.label == label) & (A.arm == arm)].iloc[0]


def sec(st, wl, mix, label, arm):
    return f"{row(st, wl, mix, label, arm).measured_ms / 1e3:.1f}"


def times(st, wl, mix, label, arm):
    return f"{row(st, wl, mix, label, arm).meas_over_best:.2f}×"


def part(st, label, arm, p, unit="s"):
    m = C[st]["measured"]
    v = m[(m.label == label) & (m.arm == arm)].iloc[0][p]
    return f"{v / 1e3:.1f} s" if unit == "s" else f"{v:.0f} ms"


runs = q("""SELECT run_id, label, host, source, round(elapsed_s / 60, 1) AS minutes
            FROM runs WHERE list_contains(campaigns, 'tune-cloud') ORDER BY source, label""")
bench = runs[runs.source == "bench"]
n_s3x = int(bench.label.str.endswith("-s3x").sum())
n_az = int(bench.label.str.endswith("-azure").sum())
n_twin = int((~bench.label.str.endswith(("-s3x", "-azure"))).sum())
s3x_min = bench[bench.label.str.endswith("-s3x")].minutes
az_min = bench[bench.label.str.endswith("-azure")].minutes
build = q("SELECT DISTINCT left(git_sha, 7) AS sha FROM runs WHERE list_contains(campaigns, 'tune-cloud') "
          "AND source = 'bench'").sha.tolist()

# K1: each cloud run and twin against havoc's run of its configuration
CFG = ["tune-upd-rg4m", "tune-upd-mor-c12", "tune-ing-rg4m", "tune-ing-rg4m-seal256"]
havoc = q("""SELECT label, run_id FROM canonical_runs
             WHERE list_contains(campaigns, 'tune') AND host = 'havoc' AND list_contains($c, label)""", c=CFG)
HELD = ["held_bytes", "held_rows", "held_chunks", "held_writer", "held_pending", "held_deletes"]
k1 = []
for _, h in havoc.iterrows():
    for _, o in bench[bench.label.isin([h.label, h.label + "-s3x", h.label + "-azure"])].iterrows():
        d = q(f"""SELECT count(*) AS records,
                         count(*) FILTER (WHERE one_run_only OR len(differs) > 0) AS differ
                  FROM steps_diff('{h.run_id}', '{o.run_id}')""").iloc[0]
        held = q(f"""SELECT count(*) AS n, count(*) FILTER (WHERE {' OR '.join(f'a.{c} IS DISTINCT FROM b.{c}' for c in HELD)}) AS differ
                     FROM steps a JOIN steps b ON a.arm = b.arm AND a.step = b.step AND a.phase = b.phase
                          AND a.reader IS NOT DISTINCT FROM b.reader
                          AND a.window_at IS NOT DISTINCT FROM b.window_at
                     WHERE a.run_id = '{h.run_id}' AND b.run_id = '{o.run_id}'
                       AND coalesce(a.reader, '') <> 'cached-p8'""").iloc[0]
        kind = ("S3 Express" if o.label.endswith("-s3x") else
                "Azure Blob" if o.label.endswith("-azure") else "twin (S3 Express VM's disk)")
        k1.append(dict(configuration=h.label, run=kind, run_id=o.run_id, records=int(d.records),
                       differ=int(d.differ), one_thread_records=int(held.n), held_differ=int(held.differ)))
k1 = pd.DataFrame(k1)
k1_held = bool((k1.differ == 0).all() and (k1.held_differ == 0).all())

k2 = A[A.pick & A.judged]
k2_held = {st: bool((g.meas_over_best <= 1.05).all()) for st, g in k2.groupby("store")}
ties = A[A.pick & ~A.judged]
k3 = A[A.store == "s3x"]
k3_err = (k3.priced_over_measured - 1).abs()
k3_held = bool((k3_err <= 0.20).all())
V = {"K1": "held" if k1_held else "contradicted",
     "K2": "held" if all(k2_held.values()) else "contradicted on " + ", ".join(ST[s] for s, v in k2_held.items() if not v),
     "K3": "held" if k3_held else "contradicted"}

# the whole order of the designs, predicted against measured
same, swap = 0, 1.0
for _, g in A.groupby(["store", "workload", "mix"]):
    p = list(g.sort_values("pred_ms").policy)
    m = g.set_index("policy").measured_ms
    same += p == list(m.sort_values().index)
    for i, a in enumerate(p):
        for b in p[i + 1:]:
            if m[b] < m[a]:
                swap = max(swap, m[a] / m[b])
n_cases = A.groupby(["store", "workload", "mix"]).ngroups

# the VMs' pace: each S3 Express twin's work over the laptop's run of its configuration, same records
pace = q("""
WITH l AS (SELECT r.label, w.arm, w.phase, sum(w.work_ms) AS work_ms, sum(w.instructions) AS instr
           FROM phase_work w JOIN canonical_runs r USING (run_id)
           WHERE r.host = 'bearyak' AND list_contains(r.campaigns, 'tune') GROUP BY ALL),
     v AS (SELECT r.label, w.arm, w.phase, sum(w.work_ms) AS work_ms, sum(w.instructions) AS instr
           FROM phase_work w JOIN canonical_runs r USING (run_id)
           WHERE list_contains(r.campaigns, 'tune-cloud') AND r.host LIKE 'ip-%' GROUP BY ALL)
SELECT v.label, v.arm, v.phase, v.work_ms / l.work_ms AS work_vm_over_laptop,
       v.instr / l.instr AS instructions_vm_over_laptop, v.work_ms AS vm_ms, l.work_ms AS laptop_ms
FROM v JOIN l USING (label, arm, phase)
WHERE v.phase IN ('batch', 'compact', 'read_full', 'read_window', 'read_recent', 'read_sorted')""")
pace = pace[[(l, a) in NAME for l, a in zip(pace.label, pace.arm)]]
pace["policy"] = [NAME[(l, a)] for l, a in zip(pace.label, pace.arm)]
pace_all = pace.vm_ms.sum() / pace.laptop_ms.sum()
pred_over_meas = {st: (A[A.store == st].pred_ms / A[A.store == st].measured_ms) for st in ST}
pred_over_priced = {st: (A[A.store == st].pred_ms / A[A.store == st].priced_ms) for st in ST}
azm = A[A.store == "azure"]

# full scans of the updates: GETs per scan under the reader the tuner prices, from the request logs
gets = q("""
SELECT CASE WHEN r.label LIKE '%-azure' THEN 'azure' ELSE 's3x' END AS store, x.arm,
       count(*) / count(DISTINCT x.step) AS gets, avg(x.length) / 1048576 AS mib,
       avg(x.dur_ns) / 1e6 AS ms, sum(x.dur_ns) / 1e9 / count(DISTINCT x.step) AS s
FROM requests x JOIN canonical_runs r USING (run_id)
WHERE list_contains(r.campaigns, 'tune-cloud')
  AND r.label IN ('tune-upd-rg4m-azure', 'tune-upd-mor-c12-azure', 'tune-upd-rg4m-s3x', 'tune-upd-mor-c12-s3x')
  AND x.phase = 'read_full' AND x.reader = 'cached-skip' AND x.step >= 5 AND x.kind = 'get_range'
  AND x.arm IN ('rmow-c', 'mor-v3')
GROUP BY ALL""").set_index(["store", "arm"])
gets_ratio = gets.loc[("azure", "rmow-c")].gets / gets.loc[("azure", "mor-v3")].gets
```

This run checks the tuner ({doc}`tune`). The tuner picks a design and its settings for each
workload and mix of reads from local runs alone: it counts each design's requests and bytes, times
its CPU on the laptop, and prices each request from a short test of the store's latency. Here its
picks and their rivals ran on S3 Express and Azure Blob, to see whether the designs it picked are
the fastest when timed on the real stores, and how close its time estimates come.

It adds no new comparison of the designs: from the local runs, the tuner had already predicted
which design would be fastest in each case below, and by about how much. What it adds is the
check that those predictions hold on both stores, and where the estimates are off and why.

## What we learned

- **The tuner's picks hold.** In each of the {eval}`len(k2)` cases where it called a clear winner
  (a design at least 5% ahead of the next), that design was the fastest measured, on both stores.
  In the {eval}`len(ties)` cases it called close, its pick was within
  {eval}`pct(ties.meas_over_best.max() - 1, 1)` of the fastest. In {eval}`same` of the
  {eval}`n_cases` cases the designs finished in exactly the predicted order; elsewhere, those
  that swapped places measured within {eval}`pct(swap - 1, 1)` of each other.
- **Its time estimates ran low on S3 Express, by
  {eval}`span(1 - pred_over_meas["s3x"], "{:.0%}")`, nearly all of it from CPU speed.** The
  cloud machine took {eval}`span(pace.work_vm_over_laptop - 1, "{:.0%}")` longer than the laptop
  for the same instructions. That hit every design about equally, so the order did not change.
  With the CPU measured on the cloud machine, the estimates came within
  {eval}`pct(k3_err.max(), 1)` of the measured times. Azure's machines count no instructions;
  borrowing the S3 Express machines' CPU, Azure's estimates were
  {eval}`span(azm.priced_over_measured)` of the measured times.
- **Which design is fastest depends on the reads,** as the tuner predicted: MoR when there are
  few reads, RMoW-append (updates) or RMoR sealed at 256 MiB (inserts) when there are many range
  reads. Full scans come out about level on S3 Express. On Azure, MoR wins the updates' full scans:
  RMoW-append's 4 MiB row groups take {eval}`f"{gets_ratio:.1f}"` times as many requests as
  MoR's 16 MiB ones, and each request costs more on Azure.
- **The cloud runs did exactly the work the local runs counted:** the same requests, bytes and
  table contents.

## Setup

**Workloads and designs.** The tuner's two workloads, on one table:
- the **updates:** 150,000 changes to existing rows a write, five updates to each delete,
  skewed towards some regions of the key space;
- the **inserts:** 350,000 new rows a write, 99% of them within 5 writes of their event time.

Seven policies (a design with its settings) ran, from four configurations. Between them they hold
the tuner's pick on S3 Express for every case but two, where they hold a setting it priced within
0.4% of the pick (MoR compacted every 12 writes, not every 6):
- **updates:** MoR compacted every 12 writes with 16 MiB row groups (`tune-upd-mor-c12`);
  RMoW-append, and RMoR with deferred merges, with 4 MiB row groups (`tune-upd-rg4m`);
- **inserts:** MoR, RMoR with row groups kept and RMoW-append with 4 MiB row groups
  (`tune-ing-rg4m`); RMoR sealed at 256 MiB with 4 MiB row groups (`tune-ing-rg4m-seal256`).

The table is narrow (11 columns), about 1 GB, and takes 30 writes.

**Time per write** is what the tuner minimizes: the write, plus the compactions spread over the
30 writes, plus the reads made for each write. Each read's time is its mean over steps 5 to 30.
Four mixes of reads:
- **few reads:** 1 range read and 0.1 full scans a write;
- **10 range reads** a write;
- **2 full scans** a write;
- **1 sorted read** a write (the whole table in key order).

A **range read** reads 1% of the table (by id for the updates, by event time for the inserts),
and is timed as the mean of five such reads.

**The runs.** Each configuration ran once, at {eval}`", ".join(build)`:
- **S3 Express:** one `c7a.4xlarge` per configuration in usw2-az3, all four at once on one bucket
  ({eval}`span(s3x_min, "{:.0f}")` minutes each). Each VM first ran its configuration against its
  own disk, counting instructions (the "twin"), then a latency test of the store (the "spike"),
  then the run itself.
- **Azure Blob:** one `D8as_v6` in westus: a latency test, then the four configurations in turn
  ({eval}`span(az_min, "{:.0f}")` minutes each).

```{code-cell} ipython3
:tags: [remove-input]

table(runs)
```

## The results: seconds per write

```{code-cell} ipython3
:label: tune-cloud-seconds
:tags: [remove-input]
:class: col-page-right

d = A[["Store", "Workload", "reads", "dname", "policy", "measured_ms", "meas_over_best", "pick", "judged"]].copy()
d["seconds"] = d.measured_ms / 1e3
d["pick"] = np.where(d.pick, np.where(d.judged, "clear winner", "called a near tie"), "")
x = alt.X("seconds:Q", title="measured seconds per write (log scale)",
          scale=alt.Scale(type="log", domain=[1.8, 70], nice=False), axis=alt.Axis(values=[2, 5, 10, 20, 50]))
y = alt.Y("reads:N", title=None, sort=list(READS.values()))
pts = alt.Chart().mark_point(size=90, filled=True, opacity=0.9).encode(
    x=x, y=y, color=dcolor, shape=dshape,
    tooltip=["Store", "Workload", "reads", "policy", alt.Tooltip("seconds:Q", format=".2f"),
             alt.Tooltip("meas_over_best:Q", title="× the fastest", format=".3f"), "pick"])
ring = alt.Chart().transform_filter(alt.datum.pick != "").mark_point(
    size=320, filled=False, color="black", strokeWidth=1.5).encode(
    x=x, y=y, strokeDash=alt.StrokeDash("pick:N", title="the tuner's pick",
                                        scale=alt.Scale(domain=["clear winner", "called a near tie"],
                                                        range=[[1, 0], [3, 2]])))
alt.layer(pts, ring, data=d).facet(
    row=alt.Row("Workload:N", title=None, sort=["updates", "inserts"]),
    column=alt.Column("Store:N", title=None, sort=["S3 Express", "Azure Blob"]))
```

Measured seconds per write for each design, by workload, store and mix of reads; further left is
faster. The tuner's pick is ringed: solid where it called a clear winner, dashed where it called
a near tie. The axis is logarithmic, so equal gaps are equal ratios.

```{code-cell} ipython3
:tags: [remove-input]

t1 = []
for (wl, mix, label, arm), g in A.groupby(["workload", "mix", "label", "arm"], sort=False):
    r = {"Workload": WL[wl], "Reads per write": READS[mix], "Policy": NAME[(label, arm)]}
    for st in ("s3x", "azure"):
        x = g[g.store == st].iloc[0]
        r[f"{ST[st]} (s)"] = round(x.measured_ms / 1e3, 2)
        r[f"{ST[st]}, × fastest"] = round(x.meas_over_best, 3)
        r[f"{ST[st]}, pick"] = ("clear" if x.judged else "near tie") if x.pick else ""
    t1.append(r)
t1 = pd.DataFrame(t1)
t1["_w"] = t1.Workload.map({"updates": 0, "inserts": 1})
t1["_m"] = t1["Reads per write"].map({v: i for i, v in enumerate(READS.values())})
table(t1.sort_values(["_w", "_m", "S3 Express (s)"]).drop(columns=["_w", "_m"]))
```

```{code-cell} ipython3
:tags: [remove-cell]

defer = A[A.arm == "rmor-density"]
ing_s3x_levels = {m: A[(A.store == "s3x") & (A.workload == "ing") & (A.mix == m)].meas_over_best.max()
                  for m in ("scans", "sorted")}
upd_s3x_level = A[(A.store == "s3x") & (A.workload == "upd") & A.mix.isin(["scans", "sorted"])
                  & (A.arm != "rmor-density")].meas_over_best.max()
```

**Few reads: the cheapest write wins.** For the updates, MoR takes
{eval}`sec("s3x", "upd", "writes", "tune-upd-mor-c12", "mor-v3")` s a write on S3 Express against
RMoW-append's {eval}`sec("s3x", "upd", "writes", "tune-upd-rg4m", "rmow-c")` s
({eval}`sec("azure", "upd", "writes", "tune-upd-mor-c12", "mor-v3")` against
{eval}`sec("azure", "upd", "writes", "tune-upd-rg4m", "rmow-c")` on Azure). Most of that time is the
write, and MoR's write is the cheaper: {eval}`part("s3x", "tune-upd-mor-c12", "mor-v3", "write_ms")`
against {eval}`part("s3x", "tune-upd-rg4m", "rmow-c", "write_ms")` on S3 Express. For the inserts,
MoR and RMoR sealed at 256 MiB come within
{eval}`pct(max(row("s3x", "ing", "writes", "tune-ing-rg4m-seal256", "rmor-c").meas_over_best, row("azure", "ing", "writes", "tune-ing-rg4m", "mor-v3").meas_over_best) - 1)`
of each other: MoR ahead on S3 Express, RMoR on Azure.

**Many range reads: the designs that merge on write win.** For the updates, RMoW-append takes
{eval}`sec("s3x", "upd", "windows", "tune-upd-rg4m", "rmow-c")` s a write on S3 Express, and MoR
{eval}`times("s3x", "upd", "windows", "tune-upd-mor-c12", "mor-v3")` as long
({eval}`times("azure", "upd", "windows", "tune-upd-mor-c12", "mor-v3")` on Azure): a range read
takes {eval}`part("s3x", "tune-upd-rg4m", "rmow-c", "window_ms", "ms")` under RMoW-append against
MoR's {eval}`part("s3x", "tune-upd-mor-c12", "mor-v3", "window_ms", "ms")`. For the inserts, RMoR
sealed at 256 MiB takes {eval}`sec("s3x", "ing", "windows", "tune-ing-rg4m-seal256", "rmor-c")` s,
MoR {eval}`times("s3x", "ing", "windows", "tune-ing-rg4m", "mor-v3")` as long
({eval}`times("azure", "ing", "windows", "tune-ing-rg4m", "mor-v3")` on Azure) and RMoW-append
{eval}`times("s3x", "ing", "windows", "tune-ing-rg4m", "rmow-c")`
({eval}`times("azure", "ing", "windows", "tune-ing-rg4m", "rmow-c")`).

**Full scans and sorted reads: level on S3 Express; on Azure, MoR wins the updates'.** On S3
Express, MoR and RMoW-append come within {eval}`pct(upd_s3x_level - 1, 1)` of each other on the
updates, and the four insert designs within {eval}`pct(ing_s3x_levels["scans"] - 1, 1)` on full
scans and {eval}`pct(ing_s3x_levels["sorted"] - 1, 1)` on sorted reads. On Azure, RMoW-append takes
{eval}`times("azure", "upd", "scans", "tune-upd-rg4m", "rmow-c")` as long as MoR on the updates'
full scans and {eval}`times("azure", "upd", "sorted", "tune-upd-rg4m", "rmow-c")` on the sorted
read. A full scan reads about the same bytes either way, but in
{eval}`f'{gets.loc[("azure", "rmow-c")].gets:.0f}'` requests of
{eval}`f'{gets.loc[("azure", "rmow-c")].mib:.1f}'` MiB on average under RMoW-append (4 MiB row
groups), against {eval}`f'{gets.loc[("azure", "mor-v3")].gets:.0f}'` of
{eval}`f'{gets.loc[("azure", "mor-v3")].mib:.1f}'` MiB under MoR (16 MiB row groups). One of
RMoW-append's requests took {eval}`f'{gets.loc[("azure", "rmow-c")].ms:.0f}'` ms on Azure and
{eval}`f'{gets.loc[("s3x", "rmow-c")].ms:.0f}'` ms on S3 Express, so its requests add up to
{eval}`f'{gets.loc[("azure", "rmow-c")].s:.1f}'` s a scan on Azure against MoR's
{eval}`f'{gets.loc[("azure", "mor-v3")].s:.1f}'` s, but
{eval}`f'{gets.loc[("s3x", "rmow-c")].s:.1f}'` s against
{eval}`f'{gets.loc[("s3x", "mor-v3")].s:.1f}'` s on S3 Express. On Azure's inserts, RMoR with row
groups kept and RMoW-append lead MoR and RMoR sealed by
{eval}`span([row("azure", "ing", m, "tune-ing-rg4m", "mor-v3").meas_over_best - 1 for m in ("scans", "sorted")] + [row("azure", "ing", m, "tune-ing-rg4m-seal256", "rmor-c").meas_over_best - 1 for m in ("scans", "sorted")], "{:.0%}")`.

**RMoR with deferred merges comes last everywhere,** at
{eval}`span(defer[defer.store == "s3x"].meas_over_best, "{:.1f}×")` the fastest on S3 Express and
{eval}`span(defer[defer.store == "azure"].meas_over_best, "{:.1f}×")` on Azure. It ran only at
4 MiB row groups, which is not its best setting (see Limits).

## How close the tuner's estimates came

The tuner estimates a design's time per write without a cloud run, as CPU time plus request time:
- **CPU time:** the design's instructions, counted on the laptop, at the laptop's speed;
- **request time:** each request priced by its size, from a latency test of the store taken
  earlier.

Two estimates are compared here with the measured time:
- **the tuner's estimate,** registered before the run: laptop CPU, priced by the latency tests of
  the earlier cloud runs;
- **with the cloud VM's CPU:** the instructions the S3 Express VM counted in its twin, at that
  VM's speed, plus the run's requests priced by the latency test it took just before. Azure's VMs
  count no instructions, so Azure's estimate takes the S3 Express VMs' CPU (both AMD Zen 4).

```{code-cell} ipython3
:label: tune-cloud-estimates
:tags: [remove-input]
:class: col-page-right

KIND = ["tuner's estimate (laptop CPU)", "with the cloud VM's CPU"]
e = pd.concat([A.assign(kind=KIND[0], ratio=A.pred_ms / A.measured_ms),
               A.assign(kind=KIND[1], ratio=A.priced_ms / A.measured_ms)])
e = e[["Store", "Workload", "reads", "dname", "policy", "kind", "ratio"]]
x = alt.X("ratio:Q", title="estimated over measured time per write (1 = exact)",
          scale=alt.Scale(domain=[0.8, 1.2], nice=False, zero=False))
one = alt.Chart().transform_aggregate(n="count()").mark_rule(color="gray").encode(x=alt.datum(1.0))
pts = alt.Chart().mark_point(size=60, filled=True, opacity=0.85).encode(
    x=x, y=alt.Y("kind:N", title=None, sort=KIND), yOffset=alt.YOffset("policy:N"),
    color=dcolor, shape=dshape,
    tooltip=["Store", "Workload", "reads", "policy", "kind", alt.Tooltip("ratio:Q", format=".3f")])
alt.layer(one, pts, data=e).facet(column=alt.Column("Store:N", title=None, sort=["S3 Express", "Azure Blob"]))
```

Each policy's estimated time per write over its measured time, one mark per mix of reads; left of
the line, the estimate was too low.

```{code-cell} ipython3
:tags: [remove-input]

table(A.assign(measured_s=A.measured_ms / 1e3, tuner_s=A.pred_ms / 1e3, cloud_cpu_s=A.priced_ms / 1e3,
               tuner_over_measured=A.pred_ms / A.measured_ms, cloud_cpu_over_measured=A.priced_over_measured)[
    ["Store", "Workload", "reads", "policy", "measured_s", "tuner_s", "cloud_cpu_s",
     "tuner_over_measured", "cloud_cpu_over_measured"]].round(3))
```

```{code-cell} ipython3
:tags: [remove-cell]

# the latency tests: each cloud run's requests priced by its own test and by the earlier one
sp = []
for wl, label, arm in tune.CLOUD_POLICIES:
    for st in ("s3x", "azure"):
        c = C[st]["measured"]
        r = c[(c.label == label) & (c.arm == arm)].iloc[0]
        for spike in (r.spike, tune.SPIKES[st]):
            ms = q(f"SELECT sum(request_ms * records) AS ms FROM sized_latency('{r.run}', '{spike}') "
                   f"WHERE arm = '{arm}'").ms.iloc[0]
            sp.append(dict(store=st, policy=NAME[(label, arm)], test="this run's" if spike == r.spike else "earlier", ms=ms))
sp = pd.DataFrame(sp).pivot_table(index=["store", "policy"], columns="test", values="ms").reset_index()
sp["this_over_earlier"] = sp["this run's"] / sp["earlier"]
defer_az = azm[azm.arm == "rmor-density"].priced_over_measured
ingscan_az = azm[(azm.workload == "ing") & (azm.mix == "scans")].priced_over_measured
```

- **On S3 Express the tuner's estimates were {eval}`span(pred_over_meas["s3x"])` of the measured
  times.** Its CPU times were the laptop's, and the cloud VM took
  {eval}`span(pace.work_vm_over_laptop)` times as long as the laptop for the same work
  ({eval}`f"{pace_all:.2f}"` over all), on the same instructions
  ({eval}`span(pace.instructions_vm_over_laptop, "{:.3f}")` times the laptop's count): it runs
  them more slowly. With the CPU measured on the VM, the estimates were
  {eval}`span(k3.priced_over_measured, "{:.3f}")` of the measured times.
- **The latency tests.** Each S3 Express VM's test, taken while the other three VMs ran, priced
  its run's requests at {eval}`span(sp[sp.store == "s3x"].this_over_earlier)` times the earlier
  test's prices; Azure's, at {eval}`span(sp[sp.store == "azure"].this_over_earlier)` times.
- **On Azure the estimates were less close:** the tuner's {eval}`span(pred_over_meas["azure"])` of
  the measured times, and {eval}`span(azm.priced_over_measured)` with the S3 Express VMs' CPU.
  RMoR with deferred merges is estimated low ({eval}`span(defer_az)`), and the inserts' full scans
  high ({eval}`span(ingscan_az)`).

The errors moved the designs together, so they did not change which was fastest.

```{code-cell} ipython3
:tags: [remove-input]

table(pace[["policy", "phase", "work_vm_over_laptop", "instructions_vm_over_laptop"]].round(3))
```

```{code-cell} ipython3
:tags: [remove-input]

table(sp.round(3))
```

## The checks registered before the run

The run was to judge three hypotheses, registered in docs/HYPOTHESES.md ("The tuner's picks in
the cloud", 2026-11-08):

- **K1, the cloud runs do the work the local runs counted: {eval}`V["K1"]`.** Each of the
  {eval}`len(k1)` runs ({eval}`n_s3x` on S3 Express, {eval}`n_az` on Azure Blob and
  {eval}`n_twin` twins) has every record of havoc's run of its configuration, and
  {eval}`"none differs" if (k1.differ == 0).all() else f"{int(k1.differ.sum())} differ"` in requests,
  bytes or table state; the memory peaks of the one-thread phases
  {eval}`"match too" if (k1.held_differ == 0).all() else f"differ in {int(k1.held_differ.sum())} records"`.
- **K2, the picks hold: {eval}`V["K2"]`.** Wherever the tuner priced the runner-up more than 5%
  behind its pick ({eval}`int((k2.store == "s3x").sum())` cases on S3 Express,
  {eval}`int((k2.store == "azure").sum())` on Azure Blob), the pick is within 5% of the fastest
  measured. {eval}`"It is the fastest in every case." if (k2.meas_over_best == 1).all() else f"It is the fastest in {int((k2.meas_over_best == 1).sum())} of them."`
- **K3, the estimate with the cloud VM's CPU comes within 20% on S3 Express: {eval}`V["K3"]`,**
  within {eval}`pct(k3_err.max(), 1)`.

```{code-cell} ipython3
:tags: [remove-input]

table(k1)
```

## Limits

```{code-cell} ipython3
:tags: [remove-cell]

# the deferred merges ran at 4 MiB with RMoW-append; their own best setting in the tuner's grid
allr = tune.rank(pd.concat([tune.policies(con, wl, formula, ns, True) for wl in tune.WORKLOADS]))
upd = allr[allr.workload == "upd"]
cheapest = upd.groupby(["store", "mix"]).cost_ms.min()
dz = upd[upd.arm == "rmor-density"]
dbest = dz.sort_values("cost_ms").drop_duplicates(["store", "mix"]).set_index(["store", "mix"])
d4_over = dz[dz.label == "tune-upd-rg4m"].set_index(["store", "mix"]).cost_ms / dbest.cost_ms
dbest_over = dbest.cost_ms / cheapest
won = dbest[dbest["rank"] == 1]
wrote = q("""
SELECT r.rg_bytes / 1048576 AS rg_mib, s.arm,
       sum(s.bytes_written) FILTER (WHERE s.phase IN ('write', 'delete-write')) / 30 / 1e6 AS mb
FROM steps s JOIN canonical_runs r USING (run_id)
WHERE r.host = 'havoc' AND r.label IN ('win-upd-base', 'tune-upd-rg4m', 'tune-upd-rg8m', 'tune-upd-rg32m')
  AND s.step BETWEEN 1 AND 30 AND s.arm IN ('rmow-c', 'rmor-density')
GROUP BY ALL""").pivot(index="rg_mib", columns="arm", values="mb")
saved = wrote["rmow-c"] / wrote["rmor-density"]
```

- **RMoR with deferred merges ran at 4 MiB row groups,** because the configuration it shares with
  RMoW-append has them, and that is not its best setting. The tuner prices it at
  {eval}`span(d4_over.loc["s3x"])` times its best on S3 Express and
  {eval}`span(d4_over.loc["azure"])` on Azure Blob. A deferred merge saves rewriting a row group,
  which saves less the smaller the row group: RMoW-append writes
  {eval}`f"{saved.loc[32]:.1f}"` times as many bytes a write as RMoR with deferred merges at
  32 MiB row groups, but only {eval}`f"{saved.loc[4]:.1f}"` times at 4 MiB (havoc). At its best setting the tuner prices it at
  {eval}`span(dbest_over.loc["s3x"])` times the fastest policy on S3 Express;
  {eval}`"on " + ", ".join(f"{ST[s]} under {READS[m]}" for s, m in won.index) + " it is the tuner's pick over the whole grid (" + ", ".join(won.label) + "), which no cloud run has timed" if len(won) else "it is never the pick"`.
- **One run of each configuration,** one seed, 30 writes, one narrow table (11 columns, about
  1 GB). A 4 MiB row group holds about 98,000 of its rows, so its column chunks average a few
  hundred KB. A wide table read by a few columns would cut them far smaller; the 4 MiB picks are
  untested there.
- **The S3 Express runs shared one bucket,** each timed under the others' load; the Azure runs
  came one after another, over {eval}`f"{az_min.sum() / 60:.1f}"` hours.
- **Azure's estimates take the S3 Express VMs' CPU** (both Zen 4); Azure's VM counts no
  instructions.
- **The tuner times one read at a time, on one thread,** as these runs do. Readers that fetch row
  groups in parallel hide request time, which would favour the designs that send more requests,
  RMoW-append's full scans on Azure among them.
