---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Updates and inserts in the cloud: measured'
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

# The cloud runs (each configuration on S3 Express and on Azure Blob, labelled <label>-s3x and
# <label>-azure), their twins on the S3 Express VMs' disks and the VMs' latency tests: campaign
# CLOUD. The simulation against them is a calibration page (simulation-cloud.md).
CLOUD = "workloads-cloud"
# These configurations read through the skipping reader alone, the sorted read too.
tune.READS["sorted"] = ("read_sorted", "cached-skip")
tune.WORKLOADS["wl_upd"] = "regexp_matches(label, '^wl-upd-')"
tune.WORKLOADS["wl_ing"] = "regexp_matches(label, '^wl-ing-')"

# The designs: (configuration, arm, family, curve, knob, name, order), as on the workloads page,
# main points only
UPD = [
    ("wl-upd-base", "mor-v3", "MoR", "", "", "MoR", 0),
    ("wl-upd-compact", "mor-v3", "MoR", "", "", "MoR (compacted)", 0),
    ("wl-upd-base", "rmor-c-kept", "RMoR", "", "", "RMoR", 0),
    ("wl-upd-base", "rmor-c-footer-kept", "RMoW-external", "", "", "RMoW-external", 0),
    ("wl-upd-base", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
    ("wl-upd-base", "rmor-density", "RMoR-density", "", "", "RMoR-density", 0),
]
ING = [
    ("wl-ing-base", "mor-v3", "MoR", "", "", "MoR", 0),
    ("wl-ing-compact", "mor-v3", "MoR", "", "", "MoR (compacted)", 0),
    ("wl-ing-seal160", "rmor-c", "RMoR", "", "", "RMoR", 0),
    ("wl-ing-seal160", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
]
STORES = fig.WL_STORES
S3X, AZ = STORES
READ, WRITE = fig.WL_READ, fig.WL_WRITE
```

```{code-cell} ipython3
:tags: [remove-cell]

WHERE = f"list_contains(campaigns, '{CLOUD}')"
POLICIES = [("upd" if "-upd-" in s[0] else "ing", s[0], s[1]) for s in UPD + ING]

# Measured: each phase's wall time in the cloud, per step (tune.cloud_steps)
M = pd.concat([tune.cloud_steps(con, st, POLICIES, WHERE) for st in ("s3x", "azure")], ignore_index=True)
M["Store"] = M.store.map(fig.WL_STORE)
S = M.assign(s=M.measured_ms / 1e3)


def means(d, col):
    """Each configuration's and arm's mean write and reads in `col` (ms), as tune.policies()."""
    rows = [dict(store=st, Store=fig.WL_STORE[st], label=l, arm=a, **tune.per_write(g, col))
            for (st, l, a), g in d.groupby(["store", "label", "arm"])]
    return pd.DataFrame(rows)


PM = means(M, "measured_ms")
W = fig.wl_whiskers(S)



def frame(spec):
    """The measured points with their whiskers."""
    return fig.wl_frame(spec, PM, W)


U, I = frame(UPD), frame(ING)


def v(d, store, label, arm, col):
    return d[(d.Store == store) & (d.label == label) & (d.arm == arm)][col].iloc[0]


def f1(x):
    return f"{x:.1f}"


def f2(x):
    return f"{x:.2f}"


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


# Where the measured time goes: each phase's components on its own thread (phase_breakdown), whose
# shares split the measured mean
B = q("""
SELECT r.label, b.arm, b.phase, r.label LIKE '%-s3x' AS s3x, b.component, sum(b.ms) AS ms
FROM phase_breakdown b JOIN canonical_runs r USING (run_id)
WHERE list_contains(r.campaigns, $c) AND (r.label LIKE '%-s3x' OR r.label LIKE '%-azure')
  AND b.thread = 'phase'
  AND ((b.phase = 'batch' AND b.step >= 1)
       OR (b.step >= 5 AND b.phase IN ('read_full', 'read_window', 'read_sorted') AND b.reader = 'cached-skip'))
GROUP BY ALL""", c=CLOUD)
B["Store"] = np.where(B.s3x, S3X, AZ)
B["label"] = B.label.str.replace(r"-(s3x|azure)$", "", regex=True)
B["part"] = B.component.map(fig.WL_CPU_PART)
SH = B.groupby(["Store", "label", "arm", "phase", "part"]).ms.sum()
SH = SH / SH.groupby(level=["Store", "label", "arm", "phase"]).transform("sum")
PARTS = fig.WL_PARTS
WHAT = {"write": ("batch", WRITE), "window": ("read_window", READ["window_s"]),
        "full": ("read_full", READ["full_s"]), "sorted": ("read_sorted", READ["sorted_s"])}


def parts(spec):
    rows = []
    for label, arm, family, curve, knob, name, order in spec:
        for _, r in PM[(PM.label == label) & (PM.arm == arm)].iterrows():
            for kind, (phase, what) in WHAT.items():
                key = (r.Store, label, arm, phase)
                if np.isnan(r[f"{kind}_ms"]) or key not in SH.index.droplevel("part"):
                    continue
                for part, s in SH.loc[key].items():
                    rows.append(dict(Store=r.Store, name=name, what=what, part=part, s=r[f"{kind}_ms"] * s / 1e3))
    d = pd.DataFrame(rows)
    d["pord"] = d.part.map(PARTS.index)
    return d


UP, IP = parts(UPD), parts(ING)
UN, IN = [s[5] for s in UPD], [s[5] for s in ING]


def parts_table(d):
    return (d.pivot_table(index=["Store", "what", "name"], columns="part", values="s", aggfunc="sum")
            .reindex(columns=[p for p in PARTS if p in set(d.part)]).reset_index().round(3))


SNU = fig.wl_snapshots(UPD, S, ["window_s", "full_s", "sorted_s"])
SNI = fig.wl_snapshots(ING, S, ["window_s", "sorted_s"])

# The designs compared with nothing compacted: compaction has sections of its own (5 and 10)
UN_M = [n for n in UN if n != "MoR (compacted)"]
IN_M = [n for n in IN if n != "MoR (compacted)"]
SNU_M, SNI_M = SNU[SNU.name.isin(UN_M)], SNI[SNI.name.isin(IN_M)]
UM, IM = U[U.name.isin(UN_M)], I[I.name.isin(IN_M)]

# What each design has written after every write, MoR (compacted) beside them: havoc's runs, whose
# requests and bytes every cloud run repeats exactly (simulation-cloud.md, L1)
WRU = fig.wl_written([(s[0], s[1], s[5]) for s in UPD])
WRI = fig.wl_written([(s[0], s[1], s[5]) for s in ING])
FAM = {s[5]: s[2] for s in UPD + ING}


def written_chart(d, names):
    return fig.wl_written_chart(d, {n: fig.WL_NCOLOR.get(n, fig.WL_COLOR[FAM[n]]) for n in names},
                                {n: fig.WL_SHAPE[FAM[n]] for n in names}, dashed=["MoR (compacted)"])


def gbw(d, name, step=30):
    return d[(d.name == name) & (d.step == step)].gb.iloc[0]


# Compaction, measured: MoR never compacted and every 10 writes (the interval the cloud ran), beside
# the design that reads a range fastest with nothing compacted
CU = [("wl-upd-base", "never"), ("wl-upd-compact", "every 10 writes")]
CI = [("wl-ing-base", "never"), ("wl-ing-compact", "every 10 writes")]


def compaction(spec, reads, ref):
    """MoR's measured time at every write, compaction and read probe at each interval of spec, and
    ref's (label, arm, family, name), never compacted."""
    sp = [(l, "mor-v3", "MoR", "", "", n, 0) for l, n in spec] + [(ref[0], ref[1], ref[2], "", "", ref[3], 0)]
    return fig.wl_snapshots(sp, S, reads)


CMU = compaction(CU, ["window_s", "full_s"], ("wl-upd-base", "rmow-c", "RMoW-append", "RMoW-append"))
CMI = compaction(CI, ["window_s"], ("wl-ing-seal160", "rmor-c", "RMoR", "RMoR"))
WRCU, WRCI = (fig.wl_written([(l, "mor-v3", n) for l, n in c]) for c in (CU, CI))
TU, TI = fig.wl_compaction_costs(CU, PM, CMU, WRCU), fig.wl_compaction_costs(CI, PM, CMI, WRCI)


def ct(T, st, name, col):
    return T[(T.Store == st) & (T.compacted == name)][col].iloc[0]

runs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build, round(elapsed_s / 60) AS minutes
            FROM runs WHERE list_contains(campaigns, $c) ORDER BY label, run_id""", c=CLOUD)
```

```{code-cell} ipython3
:tags: [remove-cell]

# The checks registered before the run (docs/HYPOTHESES.md, "The two workloads in the cloud",
# 2026-11-10): L2 and L3, on the trade-offs. L1 and L4, on the measurement, are on the calibration
# page (simulation-cloud.md).

def m(d, store, name, col="write_s"):
    return d[(d.Store == store) & (d["name"] == name)][col].iloc[0]


# L2: on the updates, per store
REG, MORS = ["RMoR", "RMoW-external", "RMoW-append"], ["MoR", "MoR (compacted)"]
l2 = {}
for st in STORES:
    l2[st] = dict(
        write=[m(U, st, r) / m(U, st, b) for r in REG for b in MORS],
        window=[m(U, st, b, "window_s") / m(U, st, r, "window_s") for r in REG for b in MORS],
        compacted=m(U, st, "MoR", "window_s") / m(U, st, "MoR (compacted)", "window_s"),
        deferred_window=m(U, st, "MoR", "window_s") / m(U, st, "RMoR-density", "window_s"),
        deferred_write=[m(U, st, r) / m(U, st, "RMoR-density") for r in REG])
L2_PARTS = {"write": "the region designs write slower than MoR and MoR (compacted)",
            "window": "they read a window faster than both",
            "compacted": "MoR (compacted) reads a window faster than MoR",
            "deferred_window": "RMoR-density reads a window faster than MoR",
            "deferred_write": "RMoR-density writes faster than each region design"}
l2_fail = [(st, k) for st, parts in l2.items() for k, x in parts.items() if min(np.atleast_1d(x)) <= 1]
l2_held = not l2_fail

# L3: on the inserts, per store, MoR (compacted) and RMoR against MoR
MERGE = ["MoR (compacted)", "RMoR"]
l3 = {st: dict(
    window=[m(I, st, "MoR", "window_s") / m(I, st, n, "window_s") for n in MERGE],
    write=[m(I, st, n) / m(I, st, "MoR") for n in MERGE],
    extra=[m(I, st, n) - m(I, st, "MoR") for n in MERGE]) for st in STORES}
l3_fail = ([(st, "window") for st, p in l3.items() if min(p["window"]) <= 1]
           + [(st, "write") for st, p in l3.items() if min(p["write"]) <= 1]
           + [(st, "extra") for st, p in l3.items() if max(p["extra"]) >= 1])
l3_held = not l3_fail

V = {"L2": "held" if l2_held else "contradicted", "L3": "held" if l3_held else "contradicted"}


def pct(x, nd=0):
    return f"{100 * x:.{nd}f}%"


# For the prose. The updates' range reads against MoR and against MoR (compacted), apart
win_mor = {st: [m(U, st, "MoR", "window_s") / m(U, st, r, "window_s") for r in REG] for st in STORES}
win_morc = {st: [m(U, st, "MoR (compacted)", "window_s") / m(U, st, r, "window_s") for r in REG] for st in STORES}

# Latency early and late in the run: a write's mean over writes 1-5 and 26-30, a read's at the
# probes after 5 and 30 writes
def drift(st, name, what):
    g = SNU[(SNU.Store == st) & (SNU.name == name) & (SNU.what == what)]
    if what == WRITE:
        return g[g.step <= 5].s.mean(), g[g.step >= 26].s.mean()
    return g[g.step == 5].s.iloc[0], g[g.step == 30].s.iloc[0]


WIN = READ["window_s"]
grow = {(st, n, w): (lambda a, b: b / a)(*drift(st, n, w)) for st in STORES for n in ["MoR", *REG]
        for w in (WRITE, WIN)}
late_win = {st: [drift(st, "MoR", WIN)[1] / drift(st, r, WIN)[1] for r in REG] for st in STORES}
late_write = {st: [drift(st, r, WRITE)[1] / drift(st, "MoR", WRITE)[1] for r in REG] for st in STORES}

# CPU's share of an update write's measured time on its own thread, compactions aside
CPU = ["decode", "encode", "DVs and footers", "other CPU"]


def share(st, name, parts, phase="batch"):
    label, arm = next((s[0], s[1]) for s in UPD if s[5] == name)
    return SH.loc[(st, label, arm, phase)].reindex(parts).fillna(0).sum()


reg_cpu = {st: [share(st, r, CPU) for r in REG] for st in STORES}
reg_enc = {st: [share(st, r, ["encode"]) for r in REG] for st in STORES}

# The near pairs, measured, not judged
near_full = {st: [m(U, st, r, c) / m(U, st, b, c) for r in REG for b in MORS for c in ("full_s", "sorted_s")]
             for st in STORES}
ins_names = [s[5] for s in ING]
ins_spread = {st: [max(m(I, st, n, c) for n in ins_names) / min(m(I, st, n, c) for n in ins_names) - 1
                   for c in ("full_s", "sorted_s")] for st in STORES}
morc_write = {st: m(U, st, "MoR (compacted)") / m(U, st, "MoR") for st in STORES}
def_write = {st: [m(U, st, "RMoR-density") / m(U, st, b) for b in MORS] for st in STORES}
def_win = {st: m(U, st, "MoR (compacted)", "window_s") / m(U, st, "RMoR-density", "window_s") for st in STORES}
seal_win = {st: m(I, st, "MoR", "window_s") / m(I, st, "RMoW-append", "window_s") for st in STORES}

az_span = q("""SELECT min(started_at) AS a, max(finished_at) AS b FROM runs
               WHERE list_contains(campaigns, $c) AND (label LIKE '%-azure' OR label = 'azure-spike')""",
            c=CLOUD).iloc[0]
az_hours = (az_span.b - az_span.a).total_seconds() / 3600


def pspan(xs):
    return span([100 * x for x in xs], "{:.0f}") + "%"


# The designs against MoR never compacted, and RMoR's merge against MoR on the inserts
reg_write_mor = {st: [m(U, st, r) / m(U, st, "MoR") for r in REG] for st in STORES}
seal_r = {st: dict(window=m(I, st, "MoR", "window_s") / m(I, st, "RMoR", "window_s"),
                   extra=m(I, st, "RMoR") - m(I, st, "MoR")) for st in STORES}
ref_late = {st: at30 for st, at30 in ((st, CMU[(CMU.Store == st) & (CMU.name == "RMoW-append")
                                                & (CMU.what == WIN) & (CMU.step == 30)].s.iloc[0])
                                      for st in STORES)}
```

What each design costs a writer and its readers on the two workloads, **measured** in the cloud on
S3 Express and Azure Blob: the main points of the [workloads page](./workloads.md), run on a VM
beside each store. [The simulation against the cloud](./simulation-cloud.md) (calibration) sets
these times beside the workloads page's simulation of them.

## What it shows

- **The trade-offs hold on both stores, measured.** On the updates, with nothing compacted, RMoR,
  RMoW-external and RMoW-append take {eval}`span(reg_write_mor[S3X], "{:.1f}")` times as long as MoR
  to write on S3 Express ({eval}`span(reg_write_mor[AZ], "{:.1f}")` on Azure Blob), and read a 1%
  range {eval}`span(win_mor[S3X], "{:.1f}")` times as fast
  ({eval}`span(win_mor[AZ], "{:.1f}")` on Azure Blob). On the inserts, RMoR, which merges the
  files of its four newest writes every 4th write, reads a range
  {eval}`f1(seal_r[S3X]["window"])` times as fast as MoR
  ({eval}`f1(seal_r[AZ]["window"])` on Azure Blob), for {eval}`f2(seal_r[S3X]["extra"])` s more a
  write ({eval}`f2(seal_r[AZ]["extra"])` s).
- **Compaction brings MoR's range read back in line, for little data written.** Compacted every 10
  writes, MoR read a range after 30 writes in
  {eval}`f2(ct(TU, S3X, "every 10 writes", "range read after 30"))` s against
  {eval}`f2(ct(TU, S3X, "never", "range read after 30"))` never compacted on S3 Express
  ({eval}`f2(ct(TU, AZ, "every 10 writes", "range read after 30"))` and
  {eval}`f2(ct(TU, AZ, "never", "range read after 30"))` on Azure Blob), and wrote, its compactions
  spread over the writes, in {eval}`f2(ct(TU, S3X, "every 10 writes", "write with compactions"))` s
  against {eval}`f2(ct(TU, S3X, "never", "write"))`, and wrote
  {eval}`f2(ct(TU, S3X, "every 10 writes", "GB written"))` GB over the 30 writes against
  {eval}`f2(ct(TU, S3X, "never", "GB written"))` never compacted.
- **The region designs write far more:** over the 30 writes RMoR, RMoW-external and RMoW-append
  write {eval}`span([gbw(WRU, n) for n in REG], "{:.1f}")` GB,
  {eval}`f"{gbw(WRU, 'RMoW-append') / gbw(WRU, 'MoR'):.0f}"` times MoR's
  {eval}`f2(gbw(WRU, "MoR"))` and {eval}`f"{gbw(WRU, 'RMoW-append') / gbw(WRU, 'MoR (compacted)'):.0f}"`
  times MoR's compacted every 10 writes.
- **MoR's costs grow as its changes pile up; the region designs' grow far less.** From the first
  five writes to the last five, MoR's update write grew {eval}`f2(grow[(S3X, "MoR", WRITE)])`
  times on S3 Express ({eval}`f2(grow[(AZ, "MoR", WRITE)])` on Azure Blob), and from the 5th
  write to the 30th its range read grew {eval}`f2(grow[(S3X, "MoR", WIN)])` times
  ({eval}`f2(grow[(AZ, "MoR", WIN)])`). The region designs' writes grew
  {eval}`span([grow[(S3X, r, WRITE)] for r in REG])` times
  ({eval}`span([grow[(AZ, r, WRITE)] for r in REG])`) and their range reads
  {eval}`span([grow[(S3X, r, WIN)] for r in REG])` times
  ({eval}`span([grow[(AZ, r, WIN)] for r in REG])`). So the means over the run understate the
  trade late in it: after 30 writes the region designs read a range
  {eval}`span(late_win[S3X], "{:.1f}")` times as fast as MoR on S3 Express
  ({eval}`span(late_win[AZ], "{:.1f}")` on Azure Blob), and their last five writes took
  {eval}`span(late_write[S3X], "{:.1f}")` times as long as MoR's
  ({eval}`span(late_write[AZ], "{:.1f}")`).
- **The region designs' writes are mostly CPU.** On S3 Express, CPU took
  {eval}`pspan(reg_cpu[S3X])` of their update writes' measured time (encode alone
  {eval}`pspan(reg_enc[S3X])`), against {eval}`pct(share(S3X, "MoR", CPU))` of MoR's, which spends
  {eval}`pct(share(S3X, "MoR", ["I/O"]))` on I/O. On Azure Blob, whose requests
  are slower, CPU's share falls to {eval}`pspan(reg_cpu[AZ])` of theirs and
  {eval}`pct(share(AZ, "MoR", CPU))` of MoR's, and their writes come closer to MoR's
  ({eval}`span(l2[AZ]["write"], "{:.1f}")` times, against {eval}`span(l2[S3X]["write"], "{:.1f}")`).

## Setup

**The workloads and designs** are the [workloads page](./workloads.md)'s, at 16 MiB row groups:
updates (150,000 changes a write) and inserts (350,000 new rows a write), 30 writes each. Writes
locate their rows by key, pruning row groups by each changed key, and the readers skip row groups
whose rows are all dead, the sorted read too.

**The runs.** Each configuration ran once on each store from a VM beside it (S3 Express from a
`c7a.4xlarge` in usw2-az3, one per configuration; Azure Blob from one `D8as_v6` in westus, the
configurations in turn), at {eval}`", ".join(sorted(runs.build.dropna().unique()))`. Each S3
Express VM also ran its configuration on its own disk (its twin), and each VM measured its store's
latency first; the calibration page uses both.

**The times** are each phase's wall time in the cloud run. Each read's time is its mean over
steps 5 to 30, and a write's over the 30 writes, with compactions spread over them. **Data written** is
every byte a design's writes and compactions put in the store after the initial load, the same in
every run of a configuration; no run deletes anything, so it is also what the store holds beyond
the initial load.

```{code-cell} ipython3
:tags: [remove-input]

table(runs, caption="The cloud runs, twins and latency tests")
```

## Updates

### 1. Write latency against read latency

```{code-cell} ipython3
:label: workloads-cloud-updates-tradeoff
:tags: [remove-input]
:class: col-page-right

fig.wl_tradeoff(UM, ["window_s", "full_s", "sorted_s"])
```

Each design's measured write latency against its read latency on the updates, with nothing
compacted (section 5 compacts MoR): a write's mean over the 30 writes, its whiskers from the 10th
to the 90th percentile, and a read's mean over the reads after 5 to 30 writes, its whiskers from
the fastest to the slowest.

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(UM, ["window_s", "full_s", "sorted_s"]), caption="Measured latency (s) on the updates")
```

### 2. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-updates
:tags: [remove-input]
:class: col-page-right

fig.wl_snapshot_chart(SNU_M, ["window_s", "full_s", "sorted_s"])
```

Each design's measured write latency at every write, and its read latency after every 5 writes, on
the updates with nothing compacted (section 5 compacts MoR).

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNU_M, WRITE, "measured")
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNU_M, READ["window_s"], "measured")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNU_M, READ["full_s"], "measured")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNU_M, READ["sorted_s"], "measured")
```

- **The region designs write slower:** on average over the 30 writes,
  {eval}`span(reg_write_mor[S3X], "{:.1f}")` times MoR's time on S3 Express and
  {eval}`span(reg_write_mor[AZ], "{:.1f}")` on Azure Blob.
- **MoR's reads slow as snapshots are added, theirs do not:** from the 5th write to the 30th MoR's
  range read grew {eval}`f2(grow[(S3X, "MoR", WIN)])` times on S3 Express
  ({eval}`f2(grow[(AZ, "MoR", WIN)])` on Azure Blob), the region designs'
  {eval}`span([grow[(S3X, r, WIN)] for r in REG])` times
  ({eval}`span([grow[(AZ, r, WIN)] for r in REG])`). After 30 writes they read a range
  {eval}`span(late_win[S3X], "{:.1f}")` times as fast as MoR
  ({eval}`span(late_win[AZ], "{:.1f}")`).

### 3. Data written as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-updates-written
:tags: [remove-input]
:class: col-page-right

written_chart(WRU, UN_M + ["MoR (compacted)"])
```

What each design has written after every write on the updates, in GB, by its writes and
compactions, the same in every run, with MoR (compacted every 10 writes) dashed. RMoR,
RMoW-external and RMoW-append write the same, on one line.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_written_table(WRU, UN_M + ["MoR (compacted)"])
```

- **The region designs write about {eval}`f"{gbw(WRU, 'RMoW-append') / gbw(WRU, 'MoR'):.0f}"` times
  MoR's data:** {eval}`span([gbw(WRU, n) for n in REG], "{:.1f}")` GB over the 30 writes, against
  MoR's {eval}`f2(gbw(WRU, "MoR"))`, MoR (compacted)'s {eval}`f2(gbw(WRU, "MoR (compacted)"))` and
  RMoR-density's {eval}`f2(gbw(WRU, "RMoR-density"))`: each write copies the row groups it changes
  whole.

### 4. Where the time goes

```{code-cell} ipython3
:label: workloads-cloud-updates-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(UP[UP.name.isin(UN_M)], [WRITE, READ["window_s"], READ["full_s"]], UN_M, PARTS)
```

Each design's measured time split by what its thread was doing: Parquet's decode and encode; DVs
and footers (deletion vectors built, encoded, loaded and decoded, and RMoW's footers); other CPU
(the rest of the CPU: making the rows a write writes, planning scans, Iceberg's manifests and
commit); I/O (the store client's CPU and the wait for its requests); and paused (garbage
collection, the JIT).

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(parts_table(UP[UP.name.isin(UN_M)]), caption="Measured time (s) by part")
```

### 5. Compaction

```{code-cell} ipython3
:label: workloads-cloud-updates-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMU, ["window_s", "full_s"], "RMoW-append")
```

MoR's measured read latency after every 5 writes, never compacted and compacted every 10 writes
(the interval the cloud ran; a read at a compaction's write reads the compacted table), beside
RMoW-append's (dashed, never compacted).

```{code-cell} ipython3
:label: workloads-cloud-updates-compaction-written
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_written_chart(WRCU, ("never", "every 10 writes"))
```

What MoR has written after every write, in GB, never compacted and compacted every 10 writes.

```{code-cell} ipython3
:tags: [remove-input]

table(TU, caption="MoR, compacted and not, measured latency (s): a write on average without and with its "
      "compactions spread over the writes, a compaction on average, each read on average and after 30 "
      "writes; and the data written over the 30 writes (GB)")
```

- **Compaction brings MoR's range read back down:** after 30 writes it took
  {eval}`f2(ct(TU, S3X, "every 10 writes", "range read after 30"))` s compacted every 10 writes
  against {eval}`f2(ct(TU, S3X, "never", "range read after 30"))` never compacted on S3 Express
  ({eval}`f2(ct(TU, AZ, "every 10 writes", "range read after 30"))` and
  {eval}`f2(ct(TU, AZ, "never", "range read after 30"))` on Azure Blob), against RMoW-append's
  {eval}`f2(ref_late[S3X])` ({eval}`f2(ref_late[AZ])`) with nothing compacted.
- **The writes barely pay for it:** with its compactions spread over the writes, MoR's write took
  {eval}`f2(ct(TU, S3X, "every 10 writes", "write with compactions"))` s against
  {eval}`f2(ct(TU, S3X, "never", "write"))` never compacted on S3 Express
  ({eval}`f2(ct(TU, AZ, "every 10 writes", "write with compactions"))` and
  {eval}`f2(ct(TU, AZ, "never", "write"))` on Azure Blob), a compaction
  {eval}`f2(ct(TU, S3X, "every 10 writes", "a compaction"))` s on average
  ({eval}`f2(ct(TU, AZ, "every 10 writes", "a compaction"))`).
- **Compaction writes little:** {eval}`f2(ct(TU, S3X, "every 10 writes", "GB written"))` GB over
  the 30 writes compacted every 10 writes, against {eval}`f2(ct(TU, S3X, "never", "GB written"))`
  never compacted, and {eval}`span([gbw(WRU, n) for n in REG], "{:.1f}")` for the region designs.

## Inserts

### 6. Write latency against read latency

```{code-cell} ipython3
:label: workloads-cloud-inserts-tradeoff
:tags: [remove-input]
:class: col-page-right

fig.wl_tradeoff(IM, ["window_s", "full_s", "sorted_s"])
```

Each policy's measured write latency against its read latency on the inserts, with nothing
compacted (section 10 compacts MoR). RMoR and RMoW-append merge the files of their four newest
writes every 4th write ([the workloads page](./workloads.md), Inserts). The means and whiskers are as
in section 1.

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(IM, ["window_s", "full_s", "sorted_s"]), caption="Measured latency (s) on the inserts")
```

### 7. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-inserts
:tags: [remove-input]
:class: col-page-right

fig.wl_snapshot_chart(SNI_M, ["window_s", "sorted_s"])
```

Each policy's measured write latency at every write, and its read latency after every 5 writes, on
the inserts with nothing compacted.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNI_M, WRITE, "measured")
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNI_M, READ["window_s"], "measured")
```

- **RMoR's merge reads a range {eval}`f1(seal_r[S3X]["window"])` times as fast as MoR**
  on S3 Express ({eval}`f1(seal_r[AZ]["window"])` on Azure Blob), for
  {eval}`f2(seal_r[S3X]["extra"])` s more a write on average ({eval}`f2(seal_r[AZ]["extra"])` s);
  RMoW-append's merge {eval}`f2(seal_win[S3X])` and {eval}`f2(seal_win[AZ])` times as fast.

### 8. Data written as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-inserts-written
:tags: [remove-input]
:class: col-page-right

written_chart(WRI, IN_M + ["MoR (compacted)"])
```

What each design has written after every write on the inserts, in GB, as in section 3.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_written_table(WRI, IN_M + ["MoR (compacted)"])
```

- **Merging writes the merged rows again, and compaction does the same:** over the 30 writes MoR
  writes {eval}`f2(gbw(WRI, "MoR"))` GB, RMoR {eval}`f2(gbw(WRI, "RMoR"))`, RMoW-append
  {eval}`f2(gbw(WRI, "RMoW-append"))` and MoR (compacted) {eval}`f2(gbw(WRI, "MoR (compacted)"))`.

### 9. Where the time goes

```{code-cell} ipython3
:label: workloads-cloud-inserts-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(IP[IP.name.isin(IN_M)], [WRITE, READ["window_s"], READ["sorted_s"]], IN_M, PARTS)
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(parts_table(IP[IP.name.isin(IN_M)]), caption="Measured time (s) by part")
```

### 10. Compaction

```{code-cell} ipython3
:label: workloads-cloud-inserts-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMI, ["window_s"], "RMoR")
```

MoR's measured range-read latency after every 5 writes on the inserts, never compacted and
compacted every 10 writes, beside RMoR's (dashed).

```{code-cell} ipython3
:label: workloads-cloud-inserts-compaction-written
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_written_chart(WRCI, ("never", "every 10 writes"))
```

What MoR has written after every write on the inserts, in GB, as in section 5.

```{code-cell} ipython3
:tags: [remove-input]

table(TI, caption="MoR, compacted and not, on the inserts, as in section 5")
```

- **Compacting merges late rows after the fact, as RMoR's merge does every 4th write:** after 30 writes
  MoR read a range in {eval}`f2(ct(TI, S3X, "every 10 writes", "range read after 30"))` s compacted
  every 10 writes and {eval}`f2(ct(TI, S3X, "never", "range read after 30"))` never compacted on S3
  Express ({eval}`f2(ct(TI, AZ, "every 10 writes", "range read after 30"))` and
  {eval}`f2(ct(TI, AZ, "never", "range read after 30"))` on Azure Blob), for
  {eval}`f2(ct(TI, S3X, "every 10 writes", "write with compactions") - ct(TI, S3X, "never", "write"))` s
  more a write with its compactions spread over the writes
  ({eval}`f2(ct(TI, AZ, "every 10 writes", "write with compactions") - ct(TI, AZ, "never", "write"))` s),
  and {eval}`f2(ct(TI, S3X, "every 10 writes", "GB written"))` GB written over the 30 writes,
  against {eval}`f2(ct(TI, S3X, "never", "GB written"))` never compacted.

## The checks registered before the run

The run was to judge four hypotheses, registered in docs/HYPOTHESES.md ("The two workloads in the
cloud", 2026-11-10). Two are about the trade-offs, here; the other two, L1 (the cloud runs do the
work the simulated runs counted) and L4 (the simulation prices the cloud runs within 15%), are about
the measurement, on [the simulation against the cloud](./simulation-cloud.md).

- **L2, on the updates the region designs trade a slower write for a faster range read:
  {eval}`V["L2"]`.** On each store, RMoR, RMoW-external and RMoW-append each take longer than MoR
  and MoR (compacted) to write ({eval}`span(l2[S3X]["write"], "{:.1f}")` times on S3 Express,
  {eval}`span(l2[AZ]["write"], "{:.1f}")` on Azure Blob) and read a 1% range faster than both
  ({eval}`span(l2[S3X]["window"], "{:.1f}")` and {eval}`span(l2[AZ]["window"], "{:.1f}")` times).
  MoR (compacted) reads a range {eval}`f"{l2[S3X]['compacted']:.1f}"` and
  {eval}`f"{l2[AZ]['compacted']:.1f}"` times as fast as MoR. RMoR-density reads a range
  {eval}`f"{l2[S3X]['deferred_window']:.1f}"` and {eval}`f"{l2[AZ]['deferred_window']:.1f}"` times
  as fast as MoR, and writes {eval}`span(l2[S3X]["deferred_write"], "{:.1f}")` and
  {eval}`span(l2[AZ]["deferred_write"], "{:.1f}")` times as fast as the region designs.
  {eval}`"" if l2_held else "Not so on " + "; ".join(f"{st}: {L2_PARTS[k]}" for st, k in l2_fail) + "."`
- **L3, on the inserts merging late rows trades a little write time for a faster range read:
  {eval}`V["L3"]`.** MoR (compacted) and RMoR read a 1% range
  {eval}`span(l3[S3X]["window"], "{:.1f}")` times as fast as MoR on S3 Express and
  {eval}`span(l3[AZ]["window"], "{:.1f}")` on Azure Blob, and take
  {eval}`span(l3[S3X]["write"], "{:.1f}")` and {eval}`span(l3[AZ]["write"], "{:.1f}")` times as
  long to write: {eval}`span(l3[S3X]["extra"])` s and {eval}`span(l3[AZ]["extra"])` s more a
  write, under the 1 s registered.
  {eval}`"" if l3_held else "Not so on " + "; ".join(f"{st}: {k}" for st, k in l3_fail) + "."`

**Measured, not judged:**
- **The full scans and sorted reads on the updates:** the region designs' take
  {eval}`span(near_full[S3X])` of MoR's and MoR (compacted)'s time on S3 Express
  ({eval}`span(near_full[AZ])` on Azure Blob). On the inserts, the four designs' full scans are
  within {eval}`pct(ins_spread[S3X][0])` of each other and their sorted reads within
  {eval}`pct(ins_spread[S3X][1])` ({eval}`pct(ins_spread[AZ][0])` and
  {eval}`pct(ins_spread[AZ][1])` on Azure Blob).
- **MoR (compacted)'s write against MoR's:** {eval}`f2(morc_write[S3X])` and
  {eval}`f2(morc_write[AZ])` times.
- **RMoR-density against MoR:** its write {eval}`span(def_write[S3X])` and
  {eval}`span(def_write[AZ])` times MoR's and MoR (compacted)'s; MoR (compacted)'s range read
  {eval}`f2(def_win[S3X])` and {eval}`f2(def_win[AZ])` times its.
- **RMoW-append on the inserts** reads a range {eval}`f2(seal_win[S3X])` and
  {eval}`f2(seal_win[AZ])` times as fast as MoR.

## Limits

- One run of each configuration on each store, and one latency test per VM: the whiskers are the
  spread over a run's writes and reads, not over repeated runs.
- The S3 Express runs share one bucket, four at once, each measured under the others' load. The
  Azure Blob runs ran one after another over {eval}`f"{az_hours:.1f}"` hours.
- The region designs' and MoR (compacted)'s range reads take a fraction of a second, so a few
  requests' first bytes decide them.
- MoR's write and range read grow over the run, so its means depend on the run's length: 30
  writes.
