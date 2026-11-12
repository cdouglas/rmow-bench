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
# CLOUD. The forecast is the simulation of the reference runs (campaign REF: havoc's requests and
# bytes, the laptop's CPU), priced as the workloads page prices them.
CLOUD = "workloads-cloud"
REF = "workloads-ref"
# These configurations read through the skipping reader alone, the sorted read too.
tune.READS["sorted"] = ("read_sorted", "cached-skip")
tune.WORKLOADS["wl_upd"] = "regexp_matches(label, '^wl-upd-')"
tune.WORKLOADS["wl_ing"] = "regexp_matches(label, '^wl-ing-')"

# The designs: (configuration, arm, family, curve, knob, name, order), as on the workloads page,
# main points only
UPD = [
    ("wl-upd-base", "mor-v3", "MoR", "", "", "MoR", 0),
    ("wl-upd-compact", "mor-v3", "MoR", "", "", "MoR compacted", 0),
    ("wl-upd-base", "rmor-c-kept", "RMoR", "", "", "RMoR", 0),
    ("wl-upd-base", "rmor-c-footer-kept", "RMoW-external", "", "", "RMoW-external", 0),
    ("wl-upd-base", "rmow-c", "RMoW-append", "", "", "RMoW-append", 0),
    ("wl-upd-base", "rmor-density", "deferred merges", "", "", "deferred merges", 0),
]
ING = [
    ("wl-ing-base", "mor-v3", "appending", "", "", "appending", 0),
    ("wl-ing-compact", "mor-v3", "MoR", "", "", "MoR compacted", 0),
    ("wl-ing-seal160", "rmor-c", "RMoR", "", "", "RMoR sealed", 0),
    ("wl-ing-seal160", "rmow-c", "RMoW-append", "", "", "RMoW-append sealed", 0),
]
STORES = fig.WL_STORES
S3X, AZ = STORES
READ = fig.WL_READ
```

```{code-cell} ipython3
:tags: [remove-cell]

WHERE = f"list_contains(campaigns, '{CLOUD}')"
POLICIES = [("upd" if "-upd-" in s[0] else "ing", s[0], s[1]) for s in UPD + ING]

# Measured: each phase's wall time in the cloud, per step; and priced, its twin's work on the S3
# Express VM plus its requests priced by the VM's own latency test (tune.cloud_steps)
M = pd.concat([tune.cloud_steps(con, st, POLICIES, WHERE) for st in ("s3x", "azure")], ignore_index=True)
M["Store"] = M.store.map(fig.WL_STORE)
S = M.assign(s=M.measured_ms / 1e3)


def means(d, col):
    """Each configuration's and arm's mean write and reads in `col` (ms), as tune.policies()."""
    rows = [dict(store=st, Store=fig.WL_STORE[st], label=l, arm=a, **tune.per_write(g, col))
            for (st, l, a), g in d.groupby(["store", "label", "arm"])]
    return pd.DataFrame(rows)


PM, PP = means(M, "measured_ms"), means(M, "priced_ms")
W = fig.wl_whiskers(S)

# The forecast: the reference runs simulated (requests priced by the registered latency tests, the
# laptop's CPU at its pace), as on the workloads page
train = tune.records(con, tune.TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
formula = tune.Formula(train)
ns = tune.pace(con)
GRID = f"list_contains(campaigns, '{REF}')"
F = pd.concat([tune.policies(con, wl, formula, ns, True, tune.steps(con, wl, formula, ns, True, GRID))
               for wl in ("wl_upd", "wl_ing")])
F["Store"] = F.store.map(fig.WL_STORE)
RK = ["window_s", "full_s", "sorted_s"]


def frame(spec):
    """The measured points with their whiskers, and each one's forecast (pred_*) and priced time."""
    d = fig.wl_frame(spec, PM, W)
    k = ["Store", "label", "arm"]
    for name, src in (("pred", F), ("priced", PP)):
        f = fig.wl_frame(spec, src, W[k])[k + ["write_s", *RK]]
        d = d.merge(f.rename(columns={c: f"{name}_{c}" for c in ["write_s", *RK]}), on=k, how="left")
    return d


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


def against(d, reads):
    """Per store, design and operation: the forecast, priced and measured latency (s), and the
    measured over the forecast."""
    rows = []
    for _, r in d.iterrows():
        for what, col in (("a write", "write_s"), *[(READ[k], k) for k in reads]):
            rows.append(dict(Store=r.Store, name=r["name"], what=what, forecast=r[f"pred_{col}"],
                             priced=r[f"priced_{col}"], measured=r[col]))
    t = pd.DataFrame(rows)
    t["measured / forecast"] = t.measured / t.forecast
    return t


AU, AI = against(U, ["window_s", "full_s", "sorted_s"]), against(I, ["window_s", "sorted_s"])
A = pd.concat([AU.assign(workload="updates"), AI.assign(workload="inserts")])

# Where the measured time goes: each phase's components on its own thread (phase_breakdown), whose
# shares split the measured mean; compactions spread over the writes
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
PCOL = {**fig.WL_PCOL, "compactions, spread over the writes": "#b9a37e"}
WHAT = {"write": ("batch", "a write"), "window": ("read_window", READ["window_s"]),
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
                if kind == "write" and r.compact_ms > 0:
                    rows.append(dict(Store=r.Store, name=name, what=what, part=PARTS[-1], s=r.compact_ms / 1e3))
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

# The designs compared as snapshots are added, with nothing compacted: compaction has sections of
# its own (3 and 6)
UN_M = [n for n in UN if n != "MoR compacted"]
IN_M = [n for n in IN if n != "MoR compacted"]
SNU_M, SNI_M = SNU[SNU.name.isin(UN_M)], SNI[SNI.name.isin(IN_M)]

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
CMI = compaction(CI, ["window_s"], ("wl-ing-seal160", "rmor-c", "RMoR", "RMoR sealed"))
STU, STI = fig.wl_stored(CU), fig.wl_stored(CI)
TU, TI = fig.wl_compaction_costs(CU, PM, CMU, STU), fig.wl_compaction_costs(CI, PM, CMI, STI)


def ct(T, st, name, col):
    return T[(T.Store == st) & (T.compacted == name)][col].iloc[0]

runs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build, round(elapsed_s / 60) AS minutes
            FROM runs WHERE list_contains(campaigns, $c) ORDER BY label, run_id""", c=CLOUD)
refs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
            WHERE list_contains(campaigns, $c) ORDER BY label, host""", c=REF)
```

```{code-cell} ipython3
:tags: [remove-cell]

# The checks registered before the run (docs/HYPOTHESES.md, "The two workloads in the cloud",
# 2026-11-10): L1-L4.

# L1: each cloud run and each S3 Express twin against havoc's run of its configuration: every
# record's requests, bytes and table state (steps_diff), and the held peaks of the phases on one
# thread (no pool time in either run)
HELD = ["held_bytes", "held_rows", "held_chunks", "held_writer", "held_pending", "held_deletes"]
havoc = q("SELECT label, run_id FROM canonical_runs WHERE list_contains(campaigns, $c) AND host = 'havoc'", c=REF)
bench = q("""SELECT run_id, label FROM canonical_runs WHERE list_contains(campaigns, $c) AND source = 'bench'
             ORDER BY label""", c=CLOUD)
l1 = []
for _, o in bench.iterrows():
    base = o.label.removesuffix("-s3x").removesuffix("-azure")
    h = havoc[havoc.label == base].run_id.iloc[0]
    d = q(f"""SELECT count(*) AS records, count(*) FILTER (WHERE one_run_only OR len(differs) > 0) AS differ
              FROM steps_diff('{h}', '{o.run_id}')""").iloc[0]
    held = q(f"""SELECT count(*) AS n,
                        count(*) FILTER (WHERE {' OR '.join(f'a.{c} IS DISTINCT FROM b.{c}' for c in HELD)}) AS differ
                 FROM steps a JOIN steps b ON a.arm = b.arm AND a.step = b.step AND a.phase = b.phase
                      AND a.reader IS NOT DISTINCT FROM b.reader AND a.window_at IS NOT DISTINCT FROM b.window_at
                 WHERE a.run_id = '{h}' AND b.run_id = '{o.run_id}'
                   AND coalesce(a.pool_wall_ns, 0) = 0 AND coalesce(b.pool_wall_ns, 0) = 0""").iloc[0]
    kind = (S3X if o.label.endswith("-s3x") else AZ if o.label.endswith("-azure")
            else "twin (S3 Express VM's disk)")
    l1.append(dict(configuration=base, run=kind, run_id=o.run_id, records=int(d.records), differ=int(d.differ),
                   one_thread_records=int(held.n), held_differ=int(held.differ)))
l1 = pd.DataFrame(l1)
l1_held = bool(len(l1) == 15 and (l1.differ == 0).all() and (l1.held_differ == 0).all())


def m(d, store, name, col="write_s"):
    return d[(d.Store == store) & (d["name"] == name)][col].iloc[0]


# L2: on the updates, per store
REG, MORS = ["RMoR", "RMoW-external", "RMoW-append"], ["MoR", "MoR compacted"]
l2 = {}
for st in STORES:
    l2[st] = dict(
        write=[m(U, st, r) / m(U, st, b) for r in REG for b in MORS],
        window=[m(U, st, b, "window_s") / m(U, st, r, "window_s") for r in REG for b in MORS],
        compacted=m(U, st, "MoR", "window_s") / m(U, st, "MoR compacted", "window_s"),
        deferred_window=m(U, st, "MoR", "window_s") / m(U, st, "deferred merges", "window_s"),
        deferred_write=[m(U, st, r) / m(U, st, "deferred merges") for r in REG])
L2_PARTS = {"write": "the region designs write slower than MoR and MoR compacted",
            "window": "they read a window faster than both",
            "compacted": "MoR compacted reads a window faster than MoR",
            "deferred_window": "the deferred merges read a window faster than MoR",
            "deferred_write": "the deferred merges write faster than each region design"}
l2_fail = [(st, k) for st, parts in l2.items() for k, x in parts.items() if min(np.atleast_1d(x)) <= 1]
l2_held = not l2_fail

# L3: on the inserts, per store, MoR compacted and RMoR sealed against appending
MERGE = ["MoR compacted", "RMoR sealed"]
l3 = {st: dict(
    window=[m(I, st, "appending", "window_s") / m(I, st, n, "window_s") for n in MERGE],
    write=[m(I, st, n) / m(I, st, "appending") for n in MERGE],
    extra=[m(I, st, n) - m(I, st, "appending") for n in MERGE]) for st in STORES}
l3_fail = ([(st, "window") for st, p in l3.items() if min(p["window"]) <= 1]
           + [(st, "write") for st, p in l3.items() if min(p["write"]) <= 1]
           + [(st, "extra") for st, p in l3.items() if max(p["extra"]) >= 1])
l3_held = not l3_fail

# L4: on S3 Express, each design's write and each of its reads priced within 15% (its twin's work
# on the VM plus its requests at the VM's spike)
l4 = pd.concat([d[d.Store == S3X].melt(id_vars=["name"], value_vars=["write_s", *RK], var_name="col",
                                       value_name="measured")
                .merge(d[d.Store == S3X].melt(id_vars=["name"], value_vars=[f"priced_{c}" for c in ["write_s", *RK]],
                                              var_name="col", value_name="priced")
                       .assign(col=lambda x: x.col.str.removeprefix("priced_")), on=["name", "col"])
                .assign(workload=w) for d, w in ((U, "updates"), (I, "inserts"))], ignore_index=True)
l4["priced_over_measured"] = l4.priced / l4.measured
l4_err = (l4.priced_over_measured - 1).abs()
l4_held = bool((l4_err <= 0.15).all())
l4_out = l4[l4_err > 0.15]

V = {"L1": "held" if l1_held else "contradicted", "L2": "held" if l2_held else "contradicted",
     "L3": "held" if l3_held else "contradicted", "L4": "held" if l4_held else "contradicted"}


def pct(x, nd=0):
    return f"{100 * x:.{nd}f}%"


# For the prose. The updates' range reads against MoR and against MoR compacted, apart
win_mor = {st: [m(U, st, "MoR", "window_s") / m(U, st, r, "window_s") for r in REG] for st in STORES}
win_morc = {st: [m(U, st, "MoR compacted", "window_s") / m(U, st, r, "window_s") for r in REG] for st in STORES}

# Measured over forecast, priced over forecast and priced over measured, per store (writes, reads)
A["priced / forecast"] = A.priced / A.forecast
A["priced / measured"] = A.priced / A.measured


def by(st, col, writes=None):
    g = A[A.Store == st]
    if writes is not None:
        g = g[(g.what == "a write") == writes]
    return g[col]


# Latency early and late in the run: a write's mean over writes 1-5 and 26-30, a read's at the
# probes after 5 and 30 writes
def drift(st, name, what):
    g = SNU[(SNU.Store == st) & (SNU.name == name) & (SNU.what == what)]
    if what == "a write":
        return g[g.step <= 5].s.mean(), g[g.step >= 26].s.mean()
    return g[g.step == 5].s.iloc[0], g[g.step == 30].s.iloc[0]


WIN = READ["window_s"]
grow = {(st, n, w): (lambda a, b: b / a)(*drift(st, n, w)) for st in STORES for n in ["MoR", *REG]
        for w in ("a write", WIN)}
late_win = {st: [drift(st, "MoR", WIN)[1] / drift(st, r, WIN)[1] for r in REG] for st in STORES}
late_write = {st: [drift(st, r, "a write")[1] / drift(st, "MoR", "a write")[1] for r in REG] for st in STORES}

# CPU's share of an update write's measured time on its own thread, compactions aside
CPU = ["decoding", "encoding", "deletion vectors and footers", "rows and planning"]


def share(st, name, parts, phase="batch"):
    label, arm = next((s[0], s[1]) for s in UPD if s[5] == name)
    return SH.loc[(st, label, arm, phase)].reindex(parts).fillna(0).sum()


reg_cpu = {st: [share(st, r, CPU) for r in REG] for st in STORES}
reg_enc = {st: [share(st, r, ["encoding"]) for r in REG] for st in STORES}

# The near pairs, measured, not judged
near_full = {st: [m(U, st, r, c) / m(U, st, b, c) for r in REG for b in MORS for c in ("full_s", "sorted_s")]
             for st in STORES}
ins_names = [s[5] for s in ING]
ins_spread = {st: [max(m(I, st, n, c) for n in ins_names) / min(m(I, st, n, c) for n in ins_names) - 1
                   for c in ("full_s", "sorted_s")] for st in STORES}
morc_write = {st: m(U, st, "MoR compacted") / m(U, st, "MoR") for st in STORES}
def_write = {st: [m(U, st, "deferred merges") / m(U, st, b) for b in MORS] for st in STORES}
def_win = {st: m(U, st, "MoR compacted", "window_s") / m(U, st, "deferred merges", "window_s") for st in STORES}
seal_win = {st: m(I, st, "appending", "window_s") / m(I, st, "RMoW-append sealed", "window_s") for st in STORES}

az_span = q("""SELECT min(started_at) AS a, max(finished_at) AS b FROM runs
               WHERE list_contains(campaigns, $c) AND (label LIKE '%-azure' OR label = 'azure-spike')""",
            c=CLOUD).iloc[0]
az_hours = (az_span.b - az_span.a).total_seconds() / 3600
n_s3x, n_az = int((l1.run == S3X).sum()), int((l1.run == AZ).sum())
n_twin = len(l1) - n_s3x - n_az
l4_worst = l4.loc[l4_err.idxmax()]
COL_WHAT = {"write_s": "a write", **READ}


def pspan(xs):
    return span([100 * x for x in xs], "{:.0f}") + "%"


# The designs against MoR never compacted, and RMoR's seal against appending
reg_write_mor = {st: [m(U, st, r) / m(U, st, "MoR") for r in REG] for st in STORES}
seal_r = {st: dict(window=m(I, st, "appending", "window_s") / m(I, st, "RMoR sealed", "window_s"),
                   extra=m(I, st, "RMoR sealed") - m(I, st, "appending")) for st in STORES}
ref_late = {st: at30 for st, at30 in ((st, CMU[(CMU.Store == st) & (CMU.name == "RMoW-append")
                                                & (CMU.what == WIN) & (CMU.step == 30)].s.iloc[0])
                                      for st in STORES)}
```

What each design costs a writer and its readers on the two workloads, **measured** in the cloud on
S3 Express and Azure Blob: the main points of the [workloads page](./workloads.md), run on a VM
beside each store, with that page's forecast for them beside each measurement.

## What it shows

- **The trade-offs hold on both stores, measured.** On the updates, with nothing compacted, RMoR,
  RMoW-external and RMoW-append take {eval}`span(reg_write_mor[S3X], "{:.1f}")` times as long as MoR
  to write on S3 Express ({eval}`span(reg_write_mor[AZ], "{:.1f}")` on Azure Blob), and read a 1%
  range {eval}`span(win_mor[S3X], "{:.1f}")` times as fast
  ({eval}`span(win_mor[AZ], "{:.1f}")` on Azure Blob). On the inserts, sealing RMoR's tail reads a
  range {eval}`f1(seal_r[S3X]["window"])` times as fast as appending
  ({eval}`f1(seal_r[AZ]["window"])` on Azure Blob), for {eval}`f2(seal_r[S3X]["extra"])` s more a
  write ({eval}`f2(seal_r[AZ]["extra"])` s).
- **Compaction brings MoR's range read back in line, at the cost of storage.** Compacted every 10
  writes, MoR read a range after 30 writes in
  {eval}`f2(ct(TU, S3X, "every 10 writes", "range read after 30"))` s against
  {eval}`f2(ct(TU, S3X, "never", "range read after 30"))` never compacted on S3 Express
  ({eval}`f2(ct(TU, AZ, "every 10 writes", "range read after 30"))` and
  {eval}`f2(ct(TU, AZ, "never", "range read after 30"))` on Azure Blob), and wrote, its compactions
  spread over the writes, in {eval}`f2(ct(TU, S3X, "every 10 writes", "write with compactions"))` s
  against {eval}`f2(ct(TU, S3X, "never", "write"))`; while older snapshots are kept, its table
  stores {eval}`f2(ct(TU, S3X, "every 10 writes", "stored ÷ live"))` times its live bytes.
- **MoR's costs grow as its changes pile up; the region designs' grow far less.** From the first
  five writes to the last five, MoR's update write grew {eval}`f2(grow[(S3X, "MoR", "a write")])`
  times on S3 Express ({eval}`f2(grow[(AZ, "MoR", "a write")])` on Azure Blob), and from the 5th
  write to the 30th its range read grew {eval}`f2(grow[(S3X, "MoR", WIN)])` times
  ({eval}`f2(grow[(AZ, "MoR", WIN)])`). The region designs' writes grew
  {eval}`span([grow[(S3X, r, "a write")] for r in REG])` times
  ({eval}`span([grow[(AZ, r, "a write")] for r in REG])`) and their range reads
  {eval}`span([grow[(S3X, r, WIN)] for r in REG])` times
  ({eval}`span([grow[(AZ, r, WIN)] for r in REG])`). So the means over the run understate the
  trade late in it: after 30 writes the region designs read a range
  {eval}`span(late_win[S3X], "{:.1f}")` times as fast as MoR on S3 Express
  ({eval}`span(late_win[AZ], "{:.1f}")` on Azure Blob), and their last five writes took
  {eval}`span(late_write[S3X], "{:.1f}")` times as long as MoR's
  ({eval}`span(late_write[AZ], "{:.1f}")`).
- **The cloud ran slower than the forecast, and the model, priced from the cloud runs, closes most
  of the gap.** Each operation took {eval}`pspan(by(S3X, "measured / forecast") - 1)` longer than its forecast
  on S3 Express (writes {eval}`pspan(by(S3X, "measured / forecast", True) - 1)`, reads
  {eval}`pspan(by(S3X, "measured / forecast", False) - 1)`), and
  {eval}`pspan(by(AZ, "measured / forecast") - 1)` on Azure Blob (writes
  {eval}`pspan(by(AZ, "measured / forecast", True) - 1)`, reads
  {eval}`pspan(by(AZ, "measured / forecast", False) - 1)`). Priced from the cloud run itself
  (its requests at its VM's latency test, its twin's CPU on the VM), the time is already
  {eval}`pspan(by(S3X, "priced / forecast") - 1)` above the forecast on S3 Express and
  {eval}`pspan(by(AZ, "priced / forecast") - 1)` on Azure Blob: the VMs' CPU and the stores'
  latency that day, against the laptop's CPU and the registered latency tests. Priced and
  measured then differ by at most {eval}`pct((by(S3X, "priced / measured") - 1).abs().max(), 1)`
  on S3 Express and {eval}`pct((by(AZ, "priced / measured") - 1).abs().max(), 1)` on Azure Blob.
- **The region designs' writes are mostly CPU.** On S3 Express, CPU took
  {eval}`pspan(reg_cpu[S3X])` of their update writes' measured time (encoding alone
  {eval}`pspan(reg_enc[S3X])`), against {eval}`pct(share(S3X, "MoR", CPU))` of MoR's, which waits
  on the store for {eval}`pct(share(S3X, "MoR", ["the store"]))`. On Azure Blob, whose requests
  are slower, CPU's share falls to {eval}`pspan(reg_cpu[AZ])` of theirs and
  {eval}`pct(share(AZ, "MoR", CPU))` of MoR's, and their writes come closer to MoR's
  ({eval}`span(l2[AZ]["write"], "{:.1f}")` times, against {eval}`span(l2[S3X]["write"], "{:.1f}")`).
- **The cloud runs did exactly the work havoc's runs counted:** the same requests, bytes and table
  contents in every record, and the same memory peaks in every phase on one thread.

## Setup

**The workloads and designs** are the [workloads page](./workloads.md)'s, at 16 MiB row groups:
updates (150,000 changes a write) and inserts (350,000 new rows a write), 30 writes each. Writes
locate their rows by key, pruning row groups by each changed key, and the readers skip row groups
whose rows are all dead, the sorted read too.

**The runs.** Each configuration ran once on each store from a VM beside it (S3 Express from a
`c7a.4xlarge` in usw2-az3, one per configuration; Azure Blob from one `D8as_v6` in westus, the
configurations in turn), at {eval}`", ".join(sorted(runs.build.dropna().unique()))`. Each S3
Express VM also ran its configuration on its own disk (its twin), and each VM measured its store's
latency first.

**The times.** *Measured* is each phase's wall time in the cloud run. *Priced* is the
simulation's model applied to the cloud run: the twin's CPU on the S3 Express VM plus the cloud
run's requests priced by its VM's latency test. *Forecast* is the workloads page's simulation of
the reference runs (havoc's requests and bytes, the laptop's CPU at its pace, requests priced by the
latency tests of 2026-10-31). Each read's time is its mean over steps 5 to 30, and a write's over
the 30 writes, with compactions spread over them.

```{code-cell} ipython3
:tags: [remove-input]

table(runs, caption="The cloud runs, twins and latency tests")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(refs, caption="The reference runs the forecast simulates")
```

## Updates

### 1. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-updates
:tags: [remove-input]
:class: col-page-right

fig.wl_snapshot_chart(SNU_M, ["window_s", "full_s", "sorted_s"])
```

Each design's measured write latency at every write, and its read latency after every 5 writes, on
the updates with nothing compacted (section 3 compacts MoR). Section 7 sets each design's mean
beside its forecast.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNU_M, "a write", "measured")
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

### 2. Where the time goes

```{code-cell} ipython3
:label: workloads-cloud-updates-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(UP[UP.name.isin(UN_M)], ["a write", READ["window_s"], READ["full_s"]], UN_M, PARTS, PCOL)
```

Each design's measured time split by what its thread was doing: CPU by kind, the store (its
client's CPU and the wait for its requests), and paused (garbage collection, the JIT).

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(parts_table(UP[UP.name.isin(UN_M)]), caption="Measured time (s) by part")
```

### 3. Compaction

```{code-cell} ipython3
:label: workloads-cloud-updates-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMU, ["window_s", "full_s"], STU, "RMoW-append")
```

MoR's measured read latency after every 5 writes, never compacted and compacted every 10 writes
(the interval the cloud ran; a read at a compaction's write reads the compacted table), beside
RMoW-append's (dashed, never compacted); and what MoR's table stores while every snapshot is kept,
over its live bytes, the same in every run.

```{code-cell} ipython3
:tags: [remove-input]

table(TU, caption="MoR compacted and not, measured latency (s): a write on average without and with its "
      "compactions spread over the writes, a compaction on average, each read on average and after 30 "
      "writes; and what the table stores after 30 writes, every snapshot kept, over its live bytes")
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
- **Storage pays while older snapshots are kept:** after 30 writes the compacted table stores
  {eval}`f2(ct(TU, S3X, "every 10 writes", "stored ÷ live"))` times its live bytes, against
  {eval}`f2(ct(TU, S3X, "never", "stored ÷ live"))` never compacted; once they expire, both keep
  only their live bytes.

## Inserts

### 4. Latency as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-inserts
:tags: [remove-input]
:class: col-page-right

fig.wl_snapshot_chart(SNI_M, ["window_s", "sorted_s"])
```

Each policy's measured write latency at every write, and its read latency after every 5 writes, on
the inserts with nothing compacted: appending, and RMoR's and RMoW-append's tails sealed at 160 MiB.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNI_M, "a write", "measured")
```

```{code-cell} ipython3
:tags: [remove-input]

fig.wl_snapshot_table(SNI_M, READ["window_s"], "measured")
```

- **Sealing RMoR's tail reads a range {eval}`f1(seal_r[S3X]["window"])` times as fast as appending**
  on S3 Express ({eval}`f1(seal_r[AZ]["window"])` on Azure Blob), for
  {eval}`f2(seal_r[S3X]["extra"])` s more a write on average ({eval}`f2(seal_r[AZ]["extra"])` s);
  RMoW-append's seal reads {eval}`f2(seal_win[S3X])` and {eval}`f2(seal_win[AZ])` times as fast.

### 5. Where the time goes

```{code-cell} ipython3
:label: workloads-cloud-inserts-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(IP[IP.name.isin(IN_M)], ["a write", READ["window_s"], READ["sorted_s"]], IN_M, PARTS, PCOL)
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(parts_table(IP[IP.name.isin(IN_M)]), caption="Measured time (s) by part")
```

### 6. Compaction

```{code-cell} ipython3
:label: workloads-cloud-inserts-compaction
:tags: [remove-input]
:class: col-page-right

fig.wl_compaction_chart(CMI, ["window_s"], STI, "RMoR sealed")
```

MoR's measured range-read latency after every 5 writes on the inserts, appending (never compacted)
and compacted every 10 writes, beside RMoR's tail sealed at 160 MiB (dashed); and what MoR's table
stores while every snapshot is kept, over its live bytes.

```{code-cell} ipython3
:tags: [remove-input]

table(TI, caption="MoR compacted and not on the inserts, as in section 3")
```

- **Compacting merges late rows after the fact, as the seal does as they arrive:** after 30 writes
  MoR read a range in {eval}`f2(ct(TI, S3X, "every 10 writes", "range read after 30"))` s compacted
  every 10 writes and {eval}`f2(ct(TI, S3X, "never", "range read after 30"))` appending on S3
  Express ({eval}`f2(ct(TI, AZ, "every 10 writes", "range read after 30"))` and
  {eval}`f2(ct(TI, AZ, "never", "range read after 30"))` on Azure Blob), for
  {eval}`f2(ct(TI, S3X, "every 10 writes", "write with compactions") - ct(TI, S3X, "never", "write"))` s
  more a write with its compactions spread over the writes
  ({eval}`f2(ct(TI, AZ, "every 10 writes", "write with compactions") - ct(TI, AZ, "never", "write"))` s),
  and {eval}`f2(ct(TI, S3X, "every 10 writes", "stored ÷ live"))` times its live bytes stored after
  30 writes while older snapshots are kept.

## 7. Forecast against measured

```{code-cell} ipython3
:label: workloads-cloud-forecast
:tags: [remove-input]
:class: col-page-right

alt.Chart(A).mark_point(filled=True, size=60).encode(
    x=alt.X("measured / forecast:Q", scale=alt.Scale(type="log", domain=[0.5, 2]), title="measured over forecast"),
    y=alt.Y("name:N", title=None, sort=UN + [n for n in IN if n not in UN]),
    color=alt.Color("what:N", title=None, legend=alt.Legend(orient="bottom")),
    tooltip=["Store", "workload", "name", "what", alt.Tooltip("forecast:Q", format=".2f"),
             alt.Tooltip("priced:Q", format=".2f"), alt.Tooltip("measured:Q", format=".2f")],
).properties(width=260, height=150).facet(
    row=alt.Row("workload:N", title=None, sort=["updates", "inserts"]),
    column=alt.Column("Store:N", title=None, sort=STORES)).resolve_scale(y="independent")
```

Each design's measured latency over its forecast, per operation: 1 is a perfect forecast, right of
it the cloud was slower.

```{code-cell} ipython3
:tags: [remove-input]

table(A[["workload", "Store", "name", "what", "forecast", "priced", "measured", "measured / forecast"]].round(3),
      caption="Latency (s): forecast (the reference runs simulated), priced (the cloud run's requests and "
      "its twin's CPU) and measured")
```

## The checks registered before the run

The run was to judge four hypotheses, registered in docs/HYPOTHESES.md ("The two workloads in the
cloud", 2026-11-10):

- **L1, the cloud runs do the work havoc's runs counted: {eval}`V["L1"]`.** Each of the
  {eval}`len(l1)` runs ({eval}`n_s3x` on S3 Express, {eval}`n_az` on Azure Blob and
  {eval}`n_twin` twins on the S3 Express VMs' disks) has every record of havoc's run of its
  configuration, and
  {eval}`"none differs" if (l1.differ == 0).all() else f"{int(l1.differ.sum())} differ"` in
  requests, bytes or table state; the memory peaks of the one-thread phases
  {eval}`"match too" if (l1.held_differ == 0).all() else f"differ in {int(l1.held_differ.sum())} records"`.
- **L2, on the updates the region designs trade a slower write for a faster range read:
  {eval}`V["L2"]`.** On each store, RMoR, RMoW-external and RMoW-append each take longer than MoR
  and MoR compacted to write ({eval}`span(l2[S3X]["write"], "{:.1f}")` times on S3 Express,
  {eval}`span(l2[AZ]["write"], "{:.1f}")` on Azure Blob) and read a 1% range faster than both
  ({eval}`span(l2[S3X]["window"], "{:.1f}")` and {eval}`span(l2[AZ]["window"], "{:.1f}")` times).
  MoR compacted reads a range {eval}`f"{l2[S3X]['compacted']:.1f}"` and
  {eval}`f"{l2[AZ]['compacted']:.1f}"` times as fast as MoR. The deferred merges read a range
  {eval}`f"{l2[S3X]['deferred_window']:.1f}"` and {eval}`f"{l2[AZ]['deferred_window']:.1f}"` times
  as fast as MoR, and write {eval}`span(l2[S3X]["deferred_write"], "{:.1f}")` and
  {eval}`span(l2[AZ]["deferred_write"], "{:.1f}")` times as fast as the region designs.
  {eval}`"" if l2_held else "Not so on " + "; ".join(f"{st}: {L2_PARTS[k]}" for st, k in l2_fail) + "."`
- **L3, on the inserts merging late rows trades a little write time for a faster range read:
  {eval}`V["L3"]`.** MoR compacted and RMoR sealed read a 1% range
  {eval}`span(l3[S3X]["window"], "{:.1f}")` times as fast as appending on S3 Express and
  {eval}`span(l3[AZ]["window"], "{:.1f}")` on Azure Blob, and take
  {eval}`span(l3[S3X]["write"], "{:.1f}")` and {eval}`span(l3[AZ]["write"], "{:.1f}")` times as
  long to write: {eval}`span(l3[S3X]["extra"])` s and {eval}`span(l3[AZ]["extra"])` s more a
  write, under the 1 s registered.
  {eval}`"" if l3_held else "Not so on " + "; ".join(f"{st}: {k}" for st, k in l3_fail) + "."`
- **L4, on S3 Express each design's write and reads are priced within 15%: {eval}`V["L4"]`,**
  within {eval}`pct(l4_err.max(), 1)` (the furthest, {eval}`l4_worst["name"]`'s
  {eval}`COL_WHAT[l4_worst.col].removeprefix("a ")`, priced at
  {eval}`f2(l4_worst.priced_over_measured)` of its measured time).
  {eval}`"" if l4_held else "Not so: " + ", ".join(f"{r['name']} {COL_WHAT[r.col]}" for _, r in l4_out.iterrows()) + "."`

**Measured, not judged:**
- **The full scans and sorted reads on the updates:** the region designs' take
  {eval}`span(near_full[S3X])` of MoR's and MoR compacted's time on S3 Express
  ({eval}`span(near_full[AZ])` on Azure Blob). On the inserts, the four designs' full scans are
  within {eval}`pct(ins_spread[S3X][0])` of each other and their sorted reads within
  {eval}`pct(ins_spread[S3X][1])` ({eval}`pct(ins_spread[AZ][0])` and
  {eval}`pct(ins_spread[AZ][1])` on Azure Blob).
- **MoR compacted's write against MoR's:** {eval}`f2(morc_write[S3X])` and
  {eval}`f2(morc_write[AZ])` times (forecast 0.97 and 0.88).
- **The deferred merges against MoR:** their write {eval}`span(def_write[S3X])` and
  {eval}`span(def_write[AZ])` times MoR's and MoR compacted's; MoR compacted's range read
  {eval}`f2(def_win[S3X])` and {eval}`f2(def_win[AZ])` times theirs.
- **RMoW-append sealed** reads a range {eval}`f2(seal_win[S3X])` and {eval}`f2(seal_win[AZ])`
  times as fast as appending.
- **Azure Blob's priced time,** with the S3 Express twins' CPU: {eval}`span(by(AZ, "priced / measured"))`
  of the measured.

```{code-cell} ipython3
:tags: [remove-input]

table(l1, caption="L1: each cloud run and twin against havoc's run of its configuration: records, "
      "records differing in requests, bytes or table state, records on one thread, and those whose "
      "held peaks differ")
```

## Limits

- One run of each configuration on each store, and one latency test per VM: the whiskers are the
  spread over a run's writes and reads, not over repeated runs.
- The S3 Express runs share one bucket, four at once, each measured under the others' load. The
  Azure Blob runs ran one after another over {eval}`f"{az_hours:.1f}"` hours.
- The region designs' and MoR compacted's range reads take a fraction of a second, so a few
  requests' first bytes decide them.
- MoR's write and range read grow over the run, so its means depend on the run's length: 30
  writes.
- Azure Blob has no twin of its own: its priced time takes the S3 Express VM's CPU.
