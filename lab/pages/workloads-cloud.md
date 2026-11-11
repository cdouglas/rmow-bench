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


SNU = fig.wl_snapshots(UPD, S, ["window_s", "full_s"])
SNI = fig.wl_snapshots(ING, S, ["window_s", "sorted_s"])

runs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build, round(elapsed_s / 60) AS minutes
            FROM runs WHERE list_contains(campaigns, $c) ORDER BY label, run_id""", c=CLOUD)
refs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
            WHERE list_contains(campaigns, $c) ORDER BY label, host""", c=REF)
```

What each design costs a writer and its readers on the two workloads, **measured** in the cloud on
S3 Express and Azure Blob: the main points of the [workloads page](./workloads.md), run on a VM
beside each store, with that page's forecast for them beside each measurement.

% TODO once the hypotheses for this run are registered (docs/HYPOTHESES.md): "What it shows", each
% claim computed, and the verdicts from the table in "Forecast against measured".

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

## 1. Updates: writing against reading

```{code-cell} ipython3
:label: workloads-cloud-updates
:tags: [remove-input]
:class: col-page-right

fig.wl_tradeoff(U, ["window_s", "full_s", "sorted_s"], {
    "MoR": ("left", -10), "MoR compacted": ("right", 0), "deferred merges": ("right", 14),
    "RMoW-append": ("right", -10), "RMoW-external": ("right", 14), "RMoR": ("left", -10)}, width=240)
```

Each design's measured write latency (with its compactions spread over the writes) against its
measured read latency. The filled marks are the measurements, with whiskers: across, from the 10th
to the 90th percentile write; up, from the fastest to the slowest read after 5 to 30 writes. The
hollow marks, joined to them by dashed lines, are the forecast.

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(U), caption="Measured latency (s): the mean write (with compactions spread over the writes), "
      "its 10th and 90th percentiles (`_lo`, `_hi`), each read's mean, fastest and slowest; `pred_` the forecast")
```

### Updates: latency as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-updates-snapshots
:tags: [remove-input]
:class: col-page-right

fig.wl_snapshot_chart(SNU, ["window_s", "full_s"])
```

Each design's measured write latency at every write, its compactions (crosses), and its read
latency after every 5 writes.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNU, "a write", "measured")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNU, READ["window_s"], "measured")
```

### Updates: where the time goes

```{code-cell} ipython3
:label: workloads-cloud-updates-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(UP, ["a write", READ["window_s"], READ["full_s"]], UN, PARTS, PCOL)
```

Each design's measured time split by what its thread was doing: CPU by kind, the store (its
client's CPU and the wait for its requests), and paused (garbage collection, the JIT); a write's
compactions spread over the writes.

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(parts_table(UP), caption="Measured time (s) by part")
```

## 2. Inserts: writing against reading

```{code-cell} ipython3
:label: workloads-cloud-inserts
:tags: [remove-input]
:class: col-page-right

fig.wl_tradeoff(I, ["window_s", "sorted_s"], {
    "appending": ("left", -10), "MoR compacted": ("right", -10),
    "RMoR sealed": ("left", 14), "RMoW-append sealed": ("right", 14)})
```

As for the updates: measured, with whiskers, and the forecast hollow.

```{code-cell} ipython3
:tags: [remove-input]

table(fig.wl_twin(I, ("window_s", "sorted_s")), caption="Measured latency (s), as in the updates' table")
```

### Inserts: latency as snapshots are added

```{code-cell} ipython3
:label: workloads-cloud-inserts-snapshots
:tags: [remove-input]
:class: col-page-right

fig.wl_snapshot_chart(SNI, ["window_s", "sorted_s"])
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

fig.wl_snapshot_table(SNI, "a write", "measured")
```

### Inserts: where the time goes

```{code-cell} ipython3
:label: workloads-cloud-inserts-parts
:tags: [remove-input]
:class: col-page-right

fig.wl_parts_chart(IP, ["a write", READ["window_s"], READ["sorted_s"]], IN, PARTS, PCOL)
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(parts_table(IP), caption="Measured time (s) by part")
```

## 3. Forecast against measured

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

## Limits

- One run of each configuration on each store, and one latency test per VM: the whiskers are the
  spread over a run's writes and reads, not over repeated runs.
- Azure has no twin of its own: its priced time takes the S3 Express VM's CPU.
