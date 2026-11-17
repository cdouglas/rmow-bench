---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The simulation against the cloud
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

# The main points of the workloads pages: (configuration, arm, family, curve, knob, name, order)
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
UN, IN = [s[5] for s in UPD], [s[5] for s in ING]
STORES = fig.WL_STORES
S3X, AZ = STORES
READ, WRITE = fig.WL_READ, fig.WL_WRITE
RK = ["window_s", "full_s", "sorted_s"]
POLICIES = [("upd" if "-upd-" in s[0] else "ing", s[0], s[1]) for s in UPD + ING]

# Measured: each phase's wall time in the cloud, per step; and priced, its twin's work on the S3
# Express VM plus its requests priced by the VM's own latency test (tune.cloud_steps)
M = pd.concat([tune.cloud_steps(con, st, POLICIES, f"list_contains(campaigns, '{CLOUD}')")
               for st in ("s3x", "azure")], ignore_index=True)
M["Store"] = M.store.map(fig.WL_STORE)
W = fig.wl_whiskers(M.assign(s=M.measured_ms / 1e3))


def means(d, col):
    """Each configuration's and arm's mean write and reads in `col` (ms), as tune.policies()."""
    rows = [dict(store=st, Store=fig.WL_STORE[st], label=l, arm=a, **tune.per_write(g, col))
            for (st, l, a), g in d.groupby(["store", "label", "arm"])]
    return pd.DataFrame(rows)


PM, PP = means(M, "measured_ms"), means(M, "priced_ms")

# The forecast: the reference runs simulated (requests priced by the registered latency tests, the
# laptop's CPU at its pace), as on the workloads page
train = tune.records(con, tune.TRAIN.replace("campaigns", "r.campaigns").replace("host", "r.host"))
formula = tune.Formula(train)
ns = tune.pace(con)
GRID = f"list_contains(campaigns, '{REF}')"
F = pd.concat([tune.policies(con, wl, formula, ns, True, tune.steps(con, wl, formula, ns, True, GRID))
               for wl in ("wl_upd", "wl_ing")])
F["Store"] = F.store.map(fig.WL_STORE)


def frame(spec):
    """The measured means, and each one's forecast (pred_*) and priced time (priced_*)."""
    d = fig.wl_frame(spec, PM, W)
    k = ["Store", "label", "arm"]
    for name, src in (("pred", F), ("priced", PP)):
        f = fig.wl_frame(spec, src, W[k])[k + ["write_s", *RK]]
        d = d.merge(f.rename(columns={c: f"{name}_{c}" for c in ["write_s", *RK]}), on=k, how="left")
    return d


U, I = frame(UPD), frame(ING)


def against(d, reads):
    """Per store, design and operation: the forecast, priced and measured latency (s), and the
    measured over the forecast."""
    rows = []
    for _, r in d.iterrows():
        for what, col in ((WRITE, "write_s"), *[(READ[k], k) for k in reads]):
            rows.append(dict(Store=r.Store, name=r["name"], what=what, forecast=r[f"pred_{col}"],
                             priced=r[f"priced_{col}"], measured=r[col]))
    t = pd.DataFrame(rows)
    t["measured / forecast"] = t.measured / t.forecast
    return t


A = pd.concat([against(U, RK).assign(workload="updates"), against(I, ["window_s", "sorted_s"]).assign(workload="inserts")])
A["priced / forecast"] = A.priced / A.forecast
A["priced / measured"] = A.priced / A.measured


def by(st, col, writes=None):
    g = A[A.Store == st]
    if writes is not None:
        g = g[(g.what == WRITE) == writes]
    return g[col]


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


def pct(x, nd=0):
    return f"{100 * x:.{nd}f}%"


def pspan(xs):
    return span([100 * x for x in xs], "{:.0f}") + "%"


runs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build, round(elapsed_s / 60) AS minutes
            FROM runs WHERE list_contains(campaigns, $c) ORDER BY label, run_id""", c=CLOUD)
refs = q("""SELECT run_id, label, host, left(git_sha, 7) AS build FROM canonical_runs
            WHERE list_contains(campaigns, $c) ORDER BY label, host""", c=REF)
```

```{code-cell} ipython3
:tags: [remove-cell]

# The checks registered before the run (docs/HYPOTHESES.md, "The two workloads in the cloud",
# 2026-11-10) that are about the measurement: L1 and L4. L2 and L3, on the trade-offs, were on the
# measured page (workloads-cloud.md, retired 2026-11-16; docs/EXPERIMENTS.md has their verdicts).

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
n_s3x, n_az = int((l1.run == S3X).sum()), int((l1.run == AZ).sum())
n_twin = len(l1) - n_s3x - n_az

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
l4_worst = l4.loc[l4_err.idxmax()]
COL_WHAT = {"write_s": WRITE, **READ}

V = {"L1": "held" if l1_held else "contradicted", "L4": "held" if l4_held else "contradicted"}
```

How the simulation of the two workloads at 16 MiB row groups compares with their main points
measured in the cloud on 2026-11-11, and whether the cloud runs did the work the simulated runs
counted. The results at 128 MiB ([workloads-128.md](./workloads-128.md)) are simulated the same way;
until a cloud run at 128 MiB, this is the simulation's only measured check. The 16 MiB results pages
were retired on 2026-11-16 (docs/LESSONS.md, The 16 MiB workloads).

## What it shows

- **The cloud ran slower than the forecast, and the model, priced from the cloud runs, closes most
  of the gap.** Each operation took {eval}`pspan(by(S3X, "measured / forecast") - 1)` longer than
  its forecast on S3 Express (writes {eval}`pspan(by(S3X, "measured / forecast", True) - 1)`, reads
  {eval}`pspan(by(S3X, "measured / forecast", False) - 1)`), and
  {eval}`pspan(by(AZ, "measured / forecast") - 1)` on Azure Blob (writes
  {eval}`pspan(by(AZ, "measured / forecast", True) - 1)`, reads
  {eval}`pspan(by(AZ, "measured / forecast", False) - 1)`). Priced from the cloud run itself (its
  requests at its VM's latency test, its twin's CPU on the VM), the time is already
  {eval}`pspan(by(S3X, "priced / forecast") - 1)` above the forecast on S3 Express and
  {eval}`pspan(by(AZ, "priced / forecast") - 1)` on Azure Blob: the VMs' CPU and the stores'
  latency that day, against the laptop's CPU and the registered latency tests. Priced and measured
  then differ by at most {eval}`pct((by(S3X, "priced / measured") - 1).abs().max(), 1)` on S3
  Express and {eval}`pct((by(AZ, "priced / measured") - 1).abs().max(), 1)` on Azure Blob.
- **The cloud runs did exactly the work havoc's runs counted:** the same requests, bytes and table
  contents in every record, and the same memory peaks in every phase on one thread.

## Setup

**The runs.** Five configurations of the two workloads at 16 MiB row groups (on the updates every
design, and MoR compacted every 10 writes; on the inserts MoR, MoR compacted, and RMoR and
RMoW-append merging their four newest writes), each once on each store
from a VM beside it, at {eval}`", ".join(sorted(runs.build.dropna().unique()))`. Each S3 Express VM
also ran its configuration on its own disk (its twin), and each VM measured its store's latency
first.

**Three times for each operation:**
- *measured*, each phase's wall time in the cloud run;
- *priced*, the simulation's model applied to the cloud run: the twin's CPU on the S3 Express VM
  plus the cloud run's requests priced by its VM's latency test;
- *forecast*, the simulation of the reference runs, as the results pages simulate: havoc's requests and bytes,
  the laptop's CPU at its pace, requests priced by the latency tests of 2026-10-31.

Each read's time is its mean over steps 5 to 30, and a write's over the 30 writes, with
compactions spread over them. An earlier check, the tuner's picks in the cloud (2026-11-09), is
recorded in docs/LESSONS.md.

```{code-cell} ipython3
:tags: [remove-input]

table(runs, caption="The cloud runs, twins and latency tests")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

table(refs, caption="The reference runs the forecast simulates")
```

## Forecast against measured

```{code-cell} ipython3
:label: simulation-cloud-forecast
:tags: [remove-input]
:class: col-page-right

alt.Chart(A).mark_point(filled=True, size=60).encode(
    x=alt.X("measured / forecast:Q", scale=alt.Scale(type="log", domain=[0.5, 2]), title="measured over forecast"),
    y=alt.Y("name:N", title=None, sort=UN + [n for n in IN if n not in UN]),
    color=alt.Color("what:N", title=None, sort=[WRITE, *READ.values()], legend=alt.Legend(orient="bottom")),
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

Of the four hypotheses registered for the run in docs/HYPOTHESES.md ("The two workloads in the
cloud", 2026-11-10), two are about the measurement; L2 and L3, on the trade-offs, held (docs/EXPERIMENTS.md;
their page was retired with the 16 MiB workloads).

- **L1, the cloud runs do the work havoc's runs counted: {eval}`V["L1"]`.** Each of the
  {eval}`len(l1)` runs ({eval}`n_s3x` on S3 Express, {eval}`n_az` on Azure Blob and
  {eval}`n_twin` twins on the S3 Express VMs' disks) has every record of havoc's run of its
  configuration, and
  {eval}`"none differs" if (l1.differ == 0).all() else f"{int(l1.differ.sum())} differ"` in
  requests, bytes or table state; the memory peaks of the one-thread phases
  {eval}`"match too" if (l1.held_differ == 0).all() else f"differ in {int(l1.held_differ.sum())} records"`.
- **L4, on S3 Express each design's write and reads are priced within 15%: {eval}`V["L4"]`,**
  within {eval}`pct(l4_err.max(), 1)` (the furthest, {eval}`l4_worst["name"]`'s
  {eval}`COL_WHAT[l4_worst.col]`, priced at {eval}`f"{l4_worst.priced_over_measured:.2f}"` of its
  measured time).
  {eval}`"" if l4_held else "Not so: " + ", ".join(f"{r['name']} {COL_WHAT[r.col]}" for _, r in l4_out.iterrows()) + "."`

**Measured, not judged:** Azure Blob's priced time, with the S3 Express twins' CPU, is
{eval}`span(by(AZ, "priced / measured"))` of the measured.

```{code-cell} ipython3
:tags: [remove-input]

table(l1, caption="L1: each cloud run and twin against havoc's run of its configuration: records, "
      "records differing in requests, bytes or table state, records on one thread, and those whose "
      "held peaks differ")
```

## Limits

- One run of each configuration on each store, and one latency test per VM.
- Azure Blob has no twin of its own: its priced time takes the S3 Express VM's CPU.
- The forecast prices CPU at the laptop's speed, faster than the VMs'.
