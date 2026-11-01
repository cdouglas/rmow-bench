---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The model on the cloud sweeps
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

STORES = fig.CS_STORES
S3X, AZ = STORES
RUNS = fig.CS_RUNS
TWINS, HAVOC, PRED = fig.CS_TWINS, fig.CS_HAVOC, fig.CS_PRED
REGION = ["rmow-c", "rmor-c", "rmor-c-footer"]
labels = q("SELECT arm, arm_label, ord FROM arm_meta")
HELD = """count(*) FILTER (WHERE (x.held_bytes, x.held_rows, x.held_chunks, x.held_writer,
                                  x.held_pending, x.held_deletes)
                           IS DISTINCT FROM (y.held_bytes, y.held_rows, y.held_chunks,
                                  y.held_writer, y.held_pending, y.held_deletes))"""


def same_work(a, b):
    """records whose requests, bytes or table state differ between runs a and b, and one-thread
    held peaks that differ"""
    d = q("""SELECT count(*) AS records, count(*) FILTER (WHERE len(differs) > 0) AS differing
             FROM steps_diff($a, $b)""", a=a, b=b).iloc[0]
    h = q(f"""SELECT count(*) AS held_records, {HELD} AS held_differing
              FROM steps x JOIN steps y ON x.arm = y.arm AND x.step = y.step AND x.phase = y.phase
                                       AND x.reader IS NOT DISTINCT FROM y.reader
              WHERE x.run_id = $a AND y.run_id = $b AND coalesce(x.reader, '') <> 'cached-p8'""",
          a=a, b=b).iloc[0]
    return {**d.to_dict(), **h.to_dict()}


# The twins against havoc's runs (counts only: havoc's held peaks are its own build's), then each
# cloud run against its twin (S1)
twins = pd.DataFrame([{"config": c, **same_work(HAVOC[c], t)} for c, t in TWINS.items()
                      if c != "sweep-base"])
s1 = pd.DataFrame([{"store": st, "config": c, "run": r, **same_work(TWINS[c], r)}
                   for st in STORES for c, (r, _) in RUNS[st].items()])

# The model: each one-thread mean phase, its thread's CPU plus its requests priced with its VM's
# spike, over its measured wall time
model = fig.cs_frame(fig.bp_model, RUNS)
model["within"] = (model.sim_over_measured - 1).abs() <= 0.25
within = model.groupby("store").within.mean()

# The ratios, measured and predicted
times = fig.cs_frame(fig.bp_times, RUNS)
over = fig.cs_over_mor(times)
O = over.set_index(["store", "config", "arm", "what"]).over_mor_v3.sort_index()
w = over[over.arm.isin(REGION) & (over.what == "a write")].copy()
w["predicted"] = [PRED[st][c][a] for st, c, a in zip(w.store, w.config, w.arm)]
rr = over[(over.arm == "rmow-c") & (over.what == "a full read")].copy()
rr["predicted"] = [PRED[st][c]["rmow-c read"] for st, c in zip(rr.store, rr.config)]
ratios = pd.concat([w, rr], ignore_index=True)


def o(st, c, a, what="a write"):
    return O[(st, c, a, what)]


def held(ok):
    return "held" if ok else "contradicted"


# U1: each region design's write over MoR-v3's lower at p = 0.3 than at 0.03, on both stores
u1 = {st: {a: (o(st, "sweep-base", a), o(st, "sweep-p30", a)) for a in REGION} for st in STORES}
u1_ok = all(lo > hi for st in STORES for lo, hi in u1[st].values())
# U2: RMoW-C's write the fastest region rewrite at p = 0.03, 0.1, 0.3; and (revised) its write over
# RMoR-C's higher at 0.3 than at 0.03
P = ["sweep-base", "sweep-p10", "sweep-p30"]
u2a_ok = all(o(st, c, "rmow-c") < min(o(st, c, "rmor-c"), o(st, c, "rmor-c-footer"))
             for st in STORES for c in P)
u2b = {st: (o(st, "sweep-base", "rmow-c") / o(st, "sweep-base", "rmor-c"),
            o(st, "sweep-p30", "rmow-c") / o(st, "sweep-p30", "rmor-c")) for st in STORES}
u2b_ok = all(hi > lo for lo, hi in u2b.values())
# G1: each region design's write over MoR-v3's rises from 16 to 32 to 64 MiB row groups
G = ["sweep-base", "sweep-rg32m", "sweep-rg64m"]
g1 = {st: {a: [o(st, c, a) for c in G] for a in REGION} for st in STORES}
g1_ok = all(v[0] < v[1] < v[2] for st in STORES for v in g1[st].values())

# The spikes: each run's VM's mean latency at a few sizes, against the base point's spike on S3
# Express (a VM alone on the bucket)
spikes = sorted({sp for st in STORES for _, sp in RUNS[st].values()} | {fig.BP_SPIKE[fig.BP_NEW]})
spk = q("""SELECT profile AS spike, op, bytes // 1048576 AS mib, ms FROM sized_points
           WHERE list_contains($spikes, profile) AND op IN ('get', 'put', 'append')
             AND bytes IN (1048576, 16777216, 134217728)""", spikes=spikes)
```

How well the latency model priced each cloud run of the sweeps, whether each run did exactly the
work its twin counts, and the verdicts of the hypotheses registered for them (docs/HYPOTHESES.md,
The sweeps in the cloud: S1, U1, U2 revised, G1). The results are in
[The sweeps on S3 Express and Azure](./cloud-sweep.md).

## Setup

Each configuration ran first on its S3 Express VM with a local store (its twin: the same build and
cores, counting instructions), then on the store, after its VM's spike. On Azure one VM ran them in
turn, each after a spike of its own (`figures.py`, `CS_*`):

```{code-cell} ipython3
:tags: [remove-input]

rows = [{"run": c, "twin": TWINS[c], "havoc": HAVOC.get(c),
         **{f"{st}": RUNS[st].get(c, (None,))[0] for st in STORES},
         **{f"{st}'s spike": RUNS[st].get(c, (None, None))[1] for st in STORES}}
        for c in fig.CS_CONFIGS]
table(pd.DataFrame(rows))
```

## The same work everywhere

Requests, bytes and table state follow from the op log, the configuration and the code. Each twin
against havoc's run of its configuration:

```{code-cell} ipython3
:tags: [remove-input]

table(twins[["config", "records", "differing"]])
```

And each cloud run against its twin, held peaks of one-thread phases included (S1):

```{code-cell} ipython3
:tags: [remove-input]

table(s1.drop(columns="run"))
```

## The model

Each mean write and full read (under `cached`, RMoR-C's also skipping dead row groups), simulated as
its thread's CPU plus its requests priced at their sizes with its VM's spike, over its measured wall
time. The shaded band is 25%.

```{code-cell} ipython3
:label: cloud-sweep-model
:tags: [remove-input]

color, shape = fig.arm_encoding(list(model.arm.unique()), title="design")
x_scale = alt.Scale(domain=[0.5, 1.5], zero=False, clamp=True)
band = alt.Chart(pd.DataFrame({"lo": [0.75], "hi": [1.25]})).mark_rect(opacity=0.12).encode(
    x=alt.X("lo:Q", scale=x_scale), x2="hi:Q")


def panel(what):
    d = model[model.what == what]
    dots = alt.Chart().mark_point(filled=True, size=60).encode(
        x=alt.X("sim_over_measured:Q", title="simulated over measured", scale=x_scale),
        y=alt.Y("config:N", title=None, sort=fig.CS_CONFIGS),
        color=color, shape=shape,
        tooltip=["store", "config", "arm_label", alt.Tooltip("measured_s:Q", format=".2f"),
                 alt.Tooltip("simulated_s:Q", format=".2f")])
    return (alt.layer(band, dots, data=d).properties(width=360, height=260)
            .facet(column=alt.Column("store:N", title=None, sort=STORES))
            .properties(title=what))


alt.vconcat(*[panel(x) for x in fig.BP_WHATS]).configure_legend(orient="bottom")
```

Within 25%: {eval}`f"{100 * within[S3X]:.0f}"`% of the means on S3 Express and
{eval}`f"{100 * within[AZ]:.0f}"`% on Azure.

```{code-cell} ipython3
:tags: [remove-input]

m = model[~model.within].copy()
m["config"] = pd.Categorical(m.config, fig.CS_CONFIGS)
table(fig.bp_sorted(m, "store", "config", "ord")[
    ["store", "config", "what", "arm_label", "measured_s", "cpu_s", "request_s", "simulated_s",
     "sim_over_measured"]].round(2))
```

## Predicted and measured

Each region design's mean write over MoR-v3's, and RMoW-C's full read, measured (dots) and as
registered from the twins (ticks).

```{code-cell} ipython3
:label: cloud-sweep-ratios
:tags: [remove-input]

ratios["kind"] = [f"{l}: {'a write' if wt == 'a write' else 'a full read'}"
                  for l, wt in zip(ratios.arm_label, ratios.what)]
kinds = list(ratios.sort_values(["what", "ord"], ascending=[False, True]).kind.unique())
log_x = alt.Scale(type="log", domain=[0.6, 20], nice=False)
base = alt.Chart().encode(
    y=alt.Y("config:N", title=None, sort=fig.CS_CONFIGS),
    color=alt.Color("kind:N", title=None, sort=kinds),
    yOffset=alt.YOffset("kind:N", sort=kinds))
dots = base.mark_point(filled=True, size=50).encode(
    x=alt.X("over_mor_v3:Q", title="over MoR-v3's (dot: measured; tick: predicted)", scale=log_x),
    tooltip=["store", "config", "kind", alt.Tooltip("over_mor_v3:Q", format=".2f"),
             alt.Tooltip("predicted:Q", format=".2f")])
ticks = base.mark_tick(thickness=2, size=8).encode(x="predicted:Q")
one = alt.Chart(pd.DataFrame({"x": [1.0]})).mark_rule(strokeDash=[4, 3], color="gray").encode(
    x=alt.X("x:Q", scale=log_x))
(alt.layer(one, dots, ticks, data=ratios).properties(width=380, height=420)
 .facet(column=alt.Column("store:N", title=None, sort=STORES))
 .configure_legend(orient="bottom"))
```

```{code-cell} ipython3
:tags: [remove-input]

t = ratios.copy()
t["config"] = pd.Categorical(t.config, fig.CS_CONFIGS)
table(t.sort_values(["store", "config", "what", "ord"])[
    ["store", "config", "kind", "over_mor_v3", "predicted"]]
      .rename(columns={"kind": "what", "over_mor_v3": "measured"}).round(2))
```

## The spikes

Each VM's spike, the mean latency of a request at three sizes, in ms. On S3 Express the fleet's
spikes ran while the other VMs' runs loaded the bucket; the base point's VM ran alone.

```{code-cell} ipython3
:tags: [remove-input]

table(spk.pivot_table(index="spike", columns=["op", "mib"], values="ms").round(0).reset_index())
```

## Verdicts

- **S1, each cloud run sends what its twin counts:**
  {eval}`held((s1.differing == 0).all() and (s1.held_differing == 0).all())`, in
  {eval}`f"{len(s1)}"` runs.
- **U1, a region rewrite pays more as p rises:** {eval}`held(u1_ok)`. From p = 0.03 to 0.3,
  RMoW-C's write over MoR-v3's went from {eval}`f"{u1[S3X]['rmow-c'][0]:.2f}"` to
  {eval}`f"{u1[S3X]['rmow-c'][1]:.2f}"` on S3 Express and from {eval}`f"{u1[AZ]['rmow-c'][0]:.2f}"`
  to {eval}`f"{u1[AZ]['rmow-c'][1]:.2f}"` on Azure; RMoR-C's from
  {eval}`f"{u1[S3X]['rmor-c'][0]:.2f}"` to {eval}`f"{u1[S3X]['rmor-c'][1]:.2f}"` and
  {eval}`f"{u1[AZ]['rmor-c'][0]:.2f}"` to {eval}`f"{u1[AZ]['rmor-c'][1]:.2f}"`.
- **U2, revised: RMoW-C's write the fastest region rewrite, its lead narrowing as p rises:** the
  first half {eval}`held(u2a_ok)`; the second {eval}`held(u2b_ok)`, RMoW-C's write over RMoR-C's
  going from {eval}`f"{u2b[S3X][0]:.2f}"` to {eval}`f"{u2b[S3X][1]:.2f}"` on S3 Express and from
  {eval}`f"{u2b[AZ][0]:.2f}"` to {eval}`f"{u2b[AZ][1]:.2f}"` on Azure. U2's second half as
  registered on 2026-10-29 (no higher at 0.3 than at 0.03) is therefore
  {eval}`"contradicted" if u2b_ok else "held"`.
- **G1, bigger row groups cost the region designs' writes:** {eval}`held(g1_ok)`. RMoW-C's write over
  MoR-v3's at 16, 32 and 64 MiB: {eval}`", ".join(f"{v:.2f}" for v in g1[S3X]["rmow-c"])` on S3
  Express and {eval}`", ".join(f"{v:.2f}" for v in g1[AZ]["rmow-c"])` on Azure.

## Limits

- One run of each configuration. The base point's runs on both stores are the base point's
  ([The model on the cloud base point](./base-point-model.md)).
- The S3 Express runs loaded one bucket at once, and their spikes measured it so loaded; the Azure
  runs came one after another over about a day.
- The model knows neither S3's multipart PUTs overlapping the write nor Azure's block blobs, which
  read slower than blobs written in one request (the base point's page).
- The predictions priced CPU at the S3 Express VMs' pace; the Azure VM, which counts no
  instructions, spent less thread CPU for the same work, most of all in reads.
