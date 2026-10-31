---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The model on the cloud base point
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()
OLD, NEW, BUILDS = fig.BP_OLD, fig.BP_NEW, fig.BP_BUILDS
LAPTOP, CLOUD, SPIKE = fig.BP_LAPTOP, fig.BP_CLOUD, fig.BP_SPIKE

# docs/HYPOTHESES.md, registered before each cloud run and predicted from its laptop twin: each
# arm's mean write and mean full read over MoR-v3's (C7 on the row reader, V6 on the vectorized
# one), RMoR-C's full read also skipping dead row groups
PRED = {OLD: {"tailcow-c": (1.02, 1.00), "rmor-c": (6.59, 3.85),
              "rmor-c-footer": (6.23, 0.87), "rmow-c": (2.98, 0.85)},
        NEW: {"tailcow-c": (1.00, 1.00), "rmor-c": (9.43, 3.83),
              "rmor-c-footer": (8.75, 0.87), "rmow-c": (4.48, 0.87)}}
PRED_SKIP = {OLD: 0.96, NEW: 1.05}

# The control: requests, bytes and table state, and one-thread held peaks, of each pair of runs
PAIRS = [("havoc against the laptop", OLD, LAPTOP[OLD], fig.BP_HAVOC),
         ("S3 Express against the laptop", OLD, LAPTOP[OLD], CLOUD[OLD]),
         ("S3 Express against the laptop", NEW, LAPTOP[NEW], CLOUD[NEW])]
rows = []
for name, build, a, b in PAIRS:
    d = q("""SELECT count(*) AS records, count(*) FILTER (WHERE len(differs) > 0) AS differing,
                    count(*) FILTER (WHERE one_run_only) AS one_run_only
             FROM steps_diff($a, $b)""", a=a, b=b).iloc[0]
    h = q("""SELECT count(*) AS held_records,
                    count(*) FILTER (WHERE (x.held_bytes, x.held_rows, x.held_chunks, x.held_writer,
                                            x.held_pending, x.held_deletes)
                                     IS DISTINCT FROM (y.held_bytes, y.held_rows, y.held_chunks,
                                            y.held_writer, y.held_pending, y.held_deletes)) AS held_differing
             FROM steps x JOIN steps y ON x.arm = y.arm AND x.step = y.step AND x.phase = y.phase
                                      AND x.reader IS NOT DISTINCT FROM y.reader
             WHERE x.run_id = $a AND y.run_id = $b AND coalesce(x.reader, '') <> 'cached-p8'""",
          a=a, b=b).iloc[0]
    rows.append({"pair": name, "build": build, **d.to_dict(), **h.to_dict()})
control = pd.DataFrame(rows)

model = fig.bp_model()
model_lo = {b: 100 * (model[model.build == b].sim_over_measured - 1).abs().min() for b in BUILDS}
model_hi = {b: 100 * (model[model.build == b].sim_over_measured - 1).abs().max() for b in BUILDS}
# the full reads: simulated over measured, and what their requests took (wall time less CPU) over
# their price, if the CPU is right and nothing overlaps
reads = model[model.what != "a write"].assign(
    took_over_price=lambda d: (d.measured_s - d.cpu_s) / d.request_s)
RD = reads.groupby("build")
read_lo, read_hi = RD.sim_over_measured.min(), RD.sim_over_measured.max()
took_lo, took_hi = RD.took_over_price.min(), RD.took_over_price.max()
M = model.set_index(["build", "arm", "what"]).sort_index()

ratios = fig.bp_over_mor(fig.bp_times())
ratios["predicted"] = [PRED_SKIP[b] if w.endswith("dead row groups")
                       else PRED[b][a][0 if w == "a write" else 1]
                       for b, a, w in zip(ratios.build, ratios.arm, ratios.what)]
ratios["judged"] = (ratios.predicted - 1).abs() >= 0.10
ratios["held"] = ~ratios.judged | ((ratios.over_mor_v3 > 1) == (ratios.predicted > 1))
R = ratios.set_index(["build", "arm", "what"]).sort_index()

# The store's share of each phase: measured (phase_breakdown) and simulated (the requests' price
# over the simulated time)
parts = fig.bp_parts()
measured = parts.pivot_table(index=["build", "arm", "arm_label", "ord", "what"], columns="part",
                             values="s").reset_index()
measured["measured"] = measured["the store"] / measured[fig.BP_PARTS].sum(axis=1)
shares = measured[["build", "arm", "arm_label", "ord", "what", "measured"]].merge(
    model.assign(simulated=model.request_s / model.simulated_s)[["build", "arm", "what", "simulated"]],
    on=["build", "arm", "what"])
shares["phase"] = ["a write" if w == "a write" else "a full read" for w in shares.what]
SH = shares.groupby(["build", "phase"]).measured
store = {k: (lo, hi) for k, lo, hi in zip(SH.min().index, SH.min(), SH.max())}
```

Whether the latency model, which prices each phase from its CPU and its requests, predicts what
S3 Express measured at the base point (`configs/sweep-base.json`), and whether each cloud run did
exactly the work its laptop twin counts (docs/HYPOTHESES.md: C5–C8 for the row reader, V4–V7 for
the vectorized reader). The results these runs give are in
[The base point on S3 Express](./base-point.md).

## Setup

Two builds of the harness, each run on the laptop and then on S3 Express (`figures.py`, `BP_*`):

| | Row reader (f51796a) | Vectorized reader (6f24575) |
|---|---|---|
| laptop twin | `sweep-base-f970149711a9/20261030T054758Z` | `sweep-base-f970149711a9/20261031T083323Z` |
| S3 Express | `sweep-base-s3x-89a55338cd9a/20261030T205713Z` | `sweep-base-s3x-89a55338cd9a/20261031T164030Z` |
| its VM's spike | `spike/s3x-spike-20261030T205211Z` | `spike/s3x-spike-20261031T163533Z` |

The laptop counts instructions, so its twin predicts each phase's time: its work at the arm's
pace, plus its requests priced at their sizes from a spike (lab/views/65_sized.sql). Each S3
Express run came from a `c7a.4xlarge` in usw2-az3, whose spike ran just before it. Havoc's run of
the row reader's build (`sweep-base-f970149711a9/20261030T055938Z`) counts requests and bytes
only: it ran under foreign load.

## The same work everywhere

Requests, bytes and table state follow from the op log, the configuration and the code, so twins
must match exactly; so must what a one-thread phase holds in memory at its peak (the 8-thread
reader's peaks move with how its tasks interleave, and are left out).

```{code-cell} ipython3
:tags: [remove-input]

table(control)
```

## The model

Each mean phase simulated, as its thread's CPU on the VM plus its requests priced at their sizes
with the VM's own spike, over its measured wall time. The shaded band is the registered 25%.

```{code-cell} ipython3
:label: base-point-model
:tags: [remove-input]

ARMS = list(model.sort_values("ord").arm_label.unique())
x_scale = alt.Scale(domain=[0.7, 1.3], zero=False)
band = alt.Chart(pd.DataFrame({"lo": [0.75], "hi": [1.25]})).mark_rect(opacity=0.12).encode(
    x=alt.X("lo:Q", scale=x_scale), x2="hi:Q")


# a panel per kind of phase, writes first (a facet would order them by name)
def model_panel(what):
    d = model[model.what == what]
    dots = alt.Chart(d).mark_point(filled=True, size=90).encode(
        x=alt.X("sim_over_measured:Q", title="simulated over measured", scale=x_scale),
        y=alt.Y("arm_label:N", title=None, sort=[a for a in ARMS if a in set(d.arm_label)]),
        color=alt.Color("build:N", title="build", sort=BUILDS),
        tooltip=["build", "arm_label", "what", alt.Tooltip("measured_s:Q", format=".2f"),
                 alt.Tooltip("simulated_s:Q", format=".2f")])
    return alt.layer(band, dots).properties(width=480, title=what)


alt.vconcat(*[model_panel(w) for w in fig.BP_WHATS]).resolve_scale(y="independent")
```

```{code-cell} ipython3
:tags: [remove-input]

table(fig.bp_sorted(model, "ord", "build")[
    ["build", "what", "arm_label", "n", "measured_s", "cpu_s", "request_s", "simulated_s",
     "sim_over_measured"]].round(2))
```

On the row reader every mean was within {eval}`f"{model_lo[OLD]:.0f}"`–{eval}`f"{model_hi[OLD]:.0f}"`%
of its wall time; on the vectorized reader within
{eval}`f"{model_lo[NEW]:.0f}"`–{eval}`f"{model_hi[NEW]:.0f}"`%.
- **Full reads are priced high**, {eval}`f"{read_lo[NEW]:.2f}"`–{eval}`f"{read_hi[NEW]:.2f}"` of
  their wall time on the vectorized reader against {eval}`f"{read_lo[OLD]:.2f}"`–{eval}`f"{read_hi[OLD]:.2f}"`
  on the row reader. Their requests now carry most of their time, and took
  {eval}`f"{took_lo[NEW]:.2f}"`–{eval}`f"{took_hi[NEW]:.2f}"` of their price (wall time less CPU,
  over the requests' price), so the spike's slight overprice of large GETs shows more.
- **RMoR-C's write is priced highest**, {eval}`f"{M.sim_over_measured[(NEW, 'rmor-c', 'a write')]:.2f}"`:
  its new data file is a multipart PUT whose parts go out while the file is being written, where the
  model prices it after the writing.

## The trade-off, predicted and measured

Each arm's mean wall time over MoR-v3's on S3 Express (dots), and the prediction registered before
the run from its laptop twin (ticks).

```{code-cell} ipython3
:label: base-point-ratios
:tags: [remove-input]

log_x = alt.Scale(type="log", domain=[0.6, 12], nice=False)
one = alt.Chart(pd.DataFrame({"x": [1.0]})).mark_rule(strokeDash=[4, 3], color="gray").encode(
    x=alt.X("x:Q", scale=log_x))


def ratio_panel(what):
    d = ratios[ratios.what == what]
    base = alt.Chart(d).encode(
        y=alt.Y("arm_label:N", title=None, sort=[a for a in ARMS if a in set(d.arm_label)]),
        color=alt.Color("build:N", title="build", sort=BUILDS),
        yOffset=alt.YOffset("build:N", sort=BUILDS))
    measured_pts = base.mark_point(filled=True, size=90).encode(
        x=alt.X("over_mor_v3:Q", title="over MoR-v3's (dot: measured; tick: predicted)", scale=log_x),
        tooltip=["build", "arm_label", "what", alt.Tooltip("over_mor_v3:Q", format=".2f"),
                 alt.Tooltip("predicted:Q", format=".2f")])
    predicted = base.mark_tick(thickness=2, size=14).encode(x="predicted:Q")
    return alt.layer(one, measured_pts, predicted).properties(width=520, title=what)


alt.vconcat(*[ratio_panel(w) for w in fig.BP_WHATS]).resolve_scale(y="independent")
```

```{code-cell} ipython3
:tags: [remove-input]

table(fig.bp_sorted(ratios, "ord", "build")[
    ["build", "what", "arm_label", "s", "mor_v3_s", "over_mor_v3", "predicted", "judged", "held"]].round(2))
```

## The store's share

The store's share of each mean phase (waiting on it, and its client's CPU), measured on S3 Express
and simulated (the requests' price over the simulated time).

```{code-cell} ipython3
:tags: [remove-input]

table(fig.bp_sorted(shares, "ord", "build")[
    ["build", "what", "arm_label", "measured", "simulated"]].round(2))
```

## Verdicts

```{code-cell} ipython3
:tags: [remove-cell]

def held(ok):
    return "held" if ok else "contradicted"


c5 = control[control.build == OLD].iloc[1]
v4 = control[control.build == NEW].iloc[0]
c6, v5 = held(model_hi[OLD] <= 25), held(model_hi[NEW] <= 25)
c7 = held(ratios[ratios.build == OLD].held.all())
rmow_new = R.loc[(NEW, "rmow-c")].over_mor_v3
v6 = held(ratios[ratios.build == NEW].held.all() and rmow_new["a write"] > 3.05
          and 0.80 <= rmow_new["a full read"] <= 0.95)
c8 = held(store[(OLD, "a write")][1] < 0.5 and store[(OLD, "a full read")][1] < 0.5)
v7 = held(store[(NEW, "a write")][1] < 0.5 and store[(NEW, "a full read")][0] > 0.5)
```

- **C5 and V4, the cloud runs send what their twins count:**
  {eval}`held(c5.differing == 0 and c5.held_differing == 0)` and
  {eval}`held(v4.differing == 0 and v4.held_differing == 0)`, in all {eval}`f"{c5.records:,}"`
  records of each.
- **C6 and V5, the model within 25%:** {eval}`c6` on the row reader, {eval}`v5` on the vectorized
  one.
- **C7 and V6, the trade-off where the twin puts it:** {eval}`c7` and {eval}`v6`. On the vectorized
  reader RMoW-C's write took {eval}`f"{rmow_new['a write']:.2f}"` times MoR-v3's (predicted
  {eval}`f"{PRED[NEW]['rmow-c'][0]:.2f}"`, and above the row reader's
  {eval}`f"{R.over_mor_v3[(OLD, 'rmow-c', 'a write')]:.2f}"`), its full read
  {eval}`f"{rmow_new['a full read']:.2f}"` (predicted {eval}`f"{PRED[NEW]['rmow-c'][1]:.2f}"`).
- **C8, a phase mostly CPU on the row reader:** {eval}`c8`; the store took
  {eval}`f"{100 * store[(OLD, 'a write')][0]:.0f}"`–{eval}`f"{100 * store[(OLD, 'a write')][1]:.0f}"`%
  of the writes and {eval}`f"{100 * store[(OLD, 'a full read')][0]:.0f}"`–{eval}`f"{100 * store[(OLD, 'a full read')][1]:.0f}"`%
  of the full reads. **V7, on the vectorized reader full reads mostly the store's and writes mostly
  CPU:** {eval}`v7`; {eval}`f"{100 * store[(NEW, 'a full read')][0]:.0f}"`–{eval}`f"{100 * store[(NEW, 'a full read')][1]:.0f}"`%
  and {eval}`f"{100 * store[(NEW, 'a write')][0]:.0f}"`–{eval}`f"{100 * store[(NEW, 'a write')][1]:.0f}"`%.

## Limits

- One run per build. On the calibration's runs the S3 Express ratios moved by up to 0.12 from run
  to run, through the CPU's pace.
- The model prices a multipart PUT (past 48 MiB) after the CPU that writes it, where its parts go
  out while the file is still being written; and a phase's thread CPU includes the client's TLS
  and checksums, which the spike's latencies include too.
- TailCoW-C's ratios and RMoR-C's while skipping lie within 10% of 1 and are not judged.
- Azure has not run the base point.
