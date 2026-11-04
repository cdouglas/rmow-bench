---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: RMoR re-measured
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

RUNS, LAPTOP, PRED = fig.RR_RUNS, fig.RR_LAPTOP, fig.RR_PRED
S3X, AZ = fig.CS_STORES
STORES = [s for s in fig.CS_STORES if RUNS[s]]
PENDING = [s for s in fig.CS_STORES if not RUNS[s]]
CONFIGS = fig.RR_CONFIGS
VARIES = {"rmor-base": "the base point", "rmor-p10": "p = 0.1", "rmor-p30": "p = 0.3",
          "rmor-s0": "a skew of 0", "rmor-s2": "a skew of 2", "rmor-rg32m": "32 MiB row groups",
          "rmor-rg64m": "64 MiB row groups", "rmor-late99-1": "lateness, p99 of 1 batch",
          "rmor-late99-20": "lateness, p99 of 20 batches", "rmor-ingest": "the ingest window",
          "rmor-ingest-rows": "the ingest window, a row a change"}
SETTINGS = [VARIES[c] for c in CONFIGS]
NOW, WAS = "range check", "per-position check"
WRITE, READ, SKIP = "a write", "a full read", "a full read, skipping dead row groups"


def span(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


def names(cs):
    v = [VARIES[c] for c in cs]
    return v[0] if len(v) == 1 else ", ".join(v[:-1]) + " and " + v[-1]


# R1: each cloud run against its laptop run, record by record
def same(a, b):
    return q("""SELECT count(*) AS records, count(*) FILTER (WHERE one_run_only) AS one_run_only,
                       count(*) FILTER (WHERE len(differs) > 0) AS differing
                FROM steps_diff($a, $b)""", a=a, b=b).iloc[0].to_dict()


r1 = pd.DataFrame([{"store": st, "setting": VARIES[c], "laptop run": LAPTOP[c], "cloud run": r,
                    **same(LAPTOP[c], r)} for st in STORES for c, (r, _) in RUNS[st].items()])
r1_bad = {st: int(r1[r1.store == st][["one_run_only", "differing"]].to_numpy().sum())
          for st in STORES}

# The times: this run's means (range check) and the earlier run's (per-position check), each over
# MoR's in its run
t = fig.rr_times()


def pick(build, arm, what, col="over_mor_v3"):
    d = t[(t.build == build) & (t.arm == arm) & (t.what == what)]
    return d.set_index(["store", "config"])[col]


J = pd.DataFrame({
    "mor_w": pick(NOW, "rmor-c", WRITE, "mor_v3_s"), "mor_w_was": pick(WAS, "rmor-c", WRITE, "mor_v3_s"),
    "rmor_w": pick(NOW, "rmor-c", WRITE, "s"), "rmor_w_was": pick(WAS, "rmor-c", WRITE, "s"),
    "w_over": pick(NOW, "rmor-c", WRITE), "w_over_was": pick(WAS, "rmor-c", WRITE),
    "app_w_over": pick(WAS, "rmow-c", WRITE), "ext_w_over": pick(WAS, "rmor-c-footer", WRITE),
    "mor_r": pick(NOW, "rmor-c", SKIP, "mor_v3_s"), "mor_r_was": pick(WAS, "rmor-c", SKIP, "mor_v3_s"),
    "rmor_r": pick(NOW, "rmor-c", SKIP, "s"), "rmor_r_was": pick(WAS, "rmor-c", SKIP, "s"),
    "r_over": pick(NOW, "rmor-c", SKIP), "r_over_was": pick(WAS, "rmor-c", SKIP),
    "noskip_over": pick(NOW, "rmor-c", READ), "noskip_over_was": pick(WAS, "rmor-c", READ),
    "app_r_over": pick(WAS, "rmow-c", READ),
    "gi_w": pick(NOW, "rmor-c", WRITE, "g_instructions"),
    "gi_w_was": pick(WAS, "rmor-c", WRITE, "g_instructions"),
    "gi_r": pick(NOW, "rmor-c", SKIP, "g_instructions"),
    "gi_r_was": pick(WAS, "rmor-c", SKIP, "g_instructions"),
}).reset_index()
J["setting"] = J.config.map(VARIES)
J["rmor_w_pred"] = [PRED[s][c]["write"] for s, c in zip(J.store, J.config)]
J["rmor_r_pred"] = [PRED[s][c]["read"] for s, c in zip(J.store, J.config)]
J["w_over_pred"] = J.rmor_w_pred / J.mor_w_was  # MoR's predicted time is its earlier one
J["r_over_pred"] = J.rmor_r_pred / J.mor_r_was
J["r3"] = J.w_over / J.app_w_over
J["r3_pred"] = J.w_over_pred / J.app_w_over

# Verdicts as registered: R2 below 0.90 on S3 Express but at p = 0.3, above 1.1 on Azure Blob but
# at p = 0.3, a skew of 2 and the two ingest settings; R3 above 1.1 everywhere but a skew of 2 on
# S3 Express
R2_RULE = {S3X: (lambda v: v < 0.90, {"rmor-p30"}),
           AZ: (lambda v: v > 1.1, {"rmor-p30", "rmor-s2", "rmor-ingest", "rmor-ingest-rows"})}


def verdict(ok, undecided):
    return "decides nothing" if undecided else ("holds" if ok else "contradicted")


J["r2_verdict"] = [verdict(R2_RULE[s][0](v), c in R2_RULE[s][1])
                   for s, c, v in zip(J.store, J.config, J.r_over)]
J["r3_verdict"] = [verdict(v > 1.1, s == S3X and c == "rmor-s2")
                   for s, c, v in zip(J.store, J.config, J.r3)]


# Where the time went: the part of RMoR's phases that holds the check (rows and planning: the
# thread's CPU outside decoding, encoding, footers and deletes), before and after
def planning(runs):
    p = fig.bp_parts(runs, key="config", fine=True)
    p = p[(p.arm == "rmor-c") & (p.part == "rows and planning") & p.what.isin([WRITE, SKIP])]
    return p.set_index(["config", "what"]).s


plan = []
for st in STORES:
    now = planning({c: r for c, (r, _) in RUNS[st].items()})
    was = planning({c: fig.rr_earlier(st, c) for c in RUNS[st]})
    plan.append(pd.DataFrame({"plan": now, "plan_was": was}).reset_index().assign(store=st))
plan = pd.concat(plan, ignore_index=True).pivot_table(index=["store", "config"], columns="what",
                                                      values=["plan", "plan_was"])
for col, what in [("w", WRITE), ("r", SKIP)]:
    J[f"{col}_plan"] = [plan.loc[(s, c), ("plan", what)] for s, c in zip(J.store, J.config)]
    J[f"{col}_plan_was"] = [plan.loc[(s, c), ("plan_was", what)] for s, c in zip(J.store, J.config)]
    J[f"{col}_check"] = J[f"{col}_plan_was"] - J[f"{col}_plan"]  # the check's time, measured
J["w_saved"], J["r_saved"] = J.rmor_w_was - J.rmor_w, J.rmor_r_was - J.rmor_r
J["w_saved_pred"], J["r_saved_pred"] = J.rmor_w_was - J.rmor_w_pred, J.rmor_r_was - J.rmor_r_pred
J["w_rest"] = J.w_saved - J.w_check  # the rest of a write, earlier run less this one

# The check's pace (S3 Express, where the VMs count instructions): the instructions it no longer
# runs over the time it no longer takes, against its phase's in the earlier run
pace_was = q("""SELECT run_id, CASE WHEN phase = 'batch' THEN 'w' ELSE 'r' END AS what,
                       sum(instructions) / sum(user_ns) AS g_per_s
                FROM steps WHERE run_id IN (SELECT unnest($runs)) AND arm = 'rmor-c'
                  AND (phase = 'batch' OR (phase = 'read_full' AND step > 0 AND reader = 'cached-skip'))
                GROUP BY ALL""", runs=[fig.rr_earlier(S3X, c) for c in RUNS[S3X]])
pace_was = pace_was.set_index(["run_id", "what"]).g_per_s
s3 = J[J.store == S3X].copy()
for col in ["w", "r"]:
    s3[f"{col}_pace"] = (s3[f"gi_{col}_was"] - s3[f"gi_{col}"]) / s3[f"{col}_check"]
    s3[f"{col}_phase_pace"] = [pace_was[(fig.rr_earlier(S3X, c), col)] for c in s3.config]
BIG = s3.r_check > 0.2  # settings where the check took a read long enough to time
s3w = s3[s3.w_check > 0.2]


def at(store, config, col):
    return J[(J.store == store) & (J.config == config)][col].iloc[0]


def side(store, col, ok):
    d = J[J.store == store]
    return d[d[col] == ok]


status = ("Both stores' runs are in." if not PENDING else
          f"{' and '.join(STORES) or 'No store'}'s runs are in; {' and '.join(PENDING)}'s are "
          "still running, and this page judges them once they land.")
d3 = J[J.store == S3X]
faster = ("at every setting" if (d3.r_over < 1).all() else
          f"at {(d3.r_over < 1).sum()} of {len(d3)} settings")
off = d3.rmor_r - d3.rmor_r_pred
r_gap = (s3[BIG].r_saved - s3[BIG].r_check).abs()
more = ("more than predicted wherever the check had taken a read more than 0.2 s"
        if (off[BIG.values] > 0).all() else "more or less than predicted")
```

RMoR's times in the cloud now that its writers and its skipping reader check a row group against
the file's deletion vector by range, not position by position (docs/HYPOTHESES.md, "RMoR with the
check by range, in the cloud", 2026-11-02). Every earlier cloud run of RMoR checked each position,
which is why the results pages call its times upper bounds. {eval}`status`

## Setup

- **Configurations:** the sweeps' eleven (`configs/rmor-*.json`), each with MoR and RMoR as its
  only designs, one run each, at d30544a (505ba07's code, plus a failed run's exit), in a 16 GB
  heap with requests and held memory logged.
- **Machines:** one VM per store ran the eleven in turn after one spike: a `c7a.4xlarge` for S3
  Express, a `D8as_v6` for Azure Blob.
- **References:** each configuration's run on the laptop at 505ba07 (campaign `rmor-rerun`), and
  the earlier cloud run of its setting, the cloud sweep's at 6f24575 with every design (its runs
  are pinned on the cloud sweep's pages).
- **What is compared:** RMoR's mean write (the 30 writes) and mean full read after the initial
  load, skipping dead row groups, each over MoR's in the same run. MoR's full read is the one
  under the same cached reader, and the check is not on its path.

## The same requests (R1)

Each cloud run against its laptop run, record by record: the records in either run alone, and the
records whose requests, bytes or table state differ.

```{code-cell} ipython3
:tags: [remove-input]

table(r1)
```

## A full read (R2)

RMoR's mean full read, skipping dead row groups, over MoR's in the same run: in the earlier run
(the check by position) and in this one (by range), with the registered prediction (tick) and the
line the hypothesis drew (dashed: 0.90 on S3 Express, 1.1 on Azure Blob).

```{code-cell} ipython3
:label: rmor-rerun-read
:tags: [remove-input]

LINE = {S3X: 0.90, AZ: 1.1}
pts = pd.concat([J[["store", "setting", "r_over_was"]].rename(columns={"r_over_was": "over"})
                 .assign(build=WAS),
                 J[["store", "setting", "r_over"]].rename(columns={"r_over": "over"})
                 .assign(build=NOW)], ignore_index=True)
pts = pts.merge(J[["store", "setting", "r_over_pred", "r2_verdict"]], on=["store", "setting"])
pts["line"] = pts.store.map(LINE)
lo = min(pts.over.min(), pts.r_over_pred.min(), pts.line.min())
hi = max(pts.over.max(), pts.r_over_pred.max(), pts.line.max(), 1.0)
xs = alt.Scale(domain=[lo - 0.02, hi + 0.02], nice=False, zero=False)
base = alt.Chart().encode(y=alt.Y("setting:N", title=None, sort=SETTINGS))
dots = base.mark_point(filled=True, size=70, opacity=1).encode(
    x=alt.X("over:Q", title="RMoR's full read over MoR's (tick: predicted; dashed: the line R2 drew)",
            scale=xs),
    color=alt.Color("build:N", title=None, sort=[WAS, NOW]),
    tooltip=["store", "setting", "build", alt.Tooltip("over:Q", format=".3f"),
             alt.Tooltip("r_over_pred:Q", title="predicted", format=".2f"), "r2_verdict"])
ticks = base.mark_tick(thickness=2, size=14, color="#555").encode(
    x=alt.X("r_over_pred:Q", scale=xs))
line = alt.Chart().mark_rule(strokeDash=[4, 3], color="#888").encode(x=alt.X("line:Q", scale=xs))
one = alt.Chart().mark_rule(color="#bbb").encode(x=alt.X("one:Q", scale=xs))
(alt.layer(one, line, ticks, dots, data=pts.assign(one=1.0)).properties(width=420, height=300)
 .facet(column=alt.Column("store:N", title=None, sort=STORES))
 .configure_legend(orient="bottom"))
```

```{code-cell} ipython3
:tags: [remove-input]

table(J[["store", "setting", "mor_r", "rmor_r_was", "rmor_r_pred", "rmor_r", "r_over_was",
         "r_over_pred", "r_over", "r2_verdict"]]
      .rename(columns={"mor_r": "MoR (s)", "rmor_r_was": "RMoR before (s)",
                       "rmor_r_pred": "RMoR predicted (s)", "rmor_r": "RMoR (s)",
                       "r_over_was": "over MoR's, before", "r_over_pred": "predicted",
                       "r_over": "over MoR's", "r2_verdict": "R2"}).round(3))
```

On S3 Express, RMoR's skipping read now takes {eval}`span(d3.r_over)` of MoR's, where checking
each position had it at {eval}`span(d3.r_over_was)`: faster than MoR's full read
{eval}`faster`. The prediction was {eval}`span(d3.r_over_pred)`; against it, each read took
{eval}`span(off, "{:+.2f}")` s ({eval}`more`; [below](#why-less-than-predicted) is why).

## A write (R3)

RMoR's mean write over MoR's in this run, against RMoW-append's over MoR's in the earlier run of
its setting (the runs of RMoR hold no RMoW-append).

```{code-cell} ipython3
:tags: [remove-input]

table(J[["store", "setting", "mor_w", "rmor_w_was", "rmor_w_pred", "rmor_w", "w_over",
         "app_w_over", "r3_pred", "r3", "r3_verdict"]]
      .rename(columns={"mor_w": "MoR (s)", "rmor_w_was": "RMoR before (s)",
                       "rmor_w_pred": "RMoR predicted (s)", "rmor_w": "RMoR (s)",
                       "w_over": "RMoR over MoR's", "app_w_over": "RMoW-append's, earlier",
                       "r3_pred": "times RMoW-append's, predicted", "r3": "times RMoW-append's",
                       "r3_verdict": "R3"}).round(2))
```

On S3 Express, RMoR's write is still {eval}`span(d3.w_over, "{:.1f}")` times MoR's, which is
{eval}`span(d3.r3)` times RMoW-append's ratio.

(why-less-than-predicted)=
## Why less than predicted

The prediction took the instructions the check no longer runs and priced them at the pace of the
phase it ran in. The check ran faster than its phase: a tight loop over a bitmap. The part of
RMoR's phase that holds it (rows and planning, the thread's CPU outside Parquet's decoding and
encoding, footers and deletes) is where the time went, before and after.

```{code-cell} ipython3
:label: rmor-rerun-saved
:tags: [remove-input]

KINDS = ["the check's time, measured", "saved, predicted", "saved, measured"]
sv = pd.concat([
    J[["store", "setting", c]].rename(columns={c: "s"}).assign(what=w, kind=k)
    for w, cols in [(WRITE, ["w_check", "w_saved_pred", "w_saved"]),
                    (SKIP, ["r_check", "r_saved_pred", "r_saved"])]
    for c, k in zip(cols, KINDS)], ignore_index=True)
sv["what"] = sv.what.map({WRITE: "a write", SKIP: "a full read (skipping)"})
(alt.Chart(sv).mark_point(filled=True, size=60, opacity=1).encode(
    x=alt.X("s:Q", title="seconds a phase"),
    y=alt.Y("setting:N", title=None, sort=SETTINGS),
    color=alt.Color("kind:N", title=None),  # no sort: a sort keeps shape's legend apart
    shape=alt.Shape("kind:N", title=None),
    tooltip=["store", "setting", "what", "kind", alt.Tooltip("s:Q", format=".2f")])
 .properties(width=300, height=300)
 .facet(column=alt.Column("what:N", title=None), row=alt.Row("store:N", title=None, sort=STORES))
 .configure_legend(orient="bottom"))
```

```{code-cell} ipython3
:tags: [remove-input]

table(J[["store", "setting", "r_plan_was", "r_plan", "r_saved_pred", "r_saved", "w_plan_was",
         "w_plan", "w_saved_pred", "w_saved"]]
      .rename(columns={"r_plan_was": "read: planning before", "r_plan": "read: planning",
                       "r_saved_pred": "read: saved, predicted", "r_saved": "read: saved",
                       "w_plan_was": "write: planning before", "w_plan": "write: planning",
                       "w_saved_pred": "write: saved, predicted", "w_saved": "write: saved"})
      .round(2))
```

- **A full read is now its old read less the check.** Where the check took a read more than
  0.2 s on S3 Express, the read saved within {eval}`f"{r_gap.max():.2f}"` s of what its planning
  lost. At the base point, its planning took {eval}`f"{at(S3X, 'rmor-base', 'r_plan_was'):.2f}"` s
  before and {eval}`f"{at(S3X, 'rmor-base', 'r_plan'):.2f}"` s now, and the read took
  {eval}`f"{at(S3X, 'rmor-base', 'r_saved'):.2f}"` s less. Across the settings where the check
  took a read more than 0.2 s, the read saved
  {eval}`span(s3[BIG].r_saved / s3[BIG].r_saved_pred, "{:.0%}")` of what was predicted.
- **The check ran at {eval}`span(s3[BIG].r_pace, "{:.0f}")` billion instructions a second** in a
  read on the S3 Express VM (the instructions it no longer runs over the time it no longer takes),
  where its phase as a whole ran at {eval}`span(s3[BIG].r_phase_pace, "{:.0f}")` billion, the
  pace the prediction priced it at. In a write it ran at
  {eval}`span(s3w.w_pace, "{:.0f}")` billion, its phase at
  {eval}`span(s3w.w_phase_pace, "{:.0f}")`.
- **In a write, too, the check cost less than priced,** and the rest of the write moved. Its
  planning shrank by
  {eval}`span(s3w.w_check, "{:.2f}")` s on S3 Express, but the rest of the write moved by
  {eval}`span(-s3w.w_rest, "{:+.2f}")` s (positive: slower) from the earlier run on another VM,
  so a write saved
  {eval}`span(s3w.w_saved, "{:.2f}")` s against
  {eval}`span(s3w.w_saved_pred, "{:.2f}")` s predicted. In the two ingest settings the check took
  a write almost nothing, and the writes moved by
  {eval}`span(J[(J.store == S3X) & J.config.str.startswith('rmor-ingest')].w_saved, "{:+.2f}")` s.

## Verdicts

```{code-cell} ipython3
:tags: [remove-cell]


def r2_text(st):
    d = J[J.store == st]
    held, bad, und = (d[d.r2_verdict == v] for v in ["holds", "contradicted", "decides nothing"])
    out = []
    if len(bad):
        out.append(f"**contradicted** at {names(bad.config)} "
                   f"({', '.join(f'{v:.3f}' for v in bad.r_over)})")
    if len(held):
        out.append(f"holds at the other {len(held)} decided settings ({span(held.r_over)})"
                   if len(bad) else f"holds at all {len(held)} decided settings ({span(held.r_over)})")
    if len(und):
        out.append(f"{names(und.config)} decides nothing ({span(und.r_over)})")
    return "; ".join(out)


def r3_text(st):
    d = J[J.store == st]
    held, bad, und = (d[d.r3_verdict == v] for v in ["holds", "contradicted", "decides nothing"])
    out = []
    if len(bad):
        out.append(f"**contradicted** at {names(bad.config)} ({span(bad.r3)} times)")
    if len(held):
        out.append(f"holds at {'the other ' if len(bad) else 'all '}{len(held)} decided settings "
                   f"({span(held.r3)} times)")
    if len(und):
        out.append(f"{names(und.config)} decides nothing ({span(und.r3)} times)")
    return "; ".join(out)


def r1_text(st):
    d = r1[r1.store == st]
    return (f"holds: none of the {d.records.sum():,} records differs" if r1_bad[st] == 0 else
            f"**contradicted**: {r1_bad[st]} records differ or are in one run alone")


VERDICTS = "\n".join(
    f"- **{st}.** R1 {r1_text(st)}. R2 {r2_text(st)}. R3 {r3_text(st)}." for st in STORES)
VERDICTS += "".join(f"\n- **{st}.** Its runs are still running." for st in PENDING)
```

As registered:

- **R1 (a control):** each cloud run sends what its laptop run counts.
- **R2:** on S3 Express, RMoR's skipping full read over MoR's is below 0.90 at every setting but
  p = 0.3; on Azure Blob it stays above 1.1 at every setting but p = 0.3, a skew of 2 and the two
  ingest settings.
- **R3:** RMoR's write over MoR's is more than 1.1 times RMoW-append's, at every setting but a skew
  of 2 on S3 Express.

The runs:

```{code-cell} ipython3
:tags: [remove-input]

from IPython.display import Markdown
Markdown(VERDICTS)
```

## Measured, not judged

```{code-cell} ipython3
:tags: [remove-cell]

mor_drift_w = (d3.mor_w / d3.mor_w_was - 1)
mor_drift_r = (d3.mor_r / d3.mor_r_was - 1)
```

- **RMoR's skipping read against RMoW-append's full read:** on S3 Express, RMoR's ratio over MoR's
  is {eval}`span(d3.r_over / d3.app_r_over)` times RMoW-append's in the earlier run (predicted
  {eval}`span(d3.r_over_pred / d3.app_r_over)`).
- **RMoR's write against RMoW-external's:** on S3 Express, {eval}`span(d3.w_over / d3.ext_w_over)`
  times its ratio over MoR's (predicted {eval}`span(d3.w_over_pred / d3.ext_w_over)`).
- **RMoR's read without skipping,** which does no check: {eval}`span(d3.noskip_over)` times MoR's
  on S3 Express, against {eval}`span(d3.noskip_over_was)` before.
- **MoR's own times against the earlier runs':** on S3 Express a write moved by
  {eval}`span(mor_drift_w, "{:+.1%}")` and a full read by {eval}`span(mor_drift_r, "{:+.1%}")`.

## Limits

- One run of each configuration, on one seed, against one earlier run on another VM of the same
  type. On S3 Express the earlier runs had nine VMs loading the bucket at once; these had one.
- R3 compares across runs: RMoW-append's ratio is the earlier run's, with all five designs.
- Azure Blob's VMs count no instructions, so the check's pace is S3 Express's alone.
- A ratio within 10% of what it is compared with decides nothing.
