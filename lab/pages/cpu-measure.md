---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Measuring CPU
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import q, table, theme
theme()

# The calibration of the CPU measure (docs/HYPOTHESES.md at 1145f89, 2026-10-24): configs/cal-cpu-recency-4m.json
# run twice on this laptop, from build snapshots of the uncommitted measure (dirty)
RUNS = [
    "cal-cpu-recency-4m-a2ae25ccf63c/20261025T010900Z",
    "cal-cpu-recency-4m-a2ae25ccf63c/20261025T011204Z",
]
labels = dict(q("SELECT arm, arm_label FROM arm_meta").itertuples(index=False))

work = q("""
    SELECT w.*, s.cpu_ns AS thread_cpu_ns, s.pool_cpu_ns, s.user_ns AS thread_user_ns, s.pool_user_ns,
           s.cpu_ns + coalesce(s.pool_cpu_ns, 0) - s.user_ns - coalesce(s.pool_user_ns, 0) AS kernel_ns
    FROM phase_work w JOIN steps s
      ON s.run_id = w.run_id AND s.arm = w.arm AND s.step = w.step AND s.phase = w.phase
     AND s.reader IS NOT DISTINCT FROM w.reader
    WHERE list_contains($runs, w.run_id) AND w.phase IN ('read_full', 'batch')""", runs=RUNS)
work["run"] = work.run_id.map({r: i + 1 for i, r in enumerate(RUNS)})
work["what"] = np.where(work.phase == "batch", "batch", "full read, " + work.reader.fillna(""))
# the pace the registered measure times each phase at: its arm's median cycles per instruction
work["work_ms_arm"] = work.work_ms

# twins: each split arm against its appendable twin, phase by phase
key = ["run", "step", "phase", "what"]
split = work[work.arm.str.endswith("-split")].assign(pair=lambda d: d.arm.str.replace("-split", ""))
twin = split.merge(work, left_on=key + ["pair"], right_on=key + ["arm"], suffixes=("", "_twin"))
twin["pair_label"] = twin.pair.map(lambda a: f"{labels[a + '-split']} ÷ {labels[a]}")
MEASURES = {"thread CPU time": "cpu_ns", "instructions": "instructions", "modeled time": "work_ms"}
long = pd.concat([
    twin.assign(measure=m, ratio=twin[c] / twin[c + "_twin"])[["run", "step", "what", "pair_label", "measure", "ratio"]]
    for m, c in MEASURES.items()])
spread = (long.assign(dev=(long.ratio - 1).abs())
          .groupby(["what", "measure"])
          .agg(pairs=("ratio", "size"), sd=("ratio", "std"), worst=("dev", "max"),
               over_2pct=("dev", lambda d: (d > 0.02).sum()))
          .reset_index())


def sp(what, measure, col):
    return spread[(spread.what == what) & (spread.measure == measure)].iloc[0][col]


# across the two runs: the same phase's instructions
a, b = work[work.run == 1], work[work.run == 2]
across = a.merge(b, on=["arm", "step", "phase", "what"], suffixes=("_1", "_2"))
across["dev"] = (across.instructions_1 / across.instructions_2 - 1).abs()
late = across[across.step >= 10]
across_t = late.groupby("what").agg(phases=("dev", "size"), median=("dev", "median"),
                                    worst=("dev", "max")).reset_index()

# pace: each arm's cycles per instruction, per run
pace = (work.assign(cpi_phase=work.cycles / work.instructions)
        .groupby(["what", "arm", "run"])
        .agg(cpi=("cpi", "first"), cv=("cpi_phase", lambda x: x.std() / x.mean()))
        .reset_index())
pace["arm_label"] = pace.arm.map(labels)
P = pace.set_index(["what", "arm", "run"])
reads_cv = pace[pace.what == "full read, cached"]
batch_ratio = [P.loc[("batch", "rmow-c", r), "cpi"] / P.loc[("batch", "mor-v3", r), "cpi"] for r in (1, 2)]
same_arm = [abs(P.loc[(w, arm, 1), "cpi"] / P.loc[(w, arm, 2), "cpi"] - 1)
            for (w, arm, r) in P.index if r == 1]
kernel = (work.groupby(["what", "arm"]).apply(lambda g: g.kernel_ns.sum() / (g.thread_cpu_ns + g.pool_cpu_ns.fillna(0)).sum())
          .rename("kernel_share").reset_index())

N = {
    "cpu_sd_c": f"{100 * sp('full read, cached', 'thread CPU time', 'sd'):.1f}%",
    "cpu_worst_c": f"{100 * sp('full read, cached', 'thread CPU time', 'worst'):.0f}%",
    "cpu_worst_p8": f"{100 * sp('full read, cached-p8', 'thread CPU time', 'worst'):.0f}%",
    "ins_worst_c": f"{100 * sp('full read, cached', 'instructions', 'worst'):.2f}%",
    "ins_worst_p8": f"{100 * sp('full read, cached-p8', 'instructions', 'worst'):.2f}%",
    "ins_sd_b": f"{100 * sp('batch', 'instructions', 'sd'):.2f}%",
    "work_worst_c": f"{100 * sp('full read, cached', 'modeled time', 'worst'):.1f}%",
    "work_over_c": f"{sp('full read, cached', 'modeled time', 'over_2pct')}",
    "work_pairs_c": f"{sp('full read, cached', 'modeled time', 'pairs')}",
    "across_reads": f"{100 * late[late.phase == 'read_full'].dev.max():.2f}%",
    "across_batch": f"{100 * late[late.phase == 'batch'].dev.max():.1f}%",
    "across_med": f"{100 * late[late.phase == 'read_full'].dev.median():.2f}%",
    "cv_lo": f"{100 * reads_cv.cv.min():.1f}%",
    "cv_hi": f"{100 * reads_cv.cv.max():.1f}%",
    "cv_over": f"{(reads_cv.cv >= 0.05).sum()}",
    "cv_n": f"{len(reads_cv)}",
    "same_arm_max": f"{100 * max(same_arm):.1f}%",
    "rc_mor_batch": f"{min(batch_ratio):.2f}–{max(batch_ratio):.2f}",
    "kernel_lo": f"{100 * kernel.kernel_share.min():.0f}%",
    "kernel_hi": f"{100 * kernel.kernel_share.max():.0f}%",
}
```

How should CPU be measured, so that one phase's CPU can decide a question with a 10% margin?
The scale rerun found that thread CPU time cannot: twin reads, which do the same work, differed
by up to 9% in clean phases (lab/pages/scale.md). This page reads two runs of one configuration
on this laptop, with each thread's performance counters, and decides M1–M4 (docs/HYPOTHESES.md at 1145f89,
2026-10-24).

## Setup

**Runs.** `configs/cal-cpu-recency-4m.json` ran twice, one run after the other. It is the scale
campaign's 4 MiB setting with:
- all six arms;
- the readers `cached` and `cached-p8`;
- a probe every 5 batches, and requests logged.

The page pins the runs by id (`RUNS`). Both ran from build snapshots of the uncommitted measure,
so their manifests say dirty, and the snapshots hold the diff.

**The measure.** Each phase counts its threads' user-mode instructions and cycles (its own
thread's, and its read pool's) with `perf_event_open` (`ThreadCounters`).
- **Work** is the instructions.
- **Modeled time** (`phase_work.work_ms`) is work × the median cycles per instruction of that
  arm's phases of that kind in the run, ÷ the run's median clock.

Exploratory runs in scratch had already ruled out a heap collection before each phase, the
cheapest of three runs, a pre-touched heap, and user-mode time.

## Twins

```{code-cell} ipython3
:label: cpu-measure-twins
:tags: [remove-input]
:class: col-page-right

(alt.Chart(long).mark_point(size=40, filled=True, opacity=0.7)
 .encode(x=alt.X("step:Q", title="batch"),
         y=alt.Y("ratio:Q", title="split ÷ appendable",
                 scale=alt.Scale(domain=[0.8, 1.2], clamp=True)),
         color=alt.Color("measure:N", title=None, sort=list(MEASURES)),
         tooltip=["run", "step", "pair_label", "measure", alt.Tooltip("ratio:Q", format=".4f")])
 .properties(width=220, height=170)
 .facet(column=alt.Column("what:N", title=None)))
```

Each point is one phase of a split arm over the same phase of its appendable twin, in both runs,
under each measure; a point beyond the axis is drawn at its edge. The twins read the same bytes,
and their batches rewrite the same regions.

```{code-cell} ipython3
:tags: [remove-input]

table(spread.round(4))
```

- **Thread CPU time:** twins' full reads under `cached` have a standard deviation of
  {eval}`N["cpu_sd_c"]`, up to {eval}`N["cpu_worst_c"]` apart, and up to
  {eval}`N["cpu_worst_p8"]` under `cached-p8`.
- **Instructions:** within {eval}`N["ins_worst_c"]` under `cached` and {eval}`N["ins_worst_p8"]`
  under `cached-p8`. Batch twins have a standard deviation of {eval}`N["ins_sd_b"]`.
- **Modeled time:** up to {eval}`N["work_worst_c"]` apart under `cached`
  ({eval}`N["work_over_c"]` of {eval}`N["work_pairs_c"]` pairs beyond 2%). The noise comes back
  through the pace each arm is timed at, below.

## Across runs

```{code-cell} ipython3
:tags: [remove-input]

table(across_t.rename(columns={"what": "phase", "phases": "phases from batch 10",
                               "median": "median |run 1 ÷ run 2 − 1|", "worst": "worst"})
      .round(5))
```

From batch 10 on, a full read counts within {eval}`N["across_reads"]` of the same read's
instructions in the other run, and a batch within {eval}`N["across_batch"]`. Reads are off by
an almost constant {eval}`N["across_med"]`, as if the JIT compiled one run's decoding slightly
differently.

## Pace

```{code-cell} ipython3
:tags: [remove-input]

table(pace.pivot_table(index=["what", "arm_label"], columns="run", values=["cpi", "cv"])
      .round(4).reset_index())
```

`cpi` is the median cycles per instruction of an arm's phases in a run, the pace the modeled
time uses; `cv` is how much it varies across them.
- **Reads:** CPI varies by {eval}`N["cv_lo"]`–{eval}`N["cv_hi"]` across probes under `cached`.
  {eval}`N["cv_over"]` of the {eval}`N["cv_n"]` arm-run medians rest on phases varying by 5% or
  more. One arm's median moves by up to {eval}`N["same_arm_max"]` between the runs.
- **Batches:** the arms' paces differ beyond that noise. RMoW-C's batches run at
  {eval}`N["rc_mor_batch"]` times MoR-v3's CPI, in both runs, while twins stay within a few
  percent. A single pace for every arm would misstate batch time.

Kernel time, which the counters leave out, is {eval}`N["kernel_lo"]`–{eval}`N["kernel_hi"]` of
thread CPU.

```{code-cell} ipython3
:tags: [remove-input]

table(kernel.assign(arm=kernel.arm.map(labels)).pivot(index="arm", columns="what", values="kernel_share")
      .round(3).reset_index())
```

## M1–M4 (docs/HYPOTHESES.md at 1145f89)

**Work is steady; modeled time is not as steady as predicted.**
- **M1, twins agree in work: held.** Full reads within {eval}`N["ins_worst_c"]`
  ({eval}`N["ins_worst_p8"]` on eight threads); batches with a standard deviation of
  {eval}`N["ins_sd_b"]`.
- **M2, work repeats across runs: held.** Within {eval}`N["across_reads"]` for reads and
  {eval}`N["across_batch"]` for batches, from batch 10 on.
- **M3, modeled time within 2% between twins: contradicted.** Up to {eval}`N["work_worst_c"]`
  apart: each arm's pace is estimated from noisy cycles.
- **M4, pace varies by under 5% across probes: contradicted** for {eval}`N["cv_over"]` of
  {eval}`N["cv_n"]` arm-run medians of full reads under `cached`.

## Findings

- **Instructions measure work to a fraction of a percent; thread CPU time to a few tens of
  percent.** On this laptop the clock moves under a running phase, and nothing inside the JVM
  holds it still.
- **Time needs a pace, and pace is the noisy part.** Cycles per instruction moves with what the
  core shares and with memory stalls. An arm's median over 13 probes is good to a few percent.
  That is enough for a 25% margin, and too little for one of 2%.
- **Pace differs by arm where the code differs.** RMoW-C's batches re-encode regions at a slower
  pace than MoR-v3's small writes. Timing every arm at one pace would hide it.

## Limits

- **One machine, a laptop under WSL2.** havoc's server cores may hold their pace better; its
  kernel does not yet allow counters (`perf_event_paranoid` 4).
- **Kernel time is left out,** which tilts slightly toward the arms that make more system calls
  (MoR-v3's batches).
- **Two runs, one setting.** At larger row groups a read takes longer, so its pace averages over
  more work.
