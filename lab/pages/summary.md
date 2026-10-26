---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Summary of results
---

```{code-cell} ipython3
:tags: [remove-cell]

import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# Every number a write-up of these results quotes, from one build: the core experiments rerun on
# 2026-10-25 (b6c2605), and the scale campaign on the instruction measure (9f868af)
BUILD = "b6c2605"
SCALE_BUILD = "9f868af"
labels = dict(q("SELECT arm, arm_label FROM arm_meta").itertuples(index=False))
run = dict(q("""SELECT label, run_id FROM canonical_runs WHERE git_sha LIKE $b || '%'""",
             b=BUILD).itertuples(index=False))
RECENCY, UNIFORM = run["local-m1-recency-exp-nocompact"], run["local-m1-uniform-nocompact"]
LOGGED = run["sim-recency-4m-noverify"]
scale = dict(q("""SELECT CAST(rg_bytes / 1048576 AS INT), run_id FROM canonical_runs
                  WHERE label LIKE 'scale-recency-%' AND git_sha LIKE $b || '%'""",
               b=SCALE_BUILD).itertuples(index=False))
S128 = scale[128]

# a batch and a read, 4 MiB recency without compaction; reads under cached
ARMS = ["mor-v3", "cow", "tailcow-c", "rmow-b", "rmow-c"]
b = q("""
    SELECT arm, avg(bytes_written) / 1e6 AS mb, avg(requests) AS requests,
           avg(puts + appends) AS put_class, avg(gets + heads) AS gets
    FROM steps WHERE run_id = $r AND phase = 'batch' GROUP BY ALL""", r=RECENCY).set_index("arm")
rd = q("""
    SELECT arm, step, gets + heads AS gets, bytes_read / 1e6 AS mb FROM steps
    WHERE run_id = $r AND phase = 'read_full' AND reader = 'cached'""", r=RECENCY).set_index(["arm", "step"])
st = q("""SELECT arm, stored_bytes / live_bytes AS stored_live FROM steps
          WHERE run_id = $r AND phase = 'state' AND step = 60""", r=RECENCY).set_index("arm")
batch = pd.DataFrame({
    "arm": [labels[a] for a in ARMS],
    "MB written per batch": [b.loc[a, "mb"] for a in ARMS],
    "requests per batch": [b.loc[a, "requests"] for a in ARMS],
    "PUT-class": [b.loc[a, "put_class"] for a in ARMS],
    "GETs": [b.loc[a, "gets"] for a in ARMS],
    "full-read GETs, batch 10": [rd.loc[(a, 10), "gets"] for a in ARMS],
    "full-read GETs, batch 60": [rd.loc[(a, 60), "gets"] for a in ARMS],
    "full-read MB, batch 60": [rd.loc[(a, 60), "mb"] for a in ARMS],
    "stored ÷ live, batch 60": [st.loc[a, "stored_live"] for a in ARMS],
})
uni = q("""SELECT arm, avg(bytes_written) AS b FROM steps WHERE run_id = $r AND phase = 'batch'
           GROUP BY ALL""", r=UNIFORM).set_index("arm").b

# row-group size: RMoW-B and -C against MoR-v3, 1 to 8 MiB
sw = fig.sweep_summary()
swm = sw[sw.arm == "mor-v3"].set_index("rg_mib")
swr = sw[sw.arm.isin(["rmow-b", "rmow-c"])].set_index("rg_mib")
ratio = lambda col: swr[col] / swm.loc[swr.index, col]

# Iceberg's defaults, 128 MiB, the instruction measure
sb = q("""SELECT arm, avg(requests) AS requests, avg(bytes_written) AS b FROM steps
          WHERE run_id = $r AND phase = 'batch' GROUP BY ALL""", r=S128).set_index("arm")
lat128 = q("""
    SELECT arm, profile, max(work_ms + request_ms + transfer_ms)
        FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step = 60) AS read60
    FROM phase_latency WHERE run_id = $r GROUP BY ALL""", r=S128).set_index(["profile", "arm"]).read60
profiles = q("FROM store_profiles")
s3x = profiles[profiles.profile == "s3x"].iloc[0].to_dict() | {"part_ms": 0.0}
azure = profiles[profiles.profile == "azure"].iloc[0].to_dict() | {"part_ms": 0.0}


def p8(run_id, prof, step=60):
    r = fig.replay(run_id, prof, cpu="work")
    r = r[(r.phase == "read_full") & (r.reader == "cached-p8") & (r.step == step)].set_index("arm")
    return r.simulated_ms


p8_128 = p8(S128, s3x)
st128 = q("""SELECT arm, stored_bytes / live_bytes AS sl FROM steps
             WHERE run_id = $r AND phase = 'state' AND step = 60""", r=S128).set_index("arm").sl

# compaction (lab/pages/compaction.md)
C = fig.compaction_analysis(BUILD)

# store latency, 4 MiB recency, the logged run, the instruction measure
lat = q("""
    SELECT arm, profile,
           avg(work_ms + request_ms + transfer_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch,
           max(work_ms + request_ms + transfer_ms)
               FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step = 60) AS read60
    FROM phase_latency WHERE run_id = $r GROUP BY ALL""", r=LOGGED).set_index(["profile", "arm"])
p8_s3x, p8_az = p8(LOGGED, s3x), p8(LOGGED, azure)
sweep = q("""
    SELECT arm, request_ms, avg(cpu_ms + request_ms_total + transfer_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch
    FROM store_sweep([0.3, 1, 2, 3, 5, 10, 20, 30, 50, 100], [100]) WHERE run_id = $r
      AND arm IN ('mor-v3', 'rmow-c') GROUP BY ALL""", r=LOGGED).pivot(index="request_ms", columns="arm", values="batch")
cross = sweep[sweep["rmow-c"] <= sweep["mor-v3"]].index.min()


def policy_words(policy):
    """A compaction policy's name, as prose: 'any level P=5' -> 'every 5 batches, at any threshold'."""
    if policy.startswith("none"):
        return "no compaction"
    level, period = policy.split(" P=")
    every = "every batch" if period == "1" else f"every {period} batches"
    return f"{every}, at any threshold" if level == "any level" else f"{every}, at the {level} thresholds"


def rng(xs, fmt="{:.2f}"):
    lo, hi = fmt.format(min(xs)), fmt.format(max(xs))
    return lo if lo == hi else f"{lo}–{hi}"


N = {
    "rc_mb_x": f"{b.loc['rmow-c', 'mb'] / b.loc['mor-v3', 'mb']:.0f}",
    "rb_mb_x": f"{b.loc['rmow-b', 'mb'] / b.loc['mor-v3', 'mb']:.0f}",
    "rc_req": f"{b.loc['rmow-c', 'requests']:.1f}", "mor_req": f"{b.loc['mor-v3', 'requests']:.0f}",
    "rc_req_x": f"{b.loc['rmow-c', 'requests'] / b.loc['mor-v3', 'requests']:.2f}",
    "rb_req_x": f"{b.loc['rmow-b', 'requests'] / b.loc['mor-v3', 'requests']:.2f}",
    "mor_g10": f"{rd.loc[('mor-v3', 10), 'gets']:.0f}", "mor_g60": f"{rd.loc[('mor-v3', 60), 'gets']:.0f}",
    "rc_g10": f"{rd.loc[('rmow-c', 10), 'gets']:.0f}", "rc_g60": f"{rd.loc[('rmow-c', 60), 'gets']:.0f}",
    "rc_mb60": f"{rd.loc[('rmow-c', 60), 'mb']:.1f}", "mor_mb60": f"{rd.loc[('mor-v3', 60), 'mb']:.1f}",
    "rc_stored": f"{st.loc['rmow-c', 'stored_live']:.1f}",
    "uni_cow": rng([uni[a] / uni["cow"] for a in ["rmow-b", "rmow-c"]], "{:.3f}"),
    "uni_mor": f"{uni['mor-v3'] / uni['cow']:.3f}",
    "sw_mb": rng(ratio("write_mb_per_batch"), "{:.0f}"),
    "sw_req": rng(100 * ratio("requests_per_batch"), "{:.0f}%"),
    "sw_gets": rng(100 * ratio("read_full_gets"), "{:.0f}%"),
    "s_rc_req": f"{sb.loc['rmow-c', 'requests']:.1f}", "s_mor_req": f"{sb.loc['mor-v3', 'requests']:.0f}",
    "s_req_x": f"{sb.loc['rmow-c', 'requests'] / sb.loc['mor-v3', 'requests']:.2f}",
    "s_mb_x": f"{sb.loc['rmow-c', 'b'] / sb.loc['mor-v3', 'b']:.0f}",
    "s_read_s3x": f"{lat128[('s3x', 'rmow-c')] / lat128[('s3x', 'mor-v3')]:.2f}",
    "s_read_az": f"{lat128[('azure', 'rmow-c')] / lat128[('azure', 'mor-v3')]:.2f}",
    "s_p8": rng(p8_128 / p8_128["mor-v3"]),
    "s_stored": f"{st128['rmow-c']:.1f}",
    "c_day_app": C.N["day_app"], "c_day_split": C.N["day_split"], "c_split_vs_app": C.N["split_vs_app"],
    "c_std_best": policy_words(C.N["std_best"]), "c_std_saving": C.N["std_saving"],
    "c_mor_best": policy_words(C.N["mor_best"]), "c_mor_saving": C.N["mor_saving"],
    "c_kept_app": C.N["kept_end_app"], "c_aggr_kept": C.N["aggr_kept"], "c_share5": C.N["share5"],
    "l_batch_s3x": f"{lat.loc[('s3x', 'rmow-c'), 'batch'] / lat.loc[('s3x', 'mor-v3'), 'batch']:.1f}",
    "l_batch_az": f"{lat.loc[('azure', 'rmow-c'), 'batch'] / lat.loc[('azure', 'mor-v3'), 'batch']:.2f}",
    "l_read_s3x": f"{lat.loc[('s3x', 'rmow-c'), 'read60'] / lat.loc[('s3x', 'mor-v3'), 'read60']:.2f}",
    "l_read_az": f"{lat.loc[('azure', 'rmow-c'), 'read60'] / lat.loc[('azure', 'mor-v3'), 'read60']:.2f}",
    "l_p8_s3x": f"{p8_s3x['rmow-c'] / p8_s3x['mor-v3']:.2f}",
    "l_p8_az": f"{p8_az['rmow-c'] / p8_az['mor-v3']:.2f}",
    "l_cross": f"{cross:g}",
}
```

This page collects the numbers a write-up of these results quotes. Each comes from one build,
and is decided, with its context, on the page it links to. The core experiments are the rerun
of 2026-10-25 on `b6c2605` (docs/HYPOTHESES.md); the scale campaign is the instruction-measure
campaign on `9f868af`. Simulated latencies are simulated: requests, bytes and modeled CPU,
priced with a store profile (Store latency, simulated). The S3 Express run that would anchor
them has not happened (docs/S3X_RUNBOOK.md).

## Provenance

This section is a record. It was computed on 2026-10-26 from the reruns and the runs they
replaced, before those were deleted (docs/EXPERIMENTS.md, Retired runs). The rerun replaced 24
runs. The 13 whose old runs came from builds with the same write path, writer footers aside,
were checked against them (the compaction sweep's predate its triggers):
- **MoR family:** every MoR-v2 and MoR-v3 batch matched its old run exactly (1,700 of 1,700).
- **CoW and TailCoW:** their batches took fewer requests in 617 batches and more in none, where
  their merge reads now reuse a footer their locate scan read. None wrote different bytes.
- **RMoW:** every one of its 2,460 batches now reads its files' footers (1–4 more requests),
  which earlier builds left out (README, I/O parity rule 6). None wrote different bytes.
- **Readers that did not change:** 3,486 of 3,486 probes matched exactly.

## A batch, and a read

Recency-skewed updates at 4 MiB row groups, without compaction (`local-m1-recency-exp-nocompact`;
Layout against tail policy, Row-group size sweep). Reads are under `cached`.

```{code-cell} ipython3
:tags: [remove-input]

table(batch.round(2))
```

- **RMoW writes an order of magnitude more bytes per batch:** RMoW-C {eval}`N["rc_mb_x"]`× and
  RMoW-B {eval}`N["rb_mb_x"]`× MoR-v3's.
- **In a fraction of the requests:** RMoW-C takes {eval}`N["rc_req"]` per batch against MoR-v3's
  {eval}`N["mor_req"]` ({eval}`N["rc_req_x"]`×), RMoW-B {eval}`N["rb_req_x"]`×.
- **RMoW-C's full read stays flat while MoR-v3's grows:** {eval}`N["rc_g10"]` GETs at batch 10
  and {eval}`N["rc_g60"]` at batch 60, against MoR-v3's {eval}`N["mor_g10"]` and
  {eval}`N["mor_g60"]`. It reads {eval}`N["rc_mb60"]` MB against {eval}`N["mor_mb60"]`.
- **Uncompacted, RMoW keeps its dead regions:** RMoW-C stores {eval}`N["rc_stored"]`× its live
  bytes at batch 60.
- **Under uniform updates RMoW writes what copy-on-write writes (H2):**
  {eval}`N["uni_cow"]`× CoW's bytes per batch, against MoR-v3's {eval}`N["uni_mor"]`×.

## Row-group size

From 1 to 8 MiB (Row-group size sweep), RMoW-B and RMoW-C write
{eval}`N["sw_mb"]`× MoR-v3's bytes per batch, in {eval}`N["sw_req"]` of its requests. They read
the table in {eval}`N["sw_gets"]` of its GETs. The balance for this workload is near 2–4 MiB.

## At Iceberg's defaults

128 MiB row groups and 512 MiB files (At Iceberg's defaults), measured in instructions:
- RMoW-C takes {eval}`N["s_rc_req"]` requests per batch against MoR-v3's {eval}`N["s_mor_req"]`
  ({eval}`N["s_req_x"]`×), and writes {eval}`N["s_mb_x"]`× its bytes.
- On one thread, RMoW-C's full read at batch 60 takes {eval}`N["s_read_s3x"]`× MoR-v3's simulated
  time on S3 Express and {eval}`N["s_read_az"]`× on Azure Blob.
- On eight threads every arm comes within {eval}`N["s_p8"]`× of MoR-v3's on S3 Express.
- Uncompacted, RMoW-C stores {eval}`N["s_stored"]`× its live bytes at batch 60.

## Compacting regions

From the compaction sweep (When to compact); costs are per batch, at list prices:
- **Appendable files need compaction for space.** Uncompacted they keep
  {eval}`N["c_kept_app"]`× their live bytes at batch 60. At a day's storage weight on S3 Express,
  their cheapest policy (garbage 0.25, every batch) cuts their cost by
  {eval}`N["c_day_app"]`.
- **Split files need little.** Moving row groups out of mostly dead objects holds them at
  {eval}`N["c_aggr_kept"]`× live, and saves {eval}`N["c_day_split"]` at a day's weight. There
  they cost {eval}`N["c_split_vs_app"]` less than appendable files.
- **At 5-minute batches, no RMoW compaction pays on S3 Express**; storage is at most
  {eval}`N["c_share5"]` of any arm's cost.
- **MoR-v3's compaction depends on the store.** On S3 Standard, compacting
  {eval}`N["c_std_best"]` saves it {eval}`N["c_std_saving"]`, at one full read per batch and
  5-minute batches. On S3 Express it saves at most {eval}`N["c_mor_saving"]`
  ({eval}`N["c_mor_best"]`).

## Simulated latency

The 4 MiB recency setting, logged, measured in instructions (Store latency, simulated):
- **A batch:** RMoW-C takes {eval}`N["l_batch_s3x"]`× MoR-v3's time on S3 Express, where CPU
  dominates, and {eval}`N["l_batch_az"]`× on Azure Blob, where requests do. At 100 MiB/s a batch
  of RMoW-C is faster from {eval}`N["l_cross"]` ms per request.
- **A full read at batch 60, one thread:** {eval}`N["l_read_s3x"]`× MoR-v3's on S3 Express and
  {eval}`N["l_read_az"]`× on Azure Blob.
- **On eight threads:** {eval}`N["l_p8_s3x"]`× on S3 Express and {eval}`N["l_p8_az"]`× on Azure
  Blob.

## Limits

- **Simulated latency.** The model is checked only against itself until the S3 Express run.
- **One seed, local, one workload family.** Recency-skewed and uniform updates over 1M rows (and
  the scale campaign's 16M); no second seed yet (docs/EXPERIMENTS.md, What is next).
- **CPU is the harness's,** through Iceberg's generic row readers and writers, which are slower
  than an engine's vectorized path. That weighs most against RMoW, which re-encodes the regions
  it rewrites.
- **Not modeled:** metadata and commit costs, for every arm.
