---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Store latency, simulated
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
theme()

# The runs this page reads, rerun 2026-10-25 on b6c2605 (docs/HYPOTHESES.md at 1145f89): the logged 4 MiB
# uncompacted recency run without the payload check (every arm and reader model in one run), and
# the uniform run without compaction. Both count instructions, so CPU is modeled time (work_ms).
RECENCY = "sim-recency-4m-noverify-aae85e9fd620/20261025T222739Z"
UNIFORM = "local-m1-uniform-nocompact-717579acbd22/20261025T210333Z"
READER = "cached"  # one thread, each file's footer and deletes loaded once per read
ARMS = ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
PROFILES = ["cpu only", "s3x", "azure premium", "s3", "azure"]

profiles = q("FROM store_profiles")


def phases(run, arms):
    return q("""
        SELECT p.profile, p.arm, m.arm_label, m.ord,
               avg(coalesce(work_ms, cpu_ms)) FILTER (WHERE phase = 'batch' AND step > 0) AS batch_cpu,
               avg(request_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch_req,
               avg(transfer_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch_xfer,
               avg(coalesce(work_ms, cpu_ms)) FILTER (WHERE phase = 'read_full' AND reader = $reader AND step > 0) AS read_cpu,
               avg(request_ms) FILTER (WHERE phase = 'read_full' AND reader = $reader AND step > 0) AS read_req,
               avg(transfer_ms) FILTER (WHERE phase = 'read_full' AND reader = $reader AND step > 0) AS read_xfer,
               max(coalesce(work_ms, cpu_ms) + request_ms + transfer_ms)
                   FILTER (WHERE phase = 'read_full' AND reader = $reader AND step = 60) AS read60,
               max(coalesce(work_ms, cpu_ms) + request_ms + transfer_ms)
                   FILTER (WHERE phase = 'read_full' AND reader = 'stock' AND step = 60) AS stock60
        FROM phase_latency p JOIN arm_meta m USING (arm)
        WHERE p.run_id = $run AND list_contains($arms, p.arm)
        GROUP BY ALL""", run=run, arms=arms, reader=READER)


rec = phases(RECENCY, ARMS)
uni = phases(UNIFORM, ["mor-v3", "cow", "tailcow-c", "rmow-b", "rmow-c"])  # batches only
for t in (rec, uni):
    t["batch"] = t.batch_cpu + t.batch_req + t.batch_xfer
    t["read"] = t.read_cpu + t.read_req + t.read_xfer

# the sweep: generic stores where every request costs L ms and every MiB 1000 / B ms
L = [0.3, 1, 2, 3, 5, 10, 20, 30, 50, 100]
B = [25, 50, 100, 200, 400, 800, 1600]
sweep = q("""
    SELECT arm, request_ms, mib_per_s,
           avg(cpu_ms + request_ms_total + transfer_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch,
           avg(cpu_ms + request_ms_total + transfer_ms)
               FILTER (WHERE phase = 'read_full' AND reader = $reader AND step > 0) AS read
    FROM store_sweep($l, $b) WHERE run_id = $run AND list_contains(['mor-v3', 'rmow-c'], arm)
    GROUP BY ALL""", l=L, b=B, run=RECENCY, reader=READER)
w = sweep.pivot_table(index=["request_ms", "mib_per_s"], columns="arm", values=["batch", "read"])
ratio = pd.DataFrame({
    "batch": w[("batch", "rmow-c")] / w[("batch", "mor-v3")],
    "full read": w[("read", "rmow-c")] / w[("read", "mor-v3")],
    "batch + 1 full read": (w[("batch", "rmow-c")] + w[("read", "rmow-c")])
                           / (w[("batch", "mor-v3")] + w[("read", "mor-v3")]),
}).reset_index()


def cell(t, profile, arm, col):
    return t[(t.profile == profile) & (t.arm == arm)].iloc[0][col]


def crossover(col, mib_per_s):
    """The smallest swept request latency at which RMoW-C's latency is at most MoR-v3's."""
    r = ratio[(ratio.mib_per_s == mib_per_s) & (ratio[col] <= 1)]
    return f"{r.request_ms.min():g}" if len(r) else "none"


s3x_c, s3x_m = cell(rec, "s3x", "rmow-c", "batch"), cell(rec, "s3x", "mor-v3", "batch")
batch_req = dict(q("""SELECT arm, avg(requests) FROM steps
                      WHERE run_id = $r AND phase = 'batch' GROUP BY ALL""", r=RECENCY).itertuples(index=False))
N = {
    "req_c": f"{batch_req['rmow-c']:.1f}",
    "req_m": f"{batch_req['mor-v3']:.0f}",
    "s3x_batch_c": f"{s3x_c:.0f}",
    "s3x_batch_m": f"{s3x_m:.0f}",
    "s3x_batch_cpu_c": f"{cell(rec, 's3x', 'rmow-c', 'batch_cpu'):.0f}",
    "s3x_batch_cpu_m": f"{cell(rec, 's3x', 'mor-v3', 'batch_cpu'):.0f}",
    "az_batch_c": f"{cell(rec, 'azure', 'rmow-c', 'batch'):.0f}",
    "az_batch_m": f"{cell(rec, 'azure', 'mor-v3', 'batch'):.0f}",
    "read_cpu_c": f"{cell(rec, 's3x', 'rmow-c', 'read_cpu') / 1000:.2f}",
    "read_cpu_m": f"{cell(rec, 's3x', 'mor-v3', 'read_cpu') / 1000:.2f}",
    "s3x_read_c": f"{cell(rec, 's3x', 'rmow-c', 'read') / 1000:.2f}",
    "s3x_read_m": f"{cell(rec, 's3x', 'mor-v3', 'read') / 1000:.2f}",
    "az_cpu_share_c": f"{100 * cell(rec, 'azure', 'rmow-c', 'read_cpu') / cell(rec, 'azure', 'rmow-c', 'read'):.0f}%",
    "az_cpu_share_m": f"{100 * cell(rec, 'azure', 'mor-v3', 'read_cpu') / cell(rec, 'azure', 'mor-v3', 'read'):.0f}%",
    "s3x_read60_c": f"{cell(rec, 's3x', 'rmow-c', 'read60') / 1000:.1f}",
    "s3x_read60_m": f"{cell(rec, 's3x', 'mor-v3', 'read60') / 1000:.1f}",
    "az_read60_c": f"{cell(rec, 'azure', 'rmow-c', 'read60') / 1000:.1f}",
    "az_read60_m": f"{cell(rec, 'azure', 'mor-v3', 'read60') / 1000:.1f}",
    "cross_batch_100": crossover("batch", 100),
    "cross_batch_400": crossover("batch", 400),
    "cross_both_100": crossover("batch + 1 full read", 100),
    "read_ratio_max": f"{ratio['full read'].max():.2f}",
    "read_ratio_min": f"{ratio['full read'].min():.2f}",
    "uni_s3x_c": f"{cell(uni, 's3x', 'rmow-c', 'batch') / 1000:.1f}",
    "uni_s3x_m": f"{cell(uni, 's3x', 'mor-v3', 'batch') / 1000:.2f}",
    "uni_az_c": f"{cell(uni, 'azure', 'rmow-c', 'batch') / 1000:.1f}",
    "uni_az_m": f"{cell(uni, 'azure', 'mor-v3', 'batch') / 1000:.1f}",
}
```

What would each arm's batches and reads take on a given store? This page simulates it from the
local runs, which record exactly what every phase does: its requests by kind, its bytes read and
written, and its CPU. The store's latencies are a model, so the answers are as good as the
model. Where the S3 Express runs come in is to test it.

## The model

A phase that issues one request at a time takes about:

- **its CPU**: its instructions at its arm's pace (lab/pages/cpu-measure.md), from runs that
  count them. Thread CPU time moves with the laptop's clock;
- **plus, for every request, its expected latency.** That is a lognormal's mean, median ×
  e^(σ²/2), by kind: GET (and HEAD), PUT (and DELETE), or append;
- **plus its MiB read and written times a rate per MiB.**

That covers every batch and compaction, and every read under a reader on one thread. For a run
with a request log (`--log-requests`), `figures.replay` also schedules a read's split tasks on
its reader's threads and finds its critical path, and can charge GETs per appended part. The
same model, injected into a run (`store.latency`), makes each request sleep its expected latency;
[the validation](#validation) compares the two.

**The store profiles** come from the endive simulator (`~/work/catalog/endive`, commit 2c4c0af):
- GET and PUT latencies and the per-MiB write rate are from `endive/providers/*.toml`;
- appends are from `docs/analysis/distributions.json`, a single writer's median;
- HEADs are charged as GETs and DELETEs as PUTs (not measured);
- the per-MiB read rate is assumed equal to the write rate, because endive's reads were measured
  on small objects only. The sweep below varies it.

S3 Standard has no append, so appendable RMoW cannot run there; split RMoW can.

```{code-cell} ipython3
:tags: [remove-input]

table(profiles.rename(columns={
    "get_ms": "GET ms (median)", "get_sigma": "σ", "put_ms": "PUT ms (median)",
    "put_sigma": "σ ", "append_ms": "append ms (median)", "append_sigma": "σ  ",
    "read_ms_per_mib": "read ms/MiB", "write_ms_per_mib": "write ms/MiB"}))
```

**Runs.** These are pinned by id, from the rerun of 2026-10-25 on the current build:
- the 4 MiB uncompacted recency run without the payload check (`sim-recency-4m-noverify`), with
  every arm and reader model in one run and its requests logged. The check is the harness's, not
  an engine's, and it was most of a read's CPU ([below](#payload-check));
- the uniform run (`local-m1-uniform-nocompact`), for its batches only.

Both count each thread's instructions, so CPU is modeled time.

**Measures.** A batch is the mean over the 60 batches. A full read is under the `cached` reader
(one thread, each file's footer and deletes loaded once per read, as an engine with caches
would), the mean over the probes after the initial load, or at batch 60.

## Per store profile (recency, 4 MiB)

```{code-cell} ipython3
:label: latency-by-profile
:tags: [remove-input]
:class: col-page-right

ARM_ORDER = ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
color, shape = figures_module().arm_encoding(ARM_ORDER)
long = rec.melt(id_vars=["profile", "arm", "arm_label", "ord"], value_vars=["batch", "read"],
                var_name="measure", value_name="ms").dropna()
long["measure"] = long.measure.map({"batch": "a batch (ms)", "read": "a full read (ms)"})
(alt.Chart(long).mark_point(filled=True, size=80)
 .encode(x=alt.X("ms:Q", scale=alt.Scale(type="log"), title=None,
                 axis=alt.Axis(labelOverlap="greedy")),
         y=alt.Y("profile:N", sort=PROFILES, title=None),
         color=color, shape=shape,
         tooltip=["profile", "arm_label", alt.Tooltip("ms:Q", format=",.0f")])
 .properties(width=380, height=160)
 .facet(column=alt.Column("measure:N", title=None))
 .resolve_scale(x="independent"))
```

Each point is an arm's simulated latency on a store, on a log scale. Appendable RMoW has no
point on S3 Standard. The table splits each into CPU, requests and transfer.

```{code-cell} ipython3
:tags: [remove-input]

t = rec.sort_values(["profile", "ord"])[
    ["profile", "arm", "batch", "batch_cpu", "batch_req", "batch_xfer",
     "read", "read_cpu", "read_req", "read_xfer", "read60", "stock60"]]
table(t.rename(columns={
    "batch": "batch ms", "batch_cpu": "CPU", "batch_req": "requests", "batch_xfer": "transfer",
    "read": "full read ms", "read_cpu": "CPU ", "read_req": "requests ", "read_xfer": "transfer ",
    "read60": "full read at batch 60", "stock60": "stock reader at batch 60"})
      .round(0).reset_index(drop=True))
```

## Where RMoW-C overtakes MoR-v3

```{code-cell} ipython3
:label: latency-sweep
:tags: [remove-input]
:class: col-page-right

long = ratio.melt(id_vars=["request_ms", "mib_per_s"], var_name="measure", value_name="ratio")
long["log2"] = np.log2(long.ratio)
lim = float(np.ceil(np.abs(long.log2).max() * 2) / 2)
(alt.Chart(long).mark_rect()
 .encode(x=alt.X("request_ms:O", title="request latency (ms)"),
         y=alt.Y("mib_per_s:O", title="transfer (MiB/s)", sort="descending"),
         color=alt.Color("log2:Q", title="log2(RMoW-C / MoR-v3)",
                         scale=alt.Scale(scheme="redblue", reverse=True, domain=[-lim, lim])),
         tooltip=["measure", "request_ms", "mib_per_s", alt.Tooltip("ratio:Q", format=".2f")])
 .properties(width=260, height=180)
 .facet(column=alt.Column("measure:N", title=None,
                          sort=["batch", "full read", "batch + 1 full read"])))
```

The sweep makes every request cost the same latency, whatever its kind, and every MiB the same
time in either direction. Blue cells are where RMoW-C is faster than MoR-v3, red where it is
slower; the colour is the log2 of RMoW-C's latency over MoR-v3's. The table gives the ratios.

```{code-cell} ipython3
:tags: [remove-input]

table(ratio.pivot_table(index="request_ms", columns="mib_per_s", values="batch").round(2)
      .reset_index().rename(columns={"request_ms": "batch: request ms \\ MiB/s"}))
```

```{code-cell} ipython3
:tags: [remove-input]

table(ratio.pivot_table(index="request_ms", columns="mib_per_s", values="full read").round(2)
      .reset_index().rename(columns={"request_ms": "full read: request ms \\ MiB/s"}))
```

(validation)=
## Validation: replay against injected latency

```{code-cell} ipython3
:tags: [remove-cell]

# against wall time, so CPU is the thread CPU the phases actually took
VALIDATE = "sim-validate-s3x-4a6c99b83bb6/20261025T223400Z"
s3x = profiles[profiles.profile == "s3x"].iloc[0].to_dict() | {"part_ms": 0.0}
rv = figures_module().replay(VALIDATE, s3x)
threads = dict(q("SELECT reader, parallelism FROM run_readers WHERE run_id = $r",
                 r=VALIDATE).itertuples(index=False))
rv["threads"] = rv.reader.map(lambda r: threads.get(r, 1) if r else 1)
rv["error"] = (rv.simulated_ms - rv.wall_ms) / rv.wall_ms
one = rv[(rv.threads == 1) & (rv.wall_ms >= 20)]
eight = rv[(rv.threads == 8) & (rv.phase == "read_full")]
N |= {
    "v_one": f"{len(one)}", "v_one_ok": f"{(one.error.abs() <= 0.10).sum()}",
    "v_one_med": f"{100 * one.error.median():+.1f}%", "v_one_worst": f"{100 * one.error.min():+.1f}%",
    "v_eight": f"{len(eight)}", "v_eight_ok": f"{(eight.error.abs() <= 0.20).sum()}",
    "v_eight_med": f"{100 * eight.error.median():+.1f}%",
    "v_eight_worst": f"{100 * eight.error.abs().max():.1f}%",
    "v_one_out": f"{(one.error.abs() > 0.10).sum()}",
    "v_one_under": f"{(one.error < -0.10).sum()}",
    "v_eight_out": f"{(eight.error.abs() > 0.20).sum()}",
}
n_out, n_under = int(N["v_one_out"]), int(N["v_one_under"])
S1 = ("held" if n_out == 0
      else f"contradicted as written: {n_out} {'phase' if n_out == 1 else 'phases'} of {N['v_one']} "
           f"fell outside, {'an underestimate' if n_out == n_under == 1 else 'all underestimates' if n_out == n_under else f'{n_under} of them underestimates'}")
S2 = ("held" if (eight.error.abs() <= 0.20).all()
      else f"contradicted as written: {N['v_eight_out']} of {N['v_eight']} fell outside, at {N['v_eight_worst']}")
```

`configs/sim-validate-s3x.json` runs the S3 Express profile injected into the in-memory store:
- the calibration's workload (200k rows, 20 batches) and five arms;
- readers `one-footer`, `cached`, `cached` on 8 threads (`cached-p8`) and `stock`;
- its requests logged.

Every request sleeps its expected latency, so a phase's wall time is its CPU plus its simulated
waits, with real overlap across threads. The replay of the run's own log predicts each phase from
its requests and CPU alone. The predictions were registered beforehand (docs/HYPOTHESES.md at 1145f89,
"before validating the store simulation").

```{code-cell} ipython3
:label: replay-validation
:tags: [remove-input]

pts = rv[rv.wall_ms >= 5].assign(
    readers=lambda d: np.where(d.threads > 1, "8 threads", "one thread"))
hi = float(max(pts.wall_ms.max(), pts.simulated_ms.max()) * 1.2)
same = alt.Chart(pd.DataFrame({"x": [5, hi]})).mark_line(
    color="#9a9a9a", strokeDash=[4, 4], strokeWidth=1).encode(x="x:Q", y="x:Q")
dots = (alt.Chart(pts).mark_point(filled=True, size=40, opacity=0.7)
        .encode(x=alt.X("wall_ms:Q", scale=alt.Scale(type="log", domain=[5, hi], nice=False),
                        title="measured wall time with latency injected (ms)"),
                y=alt.Y("simulated_ms:Q", scale=alt.Scale(type="log", domain=[5, hi], nice=False),
                        title="replayed from the request log (ms)"),
                color=alt.Color("phase:N", title="phase"),
                shape=alt.Shape("readers:N", title="readers"),
                tooltip=["arm", "step", "phase", "reader", alt.Tooltip("wall_ms:Q", format=".1f"),
                         alt.Tooltip("simulated_ms:Q", format=".1f"),
                         alt.Tooltip("error:Q", format="+.1%")]))
alt.layer(same, dots).properties(width=420, height=360)
```

Each point is one outermost phase of 5 ms or more. The dashed line is "the same", and both axes
are logarithmic. The table summarizes the errors by phase and thread count.

```{code-cell} ipython3
:tags: [remove-input]

summ = (rv[rv.wall_ms >= 20].groupby(["phase", "threads"]).error
        .agg(phases="count", median="median", worst=lambda e: e.iloc[e.abs().argmax()],
             within_10_pct=lambda e: (e.abs() <= 0.10).mean())
        .reset_index())
table(summ.round(3))
```

- **One thread.** {eval}`N["v_one_ok"]` of {eval}`N["v_one"]` phases of 20 ms or more replay
  within 10% of their wall time. The median error is {eval}`N["v_one_med"]` and the worst
  {eval}`N["v_one_worst"]`. The replay runs slightly fast: wall time also holds work the model
  leaves out, such as garbage collection and scheduling.
- **Eight threads.** {eval}`N["v_eight_ok"]` of {eval}`N["v_eight"]` full reads replay within 20%
  (median {eval}`N["v_eight_med"]`, worst {eval}`N["v_eight_worst"]`).
- **Against the registered predictions:**
  - S1 (every one-thread phase within 10%): {eval}`S1`.
  - S2 (8-thread full reads within 20%): {eval}`S2`.
- **What it checks, and what it does not.** The replay and the injection agree: the model is
  applied consistently, and the critical-path scheduling matches real threads. Whether the
  model's parameters are S3 Express's is a separate question, for the S4 spike.

## Parallel reads and appended parts

```{code-cell} ipython3
:tags: [remove-cell]

LOGGED = RECENCY
rg4 = q("""SELECT run_id FROM canonical_runs WHERE label = 'local-rg-sweep-recency-exp-rg4m'
           AND git_sha = (SELECT git_sha FROM runs WHERE run_id = $r)""", r=RECENCY).run_id[0]
rgd = q("SELECT * FROM steps_diff($a, $b) WHERE NOT one_run_only", a=rg4, b=RECENCY)
N |= {"rg_records": f"{len(rgd):,}", "rg_differ": f"{int((rgd.differs.map(len) > 0).sum())}"}
fig = figures_module()
reps = []
for name in ["s3x", "s3", "azure"]:
    prof = profiles[profiles.profile == name].iloc[0].to_dict() | {"part_ms": 0.0}
    reps.append(fig.replay(LOGGED, prof, cpu="work").assign(profile=name))
lg = pd.concat(reps)
appendable = lg.arm.isin(["rmow-b", "rmow-c"])
lg = lg[~(appendable & (lg.profile == "s3"))]  # no append on S3 Standard
full60 = lg[(lg.phase == "read_full") & (lg.step == 60) & lg.reader.isin(["cached", "cached-p8"])]
PART_MS = [0.0, 0.02, 0.05, 0.1, 0.2]
s3x = profiles[profiles.profile == "s3x"].iloc[0].to_dict()
parts = pd.concat([
    fig.replay(LOGGED, s3x | {"part_ms": pm}, cpu="work").assign(part_ms=pm) for pm in PART_MS])
parts = parts[(parts.phase == "read_full") & (parts.step == 60)
              & parts.reader.isin(["cached", "cached-p8"])
              & parts.arm.isin(["mor-v3", "tailcow-c", "rmow-b", "rmow-c"])]
ev = q("""SELECT arm, seq, kind, object FROM requests WHERE run_id = $r ORDER BY arm, seq""",
       r=LOGGED)
obj = ev.arm + "/" + ev.object
gen = ev.kind.isin(["put", "delete"]).groupby(obj).cumsum()
app = (ev.kind == "append").astype(int)
ev["parts"] = app.groupby([obj, gen]).cumsum() - app
gets = ev[ev.kind.isin(["get_range", "get_stream"])]
part_stats = gets.groupby("arm").parts.agg(GETs="count", mean="mean", median="median",
                                           maximum="max").reset_index()


def f60(profile, arm, reader):
    return full60[(full60.profile == profile) & (full60.arm == arm) & (full60.reader == reader)
                  ].simulated_ms.iloc[0]


def part_ms(arm, reader, pm):
    return parts[(parts.arm == arm) & (parts.reader == reader) & (parts.part_ms == pm)
                 ].simulated_ms.iloc[0]


rc = part_stats.set_index("arm").loc["rmow-c"]
N |= {
    "p8_mor": f"{f60('s3x', 'mor-v3', 'cached-p8'):.0f}",
    "p8_tc": f"{f60('s3x', 'tailcow-c', 'cached-p8'):.0f}",
    "p8_rc": f"{f60('s3x', 'rmow-c', 'cached-p8'):.0f}",
    "p1_mor": f"{f60('s3x', 'mor-v3', 'cached') / 1000:.1f}",
    "p1_rc": f"{f60('s3x', 'rmow-c', 'cached') / 1000:.1f}",
    "az_p8_mor": f"{f60('azure', 'mor-v3', 'cached-p8'):.0f}",
    "az_p8_tc": f"{f60('azure', 'tailcow-c', 'cached-p8'):.0f}",
    "az_p8_rc": f"{f60('azure', 'rmow-c', 'cached-p8'):.0f}",
    "p8_split": f"{f60('s3x', 'rmow-c-split', 'cached-p8'):.0f}",
    "parts_mean": f"{rc['mean']:.0f}", "parts_max": f"{rc['maximum']:.0f}",
    "pm_rc_0": f"{part_ms('rmow-c', 'cached-p8', 0.0):.0f}",
    "pm_rc_01": f"{part_ms('rmow-c', 'cached-p8', 0.1):.0f}",
    "pm_mor": f"{part_ms('mor-v3', 'cached-p8', 0.1):.0f}",
}
```

`configs/sim-recency-4m-noverify.json` is the 4 MiB recency setting with the requests logged, a
JIT warmup, the payload check off, and four readers: `one-footer`, `cached` (deletes once per
file), `cached-p8` (the same on 8 threads) and `stock`. Its requests and bytes match the 4 MiB
rg-sweep run's ({eval}`N["rg_differ"]` of {eval}`N["rg_records"]` shared records differ). The replay times
every phase, reads on 8 threads included.

```{code-cell} ipython3
:label: parallel-reads
:tags: [remove-input]
:class: col-page-right

color, shape = figures_module().arm_encoding(ARM_ORDER)
pts = full60.merge(q("SELECT arm, arm_label FROM arm_meta"), on="arm").assign(
    threads=lambda d: d.reader.map({"cached": "one thread", "cached-p8": "8 threads"}))
(alt.Chart(pts).mark_point(filled=True, size=80)
 .encode(x=alt.X("simulated_ms:Q", scale=alt.Scale(type="log"), title="a full read at batch 60 (ms)",
                 axis=alt.Axis(labelOverlap="greedy")),
         y=alt.Y("threads:N", title=None, sort=["one thread", "8 threads"]),
         color=color, shape=shape,
         tooltip=["profile", "arm_label", "reader", alt.Tooltip("simulated_ms:Q", format=",.0f")])
 .properties(width=300, height=90)
 .facet(column=alt.Column("profile:N", title=None, sort=["s3x", "s3", "azure"])))
```

Each point is an arm's simulated full read at batch 60, under the `cached` reader on one thread
or on 8, on a log scale. Appendable RMoW has no point on S3 Standard.

```{code-cell} ipython3
:tags: [remove-input]

table(full60.pivot_table(index="arm", columns=["profile", "reader"], values="simulated_ms")
      .round(0).reset_index())
```

**Appended parts.** A GET of an appendable object may cost more the more parts the object was
appended in. The S4 spike will measure that on S3 Express. This sweep charges each GET an extra
`part_ms` per part, on S3 Express. It shows how much it could matter: appendable RMoW's GETs hit
objects of this many parts.

```{code-cell} ipython3
:tags: [remove-input]

table(part_stats.round(1))
```

```{code-cell} ipython3
:label: part-cost
:tags: [remove-input]

pp = parts.merge(q("SELECT arm, arm_label FROM arm_meta"), on="arm")
color4, shape4 = figures_module().arm_encoding(["mor-v3", "tailcow-c", "rmow-b", "rmow-c"])
(alt.Chart(pp).mark_line(point=True, strokeWidth=2)
 .encode(x=alt.X("part_ms:Q", title="extra GET latency per appended part (ms)"),
         y=alt.Y("simulated_ms:Q", title="full read at batch 60 (ms)"),
         color=color4, shape=shape4,
         tooltip=["arm_label", "reader", "part_ms", alt.Tooltip("simulated_ms:Q", format=",.0f")])
 .properties(width=260, height=200)
 .facet(column=alt.Column("reader:N", title=None, sort=["cached", "cached-p8"]))
 .resolve_scale(y="independent"))
```

Each line is an arm's simulated full read at batch 60 on S3 Express, against the extra GET
latency per appended part. The table gives the values.

```{code-cell} ipython3
:tags: [remove-input]

table(parts.pivot_table(index=["reader", "arm"], columns="part_ms", values="simulated_ms")
      .round(0).reset_index())
```

(payload-check)=
## Without the payload check

This section is a record, not computed. Its runs were from one build on 2026-10-23:
- `sim-recency-4m`, with the check (`sim-recency-4m-55c5aea6ba08/20261023T182633Z`);
- `sim-recency-4m-noverify`, without it (`…-f412f1efff2d/20261023T195501Z`).

The second was replaced by the rerun of 2026-10-25 and deleted on 2026-10-26. The numbers below
were computed from both runs before then, on thread CPU (docs/EXPERIMENTS.md, Retired runs).

The payload check (`verifyRows`) checks every decoded row's payload against the op log. It is
the harness's, not an engine's, and it is the same in every arm. `sim-recency-4m-noverify` was
`sim-recency-4m` with the check off. Its build also planned a split file's objects per row group,
as every other file is planned, where the earlier build ran each object as one task. The
predictions were registered beforehand (docs/HYPOTHESES.md at 1145f89, "before rerunning the logged 4 MiB
recency setting without payload checks").

| arm | read CPU with the check (s) | without | ratio | batch CPU with (s) | without | ratio |
|---|---|---|---|---|---|---|
| MoR v3 DV | 98.599 | 31.005 | 0.314 | 5.209 | 5.062 | 0.972 |
| TailCoW-C | 96.247 | 29.345 | 0.305 | 5.735 | 5.715 | 0.996 |
| RMoW-B | 91.774 | 26.409 | 0.288 | 24.074 | 24.646 | 1.024 |
| RMoW-C | 91.300 | 26.406 | 0.289 | 26.265 | 26.385 | 1.005 |
| RMoW-B-split | 92.647 | 27.229 | 0.294 | 24.623 | 24.261 | 0.985 |
| RMoW-C-split | 89.130 | 28.020 | 0.314 | 26.785 | 26.106 | 0.975 |

The table sums each arm's CPU over its one-thread reads (full, recent and old, under every
reader but `cached-p8`) and over its batches, in both runs.

- **N1, held: requests and bytes are unchanged, except stock reads of split files.** 29 of 2,277
  records differ, all of them split arms' reads under `stock`. That reader now reads the
  version's footer once per split rather than once per object: 206 GETs over RMoW-B-split's
  full reads, against 154. Their bytes grow by the footers, which N1 did not mention.
- **N2, held: reads lose most of their CPU, batches none.** Reads take 0.29–0.31× the CPU without
  the check, and batches 0.97–1.02×.
- **N3, held: on one thread, RMoW's read advantage grows.** RMoW-C's full read at batch 60 takes
  0.67× MoR-v3's on S3 Express (0.78× with the check), and 0.33× on Azure Blob (0.45×).
- **N4, held: on eight threads, parallelism still erases most of it where requests are cheap.**
  Under `cached-p8`, it takes 1.01× MoR-v3's on S3 Express (1.04×), and 0.53× on Azure Blob
  (0.59×).
- **N5, held: split files read in parallel.** RMoW-C-split's 8-thread full read takes 347 ms on
  S3 Express, against RMoW-C's 380. With one task per object it took 1,882 ms, against 563.
- **After the run**, one fix: a footer-cache reader now reads a split object of at most 1 MiB,
  which Iceberg fetches whole, in one task, as it reads small files. The run's build gave each
  such object a task per row group, and each fetched it whole again. That changes none of this
  run's records, whose footer-cache reads match the run with the check exactly, but it did change
  a compacting run's (docs/EXPERIMENTS.md).

## Findings

- **On S3 Express, RMoW-C's batches are slower, and CPU is why.**
  - A batch takes about {eval}`N["s3x_batch_c"]` ms against MoR-v3's {eval}`N["s3x_batch_m"]`.
  - Of that, {eval}`N["s3x_batch_cpu_c"]` ms is CPU, re-encoding the regions it rewrites,
    against MoR-v3's {eval}`N["s3x_batch_cpu_m"]`.
  - S3 Express's requests are too cheap for RMoW-C's {eval}`N["req_c"]` requests against
    MoR-v3's {eval}`N["req_m"]` to make up
    for it.
- **Where requests are dear, RMoW-C's batches are faster.**
  - On Azure Blob a batch takes {eval}`N["az_batch_c"]` ms against {eval}`N["az_batch_m"]`.
  - In the sweep, at 100 MiB/s the first request latency at which RMoW-C's batch is faster is
    {eval}`N["cross_batch_100"]` ms; at 400 MiB/s, {eval}`N["cross_batch_400"]` ms. The swept
    values are coarse: the crossover lies between that value and the one below it.
  - Counting one full read per batch as well, RMoW-C is faster from
    {eval}`N["cross_both_100"]` ms per request at 100 MiB/s.
- **RMoW-C's full reads are faster everywhere in the sweep.**
  - On S3 Express a full read is still mostly decoding: {eval}`N["read_cpu_c"]` of
    {eval}`N["s3x_read_c"]` s for RMoW-C, and {eval}`N["read_cpu_m"]` of {eval}`N["s3x_read_m"]`
    s for MoR-v3. On Azure Blob, CPU is {eval}`N["az_cpu_share_c"]` of RMoW-C's and
    {eval}`N["az_cpu_share_m"]` of MoR-v3's.
  - RMoW-C's fewer GETs and bytes add to that lead as requests get dearer. Its full read takes
    {eval}`N["read_ratio_min"]`–{eval}`N["read_ratio_max"]`× MoR-v3's across the sweep.
  - At batch 60 a full read takes {eval}`N["s3x_read60_c"]` s against {eval}`N["s3x_read60_m"]`
    s on S3 Express, and {eval}`N["az_read60_c"]` s against {eval}`N["az_read60_m"]` s on Azure
    Blob.
- **On 8 threads, RMoW's read advantage shrinks to nothing on S3 Express.**
  - Decode CPU spreads across threads, and so do MoR's many small files. At batch 60 a full read
    takes {eval}`N["p8_mor"]` ms for MoR-v3, {eval}`N["p8_tc"]` for TailCoW-C and
    {eval}`N["p8_rc"]` for RMoW-C. On one thread it is {eval}`N["p1_mor"]` s against
    {eval}`N["p1_rc"]` s.
  - Where requests are dear it still pays. On Azure Blob, on 8 threads, MoR-v3 takes
    {eval}`N["az_p8_mor"]` ms, TailCoW-C {eval}`N["az_p8_tc"]` and RMoW-C {eval}`N["az_p8_rc"]`.
  - Split files read like appendable ones: RMoW-C-split takes {eval}`N["p8_split"]` ms on 8
    threads, now that its objects are planned per row group.
- **Appended parts matter little to a full read.** RMoW-C's GETs hit objects of
  {eval}`N["parts_mean"]` parts on average (up to {eval}`N["parts_max"]`), but a full read makes
  few GETs. At an extra 0.1 ms per part, its 8-thread full read goes from {eval}`N["pm_rc_0"]`
  to {eval}`N["pm_rc_01"]` ms, while MoR-v3's stays at {eval}`N["pm_mor"]`. The cost would weigh
  more on reads made of many small GETs, and on files appended to for longer. The S4 spike
  measures what S3 Express does.
- **The payload check was most of a read's CPU.** Reads took
  0.29–0.31× the CPU without it, so the earlier
  simulated reads were mostly the harness's own work ([above](#payload-check)).
- **Under uniform updates, RMoW's copy-on-write costs time on any store.** A batch takes
  {eval}`N["uni_s3x_c"]` s against MoR-v3's {eval}`N["uni_s3x_m"]` s on S3 Express, and
  {eval}`N["uni_az_c"]` s against {eval}`N["uni_az_m"]` s on Azure Blob.

## Limits

- **The model is checked against itself, not against S3.** Injected latency confirms the replay
  ([above](#validation)); the S3 Express spike would check the parameters.
- **CPU is this machine's, and the harness's.** It is modeled time from each phase's instruction
  counts (lab/pages/cpu-measure.md), with the payload check off. Reads and writes go through
  Iceberg's generic readers and writers, row by row, which are slower than an engine's
  vectorized path. That weighs most against RMoW, which re-encodes the regions it rewrites.
- **Writes are serial.** An engine would overlap a batch's requests; the replay overlaps only a
  read's split tasks. Within a task, ranges are read one after another.
- **No size effects beyond a per-MiB rate.** Appended parts are only swept, not measured. There
  is no throttling and no tail latency. Expected latencies are means.
