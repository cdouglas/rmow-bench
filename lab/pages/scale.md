---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: At Iceberg's defaults
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# The scale campaign (docker/campaigns/scale.json), on havoc from 525be1c: one run per row-group
# size R, in MiB
RUNS = {
    4: "scale-recency-rg4m-4439d54649bf/20261023T233416Z",
    16: "scale-recency-rg16m-c4cc5aa06bf6/20261023T233415Z",
    64: "scale-recency-rg64m-f3a4801d207b/20261023T233414Z",
    128: "scale-recency-rg128m-3c57659206a9/20261023T233413Z",
}
IDS = list(RUNS.values())
R_OF = {v: k for k, v in RUNS.items()}
ARMS = ["mor-v3", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
RMOW = ["rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
labels = dict(q("SELECT arm, arm_label FROM arm_meta").itertuples(index=False))


def with_r(df):
    df["R"] = df.run_id.map(R_OF)
    return df


setup = with_r(q("""
    SELECT r.run_id, r.elapsed_s / 60 AS minutes, r.git_sha, r.cpu_model,
           r.versions ->> 'build_snapshot' AS image,
           max(p.value_num) FILTER (WHERE p.key = 'workload.initialRows') AS initial_rows,
           max(p.value_num) FILTER (WHERE p.key = 'workload.opsPerBatch') AS ops_per_batch,
           max(p.value_num) FILTER (WHERE p.key = 'layout.rowGroupBytes') / 1048576 AS rg_mib,
           max(p.value_num) FILTER (WHERE p.key = 'layout.tailSealBytes') / 1048576 AS seal_mib,
           max(p.value_num) FILTER (WHERE p.key = 'layout.targetFileBytes') / 1048576 AS file_mib,
           j.node, j.cpus, j.owned, j.foreign_cpu_s, j.foreign_mean_cpus, j.foreign_max_cpus
    FROM canonical_runs r JOIN run_params p USING (run_id) JOIN container_jobs j USING (run_id)
    WHERE list_contains($ids, r.run_id) GROUP BY ALL""", ids=IDS)).sort_values("R")
S = setup.set_index("R")

# per arm and run: requests and bytes per batch (mean over the 60), and the full read at batch 60
io = with_r(q("""
    SELECT run_id, arm,
           avg(requests) FILTER (WHERE phase = 'batch') AS batch_req,
           avg(bytes_written) FILTER (WHERE phase = 'batch') AS batch_bytes,
           max(requests) FILTER (WHERE phase = 'read_full' AND step = 60 AND reader = 'cached') AS read_req,
           max(requests) FILTER (WHERE phase = 'read_full' AND step = 60 AND reader = 'cached-p8') AS p8_req,
           max(requests) FILTER (WHERE phase = 'read_full' AND step = 60 AND reader = 'stock') AS stock_req,
           max(bytes_read) FILTER (WHERE phase = 'read_full' AND step = 60 AND reader = 'cached') AS read_bytes,
           max(bytes_read) FILTER (WHERE phase = 'read_full' AND step = 60 AND reader = 'stock') AS stock_bytes
    FROM steps WHERE list_contains($ids, run_id) GROUP BY ALL""", ids=IDS))
MEASURES = {  # column: (panel title, scales with R)
    "batch_req": ("requests per batch", False),
    "read_req": ("requests per full read, cached", False),
    "p8_req": ("requests per full read, cached-p8", False),
    "stock_req": ("requests per full read, stock", False),
    "batch_bytes": ("bytes written per batch", True),
    "read_bytes": ("bytes per full read, cached", True),
    "stock_bytes": ("bytes per full read, stock", True),
}
base = io[io.R == 4].set_index("arm")
rows = []
for _, x in io.iterrows():
    for col, (title, scales) in MEASURES.items():
        expected = base.loc[x.arm, col] * (x.R / 4 if scales else 1)
        rows.append((x.arm, labels[x.arm], x.R, title, x[col], x[col] / expected))
ratios = pd.DataFrame(rows, columns=["arm", "arm_label", "R", "measure", "value", "ratio"])
scaled = ratios[ratios.R > 4]


def ratio(arm, r, col):
    t = MEASURES[col][0]
    return scaled[(scaled.arm == arm) & (scaled.R == r) & (scaled.measure == t)].ratio.iloc[0]


def outside(measures):
    s = scaled[scaled.measure.isin([MEASURES[m][0] for m in measures])]
    return s[(s.ratio < 0.75) | (s.ratio > 1.25)]


p1_out = outside(["batch_req", "read_req", "p8_req", "stock_req"])
p2_out = outside(["batch_bytes", "read_bytes", "stock_bytes"])
rmow_req = scaled[scaled.arm.isin(RMOW) & scaled.measure.str.startswith("requests")]
reads_bytes = scaled[scaled.measure.str.startswith("bytes per full read")]

# the per-batch data files MoR-v3 and TailCoW-C write, against the 1 MiB whole fetch
files = with_r(q("""
    SELECT run_id, arm, median(length) / 1048576 AS mib FROM requests
    WHERE list_contains($ids, run_id) AND phase = 'batch' AND kind = 'put'
      AND object LIKE '%/data/batch-%' GROUP BY ALL""", ids=IDS))
F = files.pivot(index="R", columns="arm", values="mib")
# MoR-v3's batch requests by kind: the tail probe is the aborted open-ended GET
kinds = with_r(q("""
    SELECT run_id, avg(get_range) AS get_range, avg(get_stream) AS get_stream, avg(aborts) AS aborts
    FROM steps WHERE list_contains($ids, run_id) AND arm = 'mor-v3' AND phase = 'locate'
    GROUP BY ALL""", ids=IDS)).set_index("R")

# the table at batch 60
state = with_r(q("""
    SELECT run_id, arm, live_bytes, stored_bytes, row_groups, data_files, delete_files, objects
    FROM steps WHERE list_contains($ids, run_id) AND phase = 'state' AND step = 60""", ids=IDS))
stored = with_r(q("""
    SELECT run_id, arm, max(stored_bytes) / 2 ^ 30 AS gib FROM steps
    WHERE list_contains($ids, run_id) AND phase = 'state' GROUP BY ALL""", ids=IDS))
st128 = stored[stored.R == 128].set_index("arm").gib
G = state.set_index(["R", "arm"])

# the gauge: a split arm reads the same bytes as its appendable twin, so their read CPU should match
twins = with_r(q("""
    WITH s AS (SELECT run_id, arm, step, cpu_ns, bytes_read FROM steps
               WHERE list_contains($ids, run_id) AND phase = 'read_full' AND reader = 'cached')
    SELECT a.run_id, b.arm, a.step, a.cpu_ns / b.cpu_ns AS cpu_ratio,
           a.bytes_read / b.bytes_read AS bytes_ratio
    FROM s a JOIN s b ON b.run_id = a.run_id AND b.step = a.step AND b.arm = replace(a.arm, '-split', '')
    WHERE a.arm LIKE '%-split'""", ids=IDS))
twins["pair"] = twins.arm.map(lambda a: f"{labels[a + '-split']} ÷ {labels[a]}")
early4 = twins[(twins.R == 4) & (twins.step < 60)]
late4 = twins[(twins.R == 4) & (twins.step == 60)]
big = twins[twins.R > 4]

# P3-P5 as measured (not deciding: foreign load)
cpu = with_r(q("""
    SELECT run_id, arm,
           sum(cpu_ns) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0)
             / (sum(bytes_read) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0) / 1048576) / 1e6
             AS read_ms_per_mib,
           sum(cpu_ns) FILTER (WHERE phase = 'batch')
             / (sum(bytes_written) FILTER (WHERE phase = 'batch') / 1048576) / 1e6 AS write_ms_per_mib
    FROM steps WHERE list_contains($ids, run_id) GROUP BY ALL""", ids=IDS)).set_index(["R", "arm"])
lat = with_r(q("""
    SELECT run_id, arm, profile,
           avg(cpu_ms + request_ms + transfer_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch_ms,
           max(cpu_ms + request_ms + transfer_ms)
               FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step = 60) AS read60
    FROM phase_latency WHERE list_contains($ids, run_id) GROUP BY ALL""", ids=IDS))
L = lat[lat.R == 128].set_index(["profile", "arm"])
profiles = q("FROM store_profiles")
s3x = profiles[profiles.profile == "s3x"].iloc[0]
p8 = fig.replay(RUNS[128], s3x)
p8 = p8[(p8.phase == "read_full") & (p8.step == 60) & (p8.reader == "cached-p8")].set_index("arm")
p8_ratio = p8.simulated_ms / p8.simulated_ms["mor-v3"]
batch_profiles = [p for p in profiles.profile if p != "cpu only" and not np.isnan(L.loc[(p, "rmow-c"), "batch_ms"])]
batch_ratio = {p: L.loc[(p, "rmow-c"), "batch_ms"] / L.loc[(p, "mor-v3"), "batch_ms"] for p in batch_profiles}
face = pd.DataFrame([
    ("P3", "read CPU per MiB, cached, 128 ÷ 4 MiB (range over arms)", "within 30%",
     f"{min(cpu.loc[(128, a), 'read_ms_per_mib'] / cpu.loc[(4, a), 'read_ms_per_mib'] for a in ARMS):.2f}–"
     f"{max(cpu.loc[(128, a), 'read_ms_per_mib'] / cpu.loc[(4, a), 'read_ms_per_mib'] for a in ARMS):.2f}"),
    ("P3", "write CPU per MiB, 128 ÷ 4 MiB (range over arms)", "within 30%",
     f"{min(cpu.loc[(128, a), 'write_ms_per_mib'] / cpu.loc[(4, a), 'write_ms_per_mib'] for a in ARMS):.2f}–"
     f"{max(cpu.loc[(128, a), 'write_ms_per_mib'] / cpu.loc[(4, a), 'write_ms_per_mib'] for a in ARMS):.2f}"),
    ("P4", "RMoW-C ÷ MoR-v3, full read at batch 60, cached, S3 Express", "0.9–1.1",
     f"{L.loc[('s3x', 'rmow-c'), 'read60'] / L.loc[('s3x', 'mor-v3'), 'read60']:.2f}"),
    ("P4", "the same, Azure Blob", "at most 0.9",
     f"{L.loc[('azure', 'rmow-c'), 'read60'] / L.loc[('azure', 'mor-v3'), 'read60']:.2f}"),
    ("P4", "RMoW-C ÷ MoR-v3, a batch (range over the profiles it can run on)", "above 1",
     f"{min(batch_ratio.values()):.2f}–{max(batch_ratio.values()):.2f}"),
    ("P5", "every arm ÷ MoR-v3, full read at batch 60, cached-p8, S3 Express", "0.75–1.25",
     f"{p8_ratio.min():.2f}–{p8_ratio.max():.2f}"),
], columns=["prediction", "measure (128 MiB)", "predicted", "as measured"])

N = {
    "R4_min": f"{S.loc[4, 'minutes']:.1f}",
    "R128_min": f"{S.loc[128, 'minutes']:.0f}",
    "f4": f"{S.loc[4, 'foreign_cpu_s']:.0f}",
    "fmean_lo": f"{S.loc[[16, 64, 128], 'foreign_mean_cpus'].min():.1f}",
    "fmean_hi": f"{S.loc[[16, 64, 128], 'foreign_mean_cpus'].max():.1f}",
    "fmax_lo": f"{S.loc[[16, 64, 128], 'foreign_max_cpus'].min():.1f}",
    "fmax_hi": f"{S.loc[[16, 64, 128], 'foreign_max_cpus'].max():.1f}",
    "twin4_dev": f"{100 * (early4.cpu_ratio - 1).abs().max():.0f}%",
    "twin4_last": f"{late4.cpu_ratio.min():.2f}",
    "twin_big_lo": f"{big.cpu_ratio.min():.2f}",
    "twin_big_hi": f"{big.cpu_ratio.max():.2f}",
    "twin_bytes": f"{100 * (twins.bytes_ratio - 1).abs().max():.1f}%",
    "mor_batch_64": f"{ratio('mor-v3', 64, 'batch_req'):.2f}",
    "mor_batch_128": f"{ratio('mor-v3', 128, 'batch_req'):.2f}",
    "tc_batch_64": f"{ratio('tailcow-c', 64, 'batch_req'):.2f}",
    "tc_batch_128": f"{ratio('tailcow-c', 128, 'batch_req'):.2f}",
    "mor_read_128": f"{ratio('mor-v3', 128, 'read_req'):.2f}",
    "mor_stock_128": f"{ratio('mor-v3', 128, 'stock_req'):.2f}",
    "mor_batch_16": f"{ratio('mor-v3', 16, 'batch_req'):.2f}",
    "tc_batch_16": f"{ratio('tailcow-c', 16, 'batch_req'):.2f}",
    "rmow_req_lo": f"{rmow_req.ratio.min():.2f}",
    "rmow_req_hi": f"{rmow_req.ratio.max():.2f}",
    "file_4": f"{F.loc[4, 'mor-v3']:.2f}",
    "file_16": f"{F.loc[16, 'mor-v3']:.2f}",
    "file_64": f"{F.loc[64, 'mor-v3']:.1f}",
    "file_128": f"{F.loc[128, 'mor-v3']:.1f}",
    "locate_16": f"{kinds.loc[16, 'get_range'] + kinds.loc[16, 'get_stream']:.0f}",
    "locate_64": f"{kinds.loc[64, 'get_range'] + kinds.loc[64, 'get_stream']:.0f}",
    "aborts_64": f"{kinds.loc[64, 'aborts']:.0f}",
    "rc_bytes_64": f"{ratio('rmow-c', 64, 'batch_bytes'):.2f}",
    "rc_bytes_128": f"{ratio('rmow-c', 128, 'batch_bytes'):.2f}",
    "rb_bytes_128": f"{ratio('rmow-b', 128, 'batch_bytes'):.2f}",
    "read_bytes_dev": f"{100 * (reads_bytes.ratio - 1).abs().max():.1f}%",
    "rc_rg_4": f"{G.loc[(4, 'rmow-c'), 'row_groups']:.0f}",
    "rc_rg_16": f"{G.loc[(16, 'rmow-c'), 'row_groups']:.0f}",
    "rc_rg_128": f"{G.loc[(128, 'rmow-c'), 'row_groups']:.0f}",
    "rc_rg_shrink": f"{G.loc[(4, 'rmow-c'), 'row_groups'] / G.loc[(128, 'rmow-c'), 'row_groups']:.2f}",
    "live_dev": f"{100 * max(abs(G.loc[(r, a), 'live_bytes'] / (G.loc[(4, a), 'live_bytes'] * r / 4) - 1) for r in (16, 64, 128) for a in ARMS):.1f}%",
    "st_sum": f"{st128.sum():.1f}",
    "st_rb": f"{st128['rmow-b']:.2f}",
    "st_rc": f"{st128['rmow-c']:.2f}",
    "st_mor": f"{st128['mor-v3']:.2f}",
    "live_rc_128": f"{G.loc[(128, 'rmow-c'), 'live_bytes'] / 2 ** 30:.2f}",
    "rc_stored_x": f"{G.loc[(128, 'rmow-c'), 'stored_bytes'] / G.loc[(128, 'rmow-c'), 'live_bytes']:.1f}",
    "req_share_4": f"{base.loc['rmow-c', 'batch_req'] / base.loc['mor-v3', 'batch_req']:.2f}",
    "req_share_128": f"{io[(io.R == 128) & (io.arm == 'rmow-c')].batch_req.iloc[0] / io[(io.R == 128) & (io.arm == 'mor-v3')].batch_req.iloc[0]:.2f}",
    "bytes_x_4": f"{base.loc['rmow-c', 'batch_bytes'] / base.loc['mor-v3', 'batch_bytes']:.0f}",
    "bytes_x_128": f"{io[(io.R == 128) & (io.arm == 'rmow-c')].batch_bytes.iloc[0] / io[(io.R == 128) & (io.arm == 'mor-v3')].batch_bytes.iloc[0]:.0f}",
    "mor_req_128": f"{io[(io.R == 128) & (io.arm == 'mor-v3')].batch_req.iloc[0]:.0f}",
    "mor_req_4": f"{base.loc['mor-v3', 'batch_req']:.0f}",
    "rc_req_128": f"{io[(io.R == 128) & (io.arm == 'rmow-c')].batch_req.iloc[0]:.0f}",
}
```

Do the 4 MiB recency setting's requests, bytes, CPU and simulated latency hold at Iceberg's
defaults, 128 MiB row groups and 512 MiB files? This page reads the scale campaign: four runs on
havoc, one per row-group size R of 4, 16, 64 and 128 MiB. It decides P1–P6 (docs/HYPOTHESES.md,
2026-10-23) where the runs can decide them. A rerun judged CPU per phase
([below](#scale-rerun)), and a third campaign measured it in instructions on this laptop
([below](#scale-laptop)), which decided P3–P5.

## Setup

**Configurations.** `configs/scale-recency-rg{4,16,64,128}m.json` scale the logged 4 MiB
recency setting (`sim-recency-4m-noverify`) with R, so each batch keeps its structure:
128k × R initial rows, 1,280 × R operations per batch, R-MiB row groups and tail seal, and
4R-MiB files. The rest is as in that setting:
- the six arms;
- the readers `cached` (one thread), `cached-p8` (eight) and `stock`;
- 60 batches of recency-skewed updates, rows located by scan, no compaction;
- payload checks off, and every request logged.

**Runs.** The page pins them by id (`RUNS`). All four are commit 525be1c, clean, in one image
built from it, on havoc's Xeon E7-8870 v3. The container runner (docker/README.md) placed each
on 10 whole cores of its own NUMA node and ran it on one thread per core. The SMT siblings were
left idle.

```{code-cell} ipython3
:tags: [remove-input]

table(setup[["R", "initial_rows", "ops_per_batch", "rg_mib", "seal_mib", "file_mib", "node",
             "cpus", "minutes"]]
      .rename(columns={"R": "R (MiB)", "initial_rows": "initial rows",
                       "ops_per_batch": "operations per batch", "rg_mib": "row group (MiB)",
                       "seal_mib": "tail seal (MiB)", "file_mib": "file target (MiB)",
                       "cpus": "CPUs it ran on"})
      .round(1).reset_index(drop=True))
```

## Foreign load

The runner watches each job's cores, siblings included. Foreign load is their busy time less the
job's own CPU time. The 4 MiB job, done in {eval}`N["R4_min"]` minutes, saw
{eval}`N["f4"]` CPU-seconds of it. The other three ran on while other work arrived on the
machine. They saw {eval}`N["fmean_lo"]`–{eval}`N["fmean_hi"]` CPUs of it on average, and up to
{eval}`N["fmax_lo"]`–{eval}`N["fmax_hi"]` CPUs over 5 seconds. Leaving the siblings idle did not
keep the cores to the job: the scheduler places other users' threads on idle CPUs.

```{code-cell} ipython3
:tags: [remove-input]

table(setup[["R", "node", "owned", "minutes", "foreign_cpu_s", "foreign_mean_cpus",
             "foreign_max_cpus"]]
      .rename(columns={"R": "R (MiB)", "owned": "CPUs owned (siblings included)",
                       "foreign_cpu_s": "foreign CPU-s", "foreign_mean_cpus": "mean (CPUs)",
                       "foreign_max_cpus": "largest 5 s (CPUs)"})
      .round(2).reset_index(drop=True))
```

Each split arm reads the same bytes as its appendable twin (within {eval}`N["twin_bytes"]` at
every probe), so the two reads' CPU should match. That makes the twins a gauge of the noise in
each run's CPU.

```{code-cell} ipython3
:label: scale-twins
:tags: [remove-input]
:class: col-page-right

rule = alt.Chart().mark_rule(strokeDash=[4, 4], color="#888").encode(y=alt.datum(1.0))
pts = (alt.Chart().mark_line(point=alt.OverlayMarkDef(size=60, filled=True), strokeWidth=2)
       .encode(x=alt.X("step:Q", title="batch", scale=alt.Scale(domain=[0, 60])),
               y=alt.Y("cpu_ratio:Q", title="read CPU, split ÷ appendable",
                       scale=alt.Scale(domain=[0.8, 1.25], zero=False)),
               color=alt.Color("pair:N", title=None),
               tooltip=["R", "pair", "step", alt.Tooltip("cpu_ratio:Q", format=".3f"),
                        alt.Tooltip("bytes_ratio:Q", format=".4f")]))
(alt.layer(rule, pts, data=twins).properties(width=170, height=160)
 .facet(column=alt.Column("R:O", title="row group R (MiB)")))
```

Each point is one full read under `cached`: the split arm's thread CPU over its twin's, at that
probe. The 4 MiB run's twins agree within {eval}`N["twin4_dev"]` at every probe but its last
(batch 60, {eval}`N["twin4_last"]`), which ran in its final minutes. In the other three runs they
range from {eval}`N["twin_big_lo"]` to {eval}`N["twin_big_hi"]`.

```{code-cell} ipython3
:tags: [remove-input]

table(twins.pivot_table(index=["R", "pair"], columns="step", values="cpu_ratio")
      .round(3).reset_index())
```

So the CPU of the three larger runs carries noise of up to a fifth of any one phase, and so does
the 4 MiB run's last probe. As registered, a job with foreign load decides nothing about CPU, so
P3–P5 are not decided here ([below](#scale-cpu)). A rerun on clean cores will also show how much
of the noise is the machine's: whatever large reads add themselves, such as garbage collection,
would remain. Requests, bytes and storage do not depend on
the machine, so P1, P2 and P6 are.

## Requests and bytes (P1, P2)

```{code-cell} ipython3
:label: scale-ratios
:tags: [remove-input]
:class: col-page-right

color, shape = fig.arm_encoding(ARMS)
band = alt.Chart(pd.DataFrame({"lo": [0.75], "hi": [1.25]})).mark_rect(opacity=0.12, color="#888").encode(
    y="lo:Q", y2="hi:Q")
lines = (alt.Chart().mark_line(strokeWidth=2)
         .encode(x=alt.X("R:Q", title="row group R (MiB)",
                         scale=alt.Scale(type="log", base=2, domain=[4, 128])),
                 y=alt.Y("ratio:Q", title="÷ the 4 MiB point (bytes: × R/4)"),
                 color=color, detail="arm_label:N"))
marks = (alt.Chart().mark_point(filled=True, size=70)
         .encode(x="R:Q", y="ratio:Q", color=color, shape=shape,
                 tooltip=["arm_label", "R", "measure", alt.Tooltip("value:Q", format=",.0f"),
                          alt.Tooltip("ratio:Q", format=".3f")]))
(alt.layer(band, lines, marks, data=ratios).properties(width=200, height=150)
 .facet(facet=alt.Facet("measure:N", title=None, sort=[t for t, _ in MEASURES.values()]),
        columns=4))
```

Each point is an arm's measure at R over the 4 MiB point's, with bytes scaled by R/4 first. The
gray band is the prediction's ±25%. Batches are the mean over the 60; full reads are at batch
60.

```{code-cell} ipython3
:tags: [remove-input]

table(ratios.pivot_table(index=["measure", "arm_label"], columns="R", values="ratio")
      .round(3).reset_index())
```

**MoR-v3 and TailCoW-C cross the whole-file threshold.** Stock Iceberg reads a file of at most
1 MiB in one request, and the readers here do the same. Each batch of MoR-v3 and TailCoW-C
writes a data file of its new rows:
{eval}`N["file_4"]` and {eval}`N["file_16"]` MiB at 4 and 16 MiB, then {eval}`N["file_64"]` and
{eval}`N["file_128"]` MiB at 64 and 128 MiB. Above 1 MiB, reading such a file means its 8-byte
tail (an open-ended GET, aborted), its footer, then the row groups.
- **Batches.** MoR-v3's locate scan reads the recent files the batch's updates may touch. It
  issues {eval}`N["locate_16"]` GETs per batch at 16 MiB and {eval}`N["locate_64"]` at 64 MiB,
  with {eval}`N["aborts_64"]` tail probes.
- **Full reads.** A full read pays one more request per data file: two for footer and row group,
  instead of one whole fetch.

```{code-cell} ipython3
:tags: [remove-input]

table(files.pivot(index="R", columns="arm", values="mib").round(2).reset_index()
      .rename(columns={"R": "R (MiB)", "mor-v3": "MoR-v3 file (MiB)",
                       "tailcow-c": "TailCoW-C file (MiB)"}))
```

**RMoW-C's row groups come out larger at 64 MiB and up.** Live bytes scale with R within
{eval}`N["live_dev"]` in every arm. RMoW-C holds them in {eval}`N["rc_rg_4"]` row groups at 4
MiB, {eval}`N["rc_rg_16"]` at 16 MiB and {eval}`N["rc_rg_128"]` at 128 MiB. The writer's row
groups are smaller on disk than their target by a factor that depends on the target (README,
`rowGroupBytes`). Fewer, larger row groups mean a larger region rewritten per touched row group,
about {eval}`N["rc_rg_shrink"]`× the bytes for the same updates. The same change cuts the
RMoW arms' full-read requests.

```{code-cell} ipython3
:tags: [remove-input]

table(state.assign(arm=state.arm.map(labels), live_mib=state.live_bytes / 2 ** 20)
      .pivot_table(index="arm", columns="R", values=["row_groups", "live_mib"])
      .round(1).reset_index())
```

(scale-cpu)=
## CPU and latency (P3–P5): not decided

At face value, the measurements fall as below. They cannot decide the predictions. P4's margin is
10%, and the gauge shows noise of up to {eval}`N["twin_big_hi"]`× in one phase's CPU, which is
most of a full read's simulated time on S3 Express. They are shown to size a rerun, not to
judge anything.

```{code-cell} ipython3
:tags: [remove-input]

table(face)
```

(scale-rerun)=
## The rerun, judged per phase

```{code-cell} ipython3
:tags: [remove-cell]

# The rerun (docker/campaigns/scale-rerun.json), from 247f054: the same configurations, with
# foreign load sampled every second and writer footers held for one transaction (docs/HYPOTHESES.md,
# 2026-10-24)
RERUN = {
    4: "scale-recency-rg4m-5cc727c1e4af/20261024T082757Z",
    16: "scale-recency-rg16m-daece520dd8e/20261024T082756Z",
    64: "scale-recency-rg64m-34a1c89f1cfa/20261024T082755Z",
    128: "scale-recency-rg128m-fe22e4558e59/20261024T082754Z",
}
RIDS = list(RERUN.values())
R2_OF = {v: k for k, v in RERUN.items()}

load2 = q("""
    SELECT l.run_id, any_value(c.node) AS node, any_value(c.cpus) AS cpus, count(*) AS samples,
           avg(l.foreign_cpus) AS foreign_mean, max(l.foreign_cpus) AS foreign_max,
           avg(CASE WHEN l.foreign_cpus >= 0.25 THEN 1 ELSE 0 END) AS share_over
    FROM load l JOIN container_jobs c USING (run_id)
    WHERE list_contains($ids, l.run_id) GROUP BY ALL""", ids=RIDS)
clean2 = q("""
    SELECT run_id,
           avg(CASE WHEN clean THEN 1 ELSE 0 END) FILTER (WHERE phase = 'batch') AS clean_batches,
           count(*) FILTER (WHERE phase = 'read_full' AND step = 60 AND clean) AS clean60,
           count(*) FILTER (WHERE phase = 'read_full' AND step = 60) AS reads60
    FROM phase_load WHERE list_contains($ids, run_id) GROUP BY ALL""", ids=RIDS)
jobs2 = load2.merge(clean2, on="run_id")
jobs2["R"] = jobs2.run_id.map(R2_OF)
jobs2 = jobs2.sort_values("R")
J2 = jobs2.set_index("R")
at60 = q("""
    SELECT arm, reader, clean, foreign_max_cpus FROM phase_load
    WHERE run_id = $r AND phase = 'read_full' AND step = 60""", r=RERUN[128]).set_index(["reader", "arm"])

# C1: each record against the first campaign's (steps_diff)
c1 = []
for r in RUNS:
    d = q("SELECT * FROM steps_diff($a, $b)", a=RUNS[r], b=RERUN[r])
    d["what"] = np.where(d.phase.str.startswith("read"), "probes", d.phase)
    d = d[d.what.isin(["probes", "batch"])]
    d["differs_any"] = d.one_run_only | (d.differs.map(len) > 0)
    for (arm, what), g in d.groupby(["arm", "what"]):
        c1.append((labels[arm], what, r, int(g.differs_any.sum()), len(g)))
c1 = pd.DataFrame(c1, columns=["arm_label", "what", "R", "differ", "records"])
tc = q("""
    SELECT n.run_id, n.step, n.requests - o.requests AS dreq
    FROM steps o JOIN steps n USING (arm, step, phase)
    WHERE o.arm = 'tailcow-c' AND o.phase = 'batch' AND list_contains($pairs, [o.run_id, n.run_id])""",
          pairs=[[RUNS[r], RERUN[r]] for r in RUNS])

# C2: each RMoW batch's extra requests against the files live at its start
c2 = q("""
    SELECT n.run_id, n.arm, n.step, n.requests - o.requests AS extra,
           n.bytes_written - o.bytes_written AS dwritten, st.data_files
    FROM steps o JOIN steps n USING (arm, step, phase)
    JOIN steps st ON st.run_id = o.run_id AND st.arm = o.arm AND st.phase = 'state' AND st.step = o.step - 1
    WHERE o.arm LIKE 'rmow%' AND o.phase = 'batch' AND list_contains($pairs, [o.run_id, n.run_id])""",
          pairs=[[RUNS[r], RERUN[r]] for r in RUNS])
c2["R"] = c2.run_id.map(R2_OF)
c2["split"] = c2.arm.str.endswith("split")
c2["per_file"] = c2.extra / c2.data_files
c2["as_registered"] = np.where(c2.split, c2.extra == 2, c2.extra.between(3, 4))
c2_table = (c2.assign(arm_label=c2.arm.map(labels))
            .groupby(["arm_label", "R"])
            .agg(files_lo=("data_files", "min"), files_hi=("data_files", "max"),
                 extra_lo=("extra", "min"), extra_hi=("extra", "max"),
                 as_registered=("as_registered", "mean"))
            .reset_index())

# C3: the twins' read CPU under cached, clean or not
tw = q("""
    WITH s AS (SELECT run_id, arm, step, cpu_ns, process_cpu_ns, clean, foreign_max_cpus
               FROM phase_load
               WHERE list_contains($ids, run_id) AND phase = 'read_full' AND reader = 'cached')
    SELECT a.run_id, b.arm, a.step, a.cpu_ns / b.cpu_ns AS cpu_ratio, a.clean AND b.clean AS clean,
           (a.process_cpu_ns - a.cpu_ns) / 1e9 AS other_split_s,
           (b.process_cpu_ns - b.cpu_ns) / 1e9 AS other_twin_s
    FROM s a JOIN s b ON b.run_id = a.run_id AND b.step = a.step AND b.arm = replace(a.arm, '-split', '')
    WHERE a.arm LIKE '%-split'""", ids=RIDS)
tw["R"] = tw.run_id.map(R2_OF)
tw["pair"] = tw.arm.map(lambda a: f"{labels[a + '-split']} ÷ {labels[a]}")
tw["phase load"] = np.where(tw.clean, "clean", "foreign load")
cl = tw[tw.clean]
off = tw[(tw.cpu_ratio - 1).abs() > 0.03]
# the dearer read of a disagreeing pair: did more of the JVM's other CPU run during it?
dearer_more_other = ((off.cpu_ratio > 1) == (off.other_split_s > off.other_twin_s)).sum()

# P3-P5 at face value, over clean phases
cpu2 = q("""
    SELECT run_id, arm,
           sum(cpu_ns) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0 AND clean) / 1e6
             / (sum(bytes_read) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0 AND clean) / 2 ^ 20)
             AS rd,
           sum(cpu_ns) FILTER (WHERE phase = 'batch' AND clean) / 1e6
             / (sum(bytes_written) FILTER (WHERE phase = 'batch' AND clean) / 2 ^ 20) AS wr
    FROM phase_load WHERE list_contains($ids, run_id) GROUP BY ALL""", ids=RIDS)
cpu2["R"] = cpu2.run_id.map(R2_OF)
C2CPU = cpu2.set_index(["R", "arm"])
lat2 = q("""
    SELECT l.arm, l.profile,
           avg(l.cpu_ms + l.request_ms + l.transfer_ms) FILTER (WHERE l.phase = 'batch' AND l.step > 0 AND p.clean) AS batch_ms,
           max(l.cpu_ms + l.request_ms + l.transfer_ms)
               FILTER (WHERE l.phase = 'read_full' AND l.reader = 'cached' AND l.step = 60) AS read60
    FROM phase_latency l JOIN phase_load p
      ON p.run_id = l.run_id AND p.arm = l.arm AND p.step = l.step AND p.phase = l.phase
     AND p.reader IS NOT DISTINCT FROM l.reader
    WHERE l.run_id = $r GROUP BY ALL""", r=RERUN[128]).set_index(["profile", "arm"])
rp2 = fig.replay(RERUN[128], s3x)
rp2 = rp2[(rp2.phase == "read_full") & (rp2.step == 60) & (rp2.reader == "cached-p8")].set_index("arm")
p8_2 = rp2.simulated_ms / rp2.simulated_ms["mor-v3"]
batch2 = {p: lat2.loc[(p, "rmow-c"), "batch_ms"] / lat2.loc[(p, "mor-v3"), "batch_ms"]
          for p in batch_profiles}


def span(values):
    return f"{min(values):.2f}–{max(values):.2f}"


face2 = pd.DataFrame([
    ("P3", "read CPU per MiB, cached, 128 ÷ 4 MiB (range over arms)", "within 30%",
     span([C2CPU.loc[(128, a), "rd"] / C2CPU.loc[(4, a), "rd"] for a in ARMS]), "clean reads"),
    ("P3", "write CPU per MiB, 128 ÷ 4 MiB (range over arms)", "within 30%",
     span([C2CPU.loc[(128, a), "wr"] / C2CPU.loc[(4, a), "wr"] for a in ARMS]), "clean batches"),
    ("P4", "RMoW-C ÷ MoR-v3, full read at batch 60, cached, S3 Express", "0.9–1.1",
     f"{lat2.loc[('s3x', 'rmow-c'), 'read60'] / lat2.loc[('s3x', 'mor-v3'), 'read60']:.2f}",
     "neither read clean"),
    ("P4", "the same, Azure Blob", "at most 0.9",
     f"{lat2.loc[('azure', 'rmow-c'), 'read60'] / lat2.loc[('azure', 'mor-v3'), 'read60']:.2f}",
     "neither read clean"),
    ("P4", "RMoW-C ÷ MoR-v3, a batch (range over the profiles it can run on)", "above 1",
     span(batch2.values()), "clean batches"),
    ("P5", "every arm ÷ MoR-v3, full read at batch 60, cached-p8, S3 Express", "0.75–1.25",
     span(p8_2), "all reads clean"),
], columns=["prediction", "measure (128 MiB)", "predicted", "as measured", "phases"])

N2 = {
    "mean_lo": f"{jobs2.foreign_mean.min():.2f}",
    "mean_hi": f"{jobs2.foreign_mean.max():.2f}",
    "clean_b_lo": f"{100 * jobs2.clean_batches.min():.0f}%",
    "clean_b_hi": f"{100 * jobs2.clean_batches.max():.0f}%",
    "clean60_128": f"{J2.loc[128, 'clean60']}",
    "reads60_128": f"{J2.loc[128, 'reads60']}",
    "fm_mor": f"{at60.loc[('cached', 'mor-v3'), 'foreign_max_cpus']:.2f}",
    "fm_rc": f"{at60.loc[('cached', 'rmow-c'), 'foreign_max_cpus']:.2f}",
    "c1_probes": f"{c1[c1.what == 'probes'].records.sum():,}",
    "c1_probes_bad": f"{c1[c1.what == 'probes'].differ.sum()}",
    "c1_mor_bad": f"{c1[(c1.what == 'batch') & (c1.arm_label == labels['mor-v3'])].differ.sum()}",
    "tc_fewer": f"{(tc.dreq < 0).sum()}",
    "tc_more": f"{(tc.dreq > 0).sum()}",
    "c2_batches": f"{len(c2)}",
    "c2_reg": f"{c2.as_registered.sum()}",
    "c2_written": f"{(c2.dwritten != 0).sum()}",
    "c3_lo": f"{cl.cpu_ratio.min():.3f}",
    "c3_hi": f"{cl.cpu_ratio.max():.3f}",
    "c3_n": f"{len(cl)}",
    "c3_out": f"{((cl.cpu_ratio - 1).abs() > 0.03).sum()}",
    "c3_dev": f"{100 * (cl.cpu_ratio - 1).abs().max():.0f}%",
    "fl16_lo": f"{tw[(tw.R == 16) & ~tw.clean].cpu_ratio.min():.3f}",
    "fl16_hi": f"{tw[(tw.R == 16) & ~tw.clean].cpu_ratio.max():.3f}",
    "off_n": f"{len(off)}",
    "off_other": f"{dearer_more_other}",
    "p4_s3x": f"{lat2.loc[('s3x', 'rmow-c'), 'read60'] / lat2.loc[('s3x', 'mor-v3'), 'read60']:.2f}",
}
```

The rerun ran the same four configurations on the build that samples foreign load every second
and holds writer footers for one transaction (docs/HYPOTHESES.md, 2026-10-24). The page pins
its runs by id (`RERUN`). It was to decide P3–P5 on clean phases: those that no load sample
overlapping them found at 0.25 CPUs of foreign load or more.

```{code-cell} ipython3
:tags: [remove-input]

table(jobs2[["R", "node", "cpus", "foreign_mean", "foreign_max", "share_over", "clean_batches",
             "clean60", "reads60"]]
      .rename(columns={"R": "R (MiB)", "foreign_mean": "foreign load, mean (CPUs)",
                       "foreign_max": "largest sample (CPUs)",
                       "share_over": "samples at 0.25+", "clean_batches": "batches clean",
                       "clean60": "batch-60 reads clean", "reads60": "of"})
      .round(3).reset_index(drop=True))
```

Foreign load was lighter this time, {eval}`N2["mean_lo"]`–{eval}`N2["mean_hi"]` CPUs on average,
and {eval}`N2["clean_b_lo"]`–{eval}`N2["clean_b_hi"]` of each run's batches were clean. At 128
MiB, {eval}`N2["clean60_128"]` of the {eval}`N2["reads60_128"]` full reads at batch 60 were clean.
Every `cached-p8` read was clean. The `cached` reads that P4 compares were not: MoR-v3's peaked at
{eval}`N2["fm_mor"]` foreign CPUs and RMoW-C's at {eval}`N2["fm_rc"]`.

**C1: the footer change touches only writers.** Every one of the {eval}`N2["c1_probes"]` probe
records matches the first campaign's in every request and byte column, under every reader
({eval}`N2["c1_probes_bad"]` differ). So does every MoR-v3 batch ({eval}`N2["c1_mor_bad"]`
differ). TailCoW-C's batches take fewer requests in {eval}`N2["tc_fewer"]` batches and more in
{eval}`N2["tc_more"]`: its merge read reuses a footer its locate scan read.

```{code-cell} ipython3
:tags: [remove-input]

table(c1.pivot_table(index=["what", "arm_label"], columns="R", values="differ").reset_index())
```

**C2: RMoW's writer reads its footers every batch.** Every RMoW batch takes one footer read
more per file live at its start: two requests for an appendable file over 1 MiB, one for a
footer-only object or a file of at most 1 MiB, two for a split file's first version (a plain
Parquet object). Bytes written are unchanged ({eval}`N2["c2_written"]` batches differ). The
counts registered (3–4 more, 2 for split files) assumed two live files throughout, as at 4 MiB.
From 16 MiB up, one file holds the table until batch 10, so only {eval}`N2["c2_reg"]` of the
{eval}`N2["c2_batches"]` batches match them.

```{code-cell} ipython3
:tags: [remove-input]

table(c2_table.rename(columns={"R": "R (MiB)", "files_lo": "live files, fewest",
                               "files_hi": "most", "extra_lo": "extra requests, fewest",
                               "extra_hi": "most", "as_registered": "batches as registered"})
      .round(2))
```

**C3: clean is not clean enough.** A split arm reads the same bytes as its appendable twin, so
in clean probes their read CPU should agree within 3%.

```{code-cell} ipython3
:label: scale-rerun-twins
:tags: [remove-input]
:class: col-page-right

band3 = alt.Chart().mark_rect(opacity=0.12, color="#888").encode(y=alt.datum(0.97), y2=alt.datum(1.03))
pts3 = (alt.Chart().mark_point(size=70, filled=True)
        .encode(x=alt.X("step:Q", title="batch", scale=alt.Scale(domain=[0, 60])),
                y=alt.Y("cpu_ratio:Q", title="read CPU, split ÷ appendable",
                        scale=alt.Scale(domain=[0.85, 1.15], zero=False, clamp=True)),
                color=alt.Color("phase load:N", title=None),
                shape=alt.Shape("pair:N", title=None),
                tooltip=["R", "pair", "step", "phase load", alt.Tooltip("cpu_ratio:Q", format=".3f"),
                         alt.Tooltip("other_split_s:Q", format=".2f"),
                         alt.Tooltip("other_twin_s:Q", format=".2f")]))
(alt.layer(band3, pts3, data=tw).properties(width=170, height=160)
 .facet(column=alt.Column("R:O", title="row group R (MiB)")))
```

Each point is one probe's full read under `cached`, the split arm's thread CPU over its twin's.
The gray band is C3's ±3%; a point beyond the axis is drawn at its edge. In the
{eval}`N2["c3_n"]` clean pairs the ratio runs from {eval}`N2["c3_lo"]` to {eval}`N2["c3_hi"]`,
and {eval}`N2["c3_out"]` fall outside the band. Foreign load on the job's cores does not account
for it: at 16 MiB, every pair with foreign load stays within the band
({eval}`N2["fl16_lo"]`–{eval}`N2["fl16_hi"]`). The JVM's own work is a candidate: in {eval}`N2["off_other"]` of the
{eval}`N2["off_n"]` pairs that disagree by more than 3%, the read with more CPU also had more of
the process's other CPU (collection, compilation) running during it. Work on the rest of the
socket, which the monitor does not watch, is another.

```{code-cell} ipython3
:tags: [remove-input]

table(tw.pivot_table(index=["R", "pair", "phase load"], columns="step", values="cpu_ratio")
      .round(3).reset_index())
```

As registered, C3's failure leaves P3–P5 undecided. At face value, over clean phases:

```{code-cell} ipython3
:tags: [remove-input]

table(face2)
```

P3 and P5 would hold with room to spare. P4's read half would fail again
({eval}`N2["p4_s3x"]` on S3 Express against 0.9–1.1), but its reads were not clean, and the
twins show noise near its 10% margin.

(scale-laptop)=
## On the instruction measure

```{code-cell} ipython3
:tags: [remove-cell]

# The scale campaign on the instruction measure (docs/HYPOTHESES.md, 2026-10-25): the same four
# configurations on the laptop, from a build snapshot of 9f868af (dirty with a held docs draft)
LAPTOP = {
    4: "scale-recency-rg4m-5cc727c1e4af/20261025T015547Z",
    16: "scale-recency-rg16m-daece520dd8e/20261025T015850Z",
    64: "scale-recency-rg64m-34a1c89f1cfa/20261025T020856Z",
    128: "scale-recency-rg128m-fe22e4558e59/20261025T025418Z",
}
L_OF = {v: k for k, v in LAPTOP.items()}
LIDS = list(LAPTOP.values())

# C1 and C2
c1l = []
for r in LAPTOP:
    d = q("SELECT * FROM steps_diff($a, $b)", a=RERUN[r], b=LAPTOP[r])
    c1l.append((r, len(d), int((d.one_run_only | (d.differs.map(len) > 0)).sum())))
c1l = pd.DataFrame(c1l, columns=["R", "records", "differ"])
c2l = q("""
    SELECT s.run_id, count(*) AS timed,
           count(*) FILTER (WHERE s.instructions IS NULL OR s.instructions <= 0) AS uncounted,
           any_value(r.manifest -> 'host' ->> 'thread_counters') AS counters,
           any_value(r.elapsed_s) / 60 AS minutes
    FROM steps s JOIN runs r USING (run_id)
    WHERE list_contains($ids, s.run_id) AND s.wall_ns IS NOT NULL GROUP BY ALL""", ids=LIDS)
c2l["R"] = c2l.run_id.map(L_OF)
checks = c1l.merge(c2l[["R", "timed", "uncounted", "counters", "minutes"]], on="R").sort_values("R")

# the noise floors, from each run's twins
floor_rows, replays = [], {}
for r, run in LAPTOP.items():
    w = q("""SELECT arm, step, phase, reader, work_ms FROM phase_work
             WHERE run_id = $r AND phase IN ('read_full', 'batch')""", r=run)
    rd = w[(w.phase == "read_full") & (w.reader == "cached")].set_index(["arm", "step"]).work_ms
    steps_r = rd.index.get_level_values(1).unique()
    f_read = max(abs(rd[(a + "-split", s)] / rd[(a, s)] - 1) for a in ("rmow-b", "rmow-c") for s in steps_r)
    bm = w[w.phase == "batch"].groupby("arm").work_ms.mean()
    f_batch = max(abs(bm[a + "-split"] / bm[a] - 1) for a in ("rmow-b", "rmow-c"))
    rp = fig.replay(run, s3x, cpu="work")
    replays[r] = rp
    p8r = rp[(rp.phase == "read_full") & (rp.reader == "cached-p8")].set_index(["arm", "step"]).simulated_ms
    f_p8 = max(abs(p8r[(a + "-split", s)] / p8r[(a, s)] - 1) for a in ("rmow-b", "rmow-c") for s in steps_r)
    floor_rows.append((r, f_read, f_batch, f_p8))
floors = pd.DataFrame(floor_rows, columns=["R", "F_read", "F_batch", "F_p8"]).sort_values("R")
F = floors.set_index("R").loc[128]

# P3 in work, with modeled time per MiB beside it
p3w = q("""
    SELECT run_id, arm,
           sum(instructions) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0)
             / (sum(bytes_read) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0) / 2 ^ 20)
             AS ins_read,
           sum(instructions) FILTER (WHERE phase = 'batch')
             / (sum(bytes_written) FILTER (WHERE phase = 'batch') / 2 ^ 20) AS ins_written,
           sum(work_ms) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0)
             / (sum(bytes_read) FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step > 0) / 2 ^ 20)
             AS ms_read,
           sum(work_ms) FILTER (WHERE phase = 'batch')
             / (sum(bytes_written) FILTER (WHERE phase = 'batch') / 2 ^ 20) AS ms_written
    FROM (SELECT w.*, s.bytes_read, s.bytes_written FROM phase_work w JOIN steps s
            ON s.run_id = w.run_id AND s.arm = w.arm AND s.step = w.step AND s.phase = w.phase
           AND s.reader IS NOT DISTINCT FROM w.reader
          WHERE list_contains($ids, w.run_id))
    GROUP BY ALL""", ids=LIDS)
p3w["R"] = p3w.run_id.map(L_OF)
p3w["arm_label"] = p3w.arm.map(labels)
W = p3w.set_index(["R", "arm"])
p3_table = pd.DataFrame([
    (labels[a],
     W.loc[(128, a), "ins_read"] / W.loc[(4, a), "ins_read"],
     W.loc[(128, a), "ins_written"] / W.loc[(4, a), "ins_written"],
     W.loc[(128, a), "ms_read"] / W.loc[(4, a), "ms_read"],
     W.loc[(128, a), "ms_written"] / W.loc[(4, a), "ms_written"]) for a in ARMS],
    columns=["arm", "instructions per MiB read", "instructions per MiB written",
             "modeled ms per MiB read", "modeled ms per MiB written"])

# P4 in modeled time, P5 in the replay
latw = q("""
    SELECT arm, profile,
           avg(work_ms + request_ms + transfer_ms) FILTER (WHERE phase = 'batch' AND step > 0) AS batch_ms,
           max(work_ms + request_ms + transfer_ms)
               FILTER (WHERE phase = 'read_full' AND reader = 'cached' AND step = 60) AS read60
    FROM phase_latency WHERE run_id = $r GROUP BY ALL""", r=LAPTOP[128]).set_index(["profile", "arm"])


def band(x, lo, hi, f):
    if lo + f <= x <= hi - f:
        return "held"
    return "contradicted" if x < lo - f or x > hi + f else "undecided"


def at_most(x, hi, f):
    return "held" if x <= hi - f else "contradicted" if x > hi + f else "undecided"


p4_s3x = latw.loc[("s3x", "rmow-c"), "read60"] / latw.loc[("s3x", "mor-v3"), "read60"]
p4_az = latw.loc[("azure", "rmow-c"), "read60"] / latw.loc[("azure", "mor-v3"), "read60"]
p4_batch = {p: latw.loc[(p, "rmow-c"), "batch_ms"] / latw.loc[(p, "mor-v3"), "batch_ms"]
            for p in batch_profiles}
rp128 = replays[128]
p8_128 = rp128[(rp128.phase == "read_full") & (rp128.reader == "cached-p8") & (rp128.step == 60)].set_index("arm").simulated_ms
p5 = p8_128 / p8_128["mor-v3"]
decided = pd.DataFrame(
    [("P4", "RMoW-C ÷ MoR-v3, full read at batch 60, cached, S3 Express", "0.9–1.1", p4_s3x, F.F_read,
      band(p4_s3x, 0.9, 1.1, F.F_read)),
     ("P4", "the same, Azure Blob", "at most 0.9", p4_az, F.F_read, at_most(p4_az, 0.9, F.F_read))]
    + [("P4", f"RMoW-C ÷ MoR-v3, a batch, {p}", "above 1", x, F.F_batch,
        "held" if x > 1 + F.F_batch else "contradicted" if x < 1 - F.F_batch else "undecided")
       for p, x in p4_batch.items()]
    + [("P5", f"{labels[a]} ÷ MoR-v3, full read at batch 60, cached-p8, S3 Express", "0.75–1.25", x,
        F.F_p8, band(x, 0.75, 1.25, F.F_p8)) for a, x in p5.items() if a != "mor-v3"],
    columns=["prediction", "measure (128 MiB)", "predicted", "measured", "floor", "verdict"])

N3 = {
    "minutes": f"{checks.minutes.sum():.0f}",
    "c1_records": f"{checks.records.sum():,}",
    "c1_differ": f"{checks.differ.sum()}",
    "uncounted": f"{checks.uncounted.sum()}",
    "f_read": f"{100 * F.F_read:.1f}%",
    "f_batch": f"{100 * F.F_batch:.1f}%",
    "f_p8": f"{100 * F.F_p8:.1f}%",
    "fr_lo": f"{100 * floors.F_read.min():.1f}%",
    "fr_hi": f"{100 * floors.F_read.max():.1f}%",
    "p3_read": f"{p3_table['instructions per MiB read'].min():.2f}–{p3_table['instructions per MiB read'].max():.2f}",
    "p3_written": f"{p3_table['instructions per MiB written'].min():.2f}–{p3_table['instructions per MiB written'].max():.2f}",
    "p3_ms_written": f"{p3_table['modeled ms per MiB written'].min():.2f}–{p3_table['modeled ms per MiB written'].max():.2f}",
    "rc_written": f"{W.loc[(128, 'rmow-c'), 'ins_written'] / W.loc[(4, 'rmow-c'), 'ins_written']:.2f}",
    "p4_s3x": f"{p4_s3x:.2f}",
    "p4_az": f"{p4_az:.2f}",
    "p4_b_lo": f"{min(p4_batch.values()):.1f}",
    "p4_b_hi": f"{max(p4_batch.values()):.1f}",
    "p5_lo": f"{p5.min():.2f}",
    "p5_hi": f"{p5.max():.2f}",
}
```

The four configurations ran again, one after another, on this laptop, whose kernel allows the
instruction counters that havoc's does not (docs/HYPOTHESES.md, 2026-10-25). CPU is measured as
on the page Measuring CPU (lab/pages/cpu-measure.md):
- **work:** a phase's user-mode instructions;
- **modeled time:** work at its arm's own pace.

Each comparison is judged against a noise floor taken from the run's own twins. The page pins
the runs by id (`LAPTOP`); together they took {eval}`N3["minutes"]` minutes.

```{code-cell} ipython3
:tags: [remove-input]

table(checks.merge(floors, on="R").rename(columns={"R": "R (MiB)", "records": "records against the rerun",
                                                    "timed": "timed phases"}).round(4))
```

**The controls held.**
- **C1:** all {eval}`N3["c1_records"]` records match the rerun's on havoc in every request and
  byte column ({eval}`N3["c1_differ"]` differ).
- **C2:** every timed phase counted its instructions ({eval}`N3["uncounted"]` without).

The floors at 128 MiB are {eval}`N3["f_read"]` for full reads, {eval}`N3["f_batch"]` for batch
means and {eval}`N3["f_p8"]` for eight-thread reads. Across the four runs, F_read is
{eval}`N3["fr_lo"]`–{eval}`N3["fr_hi"]`.

**P3, in work.** From 4 to 128 MiB, instructions per MiB read change by {eval}`N3["p3_read"]`×.
Instructions per MiB written change by {eval}`N3["p3_written"]`×, RMoW-C's the most
({eval}`N3["rc_written"]`×).
Modeled time per MiB written, which does not decide, changes by {eval}`N3["p3_ms_written"]`×.

```{code-cell} ipython3
:label: scale-laptop-work
:tags: [remove-input]
:class: col-page-right

color, shape = fig.arm_encoding(ARMS)
wl = p3w.melt(id_vars=["R", "arm", "arm_label"], value_vars=["ins_read", "ins_written"],
              var_name="measure", value_name="instructions per MiB")
wl["measure"] = wl.measure.map({"ins_read": "a full read, cached", "ins_written": "a batch"})
(alt.Chart(wl).mark_line(point=alt.OverlayMarkDef(size=60, filled=True), strokeWidth=2)
 .encode(x=alt.X("R:Q", title="row group R (MiB)", scale=alt.Scale(type="log", base=2)),
         y=alt.Y("instructions per MiB:Q", title="instructions per MiB", scale=alt.Scale(zero=False)),
         color=color, shape=shape,
         tooltip=["arm_label", "R", "measure", alt.Tooltip("instructions per MiB:Q", format=",.0f")])
 .properties(width=260, height=180)
 .facet(column=alt.Column("measure:N", title=None))
 .resolve_scale(y="independent"))
```

Each point is an arm's instructions per MiB: summed over its full reads under `cached` (left),
or over its batches (right), at each row-group size.

```{code-cell} ipython3
:tags: [remove-input]

table(p3_table.round(3))
```

**P4 and P5, in modeled time at 128 MiB.**

```{code-cell} ipython3
:tags: [remove-input]

table(decided.round(3))
```

- **P4's read on S3 Express is contradicted:** RMoW-C's full read takes {eval}`N3["p4_s3x"]`×
  MoR-v3's, below 0.9 by more than the floor. On Azure Blob it takes {eval}`N3["p4_az"]`×, as
  predicted.
- **P4's batches held:** RMoW-C's take {eval}`N3["p4_b_lo"]`–{eval}`N3["p4_b_hi"]`× MoR-v3's on
  every store where it can run.
- **P5 held:** every arm's eight-thread read takes {eval}`N3["p5_lo"]`–{eval}`N3["p5_hi"]`×
  MoR-v3's.

## P1–P6 (docs/HYPOTHESES.md)

**P1, P2, P4 and P6 contradicted, in part; P3 and P5 held.** P3–P5 were decided on the
instruction measure, by the third campaign.

- **P1, structure does not depend on scale: contradicted for MoR-v3 and TailCoW-C from 64 MiB.**
  - MoR-v3's requests per batch are {eval}`N["mor_batch_64"]`× and {eval}`N["mor_batch_128"]`×
    the 4 MiB point's at 64 and 128 MiB, and TailCoW-C's {eval}`N["tc_batch_64"]`× and
    {eval}`N["tc_batch_128"]`×.
  - MoR-v3's full read at batch 60 takes {eval}`N["mor_read_128"]`× the requests under `cached`
    and {eval}`N["mor_stock_128"]`× under `stock` at 128 MiB. The cause is the whole-file
    threshold, an absolute 1 MiB that the scaling cannot carry along.
  - It held at 16 MiB for every arm (MoR-v3 {eval}`N["mor_batch_16"]`×, TailCoW-C
    {eval}`N["tc_batch_16"]`× per batch). It held at every scale for the RMoW arms, at
    {eval}`N["rmow_req_lo"]`–{eval}`N["rmow_req_hi"]`×.
- **P2, bytes scale with the row group: contradicted for RMoW-C's batches from 64 MiB.**
  - RMoW-C writes {eval}`N["rc_bytes_64"]`× and {eval}`N["rc_bytes_128"]`× its scaled 4 MiB
    bytes per batch at 64 and 128 MiB, as do RMoW-C-split's batches. Its row groups come out
    larger on disk.
  - Every other arm's batches are within 25% (RMoW-B {eval}`N["rb_bytes_128"]`× at 128 MiB).
    Full reads are within {eval}`N["read_bytes_dev"]` everywhere.
- **P3, CPU scales with bytes: held, in work.** From 4 to 128 MiB, instructions per MiB read
  change by {eval}`N3["p3_read"]`× and per MiB written by {eval}`N3["p3_written"]`×. The first
  campaign (foreign load) and the rerun (C3) could not decide it.
- **P4, on one thread requests stop mattering: contradicted.**
  - RMoW-C's full read takes {eval}`N3["p4_s3x"]`× MoR-v3's on S3 Express, not within 10%: it
    reads fewer bytes and fewer files, and on S3 Express that still shows.
  - Its other parts held. It takes {eval}`N3["p4_az"]`× on Azure Blob, and RMoW-C's batches are
    slower everywhere ({eval}`N3["p4_b_lo"]`–{eval}`N3["p4_b_hi"]`×).
- **P5, on eight threads the largest row group bounds a read: held.** Every arm within
  {eval}`N3["p5_lo"]`–{eval}`N3["p5_hi"]`× of MoR-v3's on S3 Express.
- **P6, it fits in 24 GiB: contradicted, as the pilot foresaw.** Each arm's largest
  `stored_bytes` at 128 MiB sums to {eval}`N["st_sum"]` GiB.
- **P6 revised (each RMoW arm under 8 GiB, the sum under 40 GiB): contradicted for RMoW-B, held
  for the sum.**
  - RMoW-B and RMoW-B-split store {eval}`N["st_rb"]` GiB each, just over 8 GiB.
  - RMoW-C stores {eval}`N["st_rc"]` GiB and MoR-v3 {eval}`N["st_mor"]` GiB.

The rerun's controls (docs/HYPOTHESES.md, 2026-10-24):
- **C1, the footer change touches only writers: held.** Every probe, and every MoR-v3 batch,
  matches the first campaign's exactly; TailCoW-C's batches take no more requests.
- **C2, RMoW's writer reads its footers every batch: contradicted as stated, held in substance.**
  Each batch reads one footer per live file, exactly, but the registered counts assumed two
  live files, which only the 4 MiB run had throughout.
- **C3, clean means clean: contradicted.** In clean probes, a split arm's read CPU runs from
  {eval}`N2["c3_lo"]` to {eval}`N2["c3_hi"]` times its twin's.

```{code-cell} ipython3
:tags: [remove-input]

table(stored.assign(arm=stored.arm.map(labels)).pivot(index="arm", columns="R", values="gib")
      .round(2).reset_index())
```

## Findings

- **At production sizes RMoW's request advantage grows, and so does its byte cost.**
  - MoR-v3 issues {eval}`N["mor_req_128"]` requests per batch at 128 MiB, against
    {eval}`N["mor_req_4"]` at 4 MiB. RMoW-C stays at about {eval}`N["rc_req_128"]`. RMoW-C's
    share of MoR-v3's requests falls from {eval}`N["req_share_4"]` to
    {eval}`N["req_share_128"]`.
  - RMoW-C writes {eval}`N["bytes_x_4"]`× MoR-v3's bytes per batch at 4 MiB and
    {eval}`N["bytes_x_128"]`× at 128 MiB.
- **The small settings flattered MoR-v3's requests.** Its per-batch files sat under the 1 MiB
  whole-file threshold, so each cost one request. At Iceberg's defaults they cost three in a
  locate scan and two in a full read.
- **Uncompacted, RMoW-C stores {eval}`N["rc_stored_x"]`× its live bytes at 128 MiB.** It keeps
  {eval}`N["live_rc_128"]` GiB live in {eval}`N["st_rc"]` GiB. Every batch appends a merged
  region, and nothing reclaims the dead ones in this campaign.
- **On one thread, RMoW-C's full read stays faster than MoR-v3's at Iceberg's defaults, even on
  S3 Express.** It takes {eval}`N3["p4_s3x"]`× the simulated time at batch 60. On eight threads
  the arms come together ({eval}`N3["p5_lo"]`–{eval}`N3["p5_hi"]`×). RMoW-C's batches cost
  {eval}`N3["p4_b_lo"]`–{eval}`N3["p4_b_hi"]`× MoR-v3's.
- **Thread CPU on havoc is noisy beyond foreign load.** With no foreign load on its cores, one
  full read's CPU can differ from its twin's by {eval}`N2["c3_dev"]`, near P4's 10% margin. Reads
  where more of the JVM's other work ran took more. A single phase's CPU cannot decide a
  10% question here; sums over many phases (P3) or a margin of 25% (P5) are less exposed.
- **A shared machine's idle CPUs are not reserved.** Leaving each job's SMT siblings idle invited
  other users' threads onto its cores. Twin arms doing the same work then differed by up to a
  fifth in CPU.

## Limits

- **CPU is this laptop's.** P3–P5 were decided on the laptop, in instructions and modeled time;
  havoc's kernel does not allow counters. Requests and bytes are the same on both machines (C1),
  but a server core's pace is not this laptop's.
- **Modeled time rests on each arm's pace,** good to a few percent (the floors). It leaves out
  kernel time, 1–6% of thread CPU, which tilts slightly toward MoR-v3's batches.
- **RMoW's batch requests are undercounted.** In these runs RMoW's writer kept its footers from
  one batch to the next (`writerCachesFooters`), which no Iceberg writer can, while MoR-v3's
  locate scan read each touched file's footer every batch, as it should. Writers now hold a
  footer for one batch only, in every arm (README, I/O parity rule 6). That adds a footer read
  per live file to each RMoW batch, about 4 requests at these sizes; MoR-v3's requests do not
  change. The rerun measures it.
- **One workload, uncompacted.** Recency-skewed updates only, and no compaction, so stored bytes
  grow without bound.
