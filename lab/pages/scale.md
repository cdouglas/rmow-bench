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
2026-10-23) where the runs can decide them.

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

## P1–P6 (docs/HYPOTHESES.md)

**Mostly contradicted where decided; P3–P5 not decided.**

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
- **P3, CPU scales with bytes: not decided** (foreign load).
- **P4, on one thread requests stop mattering: not decided** (foreign load).
- **P5, on eight threads the largest row group bounds a read: not decided** (foreign load).
- **P6, it fits in 24 GiB: contradicted, as the pilot foresaw.** Each arm's largest
  `stored_bytes` at 128 MiB sums to {eval}`N["st_sum"]` GiB.
- **P6 revised (each RMoW arm under 8 GiB, the sum under 40 GiB): contradicted for RMoW-B, held
  for the sum.**
  - RMoW-B and RMoW-B-split store {eval}`N["st_rb"]` GiB each, just over 8 GiB.
  - RMoW-C stores {eval}`N["st_rc"]` GiB and MoR-v3 {eval}`N["st_mor"]` GiB.

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
- **A shared machine's idle CPUs are not reserved.** Leaving each job's SMT siblings idle invited
  other users' threads onto its cores. Twin arms doing the same work then differed by up to a
  fifth in CPU.

## Limits

- **No CPU verdict.** P3–P5 need a run whose cores stay clean. Requests and bytes are
  unaffected, since they depend only on the op log, config and code.
- **RMoW's batch requests are undercounted.** In these runs RMoW's writer kept its footers from
  one batch to the next (`writerCachesFooters`), which no Iceberg writer can, while MoR-v3's
  locate scan read each touched file's footer every batch, as it should. Writers now hold a
  footer for one batch only, in every arm (README, I/O parity rule 6). That adds a footer read
  per live file to each RMoW batch, about 4 requests at these sizes; MoR-v3's requests do not
  change. The rerun measures it.
- **One workload, uncompacted.** Recency-skewed updates only, and no compaction, so stored bytes
  grow without bound.
