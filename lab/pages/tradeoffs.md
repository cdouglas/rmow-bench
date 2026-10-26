---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: When RMoW pays
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# The runs, pinned by id, all counting instructions. MAIN is the logged 4 MiB recency run without
# the payload check (lab/pages/store-latency.md). SETTINGS are the row-group sweep and the uniform
# run (b6c2605), and the scale campaign on the instruction measure (9f868af)
MAIN = "sim-recency-4m-noverify-aae85e9fd620/20261025T222739Z"
SETTINGS = [  # (series, run)
    ("recency", "local-rg-sweep-recency-exp-rg1m-b1da2d63e642/20261025T165105Z"),
    ("recency", "local-rg-sweep-recency-exp-rg2m-17d089aa92d2/20261025T173636Z"),
    ("recency", "local-rg-sweep-recency-exp-rg4m-a9f7b46157c0/20261025T185718Z"),
    ("recency", "local-rg-sweep-recency-exp-rg8m-3bdbc1d7c076/20261025T192231Z"),
    ("uniform", "local-m1-uniform-nocompact-717579acbd22/20261025T210333Z"),
    ("scaled", "scale-recency-rg4m-5cc727c1e4af/20261025T015547Z"),
    ("scaled", "scale-recency-rg16m-daece520dd8e/20261025T015850Z"),
    ("scaled", "scale-recency-rg64m-34a1c89f1cfa/20261025T020856Z"),
    ("scaled", "scale-recency-rg128m-fe22e4558e59/20261025T025418Z"),
]
SWEEP_4M, S128 = SETTINGS[2][1], SETTINGS[-1][1]
MOR, RMOW = "mor-v3", "rmow-c"
READER = "cached"  # one thread, each file's footer and deletes loaded once per read
BANDWIDTHS = [50, 100, 400]  # MiB/s, for the generic stores
BW = 100  # the bandwidth the settings are compared at
PROFILES = ["cpu only", "s3x", "azure premium", "s3", "azure"]
NAMES = {"cpu only": "CPU only", "s3x": "S3 Express", "azure premium": "Azure premium",
         "s3": "S3 Standard", "azure": "Azure Blob"}
labels = dict(q("SELECT arm, arm_label FROM arm_meta").itertuples(index=False))
RUNS = [MAIN] + [r for _, r in SETTINGS]

# per run, arm and phase (a batch; a full read under READER): means over the steps after the
# initial load
c = q("""
    SELECT run_id, arm, phase, avg(instructions) AS instructions,
           avg(gets + heads + puts + appends + deletes) AS requests, avg(gets + heads) AS gets,
           avg(bytes_read + bytes_written) / 1048576.0 AS mib,
           avg(bytes_read) / 1e6 AS mb_read, avg(bytes_written) / 1e6 AS mb_written
    FROM steps
    WHERE list_contains($runs, run_id) AND list_contains($arms, arm) AND step > 0
      AND (phase = 'batch' OR (phase = 'read_full' AND reader = $reader))
    GROUP BY ALL""", runs=RUNS, arms=[MOR, RMOW], reader=READER)

# CPU at one pace, MAIN's, per arm and phase (ms per instruction; lab/pages/cpu-measure.md): the
# runs' own paces differ on the same instructions, with the laptop's clock and load
pace = q("""
    SELECT run_id, arm, phase, sum(work_ms) / sum(instructions) AS ms_per_instr
    FROM phase_work
    WHERE list_contains($runs, run_id) AND list_contains($arms, arm) AND step > 0
      AND (phase = 'batch' OR (phase = 'read_full' AND reader = $reader))
    GROUP BY ALL""", runs=RUNS, arms=[MOR, RMOW], reader=READER).set_index(["run_id", "arm", "phase"]).ms_per_instr
ref = pace.loc[MAIN]
c["work_ms"] = [r.instructions * ref[(r.arm, r.phase)] for r in c.itertuples()]
C = c.set_index(["run_id", "arm", "phase"])


def deltas(run, reads):
    """RMoW-C minus MoR-v3 for a batch and `reads` full reads: (work ms, MiB moved, requests)."""
    def d(col):
        return (C.loc[(run, RMOW, "batch"), col] - C.loc[(run, MOR, "batch"), col]
                + reads * (C.loc[(run, RMOW, "read_full"), col] - C.loc[(run, MOR, "read_full"), col]))
    return d("work_ms"), d("mib"), d("requests")


def break_even(run, reads, mib_per_s):
    """The request latency (ms) above which RMoW-C takes less time than MoR-v3 for a batch and
    `reads` full reads, on a store where every request takes that latency and every MiB
    1000 / mib_per_s ms; 0 where RMoW-C is faster at any latency."""
    work, mib, requests = deltas(run, reads)
    assert requests < 0, "RMoW-C saves requests"
    return max(0.0, (work + mib * 1000 / mib_per_s) / -requests)


def reads_for_any(run, mib_per_s):
    """The full reads per batch from which RMoW-C is faster at any request latency."""
    (w0, m0, _), (w1, m1, _) = deltas(run, 0), deltas(run, 1)
    per_read = (w1 - w0) + (m1 - m0) * 1000 / mib_per_s
    return -(w0 + m0 * 1000 / mib_per_s) / per_read if per_read < 0 else np.inf


def ms(x):
    return "any" if x == 0 else f"{x:.2g}" if x < 10 else f"{x:.0f}"


def x(a, b, fmt="{:.2f}"):
    return fmt.format(a / b)


def pct(a, b):
    return f"{100 * abs(a / b - 1):.0f}%"


# A batch and a read, MAIN
M = C.loc[MAIN]
one = M.reset_index()
one["phase"] = one.phase.map({"batch": "a batch", "read_full": "a full read"})
one["arm"] = one.arm.map(labels)
one["G instructions"] = one.instructions / 1e9
one = one.sort_values(["phase", "arm"], ascending=[True, False])
gets = q("""
    SELECT arm, step, gets + heads AS gets FROM steps
    WHERE run_id = $r AND phase = 'read_full' AND reader = $reader AND list_contains($arms, arm)""",
         r=MAIN, reader=READER, arms=[MOR, RMOW]).set_index(["arm", "step"]).gets
FIRST = int(min(s for _, s in gets.index if s > 0))
stored = q("""SELECT stored_bytes / live_bytes AS sl FROM steps
              WHERE run_id = $r AND arm = $a AND phase = 'state' AND step = 60""", r=MAIN, a=RMOW).sl.iloc[0]

# Where the time goes: each profile, MAIN, as the latency model prices it
lat = q("""
    SELECT profile, arm, bool_or(needs_append) AS needs_append,
           avg(work_ms) FILTER (WHERE phase = 'batch') AS batch_cpu,
           avg(request_ms) FILTER (WHERE phase = 'batch') AS batch_requests,
           avg(transfer_ms) FILTER (WHERE phase = 'batch') AS batch_transfer,
           avg(work_ms) FILTER (WHERE phase = 'read_full' AND reader = $reader) AS read_cpu,
           avg(request_ms) FILTER (WHERE phase = 'read_full' AND reader = $reader) AS read_requests,
           avg(transfer_ms) FILTER (WHERE phase = 'read_full' AND reader = $reader) AS read_transfer
    FROM phase_latency WHERE run_id = $r AND step > 0 AND list_contains($arms, arm)
    GROUP BY ALL""", r=MAIN, reader=READER, arms=[MOR, RMOW, RMOW + "-split"])
lat = lat[~lat.needs_append].copy()
for ph in ("batch", "read"):
    lat[ph] = lat[f"{ph}_cpu"] + lat[f"{ph}_requests"] + lat[f"{ph}_transfer"]
L = lat.set_index(["profile", "arm"])
parts = lat.melt(id_vars=["profile", "arm"],
                 value_vars=[f"{ph}_{p}" for ph in ("batch", "read") for p in ("cpu", "requests", "transfer")],
                 value_name="ms")
parts["measure"] = parts.variable.str.split("_").str[0].map({"batch": "a batch", "read": "a full read"})
parts["part"] = parts.variable.str.split("_").str[1].map({"cpu": "CPU", "requests": "requests", "transfer": "transfer"})
parts["part_ord"] = parts.part.map({"CPU": 0, "requests": 1, "transfer": 2})
parts["store"] = parts.profile.map(NAMES)
parts["arm_label"] = parts.arm.map(labels)


def rmow_on(profile):
    """RMoW-C on a profile: appendable where the store appends, split where it cannot."""
    return RMOW if (profile, RMOW) in L.index else RMOW + "-split"


ratios = pd.DataFrame([
    (NAMES[p], labels[rmow_on(p)],
     L.loc[(p, rmow_on(p)), "batch"] / L.loc[(p, MOR), "batch"],
     L.loc[(p, rmow_on(p)), "read"] / L.loc[(p, MOR), "read"],
     (L.loc[(p, rmow_on(p)), "batch"] + L.loc[(p, rmow_on(p)), "read"])
     / (L.loc[(p, MOR), "batch"] + L.loc[(p, MOR), "read"]))
    for p in PROFILES], columns=["store", "RMoW arm", "a batch", "a full read", "a batch and a full read"])
R = ratios.set_index("store")


def share(profile, arm, part):
    return f"{100 * L.loc[(profile, arm), f'batch_{part}'] / L.loc[(profile, arm), 'batch']:.0f}%"


# When RMoW pays: the generic stores, MAIN
profiles = q("FROM store_profiles")
profiles["get_mean"] = profiles.get_ms * np.exp(profiles.get_sigma ** 2 / 2)
G = profiles.set_index("profile").get_mean
marks = pd.DataFrame({"name": [f"{NAMES[p]} GET" for p in ("s3x", "s3", "azure")],
                      "short": ["S3 Express", "S3 Std", "Azure"],
                      "ms": [G[p] for p in ("s3x", "s3", "azure")],
                      "below": [False, True, False]})  # S3 Standard's label under its line
READS = np.geomspace(0.01, 3, 81)
curve = pd.DataFrame([(f"{b} MiB/s", b, r, break_even(MAIN, r, b)) for b in BANDWIDTHS for r in READS],
                     columns=["bandwidth", "mib_per_s", "reads", "ms"])
curve = curve[curve.ms >= 0.5]  # the chart's floor; the table says where each reaches "any"
TABLE_READS = [0, 0.1, 0.3, 1, 3]
be_table = pd.DataFrame({"full reads per batch": TABLE_READS} | {
    f"{b} MiB/s": [ms(break_even(MAIN, r, b)) for r in TABLE_READS] for b in BANDWIDTHS})
any_reads = {b: reads_for_any(MAIN, b) for b in BANDWIDTHS}

# What moves the line: the settings, at BW
params = q("""
    SELECT run_id, max(value_num) FILTER (WHERE key = 'layout.rowGroupBytes') / 1048576 AS rg_mib,
           max(value_num) FILTER (WHERE key = 'workload.initialRows') AS initial_rows,
           max(value_num) FILTER (WHERE key = 'workload.opsPerBatch') AS ops
    FROM run_params WHERE list_contains($runs, run_id) GROUP BY ALL""",
           runs=[r for _, r in SETTINGS]).set_index("run_id")


def rows(n):
    return f"{n / 1e6:.3g}M" if n >= 1e6 else f"{n / 1e3:.3g}k"


set_rows = []
for i, (series, run) in enumerate(SETTINGS):
    p = params.loc[run]
    name = f"{series}, {p.rg_mib:g} MiB" + (f", {rows(p.initial_rows)} rows" if series == "scaled" else "")
    work, _, saved = deltas(run, 0)
    set_rows.append((i, series, name, run, p.rg_mib,
                     C.loc[(run, RMOW, "batch"), "instructions"] / C.loc[(run, MOR, "batch"), "instructions"],
                     C.loc[(run, RMOW, "batch"), "mb_written"] / C.loc[(run, MOR, "batch"), "mb_written"],
                     C.loc[(run, RMOW, "batch"), "requests"], C.loc[(run, MOR, "batch"), "requests"],
                     work, -saved, break_even(run, 0, BW), break_even(run, 1, BW)))
S = pd.DataFrame(set_rows, columns=["ord", "series", "setting", "run_id", "rg_mib", "instr_x", "mb_x",
                                    "rmow_requests", "mor_requests", "extra_ms", "saved", "be0", "be1"])
pts = S.melt(id_vars=["ord", "setting"], value_vars=["be0", "be1"], var_name="mix", value_name="ms")
pts["mix"] = pts.mix.map({"be0": "batches only", "be1": "a batch and a full read"})
pts = pts[pts.ms > 0]
rec = S[S.series == "recency"].set_index("rg_mib")
sca = S[S.series == "scaled"].set_index("rg_mib")
uni = S[S.series == "uniform"].iloc[0]
slow = pace[(SWEEP_4M, RMOW, "batch")] / ref[(RMOW, "batch")]

# one thread against eight, S3 Express, the full read at batch 60
s3x = profiles[profiles.profile == "s3x"].iloc[0].to_dict() | {"part_ms": 0.0}
p8 = fig.replay(MAIN, s3x, cpu="work")
p8 = p8[(p8.phase == "read_full") & (p8.reader == "cached-p8") & (p8.step == 60)].set_index("arm").simulated_ms
r60 = q("""SELECT arm, work_ms + request_ms + transfer_ms AS t FROM phase_latency
           WHERE run_id = $r AND profile = 's3x' AND phase = 'read_full' AND reader = $reader AND step = 60""",
        r=MAIN, reader=READER).set_index("arm").t

# S128 on Azure Blob: a batch, its CPU at MAIN's pace as above
az128 = q("""SELECT arm, avg(request_ms + transfer_ms) AS t FROM phase_latency
             WHERE run_id = $r AND profile = 'azure' AND phase = 'batch' AND step > 0 GROUP BY ALL""",
          r=S128).set_index("arm").t
az128 = {a: az128[a] + C.loc[(S128, a, "batch"), "work_ms"] for a in (MOR, RMOW)}

N = {
    "b_instr_x": x(M.loc[(RMOW, "batch"), "instructions"], M.loc[(MOR, "batch"), "instructions"], "{:.1f}"),
    "b_mb_x": x(M.loc[(RMOW, "batch"), "mb_written"], M.loc[(MOR, "batch"), "mb_written"], "{:.0f}"),
    "b_req_x": x(M.loc[(RMOW, "batch"), "requests"], M.loc[(MOR, "batch"), "requests"]),
    "rc_req": f"{M.loc[(RMOW, 'batch'), 'requests']:.1f}", "mor_req": f"{M.loc[(MOR, 'batch'), 'requests']:.0f}",
    "r_instr_less": pct(M.loc[(RMOW, "read_full"), "instructions"], M.loc[(MOR, "read_full"), "instructions"]),
    "r_req_x": x(M.loc[(RMOW, "read_full"), "requests"], M.loc[(MOR, "read_full"), "requests"]),
    "r_mb_more": pct(M.loc[(MOR, "read_full"), "mb_read"], M.loc[(RMOW, "read_full"), "mb_read"]),
    "r_instr_more": pct(M.loc[(MOR, "read_full"), "instructions"], M.loc[(RMOW, "read_full"), "instructions"]),
    "first": f"{FIRST}",
    "mor_g_first": f"{gets[(MOR, FIRST)]:.0f}", "mor_g60": f"{gets[(MOR, 60)]:.0f}",
    "rc_g_first": f"{gets[(RMOW, FIRST)]:.0f}", "rc_g60": f"{gets[(RMOW, 60)]:.0f}",
    "cpu_b_x": f"{R.loc['CPU only', 'a batch']:.1f}", "cpu_r_x": f"{R.loc['CPU only', 'a full read']:.2f}",
    "s3x_cpu_share_c": share("s3x", RMOW, "cpu"), "s3x_req_share_m": share("s3x", MOR, "requests"),
    "s3x_b_x": f"{R.loc['S3 Express', 'a batch']:.1f}", "s3x_r_x": f"{R.loc['S3 Express', 'a full read']:.2f}",
    "s3x_br_x": f"{R.loc['S3 Express', 'a batch and a full read']:.2f}",
    "az_req_share_m": share("azure", MOR, "requests"),
    "az_b_x": f"{R.loc['Azure Blob', 'a batch']:.2f}", "az_r_x": f"{R.loc['Azure Blob', 'a full read']:.2f}",
    "s3_b_x": f"{R.loc['S3 Standard', 'a batch']:.2f}", "s3_r_x": f"{R.loc['S3 Standard', 'a full read']:.2f}",
    "be0_50": ms(break_even(MAIN, 0, 50)), "be0_100": ms(break_even(MAIN, 0, 100)),
    "be0_400": ms(break_even(MAIN, 0, 400)), "be1_100": ms(break_even(MAIN, 1, 100)),
    "any_100": f"{any_reads[100]:.1f}",
    "get_s3x": f"{G['s3x']:.1f}", "get_s3": f"{G['s3']:.0f}", "get_az": f"{G['azure']:.0f}",
    "slow": f"{slow:.1f}",
    "rec_be0": f"{ms(rec.be0.iloc[0])} ms at {rec.index[0]:g} MiB to {ms(rec.be0.iloc[-1])} ms at {rec.index[-1]:g} MiB",
    "rec_be1": f"{ms(rec.be1.min())}–{ms(rec.be1.max())}",
    "rec_mb": f"{C.loc[(rec.run_id.iloc[0], RMOW, 'batch'), 'mb_written']:.1f}–{C.loc[(rec.run_id.iloc[-1], RMOW, 'batch'), 'mb_written']:.1f}",
    "uni_be0": ms(uni.be0), "uni_be1": ms(uni.be1), "uni_instr_x": f"{uni.instr_x:.0f}",
    "sc_lo": f"{sca.index[0]:g}", "sc_hi": f"{sca.index[-1]:g}",
    "sc_be0_lo": ms(sca.be0.iloc[0]), "sc_be0_hi": ms(sca.be0.iloc[-1]),
    "sc_be1_lo": ms(sca.be1.iloc[0]), "sc_be1_hi": ms(sca.be1.iloc[-1]),
    "sc_work_x": f"{sca.extra_ms.iloc[-1] / sca.extra_ms.iloc[0]:.0f}",
    "sc_saved_x": f"{sca.saved.iloc[-1] / sca.saved.iloc[0]:.1f}",
    "az128_b_x": x(az128[RMOW], az128[MOR], "{:.1f}"),
    "stored": f"{stored:.1f}",
    "r60_1": x(r60[RMOW], r60[MOR]), "r60_8": x(p8[RMOW], p8[MOR]),
}
```

Where does region merge on write beat merge on read, and why? This page puts the results in one
frame; the other pages hold the details. RMoW rewrites every row group a batch touches, and
appends the new regions and a new footer to the file. MoR-v3 writes the new row versions to new
files, and marks the replaced ones in deletion vectors. So RMoW spends CPU and bytes on every
batch, to save requests on every batch and every read. Whether that pays depends on three
things:
- what a request costs;
- how often the table is read;
- how much a batch rewrites.

The page compares RMoW-C, the RMoW arm that merges its tail in tiers, against MoR-v3. RMoW-B,
which merges only the open tail, is close to it; [Layout against tail policy](layout-vs-policy.md)
compares the tail policies.

## A batch and a read

**The run** is `sim-recency-4m-noverify`:
- 1M rows, 4 MiB row groups, 60 batches of 10k operations skewed to recent rows;
- no compaction, every request logged.

**Reads** are full reads under the `cached` reader. It reads on one thread and loads each file's
footer and deletes once per read, as an engine with caches would.

**Means** are over the 60 batches, and over the probes after the initial load (every 10 batches).

**CPU** is modeled time: a phase's instructions at the run's pace ([Measuring CPU](cpu-measure.md)).

```{code-cell} ipython3
:tags: [remove-input]

table(one[["phase", "arm", "G instructions", "work_ms", "requests", "mb_read", "mb_written"]]
      .rename(columns={"work_ms": "CPU (ms)", "mb_read": "MB read", "mb_written": "MB written"})
      .reset_index(drop=True))
```

- **A batch: RMoW-C does {eval}`N["b_instr_x"]`× MoR-v3's instructions and writes
  {eval}`N["b_mb_x"]`× its bytes, in {eval}`N["b_req_x"]`× its requests** ({eval}`N["rc_req"]`
  against {eval}`N["mor_req"]`).
  - RMoW-C decodes every row group an update touches, merges the update, and re-encodes the
    whole row group.
  - MoR-v3 encodes only the new row versions and a deletion vector.
- **A full read: RMoW-C does {eval}`N["r_instr_less"]` fewer instructions, in
  {eval}`N["r_req_x"]`× the requests.**
  - Both decode the same live rows, which is most of a read's work.
  - MoR-v3 also decodes the replaced versions its deletion vectors drop, and reads its delete
    files. It reads {eval}`N["r_mb_more"]` more bytes and does {eval}`N["r_instr_more"]` more
    instructions.
  - Applying a deletion vector costs a bit test per row.
- **MoR-v3's reads get dearer with every batch, until it compacts; RMoW-C's barely change.**
  - MoR-v3's full read took {eval}`N["mor_g_first"]` GETs at batch {eval}`N["first"]` and
    {eval}`N["mor_g60"]` at batch 60, as each batch added files.
  - RMoW-C's took {eval}`N["rc_g_first"]` and {eval}`N["rc_g60"]`.

## Where the time goes

The same phases, priced on each store profile. Each bar splits into CPU, requests and transfer.
A profile prices each request kind (GET, PUT, append) at its own latency, and each MiB at its own
rate ([Store latency, simulated](store-latency.md) lists them). S3 Standard has no append, so only
split RMoW-C runs there. Split files cost about what appendable ones do
([Appendable and split RMoW files](layouts.md)).

```{code-cell} ipython3
:label: tradeoffs-time
:tags: [remove-input]
:class: col-page-right

STORES = [NAMES[p] for p in PROFILES]
ARM_ORDER = [labels[a] for a in (MOR, RMOW, RMOW + "-split")]


def bars(measure):
    return (alt.Chart(parts[parts.measure == measure]).mark_bar()
            .encode(x=alt.X("ms:Q", stack="zero", title=f"ms per {measure.removeprefix('a ')}"),
                    y=alt.Y("arm_label:N", sort=ARM_ORDER, title=None),
                    color=alt.Color("part:N", sort=["CPU", "requests", "transfer"], title=None),
                    order=alt.Order("part_ord:Q"),
                    tooltip=["store", "arm_label", "part", alt.Tooltip("ms:Q", format=",.0f")])
            .properties(width=230, height=54)
            .facet(row=alt.Row("store:N", sort=STORES, title=None,
                               header=alt.Header(labelAngle=0, labelAlign="left"))))


alt.hconcat(bars("a batch"), bars("a full read"))
```

Each bar is an arm's simulated time on a store, in ms. Below it, RMoW-C's time ÷ MoR-v3's on each
store. Under 1, RMoW-C is faster.

```{code-cell} ipython3
:tags: [remove-input]

table(ratios)
```

- **On CPU alone, RMoW-C's batch takes {eval}`N["cpu_b_x"]`× MoR-v3's time, and its read
  {eval}`N["cpu_r_x"]`×.** The batch's extra CPU is what the request savings must repay.
- **On S3 Express, requests are cheap, so CPU decides.**
  - CPU is {eval}`N["s3x_cpu_share_c"]` of RMoW-C's batch time; requests are
    {eval}`N["s3x_req_share_m"]` of MoR-v3's.
  - RMoW-C's batch takes {eval}`N["s3x_b_x"]`× MoR-v3's time, and its full read
    {eval}`N["s3x_r_x"]`×.
- **On Azure Blob, requests decide.**
  - Requests are {eval}`N["az_req_share_m"]` of MoR-v3's batch time.
  - RMoW-C's batch takes {eval}`N["az_b_x"]`× MoR-v3's time, and its full read
    {eval}`N["az_r_x"]`×.
  - On S3 Standard, split RMoW-C's batch takes {eval}`N["s3_b_x"]`× MoR-v3's time, and its
    full read {eval}`N["s3_r_x"]`×.

## When RMoW pays

Consider a generic store. It charges every request the same latency, and every MiB read or written
1000 ÷ bandwidth ms. RMoW-C is faster than MoR-v3, for a batch and its reads, once the request
latency exceeds

> break-even = (extra CPU + extra MiB ÷ bandwidth) ÷ requests saved.

Each term is RMoW-C's minus MoR-v3's, over the batch and its reads. Reads lower the break-even,
since an RMoW-C read saves requests, CPU and bytes.

```{code-cell} ipython3
:label: tradeoffs-break-even
:tags: [remove-input]
:class: col-page-right

W = 420
lines = (alt.Chart(curve).mark_line()
         .encode(x=alt.X("reads:Q", scale=alt.Scale(type="log", domain=[0.01, 3], nice=False),
                         title="full reads per batch"),
                 y=alt.Y("ms:Q", scale=alt.Scale(type="log", domain=[0.5, 100], nice=False),
                         title="break-even request latency (ms)"),
                 color=alt.Color("bandwidth:N", sort=[f"{b} MiB/s" for b in BANDWIDTHS],
                                 scale=alt.Scale(scheme="blues", domain=[f"{b} MiB/s" for b in BANDWIDTHS],
                                                 range=["#9ecae1", "#4292c6", "#08306b"]),
                                 title="bandwidth"),
                 tooltip=["bandwidth", alt.Tooltip("reads:Q", format=".2g"),
                          alt.Tooltip("ms:Q", format=".2g")]))
rules = alt.Chart(marks).mark_rule(strokeDash=[4, 3], color="gray").encode(y="ms:Q")
above = (alt.Chart(marks[~marks.below]).mark_text(align="left", dx=4, dy=-7, color="gray", fontSize=11)
         .encode(y="ms:Q", x=alt.value(4), text="name:N"))
below = above.mark_text(align="left", dx=4, dy=8, color="gray", fontSize=11).properties(data=marks[marks.below])
alt.layer(rules, above, below, lines).properties(width=W, height=260)
```

Each line is the break-even request latency of the 4 MiB recency setting at one bandwidth, against
how many full reads come between two batches. Above its line, RMoW-C takes less time than MoR-v3.
A line ends where RMoW-C becomes faster at any latency. The dashed lines mark each profile's mean
GET latency; its PUTs and appends are slower, and the table above prices them exactly. The
break-even latencies (ms):

```{code-cell} ipython3
:tags: [remove-input]

table(be_table)
```

- **Batches alone: RMoW-C is faster above {eval}`N["be0_100"]` ms per request** at 100 MiB/s.
  That is {eval}`N["be0_50"]` ms at 50 MiB/s and {eval}`N["be0_400"]` ms at 400 MiB/s.
- **For comparison, a mean GET takes {eval}`N["get_s3x"]` ms on S3 Express**,
  {eval}`N["get_s3"]` ms on S3 Standard and {eval}`N["get_az"]` ms on Azure Blob.
- **One full read per batch lowers the break-even to {eval}`N["be1_100"]` ms** at 100 MiB/s.
- **From {eval}`N["any_100"]` full reads per batch, RMoW-C is faster at any latency** at
  100 MiB/s.

## What moves the line

The break-even divides two things:
- **what RMoW-C spends per batch**, which grows with the bytes it rewrites;
- **by the requests it saves**, which grow with the files a batch touches.

Three series vary these:
- **Row-group size alone** (the row-group sweep, Row-group size sweep): 1, 2, 4 and 8 MiB row
  groups, with 1M rows and 10k operations per batch.
- **Uniform updates:** 4 MiB row groups, with updates spread over the whole table.
- **Row group, table and batch scaled together** (the scale campaign, At Iceberg's defaults):
  - R-MiB row groups, 128k × R rows, 1,280 × R operations per batch, and 4R-MiB files;
  - R = 128 is Iceberg's defaults.

**Pace.** The runs ran at different paces on the same instructions. For RMoW-C's batch, the
sweep's 4 MiB run took {eval}`N["slow"]`× the logged run's time, at a lower clock and a higher
CPI. So this section converts every run's instructions at the logged run's pace, per arm and
phase.

```{code-cell} ipython3
:label: tradeoffs-settings
:tags: [remove-input]
:class: col-page-right

order = list(S.sort_values("ord").setting)
points = (alt.Chart(pts).mark_point(filled=True, size=70)
          .encode(x=alt.X("ms:Q", scale=alt.Scale(type="log", domain=[1, 200], nice=False),
                          title=f"break-even request latency (ms), {BW} MiB/s"),
                  y=alt.Y("setting:N", sort=order, title=None),
                  color=alt.Color("mix:N", sort=["batches only", "a batch and a full read"], title=None),
                  tooltip=["setting", "mix", alt.Tooltip("ms:Q", format=".2g")]))
vrules = alt.Chart(marks).mark_rule(strokeDash=[4, 3], color="gray").encode(x="ms:Q")
vtext = (alt.Chart(marks).mark_text(angle=270, align="right", baseline="bottom", dx=-2, dy=-3,
                                    color="gray", fontSize=10)
         .encode(x="ms:Q", y=alt.value(0), text="short:N"))
alt.layer(vrules, vtext, points).properties(width=W, height=230)
```

Each point is a setting's break-even request latency at 100 MiB/s, on a log scale: for batches
alone, and for a batch and a full read. The dashed lines are the mean GETs of S3 Express, S3
Standard and Azure Blob. For each setting's batch, the table gives RMoW-C's
instructions and bytes written against MoR-v3's, and both arms' requests. It also gives RMoW-C's
extra CPU (ms) and the requests it saves.

```{code-cell} ipython3
:tags: [remove-input]

table(S[["setting", "instr_x", "mb_x", "rmow_requests", "mor_requests", "extra_ms", "saved", "be0", "be1"]]
      .rename(columns={"instr_x": "instructions ×", "mb_x": "MB written ×",
                       "rmow_requests": "RMoW-C requests", "mor_requests": "MoR-v3 requests",
                       "extra_ms": "extra CPU (ms)", "saved": "requests saved",
                       "be0": "break-even, batches only", "be1": "break-even, with a read"}))
```

- **Row-group size alone moves the break-even little:** from {eval}`N["rec_be0"]`, batches
  only. With a read per batch it is {eval}`N["rec_be1"]` ms.
  - Recency-skewed updates fall on recent rows, in the last few row groups.
  - So RMoW-C's batch writes {eval}`N["rec_mb"]` MB whatever their size.
- **Uniform updates raise it to {eval}`N["uni_be0"]` ms** ({eval}`N["uni_be1"]` ms with a read
  per batch).
  - Every batch touches every row group.
  - So RMoW-C does {eval}`N["uni_instr_x"]`× MoR-v3's instructions per batch, and writes what
    copy-on-write writes (H2, [Summary of results](summary.md)).
- **Large batches over large row groups raise it most.**
  - Batches only, it goes from {eval}`N["sc_be0_lo"]` ms at R = {eval}`N["sc_lo"]` to
    {eval}`N["sc_be0_hi"]` ms at R = {eval}`N["sc_hi"]`. With a read per batch, it goes from
    {eval}`N["sc_be1_lo"]` to {eval}`N["sc_be1_hi"]` ms.
  - Between those sizes, RMoW-C's extra CPU per batch grows {eval}`N["sc_work_x"]`×, and the
    requests it saves only {eval}`N["sc_saved_x"]`×.
  - At Iceberg's defaults on Azure Blob, RMoW-C's batch takes {eval}`N["az128_b_x"]`× MoR-v3's
    time.

## When RMoW is a good idea

**RMoW-C wins when:**
- **Requests are slow compared with the CPU a batch spends.** On stores whose GETs take tens of
  ms, RMoW-C is faster at batches and reads in the 4 MiB setting. Those are S3 Standard (split
  files) and Azure Blob. On S3 Express its batches are slower ({eval}`N["s3x_b_x"]`×), and reads
  must repay them: a batch and one full read take RMoW-C {eval}`N["s3x_br_x"]`× MoR-v3's time.
- **The table is read often.** Every full read is cheaper with RMoW-C, and more so as MoR-v3's
  files accumulate. From {eval}`N["any_100"]` full reads per batch, it is faster at any request
  latency.
- **Batches are small, and updates clustered**, so a batch rewrites a few row groups. Two
  settings raise the break-even:
  - uniform updates, to {eval}`N["uni_be0"]` ms;
  - large batches over 128 MiB row groups, to {eval}`N["sc_be0_hi"]` ms.
- **You would rather not compact for read performance.** RMoW-C's reads hold without compaction,
  and it compacts only for space ([When to compact](compaction.md)).

**MoR-v3 wins when:**
- writes dominate, on a fast store;
- updates are spread over the table, or batches rewrite large row groups;
- bytes written or stored are what costs. RMoW-C writes {eval}`N["b_mb_x"]`× the bytes, and,
  uncompacted, stores {eval}`N["stored"]`× its live bytes at batch 60.

## Limits

- **Simulated latency.** The times are requests, bytes and modeled CPU, priced by a model. The S3
  Express run that checks the model has not run yet (docs/S3X_RUNBOOK.md).
- **The harness's CPU.** It decodes and encodes through Iceberg's generic row readers and
  writers, which are slower than an engine's vectorized ones. A faster encoder shrinks RMoW's
  extra CPU most, and so lowers every break-even here.
- **One thread.** With eight threads, MoR-v3's extra GETs overlap. On S3 Express, RMoW-C's full
  read at batch 60 takes {eval}`N["r60_1"]`× MoR-v3's time on one thread and
  {eval}`N["r60_8"]`× on eight ([Store latency, simulated](store-latency.md)).
- **Full reads only.** Reads of recent rows and time travel have their own pages
  ([Layout against tail policy](layout-vs-policy.md), [Time travel](time-travel.md)).
- **Time, not money.** Storage, compaction, and charges per request and per byte are priced in
  [When to compact](compaction.md). Metadata and commits are not modeled for any arm.
- **One seed and one workload family.**
