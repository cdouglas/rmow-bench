---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Cloud calibration
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()

# The runs, pinned by id, per provider (campaign cloud): the calibration on each store, three runs
# on the registered build, and each store's spike. The stand-ins this page was built on are
# deleted (docs/EXPERIMENTS.md, Retired runs).
LATENCY = {
    "S3 Express": ["s3x-calibration-f57c86f8e3e7/20261028T221905Z",
                   "s3x-calibration-f57c86f8e3e7/20261028T222037Z",
                   "s3x-calibration-f57c86f8e3e7/20261028T222208Z"],
    "Azure Blob": ["azure-calibration-cb4830f16cf3/20261028T233643Z",
                   "azure-calibration-cb4830f16cf3/20261028T233956Z",
                   "azure-calibration-cb4830f16cf3/20261028T234301Z"],
}
SENT = LATENCY  # what each provider received: the model's requests, and its SDK's own counts
SPIKES = {"S3 Express": "spike/s3x-spike-20261028T221407Z", "Azure Blob": "spike/azure-spike-20261028T233151Z"}
PROFILES = dict(SPIKES)  # the spikes' measured profiles (measured_profiles), which C2 prices with
ASSUMED = {"S3 Express": "s3x", "Azure Blob": "azure"}  # the profiles the twins were priced with
# the local twins (campaign twins), whose requests and bytes C1 checks the cloud runs against
TWINS = {"S3 Express": "s3x-calibration-local-8433ccfd4620/20261027T175847Z",
         "Azure Blob": "azure-calibration-azurite-a96fd56de2f0/20261027T175614Z"}
# docs/HYPOTHESES.md, predicted from the twins: each arm's mean batch and mean full read under
# cached, over MoR-v3's; RMoR-C's full read also under cached-skip
PRED = {"S3 Express": {"tailcow-c": (0.69, 0.89), "rmor-c": (1.54, 1.03),
                       "rmor-c-footer": (1.43, 0.76), "rmow-c": (1.37, 0.80)},
        "Azure Blob": {"tailcow-c": (0.50, 0.66), "rmor-c": (0.55, 0.78),
                       "rmor-c-footer": (0.54, 0.58), "rmow-c": (0.36, 0.59)}}
PRED_SKIP = {"S3 Express": 0.79, "Azure Blob": 0.62}
READER = "cached"  # one thread; each file's footer and deletes loaded once per read

summary = fig.summary_points(LATENCY, READER)
K = int(summary.k.max())
RUNS = int(summary.runs.min())
lat = fig.update_latency(LATENCY, READER)
sent = fig.update_requests(SENT, READER)
moved = fig.update_bytes(SENT, READER)
parts, state = fig.storage(SENT)
ops = fig.spike_ops(SPIKES)
check = fig.model_check(LATENCY, PROFILES)
mor = summary[(summary.arm == "mor-v3") & (summary.phase == "read_full")].set_index("provider")
rmow = summary[(summary.arm == "rmow-c") & (summary.phase == "read_full")].set_index("provider")
ALL = sum(LATENCY.values(), [])
clock = q("""SELECT min(json_extract(manifest, '$.host.thread_cpu_per_wall')::DOUBLE) AS lo,
                    max(json_extract(manifest, '$.host.thread_cpu_per_wall')::DOUBLE) AS hi
             FROM runs WHERE list_contains($runs, run_id)""", runs=ALL).iloc[0]
```

The calibration on the real stores: what a delta update costs a writer and a reader on S3 Express
and on Azure Blob, for every arm, and whether the twins' predictions (docs/HYPOTHESES.md, C1–C4)
hold. {eval}`K` batches run against one checkpoint (the initial load), {eval}`RUNS` runs per
provider. Latency is measured wall time; requests, bytes and storage are counted per update.
Reads are one-threaded and cache each file's footer and deletes (`cached`).

## Setup

**Runs** (campaign `cloud`), pinned by id (`LATENCY`, `SPIKES`):
- S3 Express: `s3x-calibration-f57c86f8e3e7`, three runs, and `spike/s3x-spike-20261028T221407Z`;
- Azure Blob: `azure-calibration-cb4830f16cf3`, three runs, and `spike/azure-spike-20261028T233151Z`.

**Twins** (campaign `twins`, `TWINS`): `s3x-calibration-local-8433ccfd4620/20261027T175847Z` (the
model's requests on a local store) and `azure-calibration-azurite-a96fd56de2f0/20261027T175614Z`
(on Azurite, so with the Azure SDK's own counts). C1–C4's predictions were computed from them.

**Stores and VMs.** Each store was read from one VM beside it, deployed by
[deploy/](../../deploy/README.md) on 2026-10-28:
- **S3 Express:** catalog-bench's directory bucket in usw2-az3 (us-west-2), under a prefix of
  this project's, from a `c7a.2xlarge` in the same zone (AMD Zen 4, 8 vCPUs). The bucket is
  shared with catalog-bench.
- **Azure Blob:** a Standard LRS StorageV2 account with a hierarchical namespace (ADLS Gen2) in
  westus, from a `D8as_v6` (AMD Zen 4, 8 vCPUs; the subscription has no quota there for the
  `D8as_v5` first planned).

Both VMs are the twins' laptop's microarchitecture, but not its CPU: the twins priced CPU at the
laptop's pace. Every run's two clocks agree: `host.thread_cpu_per_wall` lies between
{eval}`f"{clock.lo:.4f}"` and {eval}`f"{clock.hi:.4f}"`.

**Builds.** The calibration ran on 9a69028, where C1–C4 were registered, on the atomicio fork at
af875ccfd1, the twins' builds. The spikes ran from main (5e99be2), whose spike reaches 128 MiB.
Before the calibration, each VM ran the fork's atomic-operations contract on its store (it passed)
and rmow-bench's store contract, whose failures were the tests' own (ROADMAP.md).

## Writing and reading deltas against a checkpoint

Each point is an arm's mean write (one batch) and mean read over the reads at 0 to {eval}`K`
deltas; its whisker runs from the read at 0 deltas to the read at {eval}`K`, just before the next
checkpoint would be written. On S3 Express MoR-v3's full read goes from
{eval}`f"{mor.loc['S3 Express', 'read_k0']:.0f}"` to {eval}`f"{mor.loc['S3 Express', 'read_kk']:.0f}"` ms
and RMoW-C's from {eval}`f"{rmow.loc['S3 Express', 'read_k0']:.0f}"` to
{eval}`f"{rmow.loc['S3 Express', 'read_kk']:.0f}"` ms; on Azure, MoR-v3's from
{eval}`f"{mor.loc['Azure Blob', 'read_k0']:.0f}"` to {eval}`f"{mor.loc['Azure Blob', 'read_kk']:.0f}"` ms
and RMoW-C's from {eval}`f"{rmow.loc['Azure Blob', 'read_k0']:.0f}"` to
{eval}`f"{rmow.loc['Azure Blob', 'read_kk']:.0f}"` ms.

```{code-cell} ipython3
:label: cloud-summary
:tags: [remove-input]

fig.summary_chart(summary)
```

```{code-cell} ipython3
:tags: [remove-input]

table(summary[["provider", "read", "arm_label", "write_ms", "read_ms", "read_k0", "read_kk",
               "reads", "runs"]].round(1))
```

## Latency per update

Measured latency of each update, the mean over the runs with the least and the most as a whisker:
each batch (a write), and each read at every fifth delta.

```{code-cell} ipython3
:label: cloud-latency-s3x
:tags: [remove-input]

fig.update_latency_chart(lat, "S3 Express", ["batch", "read_full", "read_recent"])
```

```{code-cell} ipython3
:label: cloud-latency-azure
:tags: [remove-input]

fig.update_latency_chart(lat, "Azure Blob", ["batch", "read_full", "read_recent"])
```

```{code-cell} ipython3
:tags: [remove-input]

per = (lat.groupby(["provider", "arm_label", "ord", "phase"]).ms
       .agg(["mean", "min", "max", "count"]).reset_index().sort_values(["provider", "ord", "phase"]))
table(per.drop(columns="ord").round(1))
```

## Requests per update, as each provider received them

What each update sent the store, by file: Parquet (data files, footers, row groups and RMoW's
regions alike) and Puffin (DVs). On Azure an appendable file's create is three requests where
other stores take one, and appends past 100 MiB split. S3 Express opens sessions, but none fell
inside a measured phase here. The finer kinds of request (GETs, HEADs, Put Blobs, Append Blocks,
creates, renames) are in the table.

```{code-cell} ipython3
:label: cloud-requests-write
:tags: [remove-input]

fig.stacked_per_update(sent[sent.phase == "batch"], "requests", "requests per batch")
```

```{code-cell} ipython3
:label: cloud-requests-read
:tags: [remove-input]

fig.stacked_per_update(sent[sent.phase == "read_full"], "requests", "requests per full read")
```

```{code-cell} ipython3
:tags: [remove-input]

kinds = (sent.groupby(["provider", "arm_label", "ord", "phase", "file_type", "kind", "source"])
         .requests.sum().reset_index().sort_values(["provider", "ord", "phase"]))
table(kinds.drop(columns="ord"))
```

## Bytes per update

Bytes each update moved, by file: a batch's reads (MoR's DV loads, RMoW's merge reads) and writes,
and a full read's. These are the model's bytes; Azure's sequential reads fetch whole 4 MiB blocks,
which the provider's own counts (`sdk_get_bytes`) hold.

```{code-cell} ipython3
:label: cloud-bytes
:tags: [remove-input]

m = moved[moved.provider == next(iter(SENT))]
alt.hconcat(
    fig.stacked_per_update(m[m.phase == "batch"], "mb_written", "MB written per batch"),
    fig.stacked_per_update(m[m.phase == "batch"], "mb_read", "MB read per batch"),
    fig.stacked_per_update(m[m.phase == "read_full"], "mb_read", "MB read per full read"))
```

```{code-cell} ipython3
:tags: [remove-input]

b = (moved.groupby(["provider", "arm_label", "ord", "phase", "file_type", "object_class",
                    "appended"])[["mb_read", "mb_written"]].sum().reset_index()
     .sort_values(["provider", "ord", "phase"]))
table(b.drop(columns="ord").round(3))
```

## Storage after each update

What each arm stores after each update, by part (left), and how much of it is live (right): dead
bytes inside live objects are RMoW's superseded regions and footers; reclaimable objects are ones
no version refers to. Storage is the same on every store, so this is one provider's run.

```{code-cell} ipython3
:label: cloud-storage
:tags: [remove-input]

p0 = parts[parts.provider == next(iter(SENT))]
s0 = state[state.provider == next(iter(SENT))].melt(
    id_vars=["provider", "arm_label", "ord", "step"], value_vars=["live", "dead", "reclaimable"],
    var_name="kind", value_name="mb")
alt.hconcat(
    fig.stacked_per_update(p0, "mb", "MB stored", stack="part", labels=fig.PARTS),
    fig.stacked_per_update(s0, "mb", "MB stored", stack="kind",
                           labels={"live": "live", "dead": "dead in live objects",
                                   "reclaimable": "reclaimable objects"})
).resolve_scale(color="independent")
```

```{code-cell} ipython3
:tags: [remove-input]

last = state[state.step == state.step.max()]
table(last[["provider", "arm_label", "stored", "live", "dead", "reclaimable"]].round(2))
```

(cloud-spikes)=
## The stores' requests, measured

The spikes' requests by size, the median with the 10th to 90th percentiles: writes (Iceberg's
PUTs, the store's own appends and the fork's conditional ones) and reads (whole objects, and
ranges of a 128 MiB one), and a GET of an appended object against its parts.

```{code-cell} ipython3
:label: cloud-spike-writes
:tags: [remove-input]

fig.spike_size_chart(ops, ["put", "append", "append_sao"])
```

```{code-cell} ipython3
:label: cloud-spike-reads
:tags: [remove-input]

fig.spike_size_chart(ops, ["get_size", "get_range"])
```

```{code-cell} ipython3
:label: cloud-spike-parts
:tags: [remove-input]

fig.spike_parts_chart(ops)
```

```{code-cell} ipython3
:tags: [remove-input]

fits = q("""SELECT g.provider, f.* EXCLUDE (run_id)
            FROM spike_fits f JOIN (SELECT unnest($p) AS provider, unnest($r) AS run_id) g
            USING (run_id) ORDER BY g.provider, f.op""",
         p=list(SPIKES), r=list(SPIKES.values()))
table(fits.round(3))
```

```{code-cell} ipython3
:tags: [remove-cell]

prof = q("""
    WITH g AS (SELECT unnest($p) AS provider, unnest($m) AS measured, unnest($a) AS assumed),
    p AS (SELECT profile, get_ms::DOUBLE AS get_ms, put_ms::DOUBLE AS put_ms,
                 append_ms::DOUBLE AS append_ms, read_ms_per_mib::DOUBLE AS read_ms_per_mib,
                 write_ms_per_mib::DOUBLE AS write_ms_per_mib FROM store_profiles
          UNION ALL BY NAME
          SELECT profile, get_ms, put_ms, append_ms, read_ms_per_mib, write_ms_per_mib
          FROM measured_profiles)
    SELECT g.provider, 'spike' AS source, p.* EXCLUDE (profile) FROM g JOIN p ON p.profile = g.measured
    UNION ALL
    SELECT g.provider, 'assumed (' || g.assumed || ')', p.* EXCLUDE (profile)
    FROM g JOIN p ON p.profile = g.assumed ORDER BY 1, 2 DESC""",
         p=list(SPIKES), m=list(SPIKES.values()), a=[ASSUMED[p] for p in SPIKES])
P = {(r.provider, r.source.split(" ")[0]): r for r in prof.itertuples()}
creates = fits[(fits.provider == "Azure Blob") & fits.op.isin(["create", "create_sao"])].set_index("op")
```

The profile each spike gives (`measured_profiles`: a request's latency at the smallest size, and
the slope over sizes) against the one the twins were priced with. Azure is far faster than its
assumed profile, which came from a 2023 benchmark in West Europe and an estimate: a GET takes
{eval}`f"{P['Azure Blob', 'spike'].get_ms:.1f}"` ms, not
{eval}`f"{P['Azure Blob', 'assumed'].get_ms:.0f}"`, and the fork's append
{eval}`f"{P['Azure Blob', 'spike'].append_ms:.1f}"` ms, not
{eval}`f"{P['Azure Blob', 'assumed'].append_ms:.0f}"`. On S3 Express a GET is dearer than
assumed ({eval}`f"{P['S3 Express', 'spike'].get_ms:.1f}"` ms against
{eval}`f"{P['S3 Express', 'assumed'].get_ms:.1f}"`) and the fork's append cheaper
({eval}`f"{P['S3 Express', 'spike'].append_ms:.1f}"` against
{eval}`f"{P['S3 Express', 'assumed'].append_ms:.0f}"`). The fork's atomic create on Azure, a
staged Append Blob and a rename, takes {eval}`f"{creates.loc['create_sao', 'fixed_ms']:.0f}"` ms
where the store's own create takes {eval}`f"{creates.loc['create', 'fixed_ms']:.0f}"`.

```{code-cell} ipython3
:tags: [remove-input]

table(prof.round(2))
```

## Model check (internal)

Not for publication: how well the development model (lab/pages/store-latency.md) predicts each
one-thread phase, priced with the spikes' measured profiles, as C2 is judged. The model is the
phase's CPU plus its requests' expected latencies plus its bytes at the profile's rates.

```{code-cell} ipython3
:label: cloud-model-check
:tags: [remove-input]

color, shape = fig.arm_encoding(list(check.arm.unique()))
pts = alt.Chart().mark_point(filled=True, opacity=0.6).encode(
    x=alt.X("model_ms:Q", title="model, ms", scale=alt.Scale(type="log")),
    y=alt.Y("measured_ms:Q", title="measured, ms", scale=alt.Scale(type="log")),
    color=color, shape=shape, tooltip=["arm_label", "what", "step", "measured_ms", "model_ms"])
diag = alt.Chart().mark_line(color="gray", strokeDash=[4, 4]).encode(x="model_ms:Q", y="model_ms:Q")
alt.layer(pts, diag, data=check[["provider", "arm_label", "what", "step", "measured_ms",
                                 "model_ms"]]).properties(width=260, height=220).facet(
    column=alt.Column("provider:N", title=None, sort=list(LATENCY)), row=alt.Row("what:N", title=None)
).resolve_scale(x="independent", y="independent")
```

```{code-cell} ipython3
:tags: [remove-input]

check["ratio"] = check.measured_ms / check.model_ms
table(check.groupby(["provider", "arm_label", "what"]).ratio.describe()[["count", "50%", "min", "max"]]
      .round(3).reset_index())
```

```{code-cell} ipython3
:tags: [remove-cell]

# C2 as registered: each one-thread phase's simulated time within 25% of its wall time, priced with
# the spike's profile; for contrast, with the profile the twins were priced with
assumed = fig.model_check(LATENCY, ASSUMED)
c2 = []
for name, d in (("spike", check), ("assumed", assumed)):
    d = d.assign(within=(d.model_ms / d.measured_ms - 1).abs() <= 0.25,
                 sim_over_wall=d.model_ms / d.measured_ms)
    c2.append(d.groupby(["provider", "what"]).agg(phases=("within", "size"),
                                                  within_25pct=("within", "mean"),
                                                  median_sim_over_wall=("sim_over_wall", "median"))
              .reset_index().assign(profile=name))
c2 = pd.concat(c2, ignore_index=True)
W = c2.set_index(["profile", "provider", "what"])

# The model's structure: in one-thread phases, the share of wall time that is neither CPU nor
# waiting on the store
struct = q("""
    WITH g AS (SELECT unnest($p) AS provider, unnest($r) AS run_id)
    SELECT g.provider, b.phase,
           sum(b.ms) FILTER (WHERE b.component = 'other waiting') / sum(b.ms) AS other_waiting,
           sum(b.ms) FILTER (WHERE b.component = 'waiting on the store') / sum(b.ms) AS store_waiting,
           sum(b.ms) FILTER (WHERE b.component LIKE '%CPU') / sum(b.ms) AS cpu
    FROM phase_breakdown b JOIN g USING (run_id)
    LEFT JOIN run_readers rr ON rr.run_id = b.run_id AND rr.reader = b.reader
    WHERE b.thread = 'phase' AND b.phase IN ('batch', 'read_full', 'read_recent', 'read_old')
      AND coalesce(rr.parallelism, 1) = 1
    GROUP BY ALL ORDER BY 1, 2""",
           p=[p for p, rs in LATENCY.items() for _ in rs], r=ALL)

# The calibration's bounded ranged GETs, from its request log, against each profile's expected
# latency at their sizes (the lognormal's mean plus the bytes at the profile's rate)
gets = q("""
    WITH g AS (SELECT unnest($p) AS provider, unnest($r) AS run_id, unnest($m) AS measured,
                      unnest($a) AS assumed),
    r AS (SELECT g.provider, g.measured, g.assumed, x.length, x.dur_ns / 1e6 AS ms
          FROM requests x JOIN g USING (run_id) WHERE x.kind = 'get_range' AND x.dur_ns > 0),
    p AS (SELECT profile, get_ms::DOUBLE AS get_ms, get_sigma::DOUBLE AS get_sigma,
                 read_ms_per_mib::DOUBLE AS rate FROM measured_profiles
          UNION ALL SELECT profile, get_ms, get_sigma, read_ms_per_mib FROM store_profiles)
    SELECT r.provider, CASE WHEN length < 65536 THEN 'under 64 KiB' ELSE '64 KiB to 1 MiB' END AS size,
           count(*) AS gets, median(length) / 1024 AS median_kib, median(ms) AS median_ms,
           avg(ms) AS mean_ms,
           avg(pm.get_ms * exp(pm.get_sigma ^ 2 / 2) + length / 1048576.0 * pm.rate) AS spike_ms,
           avg(pa.get_ms * exp(pa.get_sigma ^ 2 / 2) + length / 1048576.0 * pa.rate) AS assumed_ms
    FROM r JOIN p pm ON pm.profile = r.measured JOIN p pa ON pa.profile = r.assumed
    GROUP BY ALL ORDER BY r.provider, min(length)""",
         p=[p for p, rs in LATENCY.items() for _ in rs], r=ALL,
         m=[SPIKES[p] for p, rs in LATENCY.items() for _ in rs],
         a=[ASSUMED[p] for p, rs in LATENCY.items() for _ in rs])
gets["spike_over_measured"] = gets.spike_ms / gets.mean_ms
GT = gets.set_index(["provider", "size"])
```

(cloud-gets)=
## The calibration's requests against the spikes

C2 prices each phase with the spike's profile, and misses ([below](#c1-c4)). The model's
structure is not what misses: in the one-thread phases, CPU and waiting on the store account for
all but {eval}`f"{100 * struct.other_waiting.max():.1f}"`% of the wall time on either store. The
profile is. The calibration logged every request with its wall time, so its GETs can be priced
both ways. The profile takes a GET's latency at the spike's smallest size (64 KiB) and adds the
slope fitted over 64 KiB to 128 MiB; the calibration's ranged GETs are mostly far smaller (a
median of {eval}`f"{GT.loc[('S3 Express', 'under 64 KiB'), 'median_kib']:.1f}"` KiB and
{eval}`f"{GT.loc[('S3 Express', '64 KiB to 1 MiB'), 'median_kib']:.0f}"` KiB), where latency is
flat on S3 Express and steps up past 128 KiB on Azure. So the spike's profile overprices S3
Express's GETs by {eval}`f"{100 * (GT.loc[('S3 Express', 'under 64 KiB'), 'spike_over_measured'] - 1):.0f}"`%
and {eval}`f"{100 * (GT.loc[('S3 Express', '64 KiB to 1 MiB'), 'spike_over_measured'] - 1):.0f}"`%,
and underprices Azure's larger ones by
{eval}`f"{100 * (1 - GT.loc[('Azure Blob', '64 KiB to 1 MiB'), 'spike_over_measured']):.0f}"`%. The
assumed `s3x` profile, a low fixed cost and a high rate, happens to price S3 Express's GETs about
right, which is why it fits the S3 Express runs better than the spike's.

```{code-cell} ipython3
:tags: [remove-input]

table(gets.round(2))
```

```{code-cell} ipython3
:tags: [remove-input]

table(struct.round(3))
```

```{code-cell} ipython3
:tags: [remove-cell]

# Time travel under the stock reader: what the SDKs' responses declared against the model's bytes
tt = q("""
    WITH g AS (SELECT unnest($p) AS provider, unnest($r) AS run_id)
    SELECT g.provider, s.arm, s.phase, median(s.wall_ns) / 1e6 AS median_ms,
           median(s.sdk_get_bytes) / 1e6 AS declared_mb, median(s.bytes_read) / 1e6 AS model_mb,
           count(*) AS reads
    FROM steps s JOIN g USING (run_id)
    WHERE s.reader = 'stock' AND s.phase IN ('read_full', 'read_old')
    GROUP BY ALL ORDER BY 1, 3, 2""", p=[p for p, rs in LATENCY.items() for _ in rs], r=ALL)
tt["arm"] = tt.arm.map(q("SELECT arm, arm_label FROM arm_meta").set_index("arm").arm_label)
TT = tt.set_index(["provider", "arm", "phase"])
tt_other = tt[(tt.phase == "read_old") & (tt.arm != "RMoW-C")].groupby("provider").median_ms.max()
```

(cloud-time-travel)=
## Time travel under the stock reader

Stock parquet-java reads each footer with open-ended GETs ([time travel](time-travel.md)). On an
old version of an appendable file the first of them runs on into the newer versions, and the
store's response declares the rest of the object. Under the stock reader, RMoW-C's time-travel
reads declared {eval}`f"{TT.loc[('S3 Express', 'RMoW-C', 'read_old'), 'declared_mb']:.0f}"` MB on
S3 Express and {eval}`f"{TT.loc[('Azure Blob', 'RMoW-C', 'read_old'), 'declared_mb']:.0f}"` MB on
Azure (medians), where the model reads
{eval}`f"{TT.loc[('Azure Blob', 'RMoW-C', 'read_old'), 'model_mb']:.1f}"` MB. On S3 Express
the aborted stream costs nothing visible: the read takes
{eval}`f"{TT.loc[('S3 Express', 'RMoW-C', 'read_old'), 'median_ms']:.0f}"` ms, like the other
arms'. On Azure it takes {eval}`f"{TT.loc[('Azure Blob', 'RMoW-C', 'read_old'), 'median_ms']:.0f}"` ms,
against at most {eval}`f"{tt_other['Azure Blob']:.0f}"` ms for the other arms', which fits the
Azure SDK reading on rather than dropping the connection. The bounded readers (`cached`,
`one-footer`) open no such GETs, and their time-travel reads are unaffected.

```{code-cell} ipython3
:tags: [remove-input]

table(tt.round(1))
```

```{code-cell} ipython3
:tags: [remove-cell]

# C1: every cloud run against its twin, model records; and the SDK's counts per phase
c1 = []
for p, runs in LATENCY.items():
    for r in runs:
        d = q("""SELECT count(*) AS records,
                        count(*) FILTER (WHERE len(differs) > 0 OR one_run_only) AS differing
                 FROM steps_diff($a, $b)""", a=r, b=TWINS[p]).iloc[0]
        c1.append(dict(provider=p, run=r, records=d.records, differing=d.differing))
c1 = pd.DataFrame(c1)
s3sdk = q("""SELECT count(*) AS phases,
                    count(*) FILTER (WHERE sdk_gets <> gets OR sdk_puts <> puts OR sdk_appends <> appends
                                     OR sdk_heads <> heads OR sdk_deletes <> deletes
                                     OR sdk_lists + sdk_other <> 0 OR sdk_put_bytes <> bytes_written)
                      AS differing,
                    sum(sdk_retries) AS retries, sum(sdk_sessions) AS sessions
             FROM steps WHERE list_contains($runs, run_id) AND sdk_requests IS NOT NULL""",
          runs=LATENCY["S3 Express"]).iloc[0]
azsdk = q("""
    WITH c AS (SELECT * FROM steps WHERE list_contains($runs, run_id) AND sdk_requests IS NOT NULL),
    t AS (SELECT * FROM steps WHERE run_id = $twin AND sdk_requests IS NOT NULL)
    SELECT c.arm, c.sdk_heads - t.sdk_heads AS heads, c.sdk_other - t.sdk_other AS other,
           (c.sdk_gets, c.sdk_puts, c.sdk_blocks, c.sdk_block_lists, c.sdk_blob_creates, c.sdk_appends,
            c.sdk_deletes, c.sdk_lists, c.sdk_get_bytes, c.sdk_put_bytes)
             = (t.sdk_gets, t.sdk_puts, t.sdk_blocks, t.sdk_block_lists, t.sdk_blob_creates,
                t.sdk_appends, t.sdk_deletes, t.sdk_lists, t.sdk_get_bytes, t.sdk_put_bytes) AS rest_equal,
           count(*) AS phases, sum(c.sdk_retries) AS retries
    FROM c JOIN t USING (arm, step, phase) WHERE coalesce(c.reader, '') = coalesce(t.reader, '')
    GROUP BY ALL ORDER BY phases DESC""", runs=LATENCY["Azure Blob"], twin=TWINS["Azure Blob"])
az_same = int(azsdk[(azsdk.heads == 0) & (azsdk.other == 0) & azsdk.rest_equal].phases.sum())
az_create = azsdk[(azsdk.heads == -1) & (azsdk.other == 1) & azsdk.rest_equal]
az_phases = int(azsdk.phases.sum())

# C3: each arm's mean batch and full read over MoR-v3's, measured against predicted; per run too
labels = q("SELECT arm, arm_label FROM arm_meta").set_index("arm").arm_label
skip = fig.update_latency(LATENCY, "cached-skip")
def ratios(l, phase):
    s = l[(l.phase == phase) & ((l.step >= 1) if phase == "batch" else True)]
    m = s.groupby(["provider", "run_id", "arm"]).ms.mean().unstack("arm")
    return m.div(m["mor-v3"], axis=0)
c3 = []
for what, l, phase in (("batch", lat, "batch"), ("full read", lat, "read_full"),
                       ("full read, cached-skip", skip, "read_full")):
    r = ratios(l, phase)
    for p in LATENCY:
        for arm, (pb, pr) in PRED[p].items():
            if what == "full read, cached-skip" and arm != "rmor-c":
                continue
            pred = {"batch": pb, "full read": pr}.get(what, PRED_SKIP[p])
            v = r.loc[p, arm]
            judged = abs(pred - 1) >= 0.10 and not (p == "S3 Express" and arm == "tailcow-c"
                                                    and what == "full read")
            held = (v.mean() < 1) == (pred < 1)
            c3.append(dict(provider=p, arm=labels[arm], what=what, predicted=pred,
                           measured=v.mean(), least=v.min(), most=v.max(),
                           verdict=("held" if held else "contradicted") if judged else "not judged"))
c3 = pd.DataFrame(c3)
R = c3.set_index(["provider", "arm", "what"])

# C4: CPU's share of each arm's phases (phase_breakdown), per provider
c4 = q("""
    WITH g AS (SELECT unnest($p) AS provider, unnest($r) AS run_id),
    b AS (SELECT g.provider, b.run_id, b.arm, b.phase, b.step, b.wall_ms,
                 sum(b.ms) FILTER (WHERE b.component LIKE '%CPU') AS cpu_ms
          FROM phase_breakdown b JOIN g USING (run_id)
          WHERE b.thread = 'phase' AND b.phase IN ('batch', 'read_full', 'read_recent', 'read_old')
          GROUP BY ALL)
    SELECT provider, arm, phase, sum(cpu_ms) / sum(wall_ms) AS cpu_share,
           max(cpu_ms / wall_ms) AS most_in_a_phase
    FROM b GROUP BY ALL ORDER BY 1, 3, 2""", p=[p for p, rs in LATENCY.items() for _ in rs], r=ALL)
c4["arm"] = c4.arm.map(labels)
C4 = c4.set_index(["provider", "arm", "phase"]).cpu_share
REGION = ["RMoR-C", "RMoR-C footer", "RMoW-C"]
s3_region = C4.loc["S3 Express"].loc[REGION].xs("batch", level="phase")
s3_rest = C4.loc["S3 Express"].drop(index=[(a, "batch") for a in REGION])
az_cpu = C4.loc["Azure Blob"]
```

## Findings

- **Each store received what the model counts (C1).** All {eval}`int(c1.records.sum())`
  records of the {eval}`len(c1)` cloud runs equal their twins' in requests and bytes
  ({eval}`int(c1.differing.sum())` differ). S3 Express's SDK sent the model's requests in every one
  of {eval}`int(s3sdk.phases)` phases ({eval}`int(s3sdk.differing)` differ,
  {eval}`int(s3sdk.retries)` retries, {eval}`int(s3sdk.sessions)` CreateSessions inside a phase).
  Azure's SDK sent its Azurite twin's in {eval}`az_same` of {eval}`az_phases` phases; the other
  {eval}`int(az_create.phases.sum())` are RMoR-C with footers' creates, a rename on Azure where
  Azurite takes a HEAD.
- **The stores' latencies are not the profiles'.** Azure is several times faster than its
  assumed profile, so it sits much nearer RMoW's break-even than the twins put it; S3 Express's
  GETs are dearer and its appends cheaper than assumed ([above](#cloud-spikes)).
- **On S3 Express the region arms' batches are faster than MoR-v3's, not slower.** RMoW-C's
  batch takes {eval}`f"{R.loc[('S3 Express', 'RMoW-C', 'batch'), 'measured']:.2f}"`× MoR-v3's
  (predicted {eval}`f"{R.loc[('S3 Express', 'RMoW-C', 'batch'), 'predicted']:.2f}"`), RMoR-C with
  footers' {eval}`f"{R.loc[('S3 Express', 'RMoR-C footer', 'batch'), 'measured']:.2f}"`×
  (predicted {eval}`f"{R.loc[('S3 Express', 'RMoR-C footer', 'batch'), 'predicted']:.2f}"`)
  and RMoR-C's {eval}`f"{R.loc[('S3 Express', 'RMoR-C', 'batch'), 'measured']:.2f}"`× (from
  {eval}`f"{R.loc[('S3 Express', 'RMoR-C', 'batch'), 'least']:.2f}"` to
  {eval}`f"{R.loc[('S3 Express', 'RMoR-C', 'batch'), 'most']:.2f}"` over the runs). That fits
  what else was measured: their batches are mostly CPU, which ran at the VM's pace rather than the
  laptop's, their appends cost less than assumed, and MoR-v3's GETs more. Their full reads beat
  MoR-v3's as predicted.
- **On Azure every judged ratio lands where predicted but one, though nearer 1.** RMoW-C's batch takes
  {eval}`f"{R.loc[('Azure Blob', 'RMoW-C', 'batch'), 'measured']:.2f}"`× MoR-v3's (predicted
  {eval}`f"{R.loc[('Azure Blob', 'RMoW-C', 'batch'), 'predicted']:.2f}"`) and its full read
  {eval}`f"{R.loc[('Azure Blob', 'RMoW-C', 'full read'), 'measured']:.2f}"`× (predicted
  {eval}`f"{R.loc[('Azure Blob', 'RMoW-C', 'full read'), 'predicted']:.2f}"`). RMoR-C with
  footers' batch is slower than MoR-v3's,
  {eval}`f"{R.loc[('Azure Blob', 'RMoR-C footer', 'batch'), 'measured']:.2f}"`× against a
  predicted {eval}`f"{R.loc[('Azure Blob', 'RMoR-C footer', 'batch'), 'predicted']:.2f}"`:
  its batches create files, and the fork's atomic create costs
  {eval}`f"{creates.loc['create_sao', 'fixed_ms']:.0f}"` ms there.
- **Stock Iceberg's time travel over RMoW's files is slow on Azure.** Under the stock reader,
  RMoW-C's time-travel reads take
  {eval}`f"{TT.loc[('Azure Blob', 'RMoW-C', 'read_old'), 'median_ms']:.0f}"` ms on Azure against
  at most {eval}`f"{tt_other['Azure Blob']:.0f}"` ms for the other arms': each old footer's
  open-ended GET runs on into newer versions ([above](#cloud-time-travel)). S3 Express aborts it
  at no visible cost. The bounded readers avoid it.
- **CPU bounds the region arms' batches on S3 Express, and weighs more on Azure than
  predicted.** It is {eval}`f"{100 * s3_region.min():.0f}"`–{eval}`f"{100 * s3_region.max():.0f}"`%
  of their batch time on S3 Express and at most {eval}`f"{100 * s3_rest.max():.0f}"`% of any other
  arm's or phase's there. On Azure it reaches {eval}`f"{100 * az_cpu.max():.0f}"`% (RMoW-C's
  batches), where the twins predicted at most 24%: with Azure faster, CPU is a larger share.
- **The model's structure holds; the spike's profile misprices the calibration's small
  requests (C2).** Priced with the spike's profiles, {eval}`f"{100 * W.loc[('spike', 'S3 Express', 'write (batch)'), 'within_25pct']:.0f}"`%
  of S3 Express's batches and {eval}`f"{100 * W.loc[('spike', 'S3 Express', 'full read'), 'within_25pct']:.0f}"`%
  of its full reads come within 25% of their wall time, and
  {eval}`f"{100 * W.loc[('spike', 'Azure Blob', 'write (batch)'), 'within_25pct']:.0f}"`% and
  {eval}`f"{100 * W.loc[('spike', 'Azure Blob', 'full read'), 'within_25pct']:.0f}"`% of Azure's
  ([above](#cloud-gets)).

(c1-c4)=
## C1–C4 (docs/HYPOTHESES.md)

```{code-cell} ipython3
:tags: [remove-input]

table(c3.round(2))
```

```{code-cell} ipython3
:tags: [remove-input]

table(c2.round(3))
```

```{code-cell} ipython3
:tags: [remove-input]

table(c4.round(3))
```

- **C1, the model counts what each store receives: held on both.** No request or byte differs
  from the twins in any record, and the SDKs' counts differ only by Azure's creates, as C1 allows.
- **C2, a phase's time is its CPU plus its requests' latencies: contradicted, as registered.**
  Priced with the spikes' measured profiles, from {eval}`f"{100 * W.loc['spike'].within_25pct.min():.0f}"`%
  to {eval}`f"{100 * W.loc['spike'].within_25pct.max():.0f}"`% of a kind of one-thread phase comes
  within 25% of its wall time, where C2 wants all of them (the second table above). The miss is the profile's, not the structure's: CPU and waiting on the store account
  for the phases' time, and the spikes' profiles misprice the calibration's GETs by up to
  {eval}`f"{100 * (gets.spike_over_measured - 1).abs().max():.0f}"`%. A profile priced by request
  size would test the structure; it was not registered, and is not judged here.
- **C3, the trade-off falls where the twins put it: contradicted on S3 Express's batches, held
  elsewhere but for RMoR-C with footers' batch on Azure.** Every judged full read lands on its
  predicted side on both stores; on S3 Express all three region arms' batches land on the other
  side of 1 (RMoR-C's barely, its runs straddling 1), and on Azure RMoR-C with footers' batch
  does. RMoR-C's full read on Azure holds only barely:
  {eval}`f"{R.loc[('Azure Blob', 'RMoR-C', 'full read'), 'measured']:.2f}"`, its runs from
  {eval}`f"{R.loc[('Azure Blob', 'RMoR-C', 'full read'), 'least']:.2f}"` to
  {eval}`f"{R.loc[('Azure Blob', 'RMoR-C', 'full read'), 'most']:.2f}"`.
- **C4, CPU is the bottleneck only of the region arms' batches on S3 Express: held on S3
  Express, contradicted on Azure.** CPU is more than half of the region arms' batch time on S3
  Express and less than half of everything else there. On Azure RMoW-C's batches are
  {eval}`f"{100 * az_cpu.loc[('RMoW-C', 'batch')]:.0f}"`% CPU, more than the third C4 allows.

## Limits

- **One zone per store, one VM, three runs, one day.** Each store's latency was measured from
  one zone at one time; the runs' spread is the whiskers above.
- **Not the twins' CPU.** Both VMs are AMD Zen 4 like the twins' laptop, but run at their own
  pace; C3's predictions priced CPU at the laptop's. The region arms' batches are CPU-bound on S3
  Express, so their ratios move with it.
- **The profile, not the model, decides C2.** `measured_profiles` was set before the runs, and C2
  is judged with it. A size-aware profile would price these runs differently.
- **A shared bucket.** The S3 Express bucket is catalog-bench's; nothing else ran on it from
  here, but this project did not control it.
- **The stock reader's time travel on Azure** is inferred from wall time and the bytes the
  responses declared; the bytes the SDK actually read are not counted.
- **The Azure account is Standard, not Premium**, and its VM is `D8as_v6`, not the planned
  `D8as_v5`.
- **The spikes came from main, the calibration from 9a69028.** Both send the fork's requests
  through the same `AtomicAppender` and `Store` (unchanged between them).
