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

# The runs, pinned by id, per provider. Until the cloud runs come back (docs/S3X_RUNBOOK.md,
# docs/AZURE_RUNBOOK.md, deploy/), stand-ins (campaign standins):
# - LATENCY: the calibration's local twin with the provider's profile injected
#   (configs/standin-*.json, three runs each), whose wall times are its CPU and the injected waits;
# - SENT: what each provider was sent: the local twin (the model's requests; S3 Express receives
#   those and its CreateSessions) and the Azurite twin (Azure's SDK, Azurite's creates);
# - SPIKES: the spikes on versitygw and Azurite, which check the code, not the stores;
# - PROFILES: the profile the model check prices with: the injected one, for the stand-ins; the
#   spike's measured one (measured_profiles), for the cloud runs.
STANDIN = True
LATENCY = {
    "S3 Express": ["standin-s3x-d1bce4a9b22a/20261028T165319Z",
                   "standin-s3x-d1bce4a9b22a/20261028T170542Z",
                   "standin-s3x-d1bce4a9b22a/20261028T171751Z"],
    "Azure Blob": ["standin-azure-6ffadad54e64/20261028T165537Z",
                   "standin-azure-6ffadad54e64/20261028T170743Z",
                   "standin-azure-6ffadad54e64/20261028T171949Z"],
}
SENT = {
    "S3 Express": ["s3x-calibration-local-8433ccfd4620/20261027T175847Z"],
    "Azure Blob": ["azure-calibration-azurite-a96fd56de2f0/20261027T175614Z"],
}
SPIKES = {"S3 Express": "spike/s3x-spike-20261028T165827Z", "Azure Blob": "spike/azure-spike-20261028T165903Z"}
PROFILES = {"S3 Express": "s3x", "Azure Blob": "azure"}
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
```

```{code-cell} ipython3
:tags: [remove-cell]

note = ("**Stand-ins.** These charts are drawn from stand-ins until the cloud runs come back: "
        "the calibration's local twin with each provider's latency injected, the Azurite twin "
        "for what Azure's SDK sends, and the spikes on emulators. Their numbers check the "
        "pipeline, not the stores.") if STANDIN else ""
```

{eval}`note`

What a delta update costs a writer and a reader on each provider, for every arm of the
calibration: {eval}`K` batches against one checkpoint (the initial load), {eval}`RUNS` runs per
provider. Latency is measured wall time; requests, bytes and storage are counted per update.
Reads are one-threaded and cache each file's footer and deletes (`cached`).

## Writing and reading deltas against a checkpoint

Each point is an arm's mean write (one batch) and mean read over the reads at 0 to {eval}`K`
deltas; its whisker runs from the read at 0 deltas to the read at {eval}`K`, just before the next
checkpoint would be written. On S3 Express MoR-v3's full read goes from
{eval}`f"{mor.loc['S3 Express', 'read_k0']:.0f}"` to {eval}`f"{mor.loc['S3 Express', 'read_kk']:.0f}"` ms
and RMoW-C's from {eval}`f"{rmow.loc['S3 Express', 'read_k0']:.0f}"` to
{eval}`f"{rmow.loc['S3 Express', 'read_kk']:.0f}"` ms.

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
other stores take one, and appends past 100 MiB split; S3 Express also opens sessions. The
finer kinds of request (GETs, HEADs, Put Blobs, Append Blocks, creates, renames) are in the table.

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

## Model check (internal)

Not for publication: how well the development model (lab/pages/store-latency.md) predicts each
one-thread phase, priced with {eval}`"the injected profiles" if STANDIN else "the spikes' measured profiles"`.
The model is the phase's CPU plus its requests' expected latencies plus its bytes at the
profile's rates.

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
