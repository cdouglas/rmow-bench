---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'The merge compaction at 128 MiB: the hypotheses'
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, q, table, theme
theme()
fig = figures_module()
tune = fig.tuner()

# (workload, configuration, design, campaign): the clustered workload's MoR, MoR (compacted) and
# RMoR-density stand from campaign w128-keys-2; the rest ran in campaign w128-merge
MERGED, MERGED15, KEYRANGE = ("RMoR-density (merged)", "RMoR-density θ 0.15 (merged)",
                              "MoR (deltas by key range)")
DESIGNS = [
    ("clustered", "w128k-upd-mor", "MoR", "w128-keys-2"),
    ("clustered", "w128k-upd-compact", "MoR (compacted)", "w128-keys-2"),
    ("clustered", "w128k-upd-density", "RMoR-density", "w128-keys-2"),
    ("clustered", "w128k-upd-density-merge", MERGED, "w128-merge"),
    ("clustered", "w128k-upd-density15-merge", MERGED15, "w128-merge"),
    ("clustered", "w128k-upd-keyrange", KEYRANGE, "w128-merge"),
    ("sparse", "w128s-upd-mor", "MoR", "w128-merge"),
    ("sparse", "w128s-upd-compact", "MoR (compacted)", "w128-merge"),
    ("sparse", "w128s-upd-density", "RMoR-density", "w128-merge"),
    ("sparse", "w128s-upd-density-merge", MERGED, "w128-merge"),
    ("sparse", "w128s-upd-density15-merge", MERGED15, "w128-merge"),
    ("sparse", "w128s-upd-keyrange", KEYRANGE, "w128-merge"),
]
NAMES = ["MoR", "MoR (compacted)", "RMoR-density", MERGED, MERGED15, KEYRANGE]
COLOR = alt.Scale(domain=NAMES, range=[fig.WL_COLOR["MoR"], fig.WL_NCOLOR["MoR (compacted)"],
                                       fig.WL_COLOR["RMoR-density"], "#6a2c91", "#e39acb", "#6b6b6b"])
WORKLOADS = ["clustered", "sparse"]
VALUES = ", ".join(f"('{w}', '{l}', '{n}', '{c}', {i})" for i, (w, l, n, c) in enumerate(DESIGNS))
D = f"""d(workload, label, name, campaign, ord) AS (VALUES {VALUES}),
        r AS (SELECT d.*, r.run_id FROM d JOIN canonical_runs r
              ON r.label = d.label AND list_contains(r.campaigns, d.campaign))"""

runs = q(f"""
    WITH {D}
    SELECT r.workload, r.name, r.label, c.run_id, c.git_sha[:7] AS build, c.git_dirty AS dirty,
           json_extract_string(c.manifest, '$.oplog_sha256')[:8] AS oplog, c.batches AS writes,
           round(c.elapsed_s / 3600, 1) AS hours
    FROM r JOIN canonical_runs c USING (run_id) ORDER BY r.ord""")
RUN = runs.set_index(["workload", "name"]).run_id

# Each measured run of campaign w128-merge against its verification run (campaign
# w128-merge-verify: --verify, every row checked, excluded): their records, those that differ in any
# field, and those in one run only
twins = q("""SELECT label, run_id AS twin FROM runs
             WHERE list_contains(campaigns, 'w128-merge-verify')""")
pairs = runs[runs.label.str.startswith("w128s") | runs.label.str.contains("merge|keyrange")] \
    .merge(twins, on="label")
verify = pd.concat([q(f"""
    SELECT '{p.label}' AS configuration, count(*) AS records,
           count(*) FILTER (WHERE len(differs) > 0) AS differ,
           count(*) FILTER (WHERE one_run_only) AS one_run_only
    FROM steps_diff('{p.run_id}', '{p.twin}')""") for p in pairs.itertuples()], ignore_index=True)

# A write's bytes by phase (data files, deletion vectors, the compaction after it) and the locate's
# requests, every write
per_write = q(f"""
    WITH {D}
    SELECT r.workload, r.name, r.ord, s.step,
           coalesce(sum(s.bytes_written) FILTER (WHERE s.phase = 'write'), 0) / 1e6 AS data_mb,
           coalesce(sum(s.bytes_written) FILTER (WHERE s.phase = 'delete-write'), 0) / 1e6 AS dv_mb,
           coalesce(sum(s.bytes_written) FILTER (WHERE s.phase = 'batch'), 0) / 1e6 AS batch_mb,
           coalesce(sum(s.bytes_written) FILTER (WHERE s.phase = 'compact'), 0) / 1e6 AS compact_mb,
           sum(s.requests) FILTER (WHERE s.phase = 'locate') AS locate_req
    FROM steps s JOIN r USING (run_id)
    WHERE s.step >= 1 AND s.phase IN ('write', 'delete-write', 'batch', 'compact', 'locate')
    GROUP BY ALL ORDER BY r.ord, s.step""")
assert (per_write.batch_mb - per_write.data_mb - per_write.dv_mb).abs().max() < 1e-6
per_write["write_mb"] = per_write.data_mb + per_write.dv_mb

# The reads: a full scan every 10 writes; five 1% windows after every write (their mean requests,
# their bytes summed); the layout after the write
reads = q(f"""
    WITH {D}
    SELECT r.workload, r.name, r.ord, s.step,
           any_value(s.requests) FILTER (WHERE s.phase = 'read_full') AS full_req,
           any_value(s.bytes_read) FILTER (WHERE s.phase = 'read_full') / 1e9 AS full_gb,
           avg(s.requests) FILTER (WHERE s.phase = 'read_window') AS window_req,
           sum(s.bytes_read) FILTER (WHERE s.phase = 'read_window') / 1e6 AS windows_mb,
           sum(s.rows) FILTER (WHERE s.phase = 'read_window') AS window_rows,
           any_value(s.row_groups) FILTER (WHERE s.phase = 'state') AS row_groups,
           any_value(s.data_files) FILTER (WHERE s.phase = 'state') AS data_files,
           any_value(s.delete_files) FILTER (WHERE s.phase = 'state') AS delete_files,
           any_value(s.stored_bytes) FILTER (WHERE s.phase = 'state') / 1e9 AS stored_gb
    FROM steps s JOIN r USING (run_id)
    WHERE (s.reader = 'cached-skip' AND s.phase IN ('read_full', 'read_window')) OR s.phase = 'state'
    GROUP BY ALL ORDER BY r.ord, s.step""")
full = reads.dropna(subset=["full_req"])


def summary(g):
    return pd.Series(dict(
        writes=len(g), data_mb=g.data_mb.mean(), dv_mb=g.dv_mb.mean(), write_mb=g.write_mb.mean(),
        compact_mb=g.compact_mb.mean(), compact_max_mb=g.compact_mb.max(),
        compactions=int((g.compact_mb > 0).sum()), locate_first10=g.locate_req.head(10).mean(),
        locate=g.locate_req.tail(10).mean()))


E = per_write.groupby(["workload", "name"], sort=False).apply(summary, include_groups=False)
last = reads[reads.step == reads.groupby(["workload", "name"]).step.transform("max")]
E = E.join(last.set_index(["workload", "name"])[
    ["step", "full_req", "full_gb", "window_req", "windows_mb", "window_rows", "row_groups",
     "data_files", "delete_files", "stored_gb"]])
E["all_mb"] = E.write_mb + E.compact_mb

# Simulated latency (reported, not judged): each logged request priced by its size on a store's
# spike, plus the phase's work at its arm's pace (views/65_sized.sql); a write's mean over the 240,
# the compaction after it likewise, and the reads after the last write
STORES = {"S3 Express": tune.SPIKES["s3x"], "Azure Blob": tune.SPIKES["azure"]}
lat = []
for (w, n), run in RUN.items():
    for store, spike in STORES.items():
        x = q(f"""SELECT phase, step, request_ms + coalesce(work_ms, 0) AS ms
                  FROM sized_latency('{run}', '{spike}')
                  WHERE phase IN ('batch', 'compact', 'read_full', 'read_window') AND step >= 1""")
        end = x.step.max()
        lat.append(dict(
            workload=w, design=n, store=store,
            write_s=x[x.phase == "batch"].ms.mean() / 1e3,
            compact_s=x[x.phase == "compact"].ms.mean() / 1e3 if (x.phase == "compact").any() else 0.0,
            full_s=x[(x.phase == "read_full") & (x.step == end)].ms.mean() / 1e3,
            window_s=x[(x.phase == "read_window") & (x.step == end)].ms.mean() / 1e3))
lat = pd.DataFrame(lat)


def f0(x):
    return f"{x:,.0f}"


def f1(x):
    return f"{x:,.1f}"


def f2(x):
    return f"{x:.2f}"


def verdict(ok):
    return "held" if ok else "contradicted"


c, s = "clustered", "sparse"
# P1: the merge stops the pile-up (locate over the last 10 writes; the last full scan)
p1 = {w: (E.locate[w, MERGED] / E.locate[w, "RMoR-density"],
          E.full_req[w, MERGED] / E.full_req[w, "RMoR-density"]) for w in WORKLOADS}
P1 = all(v <= 0.2 for v in p1[s]) and all(v <= 0.8 for v in p1[c])
# P2: the merge keeps the copies' windows (clustered; the windows read the same rows)
assert (E.window_rows.groupby(level=0).nunique() == 1).all()
p2 = (E.windows_mb[c, MERGED] / E.windows_mb[c, "RMoR-density"],
      E.window_req[c, MERGED] / E.window_req[c, "RMoR-density"])
P2 = p2[0] <= 1.15 and p2[1] <= 0.6
# P3: copies pay where changes cluster (window bytes against MoR (deltas by key range))
p3 = {w: E.windows_mb[w, MERGED] / E.windows_mb[w, KEYRANGE] for w in WORKLOADS}
P3 = p3[c] <= 0.6 and p3[s] >= 0.8
# P4: each merge run's compaction at most 30 MB a write; the control's writes and merges at most
# half RMoR-density (merged)'s
p4a = E.compact_mb[[(w, n) for w in WORKLOADS for n in (MERGED, MERGED15, KEYRANGE)]]
p4b = {w: E.all_mb[w, KEYRANGE] / E.all_mb[w, MERGED] for w in WORKLOADS}
p4data = {w: (E.data_mb[w, KEYRANGE] + E.compact_mb[w, KEYRANGE])
             / (E.data_mb[w, MERGED] + E.compact_mb[w, MERGED]) for w in WORKLOADS}
P4a = bool((p4a <= 30).all())
P4b = {w: p4b[w] <= 0.5 for w in WORKLOADS}
merged = [(w, n) for w in WORKLOADS for n in (MERGED, MERGED15, KEYRANGE)]
vs_compacted = pd.DataFrame(
    {"full": [E.full_req[k] / E.full_req[k[0], "MoR (compacted)"] for k in merged],
     "window": [E.window_req[k] / E.window_req[k[0], "MoR (compacted)"] for k in merged]},
    index=pd.MultiIndex.from_tuples(merged))
```

Whether a compaction planned from RMoR-density's density signal stops the delta row groups piling up
under sparse changes, and what RMoR-density's copies still buy once it does: twelve runs of 240
writes at 128 MiB row groups, two workloads, six designs, against the predictions registered on
2026-11-20 (docs/HYPOTHESES.md, "The merge compaction and delta files at 128 MiB").

**In short:** the merge stops the pile-up. Under sparse changes RMoR-density's locate fell from
{eval}`f0(E.locate[s, "RMoR-density"])` requests a write to {eval}`f0(E.locate[s, MERGED])`, and a
full scan from {eval}`f0(E.full_req[s, "RMoR-density"])` requests to
{eval}`f0(E.full_req[s, MERGED])`, while each write's compaction wrote
{eval}`f1(E.compact_mb[s, MERGED])` MB on average, against MoR (compacted)'s
{eval}`f1(E.compact_mb[s, "MoR (compacted)"])`. The copies still pay where changes cluster: there a
1% window of RMoR-density (merged) reads {eval}`f2(p3[c])` of the bytes MoR (deltas by key range)
reads, and under sparse changes {eval}`f2(p3[s])`. Three of the four predictions held and the fourth
held in part: the control's writes came to {eval}`f2(p4b[s])` of RMoR-density (merged)'s in the
sparse workload, not at most half, because the forecast left out the deletion vectors MoR writes.
Each of the nine new runs matched its verification twin request for request and byte for byte.

## Setup

- **Workloads.** Both change rows by key in 128 MiB row groups, 240 writes to a table loaded with
  205 million rows. The **clustered** workload (`w128k-upd-*`) is the one the 128 MiB suite ran: each
  statement changes 3% of a region's rows. The **sparse** workload (`w128s-upd-*`, `updateShare` 0)
  changes one row of a region per statement, the region picked by its heat, so a write touches many
  row groups lightly.
- **Designs.** MoR writes each write's new versions to new files plus deletion vectors; MoR
  (compacted) also compacts after any write that leaves its files past their thresholds (deletes,
  small files), into 128 MiB row groups. RMoR-density copies a
  row group once its dead rows and the write's changes reach θ = 0.06 of it, and leaves the other
  new versions in a delta row group for each region. **RMoR-density (merged)** adds the merge
  compaction: after every write, as a transaction of its own, it merges each region's delta row
  groups (at most 3 a region, leveled), merges runs of adjacent row groups no write changed for 20
  writes back into 32 MiB row groups, and drains files under half live; delta row groups go to
  files of their own (delta files), so the copies' files keep narrow key bounds. **RMoR-density θ
  0.15 (merged)** is the same at θ 0.15. **MoR (deltas by key range)** is the control that never
  copies: RMoR-density (merged) at θ 9, which keeps its new versions by key range and merges them
  the same way.
- **Build.** Snapshot `results/builds/20261120T075226Z-725a8cf399f4-dirty` of the worktree
  `merge-compaction` (committed afterwards as 43f3123 and 508a718), on the atomicio fork 62cdc89288.
  The clustered MoR, MoR (compacted) and RMoR-density runs stand from campaign w128-keys-2 (build
  6daa887); a parity check of two configurations, every arm, found the new build's records equal to
  that build's where the new settings are absent.
- **Where.** The laptop, one run at a time with a 12g heap and every request logged (campaign
  w128-merge, 2026-11-20 to 21). Each new run's verification twin (`--verify`, every row checked
  against the model; campaign w128-merge-verify, 2026-11-24 to 25) sent the same requests and bytes
  in every record (the clustered baselines' twins vouch for them in campaign w128-keys-2):

```{code-cell} ipython3
:tags: [remove-input]

table(runs)
```

```{code-cell} ipython3
:tags: [remove-input]

table(verify)
```

## The locate's requests

A write first finds the rows it changes (the locate), reading every row group whose key bounds may
hold one of its keys. Under sparse changes RMoR-density's delta row groups pile up and the locate's
requests climb with them; the merge holds them near MoR (deltas by key range)'s.

```{code-cell} ipython3
:label: merge-locate
:tags: [remove-input]

alt.Chart(per_write[["workload", "name", "step", "locate_req"]]).mark_line(strokeWidth=1.5).encode(
    x=alt.X("step:Q", title="write"),
    y=alt.Y("locate_req:Q", title="locate requests a write", scale=alt.Scale(type="log")),
    color=alt.Color("name:N", scale=COLOR, sort=NAMES, title="design"),
).properties(width=420, height=280).facet(
    column=alt.Column("workload:N", sort=WORKLOADS, title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(E[["locate_first10", "locate"]].rename(
    columns={"locate_first10": "first 10 writes", "locate": "last 10 writes"}).reset_index())
```

## The reads

A full scan every 10 writes, and five 1% key-range windows after the last write (their requests a
window, their bytes summed; every design's windows read the same rows).

```{code-cell} ipython3
:label: merge-full
:tags: [remove-input]

alt.Chart(full[["workload", "name", "step", "full_req"]]).mark_line(point=True).encode(
    x=alt.X("step:Q", title="write"),
    y=alt.Y("full_req:Q", title="full scan requests", scale=alt.Scale(type="log")),
    color=alt.Color("name:N", scale=COLOR, sort=NAMES, title="design"),
).properties(width=420, height=280).facet(
    column=alt.Column("workload:N", sort=WORKLOADS, title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(E[["full_req", "full_gb", "window_req", "windows_mb", "row_groups", "data_files",
         "delete_files", "stored_gb"]].rename(columns={
    "full_req": "full scan requests", "full_gb": "full scan GB", "window_req": "window requests",
    "windows_mb": "windows MB", "row_groups": "row groups", "data_files": "data files",
    "delete_files": "delete files", "stored_gb": "stored GB"}).reset_index())
```

## The writes

What a write puts, on average over the 240: its data files, its deletion vectors, and the merge
compaction after it.

```{code-cell} ipython3
:label: merge-writes
:tags: [remove-input]

parts = E[["data_mb", "dv_mb", "compact_mb"]].rename(columns={
    "data_mb": "data files", "dv_mb": "deletion vectors", "compact_mb": "compaction"}) \
    .reset_index().melt(["workload", "name"], var_name="part", value_name="mb")
parts["k"] = parts.part.map({"data files": 0, "deletion vectors": 1, "compaction": 2})
alt.Chart(parts).mark_bar().encode(
    y=alt.Y("name:N", sort=NAMES, title=None),
    x=alt.X("mb:Q", title="MB written a write"),
    color=alt.Color("part:N", sort=["data files", "deletion vectors", "compaction"], title=None),
    order=alt.Order("k:Q"),
).properties(width=420, height=180).facet(
    column=alt.Column("workload:N", sort=WORKLOADS, title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(E[["data_mb", "dv_mb", "write_mb", "compact_mb", "all_mb", "compactions",
         "compact_max_mb"]].rename(columns={
    "data_mb": "data MB", "dv_mb": "deletion vectors MB", "write_mb": "write MB",
    "compact_mb": "compaction MB", "all_mb": "write + compaction MB",
    "compactions": "writes that compacted", "compact_max_mb": "largest compaction MB"}).reset_index())
```

## The hypotheses

- **P1, the merge stops the pile-up: {eval}`verdict(P1)`.** In the sparse workload RMoR-density
  (merged) sent {eval}`f2(p1[s][0])` of RMoR-density's locate requests a write over the last 10
  writes and {eval}`f2(p1[s][1])` of its full scan's requests (at most 0.2 each); in the clustered,
  {eval}`f2(p1[c][0])` and {eval}`f2(p1[c][1])` (at most 0.8).
- **P2, with delta files the merge keeps the copies' windows: {eval}`verdict(P2)`.** In the
  clustered workload a 1% window of RMoR-density (merged) read {eval}`f2(p2[0])` times
  RMoR-density's bytes (at most 1.15) with {eval}`f2(p2[1])` of its requests (at most 0.6).
- **P3, where copies pay: {eval}`verdict(P3)`.** Against MoR (deltas by key range), RMoR-density
  (merged)'s windows read {eval}`f2(p3[c])` of its bytes in the clustered workload (at most 0.6) and
  {eval}`f2(p3[s])` in the sparse (at least 0.8).
- **P4, the writes: held in part.** Each merge run's compaction wrote
  {eval}`f1(p4a.min())` to {eval}`f1(p4a.max())` MB a write (at most 30), against MoR
  (compacted)'s {eval}`f1(E.compact_mb[c, "MoR (compacted)"])` and
  {eval}`f1(E.compact_mb[s, "MoR (compacted)"])`: {eval}`verdict(P4a)`. MoR (deltas by key range)
  wrote, with its merges, {eval}`f2(p4b[c])` of RMoR-density (merged)'s bytes in the clustered
  workload ({eval}`verdict(P4b[c])`) and {eval}`f2(p4b[s])` in the sparse, more than half:
  {eval}`verdict(P4b[s])`. The forecast counted data files alone, and MoR's deletion vectors add
  {eval}`f1(E.dv_mb[s, KEYRANGE])` MB to each of the control's sparse writes; counting data files
  and compactions only, the shares are {eval}`f2(p4data[c])` and {eval}`f2(p4data[s])`.

## Measured, not judged

- **θ 0.15 against θ 0.06.** RMoR-density θ 0.15 (merged) wrote
  {eval}`f1(E.all_mb[c, MERGED15])` MB a write with its compaction in the clustered workload and
  {eval}`f1(E.all_mb[s, MERGED15])` in the sparse, against θ 0.06's
  {eval}`f1(E.all_mb[c, MERGED])` and {eval}`f1(E.all_mb[s, MERGED])`; its windows read
  {eval}`f2(E.windows_mb[c, MERGED15] / E.windows_mb[c, MERGED])` and
  {eval}`f2(E.windows_mb[s, MERGED15] / E.windows_mb[s, MERGED])` times θ 0.06's bytes.
- **Against MoR (compacted).** The three merge designs' full scans sent
  {eval}`f1(vs_compacted.full.min())` to {eval}`f1(vs_compacted.full.max())` times MoR
  (compacted)'s requests, and their windows {eval}`f1(vs_compacted.window.min())` to
  {eval}`f1(vs_compacted.window.max())` times: MoR (compacted) merges whole files into 128 MiB row
  groups, the merge only what it judges cold or piled up.
- **The largest compaction** of a merge design wrote
  {eval}`f0(E.compact_max_mb[merged].max())` MB, against MoR (compacted)'s
  {eval}`f0(E.compact_max_mb[[(w, "MoR (compacted)") for w in WORKLOADS]].max())`.
- **Plain RMoR-density under sparse changes** ended with {eval}`f0(E.row_groups[s, "RMoR-density"])`
  row groups, against RMoR-density (merged)'s {eval}`f0(E.row_groups[s, MERGED])` and MoR's
  {eval}`f0(E.row_groups[s, "MoR"])`.
- **Simulated latency**, each logged request priced by its size on the store's measured spike plus
  the phase's work (seconds; a write's and its compaction's mean over the 240 writes, the reads
  after the last):

```{code-cell} ipython3
:tags: [remove-input]

table(lat.rename(columns={"write_s": "write (s)", "compact_s": "compaction (s)",
                          "full_s": "full scan (s)", "window_s": "1% window (s)"}).round(3))
```

## Limits

- **One seed a workload,** and the merge's settings fixed (at most 3 delta row groups a region,
  cold after 20 writes, 32 MiB, drain under half live), not swept.
- **The clustered baselines** (MoR, MoR (compacted), RMoR-density) ran on the earlier build;
  the parity check, not a rerun, ties them to this one.
- **The catalog's side is not measured:** a write with delta files commits two data files, and a
  compaction a transaction of its own; neither commit is priced here.
