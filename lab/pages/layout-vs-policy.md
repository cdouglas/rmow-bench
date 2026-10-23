---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Layout against tail policy (H3)
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import numpy as np
import pandas as pd
from labbook import q, table, theme
theme()

# The runs this page reads, rerun on 2026-10-22/23 from cfe40b2: the uncompacted rg-sweep runs,
# the uncompacted split run (its split arms) and the uniform run without compaction.
RG = {
    1: "local-rg-sweep-recency-exp-rg1m-ddb0f45ee310/20261023T042220Z",
    2: "local-rg-sweep-recency-exp-rg2m-9ff075ae2483/20261023T044613Z",
    4: "local-rg-sweep-recency-exp-rg4m-b2e059145e94/20261023T042224Z",
    8: "local-rg-sweep-recency-exp-rg8m-4920abe2147d/20261023T044422Z",
}
SPLIT = "local-split-recency-exp-nocompact-309eabcc0dc5/20261023T045645Z"
UNIFORM = "local-m1-uniform-nocompact-c7e50a35a7c5/20261023T042229Z"
RUNS = [*RG.values(), SPLIT, UNIFORM]
ARMS = ["mor-v3", "tailcow-b", "tailcow-c", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split"]
# analysis/compaction.py's PRICES, per request and per GB
STD_PUT, STD_GET = 0.005e-3, 0.0004e-3

arms = q("""
    SELECT a.*, m.ord, s.data_files, s.delete_files, s.row_groups, s.objects, s.live_objects,
           (s.stored_bytes - s.reclaimable_bytes) / s.live_bytes AS kept_over_live
    FROM h3_arms a JOIN arm_meta m USING (arm)
    JOIN steps s ON s.run_id = a.run_id AND s.arm = a.arm AND s.phase = 'state' AND s.step = 60
    WHERE list_contains($runs, a.run_id)""", runs=RUNS)
# the 4 MiB recency run's arms, with the split arms from the split run (its other arms match)
at4 = pd.concat([arms[(arms.run_id == RG[4]) & arms.arm.isin(ARMS)],
                 arms[(arms.run_id == SPLIT) & arms.arm.str.endswith("-split")]]).sort_values("ord")
at4 = at4.set_index("arm", drop=False)
at4["std_write_usd"] = 1e6 * ((at4.puts + at4.appends) * STD_PUT
                              + (at4.get_range + at4.get_stream + at4.heads) * STD_GET)
at4["std_full_usd"] = 1e6 * at4.full_req * STD_GET

diffs = q("""
    SELECT d.*, r.key_dist, r.rg_bytes // 1048576 AS mib, r.run_id = $split AS split_run
    FROM h3_diffs d JOIN runs_raw r USING (run_id) WHERE list_contains($runs, d.run_id)""",
          runs=RUNS, split=SPLIT)
sweep = diffs[(diffs["diff"] != "append") & ~diffs.split_run].sort_values(
    ["key_dist", "mib", "pol", "diff"])
d4 = pd.concat([diffs[diffs.run_id == RG[4]],
                diffs[(diffs.run_id == SPLIT) & (diffs["diff"] == "append")]])


def d(pol, diff, run=RG[4]):
    return diffs[(diffs.run_id == run) & (diffs.pol == pol) & (diffs["diff"] == diff)].iloc[0]


def cost(row, qq):
    """Express µ$ per batch at qq full reads per batch: the write cost plus qq read costs."""
    return row.d_write_usd + qq * row.d_full_usd


def signed(x, digits=1):
    """A signed number with a true minus sign."""
    return f"{x:+.{digits}f}".replace("-", "\u2212")


def span(s):
    return f"{signed(s.min())} to {signed(s.max())}"


recency = sweep[sweep.key_dist == "RECENCY_EXP"]
lay, pol = recency[recency["diff"] == "layout"], recency[recency["diff"] == "policy"]
c4, b4 = at4.loc["rmow-c"], at4.loc["rmow-b"]
tc, tb, mor = at4.loc["tailcow-c"], at4.loc["tailcow-b"], at4.loc["mor-v3"]
csplit = at4.loc["rmow-c-split"]
state = lambda arm: arms[(arms.run_id == RG[4]) & (arms.arm == arm)].iloc[0]
xfer = lambda a: a.write_usd - 1e6 * ((a.puts + a.appends) * 0.00113e-3
                                      + (a.get_range + a.get_stream + a.heads) * 0.00003e-3)
read_req_usd = lambda a: 1e6 * a.full_req * 0.00003e-3
split_kept = arms[(arms.run_id == SPLIT) & arms.arm.isin(["rmow-b", "rmow-c"])].kept_over_live
lay1 = recency[(recency.mib == 1) & (recency["diff"] == "layout")]
pol1b = recency[(recency.mib == 1) & (recency.pol == "B") & (recency["diff"] == "policy")].iloc[0]
N = {
    "append_req": f"{-d('C', 'append', SPLIT).d_put_class:.2f}",
    "append_usd": f"{-cost(d('C', 'append', SPLIT), 1):.1f}",
    "rmow_writes": f"{c4.puts + c4.appends:.2f}",
    "locate": f"{tc.locate_req:.1f}",
    "dv_write": f"{tc.delete_write_req:.1f}",
    "tc_files": f"{state('tailcow-c').data_files}",
    "tc_dvs": f"{state('tailcow-c').delete_files}",
    "tc_rgs": f"{state('tailcow-c').row_groups}",
    "tc_gets": f"{tc.full_req_last:.0f}",
    "rc_files": f"{state('rmow-c').data_files}",
    "rc_rgs": f"{state('rmow-c').row_groups}",
    "rc_gets": f"{c4.full_req_last:.0f}",
    "split_objects": f"{csplit.live_objects}",
    "split_gets": f"{csplit.full_req_last:.0f}",
    "lay_mb": f"{lay.d_mb_written.min():.1f}–{lay.d_mb_written.max():.1f}",
    "pol_mb": f"{pol.d_mb_written.min():.0f}–{pol.d_mb_written.max():.1f}",
    "rmow_a_mb": f"{arms[(arms.run_id == RG[4]) & (arms.arm == 'rmow-a')].mb_written.iloc[0]:.2f}",
    "lay_q1": span(cost(lay, 1)),
    "pol_q1": span(cost(pol, 1)),
    "xfer_c": signed(xfer(c4) - xfer(tc)),
    "readreq_c": signed(read_req_usd(c4) - read_req_usd(tc), 2),
    "std_w_lay": f"{tc.std_write_usd - csplit.std_write_usd:.1f}",
    "std_r_lay": f"{tc.std_full_usd - csplit.std_full_usd:.1f}",
    "std_w_pol": f"{mor.std_write_usd - tc.std_write_usd:.1f}",
    "std_r_pol": f"{mor.std_full_usd - tc.std_full_usd:.1f}",
    "recent_tc": f"{tc.recent_req_last:.0f}",
    "recent_rmow": f"{c4.recent_req_last:.0f}",
    "recent_mor": f"{mor.recent_req_last:.0f}",
    "lay_read_b": f"{-d('B', 'layout').d_full_req:.1f}",
    "lay_read_c": f"{-d('C', 'layout').d_full_req:.1f}",
    "lay_write_b": f"{-d('B', 'layout').d_requests:.1f}",
    "lay_write_c": f"{-d('C', 'layout').d_requests:.1f}",
    "pol1b_write": f"{-pol1b.d_requests:.0f}",
    "pol1b_read": f"{pol1b.d_full_req:.0f}",
    "kept_rmow": f"{split_kept.min():.1f}–{split_kept.max():.1f}",
    "kept_tc": f"{arms[(arms.run_id == SPLIT) & (arms.arm == 'tailcow-c')].kept_over_live.iloc[0]:.2f}",
}
```

H3 (docs/HYPOTHESES.md) splits RMoW's difference from MoR-v3 into two steps:

- **The layout step** (RMoW against TailCoW): what the single appendable object buys. That means
  fewer objects and footers to open, and one append instead of a new object per batch.
- **The policy step** (TailCoW against MoR-v3): what merging the tail buys.

It predicts that on object stores with cheap PUTs the first difference is the smaller one, and
that it matters mostly for read requests. It is decided by comparing the two, request type by
request type.

This page reads local runs of uncompacted configurations. They were rerun on 2026-10-22/23 for
the time-travel fix (Time travel and dead bytes, H4). In everything used here (batches, full
reads, state) the reruns match the earlier runs exactly. The one exception is the uniform run's
MoR and TailCoW arms, which now count Puffin's HEAD.

## Setup

**Runs.** The page pins them by id (`RG`, `SPLIT`, `UNIFORM`).
- `local-rg-sweep-recency-exp-rg{1,2,4,8}m`: all 9 arms. Row-group target and tail seal are
  1, 2, 4 or 8 MiB, with recency-skewed updates and no compaction.
- `local-split-recency-exp-nocompact`: the same settings at 4 MiB, with the split arms. Its four
  other arms match the 4 MiB rg-sweep run exactly in every request and byte column
  (`steps_diff`).
- `local-m1-uniform-nocompact`: uniform updates.

**Workload.** The m1 workload: 1M initial rows, then 60 batches of 10k operations, with 70%
inserts, 25% updates and 5% deletes. Rows are located by scan.

**Builds.** All are commit cfe40b2, from snapshots in `results/builds/` marked dirty by a patch
to docs only (`CLAUDE.md`, `docs/EXPERIMENTS.md`, `lab/annotations.toml`):
- rg-sweep: `20261023T042219Z-cfe40b2f65bf-dirty` (1 and 2 MiB) and
  `20261023T042224Z-cfe40b2f65bf-dirty` (4 and 8 MiB);
- split: `20261023T045645Z-cfe40b2f65bf-dirty`;
- m1 uniform: `20261023T042228Z-cfe40b2f65bf-dirty`.

**Pairs.** Each comparison holds the tail policy fixed:
- layout is RMoW-B against TailCoW-B, and RMoW-C against TailCoW-C;
- policy is TailCoW-B or -C against MoR-v3.

H3's text names RMoW-C, and defines TailCoW as plain Iceberg re-merging one small tail file (that
is, policy B). RMoW-C against TailCoW-B would mix the policy into the layout step, and the arms
were built to be compared at one policy (README, Arms). RMoW against RMoW-split, the append
alone, comes from the split run.

**Measures.**
- **Writes** are per batch: the mean over the 60 batches of the `batch` phase, which includes the
  locate and merge reads.
- **Reads** are one full read under the one-footer reader, the mean over the probes after the
  initial load. Probes come every 10 batches.

**Prices.** They are `analysis/compaction.py`'s US East list prices, in µ\$ per batch.
- **S3 Express** (cheap PUTs, \$0.00113 per 1000), which also charges bytes: \$0.0032 per GB
  written and \$0.0006 per GB read.
- **S3 Standard** (PUTs at \$0.005 per 1000, bytes free). It has no append, so split RMoW stands
  in for RMoW there.

Storage and CPU are left out. At q full reads per batch, an arm costs its write cost plus q times
its read cost.

**Views.** `h3_arms` and `h3_diffs`, in `lab/views/30_hypotheses.sql`.

## Writes per batch, by request type (recency, 4 MiB)

```{code-cell} ipython3
:tags: [remove-input]

cols = {"arm": "arm", "requests": "requests", "locate_req": "locate",
        "merge_read_req": "merge-read", "delete_write_req": "DV write", "write_req": "data write",
        "puts": "PUT", "appends": "APPEND", "heads": "HEAD", "mb_written": "MB written",
        "mb_read": "MB read"}
w = at4.assign(gets=at4.get_range + at4.get_stream)
order = ["arm", "requests", "locate_req", "merge_read_req", "delete_write_req", "write_req",
         "puts", "appends", "heads", "gets", "mb_written", "mb_read"]
table(w[order].rename(columns={**cols, "gets": "GET"}).round(3).reset_index(drop=True))
```

What the sub-phases cover:
- **Locate** is the scan that finds updated and deleted rows. RMoW looks them up in its footers'
  row-group statistics and issues no request.
- **DV write** loads a data file's previous deletion vector and writes the new Puffin file: GETs,
  one HEAD (Puffin's `create()`) and one PUT.
- **Data write** is the new data file, or the appends of region and footer.

These come from `h3_arms`, filtered to the 4 MiB rg-sweep run and to the split run's split arms.

## Reads (recency, 4 MiB)

```{code-cell} ipython3
:tags: [remove-input]

r = at4[["arm", "full_req", "full_req_last", "full_mb_last", "recent_req_last", "data_files",
         "delete_files", "row_groups", "objects", "live_objects"]]
table(r.rename(columns={
    "full_req": "full-read GETs, mean", "full_req_last": "at batch 60",
    "full_mb_last": "full-read MB, batch 60", "recent_req_last": "recent-window GETs, batch 60",
    "data_files": "data files", "delete_files": "DV files", "row_groups": "row groups",
    "objects": "objects stored", "live_objects": "live objects"}).round(1).reset_index(drop=True))
```

"Data files" and "DV files" are the `state` record at batch 60. For RMoW, "data files" are
logical files. Appendable RMoW stores just its 2 objects; live objects were recorded only from
the split runs on.

## The two differences

### Recency, 4 MiB

Each row is the first arm minus the second. Negative numbers favour the first arm.

```{code-cell} ipython3
:tags: [remove-input]

def pair_name(x):
    return f"{x.hi_arm} − {x.lo_arm}"


t = d4.assign(pair=d4.apply(pair_name, axis=1), q01=cost(d4, 0.1), q1=cost(d4, 1),
              q10=cost(d4, 10), order=d4["diff"].map({"layout": 0, "policy": 1, "append": 2}))
t = t.sort_values(["order", "pol"])
table(t[["pair", "diff", "d_requests", "d_put_class", "d_heads", "d_gets", "d_mb_written",
         "d_full_req", "q01", "q1", "q10"]].rename(columns={
    "diff": "step", "d_requests": "requests per batch", "d_put_class": "PUT-class",
    "d_heads": "HEAD", "d_gets": "GET", "d_mb_written": "MB written",
    "d_full_req": "full-read GETs", "q01": "Express µ$, q = 0.1", "q1": "q = 1",
    "q10": "q = 10"}).round(2).reset_index(drop=True))
```

#### Where the S3 Express cost goes, RMoW-C against TailCoW-C

```{code-cell} ipython3
:tags: [remove-input]

table(pd.DataFrame({
    "": ["write requests, per batch", "write transfer, per batch", "read requests, per full read",
         "read transfer, per full read"],
    "µ$": [(c4.write_usd - xfer(c4)) - (tc.write_usd - xfer(tc)), xfer(c4) - xfer(tc),
           read_req_usd(c4) - read_req_usd(tc),
           (c4.full_usd - read_req_usd(c4)) - (tc.full_usd - read_req_usd(tc))]}).round(2))
```

#### On S3 Standard, with split RMoW-C standing in for RMoW-C

```{code-cell} ipython3
:tags: [remove-input]

table(pd.DataFrame({
    "": ["writes, per batch (µ$)", "full read (µ$)"],
    "layout (RMoW-C-split − TailCoW-C)": [csplit.std_write_usd - tc.std_write_usd,
                                          csplit.std_full_usd - tc.std_full_usd],
    "policy (TailCoW-C − MoR-v3)": [tc.std_write_usd - mor.std_write_usd,
                                    tc.std_full_usd - mor.std_full_usd]}).round(1))
```

The S3 Standard figures price `h3_arms`' columns at Standard's PUT and GET prices.

### Across row-group sizes, and under uniform updates

```{code-cell} ipython3
:label: h3-cost-vs-reads
:tags: [remove-input]
:class: col-page-right

qs = np.geomspace(0.1, 10, 25)
lines = pd.DataFrame([
    {"MiB": f"{x.mib:g} MiB", "mib": x.mib, "policy": f"policy {x.pol}", "step": x.diff,
     "q": qq, "usd": cost(x, qq)}
    for x in recency.itertuples(index=False) for qq in qs])
base = alt.Chart().encode(
    x=alt.X("q:Q", scale=alt.Scale(type="log"), axis=alt.Axis(values=[0.1, 1, 10]),
            title="full reads per batch"))
curves = base.mark_line(strokeWidth=2).encode(
    y=alt.Y("usd:Q", title="µ$ per batch"),
    color=alt.Color("step:N", title="step", sort=["layout", "policy"]),
    tooltip=["MiB", "policy", "step", alt.Tooltip("q:Q", format=".2g"),
             alt.Tooltip("usd:Q", format="+.1f")])
zero = alt.Chart().mark_rule(color="#9a9a9a", strokeWidth=1).encode(y=alt.datum(0))
(alt.layer(zero, curves, data=lines)
 .properties(width=210, height=120)
 .facet(row=alt.Row("MiB:N", title=None, sort=alt.EncodingSortField("mib")),
        column=alt.Column("policy:N", title=None))
 .resolve_scale(y="independent"))
```

Each panel is one row-group size and tail policy, under recency-skewed updates. The lines are the
layout step (RMoW minus TailCoW) and the policy step (TailCoW minus MoR-v3), in µ\$ per batch on
S3 Express, against the read rate. Below zero, the first arm of the pair is the cheaper one. Each
line is a write cost plus q times a read cost; the table below gives both, with the request
counts.

```{code-cell} ipython3
:tags: [remove-input]

s = sweep.assign(keys=sweep.key_dist.str.lower().str.replace("recency_exp", "recency"),
                 q1=cost(sweep, 1), q10=cost(sweep, 10))
table(s[["keys", "mib", "pol", "diff", "d_requests", "d_full_req", "d_write_usd", "d_full_usd",
         "q1", "q10"]].rename(columns={
    "mib": "MiB", "pol": "policy", "diff": "step", "d_requests": "requests per batch",
    "d_full_req": "full-read GETs", "d_write_usd": "writes, µ$ per batch",
    "d_full_usd": "full read, µ$", "q1": "µ$, q = 1", "q10": "µ$, q = 10"}).round(1)
      .reset_index(drop=True))
```

The query, for any of these:

```sql
SELECT r.key_dist, r.rg_bytes / 1048576 AS mib, d.pol, d.diff, d_requests, d_put_class,
       d_heads, d_gets, d_mb_written, d_full_req, d_write_usd + 0.1 * d_full_usd AS q01,
       d_write_usd + d_full_usd AS q1, d_write_usd + 10 * d_full_usd AS q10
FROM h3_diffs d JOIN canonical_runs r USING (run_id)
WHERE r.label LIKE 'local-rg-sweep-%' OR r.label = 'local-split-recency-exp-nocompact'
   OR r.label = 'local-m1-uniform-nocompact'
```

## Findings

- **Counted in requests, the layout step is the smaller one.**
  - **At 2, 4 and 8 MiB, and under uniform updates**, RMoW saves fewer write requests per batch
    against TailCoW than TailCoW saves against MoR-v3, and fewer full-read GETs. This holds for
    both policies.
  - **At 1 MiB it is the other way round.**
    - TailCoW-C degenerates to MoR-v3 there: its units, the batches' files, each pass the seal on
      arrival.
    - TailCoW-B saves {eval}`N["pol1b_write"]` write requests per batch against MoR-v3, but reads
      with {eval}`N["pol1b_read"]` more GETs.
  - **Read requests dominate the layout step's request savings** from one full read per batch
    up. One full read saves {eval}`N["lay_read_b"]` or {eval}`N["lay_read_c"]` GETs; a batch's
    writes save {eval}`N["lay_write_b"]` or {eval}`N["lay_write_c"]` requests (4 MiB, B and C).
- **The single object accounts for little of the layout step.**
  - **Append buys {eval}`N["append_req"]` requests per batch and nothing else.** RMoW-C against
    RMoW-C-split isolates the append. It saves {eval}`N["append_req"]` PUT-class requests per
    batch, with the same GETs, the same bytes and the same reads; the same holds for B.
  - **RMoW issues more data writes than TailCoW.** Against TailCoW, RMoW writes
    {eval}`N["rmow_writes"]` appends per batch (one per changed file) where TailCoW writes 1 new
    data file.
  - **The write requests saved come from how updates are applied.** TailCoW-C locates updated
    rows by scanning ({eval}`N["locate"]` GETs), and writes deletion vectors
    ({eval}`N["dv_write"]` requests). RMoW does neither: it rewrites the touched row groups in
    place. Rows stay in id order, so footer statistics place each one.
  - **The reads saved come from one footer per logical file and no DVs.** TailCoW-C's full read
    at batch 60 opens {eval}`N["tc_files"]` data files with {eval}`N["tc_dvs"]` DVs, for
    {eval}`N["tc_rgs"]` row groups ({eval}`N["tc_gets"]` GETs). RMoW-C opens
    {eval}`N["rc_files"]` files, {eval}`N["rc_rgs"]` row groups ({eval}`N["rc_gets"]` GETs).
    Split RMoW-C reads from {eval}`N["split_objects"]` objects in the same
    {eval}`N["split_gets"]` GETs, so the number of objects is not what counts.
- **In bytes, the layout step is the larger one, and it costs.**
  - RMoW writes {eval}`N["lay_mb"]` MB more per batch than TailCoW under recency-skewed updates.
    TailCoW writes {eval}`N["pol_mb"]` MB more than MoR-v3 (none at 1 MiB, where TailCoW-C is
    MoR-v3).
  - The bytes come from rewriting the row groups that updates touch, not from the object:
    - split RMoW writes the same bytes;
    - RMoW-A, which never merges its tail, still writes {eval}`N["rmow_a_mb"]` MB per batch.
- **On S3 Express, the bytes decide.**
  - **At up to one full read per batch**, the layout step is the larger difference at every
    row-group size, and it makes RMoW the dearer arm: {eval}`N["lay_q1"]` µ\$ per batch at
    q = 1, against the policy step's {eval}`N["pol_q1"]`.
  - **Most of it is write transfer.** For C at 4 MiB that is {eval}`N["xfer_c"]` µ\$ per batch,
    against {eval}`N["readreq_c"]` µ\$ of read requests per full read. Express charges 3.2 µ\$ per
    MB written and 0.03 µ\$ per GET.
  - **At ten full reads per batch**, the read savings win: the layout step is the smaller
    difference at 2–8 MiB, and is the larger one only at 1 MiB.
- **On S3 Standard, it goes as H3 expected.** Standard's PUTs are 4.4 times Express's, and its
  bytes are free. Split RMoW-C against TailCoW-C saves {eval}`N["std_w_lay"]` µ\$ per batch of
  writes and {eval}`N["std_r_lay"]` µ\$ per full read, both less than the policy step
  ({eval}`N["std_w_pol"]` and {eval}`N["std_r_pol"]`), and mostly in read requests.
- **Recent-window reads favour TailCoW.** TailCoW reads the newest 1% in {eval}`N["recent_tc"]`
  GET, because the newest rows are one small tail data file, fetched whole. RMoW needs
  {eval}`N["recent_rmow"]`: a footer and a row group. MoR-v3 needs {eval}`N["recent_mor"]`.

## H3 (docs/HYPOTHESES.md)

**Mixed.**

- **The layout step is smaller than the policy step: held in request counts, contradicted on
  S3 Express prices.**
  - In requests it held for writes and reads, both policies, at 2–8 MiB and under uniform updates.
    It failed at 1 MiB, where TailCoW-C degenerates to MoR-v3.
  - On S3 Express, the store with cheap PUTs, the layout step is the larger cost at up to one
    full read per batch, at every row-group size. It is the smaller one at ten reads per batch
    (2–8 MiB).
- **It matters mostly for read requests: held in counts, not in cost.**
  - Its request savings are mostly read requests from one full read per batch up.
  - On S3 Express, its cost is mostly write bytes.
  - On S3 Standard, with split RMoW standing in, it is mostly read requests, as predicted.
- **The layout step is what the single appendable object buys: contradicted.**
  - The append is worth {eval}`N["append_req"]` requests per batch ({eval}`N["append_usd"]` µ\$
    on Express) and none of the reads or bytes.
  - The rest of the step comes from how RMoW applies updates (rewriting the touched row groups
    in place, so no scan and no DVs), and from one footer per logical file.
  - The arms that would isolate the object against TailCoW are the `-dv` ablation. `rmow-c-dv`
    keeps TailCoW's DVs on sealed row groups, inside one appendable object. It is in no
    experiment yet.

## Limits

- **One seed, local.** Requests and bytes are deterministic. The 4 MiB rg-sweep and split runs
  match exactly, and the reruns matched the earlier runs. There is no latency.
- **No compaction.** Compaction cuts MoR-v3's and TailCoW's read GETs, which shrinks the policy
  step's read savings. See the compaction sweep (H6).
- **Storage and CPU are left out.**
  - Uncompacted, appendable RMoW keeps {eval}`N["kept_rmow"]`× its live bytes and TailCoW
    {eval}`N["kept_tc"]`×. That is a layout cost (H5, H6).
  - CPU under 2% decides nothing.
- **Prices are list prices**, as `analysis/compaction.py` has them.
