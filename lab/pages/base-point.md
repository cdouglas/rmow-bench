---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The base point on S3 Express and Azure
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, table, theme
theme()
fig = figures_module()

# The vectorized reader's runs on each store (figures.py, BP_STORE_RUNS), as an application reads
# the table
STORES = fig.BP_STORES
RUNS = fig.BP_STORE_RUNS
S3X, AZ = STORES
KEYS = ["store"]
MAIN, REGION = fig.MAIN, fig.REGION_DESIGNS
WRITE, READ = fig.WRITE, fig.READ

# each design's mean write and full read (s), and what it sent and moved; `every` has the
# appendix's two variants too
every = fig.designs(fig.bp_times(RUNS, key="store"), main=False)
times = every[every.design.isin(MAIN)]
over = fig.over_mor(times, KEYS)
T = times.set_index(["store", "design", "what"]).sort_index()
O = over.set_index(["store", "design", "what"]).over_mor.sort_index()


def s(design, what=WRITE, store=S3X):
    return T.s[(store, design, what)]


def o(design, what=WRITE, store=S3X):
    return O[(store, design, what)]


# a design's extra time per write over the time it saves per full read, against MoR: how many full
# reads per write repay its write
def break_even(design, store=S3X):
    return (s(design, store=store) - s("MoR", store=store)) / (
        s("MoR", READ, store) - s(design, READ, store))


# each design's writes and full reads over its run (1. Writing against reading)
trade = fig.tradeoff(fig.bp_spread(RUNS, key="store"), KEYS)

# where the time went (phase_breakdown), by part and by the write's steps
parts = fig.designs(fig.bp_parts(RUNS, key="store", fine=True))
share = parts.pivot_table(index=["store", "design", "what"], columns="part", values="s")
share = share.div(share.sum(axis=1), axis=0)["the store"]


def share_range(store, what, designs=MAIN):
    d = share.loc[store]
    d = d[[w == what and x in designs for x, w in d.index]]
    return f"{100 * d.min():.0f}–{100 * d.max():.0f}%"


write_parts = parts[parts.what == WRITE].set_index(["store", "design", "part"]).s.sort_index()
steps = fig.designs(fig.bp_steps(RUNS, key="store"))
step_share = steps.groupby(["store", "design", "write_step"]).s.sum().unstack()
step_share = step_share.div(step_share.sum(axis=1), axis=0)
# rows and planning in each design's write, its locate scan and its read: in RMoR's, mostly its
# check of each row group against its deletion vector (Limits)
RP = "rows and planning"
rp_write = parts[(parts.what == WRITE) & (parts.part == RP)].set_index(["store", "design"]).s
rp_read = parts[(parts.what == READ) & (parts.part == RP)].set_index(["store", "design"]).s
rp_locate = steps[(steps.write_step == "locate") & (steps.part == RP)].set_index(
    ["store", "design"]).s

# each design's latency at every write and full read of its run (Latency as snapshots are added)
series = fig.designs(fig.bp_series(RUNS, key="store"))

# what a table stores follows from the updates, the same on both stores (base-point-model)
storage = fig.designs(fig.bp_storage({S3X: RUNS[S3X]}, key="store"))
ST = storage.set_index("design")

# Appendix B: RMoR read without skipping, against RMoR's read; TailCoW against MoR
noskip = every[(every.design == "RMoR without skipping") & (every.what == READ)].set_index("store").s
noskip_over = noskip / times[(times.design == "RMoR") & (times.what == READ)].set_index("store").s
tail = fig.over_mor(every[every.design.isin(["MoR", "TailCoW"])], KEYS).set_index(
    ["store", "what"]).over_mor
```

What each design costs a writer and a reader of one table on two object stores, S3 Express and
Azure Blob, written and read with the Parquet code Spark uses for an Iceberg table.

- **RMoW-append writes slower than MoR and reads faster.** Its write takes
  {eval}`f"{o('RMoW-append'):.1f}"` times as long as MoR's on S3 Express and
  {eval}`f"{o('RMoW-append', store=AZ):.1f}"` times on Azure, and its full read
  {eval}`f"{o('RMoW-append', READ):.2f}"` and {eval}`f"{o('RMoW-append', READ, AZ):.2f}"` of MoR's.
- **It repays a write only where the table is read in full more than about six times per write**
  ({eval}`f"{break_even('RMoW-append'):.1f}"` on S3 Express,
  {eval}`f"{break_even('RMoW-append', AZ):.1f}"` on Azure): Azure's slower requests narrow the
  ratio, but they lengthen what a write costs and what a read saves alike.
- **RMoW-external reads as fast as RMoW-append, but writes slower:** its write takes
  {eval}`f"{s('RMoW-external') / s('RMoW-append'):.1f}"` and
  {eval}`f"{s('RMoW-external', store=AZ) / s('RMoW-append', store=AZ):.1f}"` times as long as
  RMoW-append's.
- **RMoR writes as slowly as RMoW-external, and reads no faster than MoR**
  ({eval}`f"{o('RMoR', READ):.2f}"` and {eval}`f"{o('RMoR', READ, AZ):.2f}"` of MoR's full read),
  so it never repays its write.

## The designs

```{include} ./_designs.md
```

## Setup

**The table and its updates** (`configs/sweep-base.json`, the base point of the sweeps):
- 11 columns, 16 MiB row groups, files of 128 MiB;
- 50 regions of 513,700 rows at the initial load (25.7 million rows), 71 by the end;
- 30 writes of 500,000 operations each (70% inserts, 25% updates, 5% deletes). Updates come as
  statements that each change 3% of a region's live rows, in regions picked with a Zipf skew of 1,
  and a write's deletes fall among the rows its statements change;
- no compaction.

A **write** is one writer committing one batch, and its time is the **write latency**. A **full
read** is one thread reading the whole table, and its time is the **read latency**. Each write adds
a snapshot. Latencies are means over the 30 writes and the 6 full reads after the initial load, one
every 5 writes.

**Where it ran** (build 6f24575):
- **S3 Express:** one `c7a.4xlarge` (AMD Zen 4, 16 vCPUs) beside a directory bucket in usw2-az3, on
  2026-10-31 (run {eval}`RUNS[S3X]`);
- **Azure Blob:** one `Standard_D8as_v6` (AMD Zen 4, 8 vCPUs) beside an ADLS Gen2 account (Standard)
  in westus, on 2026-11-01 (run {eval}`RUNS[AZ]`).

## 1. Writing against reading

Each design's mean write latency against its mean read latency on each store (one scale for
both). The horizontal whisker runs from the 10th to the 90th percentile of the design's 30 writes.
The vertical one runs from its full read after 5 writes to its full read after 30: with no
compaction a full read slows as updates pile up, so it shows the table aging, not noise.

```{code-cell} ipython3
:label: base-point-tradeoff
:tags: [remove-input]
:class: col-page-right

color, shape = fig.design_encoding()
x_max, y_max = 1.05 * trade.write_p90.max(), 1.05 * trade.read_last.max()
alt.hconcat(*[fig.bp_tradeoff_chart(trade, st, color, shape, x_max, y_max) for st in STORES]
            ).configure_legend(orient="bottom", symbolOpacity=1)
```

```{code-cell} ipython3
:tags: [remove-input]

names = {"write_s": "write", "write_p10": "write p10", "write_p90": "write p90",
         "read_s": "full read", "read_first": "read after 5", "read_last": "read after 30"}
t = trade.assign(store=pd.Categorical(trade.store, STORES)).sort_values(["store", "ord"])
r = over.pivot_table(index=["store", "design"], columns="what", values="over_mor")
r.columns = [f"{w[2:]} over MoR's" for w in r.columns]
table(t[["store", "design", *names]].rename(columns=names)
      .merge(r.reset_index(), on=["store", "design"], how="left").round(2),
      caption="Seconds, and each design's mean over MoR's")
```

- **RMoW-append** takes {eval}`f"{s('RMoW-append'):.1f}"` s per write against MoR's
  {eval}`f"{s('MoR'):.1f}"` s on S3 Express, and {eval}`f"{s('RMoW-append', store=AZ):.1f}"` s
  against {eval}`f"{s('MoR', store=AZ):.1f}"` s on Azure. It reads the table in
  {eval}`f"{s('RMoW-append', READ):.1f}"` s against {eval}`f"{s('MoR', READ):.1f}"` s, and
  {eval}`f"{s('RMoW-append', READ, AZ):.1f}"` s against {eval}`f"{s('MoR', READ, AZ):.1f}"` s. Each
  write costs {eval}`f"{s('RMoW-append') - s('MoR'):.1f}"` s and
  {eval}`f"{s('RMoW-append', store=AZ) - s('MoR', store=AZ):.1f}"` s more, and each full read saves
  {eval}`f"{s('MoR', READ) - s('RMoW-append', READ):.1f}"` s and
  {eval}`f"{s('MoR', READ, AZ) - s('RMoW-append', READ, AZ):.1f}"` s.
- **RMoW-external** reads as fast as RMoW-append ({eval}`f"{o('RMoW-external', READ):.2f}"` and
  {eval}`f"{o('RMoW-external', READ, AZ):.2f}"` of MoR's) but its write takes
  {eval}`f"{o('RMoW-external'):.1f}"` and {eval}`f"{o('RMoW-external', store=AZ):.1f}"` times as
  long: it pays only past {eval}`f"{break_even('RMoW-external'):.0f}"` and
  {eval}`f"{break_even('RMoW-external', AZ):.0f}"` full reads per write.
- **RMoR** writes {eval}`f"{o('RMoR'):.1f}"` and {eval}`f"{o('RMoR', store=AZ):.1f}"` times as long
  as MoR, and reads in {eval}`f"{o('RMoR', READ):.2f}"` and {eval}`f"{o('RMoR', READ, AZ):.2f}"` of
  its time. On Azure its reads are slower for a reason of the store's: its write files average
  418 MiB, and Iceberg's Azure writer uploads a file larger than 256 MiB in blocks of 4 MiB, from
  which a 16 MiB range reads at about 1.6 times the time it takes from a file written in one
  request ([the model's page](./base-point-model.md)).

### Latency as snapshots are added

The same runs write by write: each design's write latency at every write, and its read latency
after the initial load and every 5 writes. One latency axis for both stores.

```{code-cell} ipython3
:label: base-point-latency
:tags: [remove-input]
:class: col-page-right

fig.latency_chart(series).configure_legend(orient="bottom")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

def series_table(what):
    t = (series[series.what == what]
         .pivot_table(index="step", columns=["store", "design"], values="s")
         .reindex(columns=[(st, x) for st in STORES for x in MAIN]))
    t.columns = [f"{st}: {x}" for st, x in t.columns]
    return table(t.reset_index().round(2),
                 caption=f"{fig.TIME_TITLE[what].capitalize()} (s) at each snapshot")


series_table(WRITE)
```

```{code-cell} ipython3
:tags: [remove-input]

series_table(READ)
```

The region designs' write latency varies the most from one write to the next. Reads slow as the
snapshots add up: from the read after the initial load to the one after 30 writes, MoR's goes from
{eval}`f"{series.set_index(['store', 'design', 'what', 'step']).s[(S3X, 'MoR', READ, 0)]:.1f}"` s to
{eval}`f"{series.set_index(['store', 'design', 'what', 'step']).s[(S3X, 'MoR', READ, 30)]:.1f}"` s
on S3 Express, and RMoW-append's from
{eval}`f"{series.set_index(['store', 'design', 'what', 'step']).s[(S3X, 'RMoW-append', READ, 0)]:.1f}"` s to
{eval}`f"{series.set_index(['store', 'design', 'what', 'step']).s[(S3X, 'RMoW-append', READ, 30)]:.1f}"` s.

## 2. What a write costs

Each design's mean write latency, split by what the write's thread did:
- **decoding** and **encoding** Parquet;
- **deletion vectors and footers:** building, loading and merging them;
- **rows and planning:** the rest of its CPU, making the rows it writes, merging the changes into
  the regions it rewrites and sorting them by event time, and planning its scans;
- **the store:** waiting on it, and its client's CPU;
- **paused:** neither running nor waiting on the store (garbage collection, the JIT, scheduling).

One axis for both stores.

```{code-cell} ipython3
:label: base-point-write-parts
:tags: [remove-input]

fig.parts_chart(parts, WRITE, width=260).configure_legend(orient="top")
```

```{code-cell} ipython3
:tags: [remove-input]

def parts_table(what):
    p = (parts[parts.what == what].pivot_table(index=["store", "ord", "design"], columns="part",
                                               values="s").reindex(columns=fig.TIME_PARTS)
         .fillna(0).reset_index())
    p["total"] = p[fig.TIME_PARTS].sum(axis=1)
    p["the store's share"] = p["the store"] / p.total
    p["store"] = pd.Categorical(p.store, STORES)
    return table(p.sort_values(["store", "ord"]).drop(columns="ord").round(2),
                 caption=f"Seconds of each design's mean {what[2:]}, by part")


parts_table(WRITE)
```

The same writes by step: finding the rows the write changes (locate), reading back what it rewrites
(merge-read), writing data files, regions and footers (write), writing deletion vectors and
RMoW-external's footers (delete-write), and the rest of the write, each split as above.

```{code-cell} ipython3
:label: base-point-steps
:tags: [remove-input]
:class: col-page-right

fig.steps_chart(steps).configure_legend(orient="top")
```

```{code-cell} ipython3
:tags: [remove-input, hide-output]

w = steps.assign(step=pd.Categorical(steps.write_step.map(fig.CS_STEP), list(fig.CS_STEP.values())),
                 store=pd.Categorical(steps.store, STORES))
w = w.pivot_table(index=["store", "ord", "design", "step"], columns="part", values="s",
                  observed=True).reindex(columns=fig.TIME_PARTS)
table(w.reset_index().drop(columns="ord").round(2),
      caption="Seconds of each design's mean write, by step and part")
```

- **The region designs' writes are mostly CPU on both stores**
  ({eval}`share_range(S3X, WRITE, REGION)` the store's on S3 Express,
  {eval}`share_range(AZ, WRITE, REGION)` on Azure), above all encoding the regions they rewrite
  (RMoW-append's: {eval}`f"{write_parts[(S3X, 'RMoW-append', 'encoding')]:.1f}"` s of
  {eval}`f"{s('RMoW-append'):.1f}"`).
- **Rows and planning take the region designs seconds per write:**
  {eval}`f"{rp_write[(S3X, 'RMoR')]:.1f}"` s of RMoR's, {eval}`f"{rp_write[(S3X, 'RMoW-external')]:.1f}"` s
  of RMoW-external's and {eval}`f"{rp_write[(S3X, 'RMoW-append')]:.1f}"` s of RMoW-append's on S3
  Express, against MoR's {eval}`f"{rp_write[(S3X, 'MoR')]:.2f}"` s. A region design makes and sorts
  every row of every region it rewrites; RMoR's also includes its writer's check of each row group it
  scans against its deletion vector (Limits).
- **MoR's write is mostly finding the rows it changes:** its scan of the files that may hold them
  takes {eval}`f"{100 * step_share.locate[(S3X, 'MoR')]:.0f}"`% of its write on S3 Express and
  {eval}`f"{100 * step_share.locate[(AZ, 'MoR')]:.0f}"`% on Azure, where RMoW-append finds them in
  its footers' statistics, in {eval}`f"{100 * step_share.locate[(S3X, 'RMoW-append')]:.0f}"`%. On
  S3 Express the largest part of MoR's write is decoding what it scans
  ({eval}`f"{write_parts[(S3X, 'MoR', 'decoding')]:.1f}"` s of {eval}`f"{s('MoR'):.1f}"`), and the
  store takes {eval}`f"{100 * share[(S3X, 'MoR', WRITE)]:.0f}"`%; on Azure the store takes
  {eval}`f"{100 * share[(AZ, 'MoR', WRITE)]:.0f}"`%. That is why the region designs' writes look
  less costly against MoR's on Azure.

## 3. What a read costs

Each design's mean read latency, split as a write's above. In a read, rows and planning is
planning the scan. One axis for both stores.

```{code-cell} ipython3
:label: base-point-read-parts
:tags: [remove-input]

fig.parts_chart(parts, READ, width=260).configure_legend(orient="top")
```

```{code-cell} ipython3
:tags: [remove-input]

parts_table(READ)
```

A full read is mostly the store's: {eval}`share_range(S3X, READ)` of its time on S3
Express and {eval}`share_range(AZ, READ)` on Azure, whatever the design. RMoR's read spends
{eval}`f"{rp_read[(S3X, 'RMoR')]:.1f}"` s planning on S3 Express, the others' at most
{eval}`f"{rp_read.drop('RMoR', level='design').max():.2f}"` s: our reader's check of each row group
against its deletion vector (Limits).

## 4. What each design sends and stores

Requests and megabytes per write and per full read, which follow from the updates and the design
on any store (both runs sent the same), and what each table stores after the 30 writes.

```{code-cell} ipython3
:tags: [remove-input]

c = times[times.store == S3X].sort_values(["what", "ord"], ascending=[False, True])
table(c[["design", "what", "requests", "gets", "writes", "mb_read", "mb_written"]]
      .rename(columns={"writes": "PUTs and appends", "gets": "GETs", "mb_read": "MB read",
                       "mb_written": "MB written"}).round(1))
```

```{code-cell} ipython3
:tags: [remove-input]

table(storage.sort_values("ord")[["design", "stored_gb", "live_gb"]]
      .rename(columns={"stored_gb": "stored (GB)",
                       "live_gb": "referenced by the current version (GB)"}).round(2))
```

RMoW-append's write sends {eval}`f"{T.requests[(S3X, 'RMoW-append', WRITE)]:.0f}"` requests where
MoR's sends {eval}`f"{T.requests[(S3X, 'MoR', WRITE)]:.0f}"`, but writes
{eval}`f"{T.mb_written[(S3X, 'RMoW-append', WRITE)]:.0f}"` MB against
{eval}`f"{T.mb_written[(S3X, 'MoR', WRITE)]:.0f}"`. Without compaction its object grows: after
30 writes it stores {eval}`f"{ST.stored_gb['RMoW-append']:.1f}"` GB, of which the current version
references {eval}`f"{ST.live_gb['RMoW-append']:.2f}"` GB, where MoR stores
{eval}`f"{ST.stored_gb['MoR']:.1f}"` GB. These counts are the model's (one request per file
written); on Azure the store's own requests differ where the store does: an appendable create is
three requests, an append past 100 MiB several, and a file past 256 MiB a block per 4 MiB.

## Limits

- One run per store, at one setting: statements changing 3% of a region, 16 MiB row groups, a
  region skew of 1. The sweeps vary each of these.
- One kind of read: a single thread's full scan, through the changed reader
  ([Appendix A](./reader-changes.md)). Reads of a recent window, of old versions, and in parallel
  were measured too, and are not shown here.
- In these runs RMoR's check of a row group against its deletion vector tested each row position
  in turn. It cost RMoR's full read the planning time above, and its write some more in its locate
  scan, where rows and planning take {eval}`f"{rp_locate[(S3X, 'RMoR')]:.1f}"` s on S3 Express
  against MoR's {eval}`f"{rp_locate[(S3X, 'MoR')]:.2f}"` s. The check now asks the vector's bitmap
  about each row group's whole range (since 2026-11-02), and these runs are yet to be repeated with
  it, so RMoR's latencies here are an upper bound.
- No compaction: storage grows with every write, and the cost of reclaiming it is not counted.
- Parquet is written by parquet-java, as Spark and Flink write it. Engines with Parquet code of
  their own (Trino, DuckDB, Photon) may encode and decode faster, which would move time from CPU
  to the store.
- The two VMs differ: the Azure VM has half the vCPUs, and for the same work its thread spent
  0.76–0.85 of the S3 Express VM's CPU time on a full read and 0.93–1.05 on a write (Azure's SDK does
  much of its I/O on threads of its own, which the phase's thread does not count).

How well the latency model predicted these times, and the checks that each cloud run did exactly
the work its laptop twin counts, are in [The model on the cloud base point](./base-point-model.md),
which names the designs as the code does: MoR-v3 for MoR, RMoR-C for RMoR, RMoR-C footer for
RMoW-external and RMoW-C for RMoW-append. Why every design reads through Iceberg's vectorized
reader, and what that changed, is in [Row reader or vectorized reader](./vectorized-reader.md).

## Appendix B: the variants left out

**RMoR without skipping.** Read by a reader that fetches and decodes each row group its deletion
vectors cover whole, as stock Iceberg's does, RMoR's full read takes
{eval}`f"{noskip[S3X]:.1f}"` s on S3 Express and {eval}`f"{noskip[AZ]:.1f}"` s on Azure,
{eval}`f"{noskip_over[S3X]:.1f}"` and {eval}`f"{noskip_over[AZ]:.1f}"` times its read with
skipping: the old copies of the regions it rewrote stay in their files. Its write is RMoR's.

**TailCoW**, a control, is MoR with RMoR's tail of inserts: it isolates the tail's merging from how
RMoR handles updates. Here each write's new file is already the tail's size, so it writes what MoR
writes: its write takes {eval}`f"{tail[(S3X, WRITE)]:.2f}"` and {eval}`f"{tail[(AZ, WRITE)]:.2f}"`
of MoR's time, and its full read {eval}`f"{tail[(S3X, READ)]:.2f}"` and
{eval}`f"{tail[(AZ, READ)]:.2f}"`.
