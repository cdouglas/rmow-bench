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
times = fig.bp_times(RUNS, key="store")
over = fig.bp_over_mor(times, key="store")
parts = fig.bp_parts(RUNS, key="store")
# what a table stores follows from the updates, the same on both stores (base-point-model)
storage = fig.bp_storage({S3X: RUNS[S3X]}, key="store")

T = times.set_index(["store", "arm", "what"]).sort_index()
O = over.set_index(["store", "arm", "what"]).over_mor_v3.sort_index()
ST = storage.set_index("arm")


def s(arm, what="a write", store=S3X):
    return T.s[(store, arm, what)]


def o(arm, what="a write", store=S3X):
    return O[(store, arm, what)]


# a design's extra time per write over the time it saves per full read, against MoR-v3: how many
# full reads per write repay its write
def break_even(arm, store=S3X):
    return (s(arm, store=store) - s("mor-v3", store=store)) / (
        s("mor-v3", "a full read", store) - s(arm, "a full read", store))


share = parts.pivot_table(index=["store", "arm", "what"], columns="part", values="s")
share = (share["the store"] / share.sum(axis=1)).rename("store_share")


def share_range(store, writes, arms=None):
    d = share.loc[store]
    d = d[[(w == "a write") == writes and (arms is None or a in arms) for a, w in d.index]]
    return f"{100 * d.min():.0f}–{100 * d.max():.0f}%"


REGION = ["rmow-c", "rmor-c", "rmor-c-footer"]
write_parts = parts[parts.what == "a write"].set_index(["store", "arm", "part"]).s.sort_index()
```

What each design costs a writer and a reader of one table on two object stores, S3 Express and
Azure Blob, written and read with the Parquet code Spark uses for an Iceberg table. At this setting
RMoW-C's write takes {eval}`f"{o('rmow-c'):.1f}"` times as long as MoR-v3's on S3 Express and
{eval}`f"{o('rmow-c', store=AZ):.1f}"` times on Azure, and its full read
{eval}`f"{o('rmow-c', 'a full read'):.2f}"` and {eval}`f"{o('rmow-c', 'a full read', AZ):.2f}"`
of MoR-v3's. On both stores it repays a write only where the table is read in full more than about
six times per write ({eval}`f"{break_even('rmow-c'):.1f}"` on S3 Express,
{eval}`f"{break_even('rmow-c', AZ):.1f}"` on Azure): Azure's slower requests narrow the ratio,
but they lengthen what a write costs and what a read saves alike.

## Setup

**The table and its updates** (`configs/sweep-base.json`, the base point of the sweeps):
- 11 columns, 16 MiB row groups, files of 128 MiB;
- 50 regions of 513,700 rows at the initial load (25.7 million rows), 71 by the end;
- 30 batches of 500,000 operations each (70% inserts, 25% updates, 5% deletes). Updates come as
  statements that each change 3% of a region's live rows, in regions picked with a Zipf skew of 1,
  and a write's deletes fall among the rows its statements change;
- no compaction.

**The designs**, each an Iceberg table:
- **MoR-v3**: Iceberg's merge on read, format v3: each write adds new data files and deletion
  vectors.
- **TailCoW-C**: MoR-v3 with a tail of small files copied on write; here each write's file is
  already the tail's size, so it writes what MoR-v3 writes.
- **RMoW-C**: region merge on write: each write rewrites the regions it changes and appends them, with
  a new footer, to the same object.
- **RMoR-C**: region merge on read: each write rewrites the regions it changes into one new data
  file, with the batch's inserts, and a deletion vector covers each old copy.
  **RMoR-C with footers** hides each old copy instead by appending to its file a footer that no
  longer lists it, as RMoW-C's footers do.

A **write** is one writer committing one batch. A **full read** is one thread reading the whole
table through Iceberg's vectorized reader, each file's footer and deletes loaded once; RMoR-C is
also read skipping the row groups its deletes cover whole. Times are means over the 30 writes and
the 6 full reads after the initial load.

**Where it ran** (build 6f24575):
- **S3 Express:** one `c7a.4xlarge` (AMD Zen 4, 16 vCPUs) beside a directory bucket in usw2-az3, on
  2026-10-31 (run {eval}`RUNS[S3X]`);
- **Azure Blob:** one `Standard_D8as_v6` (AMD Zen 4, 8 vCPUs) beside an ADLS Gen2 account (Standard)
  in westus, on 2026-11-01 (run {eval}`RUNS[AZ]`).

## Writing against reading

Each design's mean write against its mean full read, in seconds, on each store (one scale for both).

```{code-cell} ipython3
:label: base-point-tradeoff
:tags: [remove-input]

color, shape = fig.arm_encoding(list(times.arm.unique()), title="design")
pts = times.pivot_table(index=["store", "arm", "arm_label"], columns="what", values="s").reset_index()
skip = pts[pts.arm == "rmor-c"].copy()
skip["a full read"] = skip["a full read, skipping dead row groups"]
skip["label"] = "RMoR-C, skipping dead row groups"
pts["label"] = pts.arm_label
pts = pd.concat([pts, skip], ignore_index=True)
pts["reader"] = ["skipping" if l.endswith("groups") else "all row groups" for l in pts.label]
pts = pts.merge(times[["arm", "ord"]].drop_duplicates(), on="arm").sort_values("ord")
x_scale = alt.Scale(domain=[0, 1.05 * pts["a write"].max()], nice=False)
y_scale = alt.Scale(domain=[0, 1.05 * pts["a full read"].max()], nice=False)


def tradeoff(store):
    d = pts[pts.store == store]
    base = alt.Chart(d).encode(
        x=alt.X("a write:Q", title="a write (s)", scale=x_scale),
        y=alt.Y("a full read:Q", title="a full read (s)", scale=y_scale),
        color=color)
    points = base.mark_point(size=140, filled=True).encode(
        shape=shape,
        fillOpacity=alt.FillOpacity("reader:N", title="RMoR-C's read",
                                    scale=alt.Scale(domain=["all row groups", "skipping"],
                                                    range=[1, 0.25])),
        tooltip=["label", alt.Tooltip("a write:Q", format=".2f"),
                 alt.Tooltip("a full read:Q", format=".2f")])
    # one label per place (points within 1.5 s of a write and 3 s of a read of the first share
    # one, as MoR-v3 and TailCoW-C do), left of the points on the right; the skipping read's
    # label above the others near it
    place, firsts = [], []
    for x, y, l in zip(d["a write"], d["a full read"], d.label):
        near = [i for i, (fx, fy, fl) in enumerate(firsts)
                if abs(x - fx) < 1.5 and abs(y - fy) < 3 and not (l + fl).count("groups")]
        if not near:
            firsts.append((x, y, l))
        place.append(near[0] if near else len(firsts) - 1)
    lab = d.assign(place=place).groupby("place", sort=False).agg(**{
        "a write": ("a write", "first"), "a full read": ("a full read", "first"),
        "label": ("label", ", ".join), "arm_label": ("arm_label", "first")}).reset_index(drop=True)
    right = lab["a write"] > 0.6 * x_scale.domain[1]
    up = lab.label.str.endswith("groups")
    text = dict(fontSize=11)
    labels = [alt.Chart(lab[~right & ~up]).mark_text(align="left", dx=10, dy=-10, **text),
              alt.Chart(lab[right & ~up]).mark_text(align="right", dx=-12, dy=-10, **text),
              alt.Chart(lab[up]).mark_text(align="right", dx=-4, dy=-24, **text)]
    labels = [c.encode(x=alt.X("a write:Q", scale=x_scale), y=alt.Y("a full read:Q", scale=y_scale),
                       text="label:N", color=color) for c in labels]
    return alt.layer(points, *labels).properties(width=440, height=340, title=store)


alt.hconcat(*[tradeoff(st) for st in STORES]).configure_legend(orient="bottom")
```

```{code-cell} ipython3
:tags: [remove-input]

t = times.pivot_table(index=["store", "ord", "arm_label"], columns="what", values="s").reset_index()
r = over.pivot_table(index=["store", "ord", "arm_label"], columns="what", values="over_mor_v3").reset_index()
t = t.merge(r, on=["store", "ord", "arm_label"], how="left", suffixes=(" (s)", " over MoR-v3's"))
t["store"] = pd.Categorical(t.store, STORES)
table(t.sort_values(["store", "ord"]).drop(columns="ord").rename(columns={"arm_label": "design"})
      .round(2))
```

- **RMoW-C** takes {eval}`f"{s('rmow-c'):.1f}"` s per write against MoR-v3's
  {eval}`f"{s('mor-v3'):.1f}"` s on S3 Express, and {eval}`f"{s('rmow-c', store=AZ):.1f}"` s
  against {eval}`f"{s('mor-v3', store=AZ):.1f}"` s on Azure. It reads the table in
  {eval}`f"{s('rmow-c', 'a full read'):.1f}"` s against {eval}`f"{s('mor-v3', 'a full read'):.1f}"` s,
  and {eval}`f"{s('rmow-c', 'a full read', AZ):.1f}"` s against
  {eval}`f"{s('mor-v3', 'a full read', AZ):.1f}"` s. Each write costs
  {eval}`f"{s('rmow-c') - s('mor-v3'):.1f}"` s and {eval}`f"{s('rmow-c', store=AZ) - s('mor-v3', store=AZ):.1f}"` s
  more, and each full read saves {eval}`f"{s('mor-v3', 'a full read') - s('rmow-c', 'a full read'):.1f}"` s
  and {eval}`f"{s('mor-v3', 'a full read', AZ) - s('rmow-c', 'a full read', AZ):.1f}"` s.
- **RMoR-C with footers** reads as fast as RMoW-C ({eval}`f"{o('rmor-c-footer', 'a full read'):.2f}"`
  and {eval}`f"{o('rmor-c-footer', 'a full read', AZ):.2f}"` of MoR-v3's) but its write takes
  {eval}`f"{o('rmor-c-footer'):.1f}"` and {eval}`f"{o('rmor-c-footer', store=AZ):.1f}"` times as
  long: it pays only past {eval}`f"{break_even('rmor-c-footer'):.0f}"` and
  {eval}`f"{break_even('rmor-c-footer', AZ):.0f}"` full reads per write.
- **RMoR-C** without its footers reads {eval}`f"{o('rmor-c', 'a full read'):.1f}"` and
  {eval}`f"{o('rmor-c', 'a full read', AZ):.1f}"` times as long as MoR-v3: the old copies of the
  regions it rewrote stay in their files under deletion vectors, and Iceberg's reader still fetches
  and decodes them. A reader that skips the row groups a deletion vector covers whole takes
  {eval}`f"{o('rmor-c', 'a full read, skipping dead row groups'):.2f}"` and
  {eval}`f"{o('rmor-c', 'a full read, skipping dead row groups', AZ):.2f}"` of MoR-v3's time. On
  Azure its reads are slower still for a reason of the store's: its write files average 418 MiB, and
  Iceberg's Azure writer uploads a file larger than 256 MiB in blocks of 4 MiB, from which a 16 MiB
  range reads at about 1.6 times the time it takes from a file written in one request (the model's
  page).
- **TailCoW-C** writes and reads as MoR-v3 does here.

## Where the time goes

Each design's mean write and full read, split by what its own thread did: decoding Parquet,
encoding it, the store (waiting on it, and its client's CPU), and the rest (footers, deletes,
other work).

```{code-cell} ipython3
:label: base-point-parts
:tags: [remove-input]

fig.bp_parts_chart(parts, rows="store")
```

```{code-cell} ipython3
:tags: [remove-input]

p = parts.pivot_table(index=["store", "ord", "arm_label", "what"], columns="part", values="s").reset_index()
p["total"] = p[fig.BP_PARTS].sum(axis=1)
p["the store's share"] = p["the store"] / p.total
p["store"] = pd.Categorical(p.store, STORES)
table(fig.bp_sorted(p, "store", "ord").drop(columns="ord").rename(columns={"arm_label": "design"})
      .round(2))
```

A full read is mostly waiting on the store: {eval}`share_range(S3X, False)` of its time on S3 Express
and {eval}`share_range(AZ, False)` on Azure. The region designs' writes are mostly CPU on both
({eval}`share_range(S3X, True, REGION)` the store's on S3 Express,
{eval}`share_range(AZ, True, REGION)` on Azure), above all encoding the regions they rewrite
(RMoW-C's: {eval}`f"{write_parts[(S3X, 'rmow-c', 'encoding')]:.1f}"` s of
{eval}`f"{s('rmow-c'):.1f}"`). MoR-v3's write differs between the stores: on S3 Express its largest
part is decoding the rows it scans for those the batch deletes
({eval}`f"{write_parts[(S3X, 'mor-v3', 'decoding')]:.1f}"` s of {eval}`f"{s('mor-v3'):.1f}"`), and
the store takes {eval}`f"{100 * share[(S3X, 'mor-v3', 'a write')]:.0f}"`%; on Azure the store takes
{eval}`f"{100 * share[(AZ, 'mor-v3', 'a write')]:.0f}"`%. That is why the region designs' writes
look less costly against MoR-v3's on Azure.

## What each phase sends and stores

Requests and megabytes per write and per full read, which follow from the updates and the design
on any store (both runs sent the same), and what each table stores after the 30 writes.

```{code-cell} ipython3
:tags: [remove-input]

c = times[times.store == S3X][["ord", "arm_label", "what", "requests", "gets", "writes", "mb_read",
                               "mb_written"]]
table(fig.bp_sorted(c, "ord").drop(columns="ord")
      .rename(columns={"arm_label": "design", "writes": "PUTs and appends", "gets": "GETs",
                       "mb_read": "MB read", "mb_written": "MB written"}).round(1))
```

```{code-cell} ipython3
:tags: [remove-input]

table(storage.sort_values("ord")[["arm_label", "stored_gb", "live_gb"]]
      .rename(columns={"arm_label": "design", "stored_gb": "stored (GB)",
                       "live_gb": "referenced by the current version (GB)"}).round(2))
```

RMoW-C's write sends {eval}`f"{T.requests[(S3X, 'rmow-c', 'a write')]:.0f}"` requests where MoR-v3's
sends {eval}`f"{T.requests[(S3X, 'mor-v3', 'a write')]:.0f}"`, but writes
{eval}`f"{T.mb_written[(S3X, 'rmow-c', 'a write')]:.0f}"` MB against
{eval}`f"{T.mb_written[(S3X, 'mor-v3', 'a write')]:.0f}"`. Without compaction its object grows: after
30 writes it stores {eval}`f"{ST.stored_gb['rmow-c']:.1f}"` GB, of which the current version
references {eval}`f"{ST.live_gb['rmow-c']:.2f}"` GB, where MoR-v3 stores
{eval}`f"{ST.stored_gb['mor-v3']:.1f}"` GB. These counts are the model's (one request per file
written); on Azure the store's own requests differ where the store does: an appendable create is
three requests, an append past 100 MiB several, and a file past 256 MiB a block per 4 MiB.

## Limits

- One run per store, at one setting: statements changing 3% of a region, 16 MiB row groups, a
  region skew of 1. The sweeps vary each of these.
- One kind of read: a single thread's full scan. Reads of a recent window, of old versions, and in
  parallel were measured too, and are not shown here.
- No compaction: storage grows with every write, and the cost of reclaiming it is not counted.
- Parquet is written by parquet-java, as Spark and Flink write it. Engines with Parquet code of
  their own (Trino, DuckDB, Photon) may encode and decode faster, which would move time from CPU
  to the store.
- The two VMs differ: the Azure VM has half the vCPUs, and for the same work its thread spent
  0.76–0.85 of the S3 Express VM's CPU time on a full read and 0.93–1.05 on a write (Azure's SDK does
  much of its I/O on threads of its own, which the phase's thread does not count).

How well the latency model predicted these times, and the checks that each cloud run did exactly
the work its laptop twin counts, are in [The model on the cloud base point](./base-point-model.md).
Why every design reads through Iceberg's vectorized reader, and what that changed, is in
[Row reader or vectorized reader](./vectorized-reader.md).
