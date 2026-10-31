---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: The base point on S3 Express
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
import pandas as pd
from labbook import figures_module, table, theme
theme()
fig = figures_module()

# The vectorized reader's S3 Express run (figures.py, BP_CLOUD), as an application reads the table
B = fig.BP_NEW
RUN = fig.BP_CLOUD[B]
times = fig.bp_times()
times = times[times.build == B]
over = fig.bp_over_mor(times)
parts = fig.bp_parts()
parts = parts[parts.build == B]
storage = fig.bp_storage()
storage = storage[storage.build == B]

T = times.set_index(["arm", "what"]).sort_index()
O = over.set_index(["arm", "what"]).over_mor_v3.sort_index()
ST = storage.set_index("arm")


def s(arm, what="a write"):
    return T.s[(arm, what)]


# a design's extra time per write over the time it saves per full read, against MoR-v3: how many
# full reads per write repay its write
def break_even(arm):
    return (s(arm) - s("mor-v3")) / (s("mor-v3", "a full read") - s(arm, "a full read"))


share = parts.pivot_table(index=["arm", "what"], columns="part", values="s")
share = (share["the store"] / share.sum(axis=1)).rename("store_share")
reads_store = share[[w != "a write" for _, w in share.index]]
write_parts = parts[parts.what == "a write"].set_index(["arm", "part"]).s.sort_index()
```

What each design costs a writer and a reader of one table on S3 Express, written and read with the
Parquet code Spark uses for an Iceberg table. At this setting RMoW-C's write takes
{eval}`f"{O[('rmow-c', 'a write')]:.1f}"` times as long as MoR-v3's, and its full read
{eval}`f"{O[('rmow-c', 'a full read')]:.2f}"` of MoR-v3's: it repays a write only where the table is
read in full more than {eval}`f"{break_even('rmow-c'):.1f}"` times per write.

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

**Where it ran:** one `c7a.4xlarge` (AMD Zen 4, 16 vCPUs) beside an S3 Express directory bucket in
usw2-az3, on 2026-10-31 (run {eval}`RUN`, build 6f24575).

## Writing against reading

Each design's mean write against its mean full read, in seconds.

```{code-cell} ipython3
:label: base-point-tradeoff
:tags: [remove-input]

color, shape = fig.arm_encoding(list(times.arm.unique()), title="design")
pts = times.pivot_table(index=["arm", "arm_label"], columns="what", values="s").reset_index()
skip = pts[pts.arm == "rmor-c"].copy()
skip["a full read"] = skip["a full read, skipping dead row groups"]
skip["label"] = "RMoR-C, skipping dead row groups"
pts["label"] = pts.arm_label
pts = pd.concat([pts, skip], ignore_index=True)
pts["reader"] = ["skipping" if l.endswith("groups") else "all row groups" for l in pts.label]
base = alt.Chart(pts).encode(
    x=alt.X("a write:Q", title="a write (s)"),
    y=alt.Y("a full read:Q", title="a full read (s)"),
    color=color)
points = base.mark_point(size=140, filled=True).encode(
    shape=shape,
    fillOpacity=alt.FillOpacity("reader:N", title="RMoR-C's read",
                                scale=alt.Scale(domain=["all row groups", "skipping"], range=[1, 0.25])),
    tooltip=["label", alt.Tooltip("a write:Q", format=".2f"), alt.Tooltip("a full read:Q", format=".2f")])
# one label per place (MoR-v3 and TailCoW-C coincide here), left of the points on the right
pts = pts.merge(times[["arm", "ord"]].drop_duplicates(), on="arm").sort_values("ord")
pts["place"] = list(zip(pts["a write"].round(1), pts["a full read"].round(1)))
lab = pts.groupby("place", sort=False).agg(**{
    "a write": ("a write", "first"), "a full read": ("a full read", "first"),
    "label": ("label", ", ".join), "arm_label": ("arm_label", "first")}).reset_index(drop=True)
right = lab["a write"] > 0.6 * lab["a write"].max()
text = dict(dy=-10, fontSize=12)
labels = [alt.Chart(lab[~right]).mark_text(align="left", dx=10, **text),
          alt.Chart(lab[right]).mark_text(align="right", dx=-12, **text)]
labels = [c.encode(x="a write:Q", y="a full read:Q", text="label:N", color=color) for c in labels]
alt.layer(points, *labels).properties(width=640, height=380).configure_legend(orient="bottom")
```

```{code-cell} ipython3
:tags: [remove-input]

t = times.pivot_table(index=["ord", "arm_label"], columns="what", values="s").reset_index()
o = over.pivot_table(index=["ord", "arm_label"], columns="what", values="over_mor_v3").reset_index()
t = t.merge(o, on=["ord", "arm_label"], how="left", suffixes=(" (s)", " over MoR-v3's"))
table(t.sort_values("ord").drop(columns="ord").rename(columns={"arm_label": "design"}).round(2))
```

- **RMoW-C** takes {eval}`f"{s('rmow-c'):.1f}"` s per write against MoR-v3's
  {eval}`f"{s('mor-v3'):.1f}"` s, and reads the table in {eval}`f"{s('rmow-c', 'a full read'):.1f}"` s
  against {eval}`f"{s('mor-v3', 'a full read'):.1f}"` s. Each write costs
  {eval}`f"{s('rmow-c') - s('mor-v3'):.1f}"` s more and each full read saves
  {eval}`f"{s('mor-v3', 'a full read') - s('rmow-c', 'a full read'):.1f}"` s, so it pays where the
  table is read in full more than {eval}`f"{break_even('rmow-c'):.1f}"` times per write.
- **RMoR-C with footers** reads as fast as RMoW-C ({eval}`f"{O[('rmor-c-footer', 'a full read')]:.2f}"`
  of MoR-v3's) but its write takes {eval}`f"{O[('rmor-c-footer', 'a write')]:.1f}"` times as long:
  it pays only past {eval}`f"{break_even('rmor-c-footer'):.0f}"` full reads per write.
- **RMoR-C** without its footers reads {eval}`f"{O[('rmor-c', 'a full read')]:.1f}"` times as long
  as MoR-v3: the old copies of the regions it rewrote stay in their files under deletion vectors,
  and Iceberg's reader still fetches and decodes them. A reader that skips the row groups a
  deletion vector covers whole takes {eval}`f"{O[('rmor-c', 'a full read, skipping dead row groups')]:.2f}"`
  of MoR-v3's time.
- **TailCoW-C** writes and reads as MoR-v3 does here.

## Where the time goes

Each design's mean write and full read, split by what its own thread did: decoding Parquet,
encoding it, the store (waiting on it, and its client's CPU), and the rest (footers, deletes,
other work).

```{code-cell} ipython3
:label: base-point-parts
:tags: [remove-input]

fig.bp_parts_chart(parts, rows=None)
```

```{code-cell} ipython3
:tags: [remove-input]

p = parts.pivot_table(index=["ord", "arm_label", "what"], columns="part", values="s").reset_index()
p["total"] = p[fig.BP_PARTS].sum(axis=1)
p["the store's share"] = p["the store"] / p.total
table(fig.bp_sorted(p, "ord").drop(columns="ord").rename(columns={"arm_label": "design"}).round(2))
```

A full read is mostly waiting on the store: {eval}`f"{100 * reads_store.min():.0f}"`–{eval}`f"{100 * reads_store.max():.0f}"`%
of its time. A write is mostly CPU. MoR-v3's largest part is decoding the rows it scans for those
the batch deletes ({eval}`f"{write_parts[('mor-v3', 'decoding')]:.1f}"` s of
{eval}`f"{s('mor-v3'):.1f}"`); RMoW-C's largest part is encoding the regions it rewrites
({eval}`f"{write_parts[('rmow-c', 'encoding')]:.1f}"` s of {eval}`f"{s('rmow-c'):.1f}"`), and
both RMoR-C designs spend most of theirs encoding a whole new data file.

## What each phase sends and stores

Requests and megabytes per write and per full read, which follow from the updates and the design
on any store, and what each table stores after the 30 writes.

```{code-cell} ipython3
:tags: [remove-input]

c = times[["ord", "arm_label", "what", "requests", "gets", "writes", "mb_read", "mb_written"]]
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

RMoW-C's write sends {eval}`f"{T.requests[('rmow-c', 'a write')]:.0f}"` requests where MoR-v3's
sends {eval}`f"{T.requests[('mor-v3', 'a write')]:.0f}"`, but writes
{eval}`f"{T.mb_written[('rmow-c', 'a write')]:.0f}"` MB against
{eval}`f"{T.mb_written[('mor-v3', 'a write')]:.0f}"`. Without compaction its object grows: after 30
writes it stores {eval}`f"{ST.stored_gb['rmow-c']:.1f}"` GB, of which the current version
references {eval}`f"{ST.live_gb['rmow-c']:.2f}"` GB, where MoR-v3 stores
{eval}`f"{ST.stored_gb['mor-v3']:.1f}"` GB.

## Limits

- One run, at one setting: statements changing 3% of a region, 16 MiB row groups, a region skew
  of 1, one store. The sweeps vary each of these.
- One kind of read: a single thread's full scan. Reads of a recent window, of old versions, and in
  parallel were measured too, and are not shown here.
- No compaction: storage grows with every write, and the cost of reclaiming it is not counted.
- Parquet is written by parquet-java, as Spark and Flink write it. Engines with Parquet code of
  their own (Trino, DuckDB, Photon) may encode and decode faster, which would move time from CPU
  to the store.

How well the latency model predicted these times, and the checks that this run did exactly the
work its laptop twin counts, are in [The model on the cloud base point](./base-point-model.md). Why
every design reads through Iceberg's vectorized reader, and what that changed, is in
[Row reader or vectorized reader](./vectorized-reader.md).
