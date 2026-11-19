---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: 'Appendix A: Changes to the readers'
---

```{code-cell} ipython3
:tags: [remove-cell]

import pandas as pd
from labbook import figures_module, q, table
fig = figures_module()
tune = fig.tuner()

# What pruning row groups by key takes off the updates' writes at 128 MiB: each w128-keys-2 run (locate
# keys, the laptop) against its configuration's w128 run (locate scan: havoc's requests, its laptop
# twin's work), a write's simulated latency (s) at every write
KD = [("MoR", "mor", "mor-v3"), ("MoR (compacted)", "compact", "mor-v3"), ("RMoR", "rmor", "rmor-c-kept"),
      ("RMoW-external", "external", "rmor-c-footer-kept"), ("RMoR-density", "density", "rmor-density")]
STORES = {"S3 Express": tune.SPIKES["s3x"], "Azure Blob": tune.SPIKES["azure"]}


def run_in(campaign, label):
    return q(f"""SELECT run_id FROM canonical_runs
                 WHERE list_contains(campaigns, '{campaign}') AND label = '{label}'""").run_id.iloc[0]


def write_s(run, arm, spike, work_run=None):
    d = q(f"""SELECT step, request_ms FROM sized_latency('{run}', '{spike}')
              WHERE arm = '{arm}' AND phase = 'batch' AND step >= 1""")
    w = q(f"""SELECT step, work_ms FROM sized_latency('{work_run or run}', '{spike}')
              WHERE arm = '{arm}' AND phase = 'batch' AND step >= 1""")
    d = d.merge(w, on="step")
    return d.assign(s=(d.request_ms + d.work_ms) / 1e3).set_index("step").s.sort_index()


rows = []
for n, k, arm in KD:
    keys, scan, twin = (run_in("w128-keys-2", f"w128k-upd-{k}"), run_in("w128", f"w128-upd-{k}"),
                        run_in("w128-twins", f"w128-upd-{k}"))
    for store, spike in STORES.items():
        sc, ke = write_s(scan, arm, spike, twin), write_s(keys, arm, spike)
        rows.append(dict(store=store, design=n, scan=sc.mean(), keys=ke.mean(),
                         scan_first10=sc.loc[1:10].mean(), scan_last10=sc.loc[231:240].mean(),
                         keys_first10=ke.loc[1:10].mean(), keys_last10=ke.loc[231:240].mean()))
KT = pd.DataFrame(rows)
KT["less"] = 1 - KT["keys"] / KT["scan"]
K = KT.set_index(["store", "design"])


def w(n, col, store="S3 Express"):
    return f"{K.loc[(store, n), col]:.1f}"


def less(n, store="S3 Express"):
    return f"{K.loc[(store, n), 'less']:.0%}"
```

Every design is an Iceberg table, and stock Iceberg 1.12 reads each one correctly: an appending
design's file is a Parquet file whose newest footer, at the length the table records, lists its
live row groups (docs/HARNESS.md, Compatibility). But stock Iceberg does work some layouts multiply, so
every design is read through Iceberg's vectorized reader with the changes below, which an engine
makes or could make. Each changes what a read fetches, never which rows it returns, and each
applies to every design that has what it changes.

The changes are of two kinds. A **pattern** uses only what Parquet and Iceberg already record: a
stock reader reads the same table, and a reader or writer that follows the pattern fetches less.
Every pattern goes to every design that can use it, MoR included. A **format change** is something
a stock writer, the table's maintenance or the store must support. Only the appending designs need
one: they extend their files with appends.

| Design | Needs appends | What it needs beyond patterns |
|---|---|---|
| MoR, MoR (compacted) | no | nothing |
| RMoR | no | nothing; its reads skip the row groups its deletion vectors cover whole (a pattern) |
| RMoR-density | no | nothing; as RMoR |
| RMoW-external | yes | a footer appended to a data file, which then grows under its name |
| RMoW-append | yes | row groups and a footer appended to a data file, which then grows under its name; on Azure, files created as Append Blobs |

## Patterns: readers

**A footer once per file.** Stock Iceberg plans a split per row group and reopens the file for
each, reading its footer every time, so a file of many row groups, as RMoW-append's grow to, pays
a footer read per row group in every scan. Our reader reads each file's footer once per read, as
engines with a footer cache, and DuckDB, do.

**A footer in one request.** parquet-java reads a footer in two requests: the file's last 8 bytes,
which give the footer's length, then the footer. Our reader fetches the file's last 64 KiB in one,
as Arrow and DuckDB do; the writers read footers as parquet-java does.

**Deletion vectors once per file.** Stock Iceberg loads a data file's deletion vector once per
split, so once per row group. Our reader loads each file's deletes once per read, as an engine with
a delete cache does, which helps the designs with deletion vectors, MoR and RMoR.

**Skipping the row groups a deletion vector covers whole.** RMoR leaves each region it rewrites in
its old file under a deletion vector, and stock Iceberg fetches and decodes that row group only to
drop every one of its rows. Our reader plans no split for a row group whose rows the file's
deletion vector all covers, nor for a file whose vector covers every row, and RMoR's writers skip
them the same way when they scan for the rows a write changes. The sorted read skips them too (since
2026-11-10; before, it fetched every row group of a file it did not skip whole, charging RMoR and
RMoR-density for the row groups they had copied out). Our check asks the vector's bitmap
whether each row group's range is deleted whole, at the cost of a few of its containers. The
results pages take RMoR's times from runs with this check; earlier runs tested each row position in
turn, which cost RMoR seconds of each write and read (RMoR re-measured, 2026-11-04: docs/LESSONS.md).

**Delete files opened with their length.** Stock Iceberg's delete filter opens a delete file
without the length the table records, so the store client asks the store for it: a HEAD per file
on S3 and Azure. Our reader passes the length.

**Azure reads of exactly their range.** Iceberg 1.12's Azure stream opens every file with a GET of
its first 4 MiB, and a ranged read fetches up to 4 MiB more than it needs, since it asks for its
end offset as its length. In our fork of Iceberg 1.12 (`atomicio`) the stream opens nothing until
it reads, and asks for exactly its range, as Iceberg's S3 stream does.

## Patterns: writers

**Finding a write's rows by key.** A MERGE finds the rows a write changes by scanning the table's
key column. Iceberg prunes the scan's files and row groups by its filter, but stops pruning by a
list of values past 200 of them, so a write of thousands of changes prunes at best to the range
between its smallest and largest key, which spans most of the table when the changes fall in
several regions. Our writers prune files by each changed key, and since 2026-11-10 row groups too:
they read the key column only of the row groups whose statistics, in the footer the writer already
holds, may hold one of the keys (`layout.locate` `keys`). It helps MoR, RMoR, RMoW-external and
RMoR-density alike; RMoW-append finds its rows from its footers' statistics alone.

On the updates at 128 MiB the results pages use it ([the hypotheses page](./suite-128.md), K1–K3).
It takes RMoR's write on S3 Express from {eval}`w("RMoR", "scan")` s to {eval}`w("RMoR", "keys")` s
on average ({eval}`less("RMoR")` less), RMoW-external's from {eval}`w("RMoW-external", "scan")` to
{eval}`w("RMoW-external", "keys")` ({eval}`less("RMoW-external")`), RMoR-density's from
{eval}`w("RMoR-density", "scan")` to {eval}`w("RMoR-density", "keys")` ({eval}`less("RMoR-density")`)
and MoR's from {eval}`w("MoR", "scan")` to {eval}`w("MoR", "keys")` ({eval}`less("MoR")`). The cut
designs gain most, since they cut what they copy into many small row groups, and a write without it
reads the key column of every one in its range: without it RMoR-density's writes slowed from
{eval}`w("RMoR-density", "scan_first10")` s over the first 10 writes to
{eval}`w("RMoR-density", "scan_last10")` over the last 10, and with it they slow from
{eval}`w("RMoR-density", "keys_first10")` to {eval}`w("RMoR-density", "keys_last10")`. MoR gains from
the bytes it skips, not the requests: the row groups it prunes are the initial load's large ones.
What it leaves is a file's: each file whose keys may hold a changed key still costs its footer, in
two requests a write, and its deletion vector, loaded before its row groups are pruned.

```{code-cell} ipython3
:tags: [remove-input]

table(KT[["store", "design", "scan", "keys", "less", "scan_first10", "scan_last10", "keys_first10",
          "keys_last10"]].round(2),
      caption="The updates at 128 MiB: a write's simulated latency (s), its mean over the 240 writes "
      "and over the first and last 10, with writers that prune row groups to the write's range of keys "
      "(scan) and by each changed key (keys)")
```

## Format changes and appends

A stock reader reads every design's files (the first paragraph). What the appending designs change
is what Iceberg's writers and the table's maintenance assume: that a data file's object never
changes once written.

**Conditional appends.** RMoW-append and RMoW-external extend an existing object, which Iceberg's
FileIO cannot do. Our fork's `SupportsAtomicOperations` appends at the object's length on the
condition that it is still the version the writer read, so of two writers appending, one lands: a
PutObject append on S3 Express, an Append Block on Azure (one per 100 MiB). Standard S3 has no
append.

**Appendable files on Azure.** Azure appends only to Append Blobs, and Iceberg writes block blobs.
The appending designs create each file as a temporary Append Blob that they rename onto its name,
three requests where Iceberg's own write is one.

**A data file that grows under its name.** Iceberg assumes a data file never changes, but an
appending design's file grows, and each append is a new version of it, which the write commits to
the table with the file's new length, split offsets and metrics. A reader of any snapshot opens the
object at the length that snapshot records and finds that version's footer at its end, as stock
readers already do.
