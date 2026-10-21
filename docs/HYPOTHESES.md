# Hypotheses

Written 2026-10-20, before any cloud run, so that the results cannot choose what gets reported.
Each hypothesis names the measurement that decides it. A run that contradicts one is reported as
such; a hypothesis is revised only in a new, dated section below, never edited in place.

Terms: **MoR-v3** = new data file per batch plus deletion vectors; **RMoW-A/B/C** = region merge on
write with policies A (rewrite touched row groups), B (trailing merge) and C (tiered); **TailCoW**
= plain Iceberg rewriting one small tail data file per batch until it reaches the target size.

## H1 — recency-skewed updates

With updates and deletes concentrated on recent rows (`RECENCY_EXP`, mean 2 batches), RMoW-C's
`read_full` cost (requests, bytes; latency on cloud stores) stays flat as batches accumulate,
while MoR-v3's grows with the number of data files and DVs until compaction. RMoW-C's per-batch
write cost (`batch` phase: requests, bytes written including amortized compaction) is within a
small constant factor (≤ 3×) of MoR-v3's.

*Decided by:* `read_full` and `batch` records against step, on S3 Express.

## H2 — uniform updates

With updates spread uniformly over the table (`UNIFORM`), every batch touches most row groups, so
RMoW's write cost approaches copy-on-write of the whole table and MoR-v3 is cheaper to write by an
order of magnitude or more; RMoW keeps its read advantage only until MoR compacts.

*Decided by:* `batch` bytes written per row changed, RMoW-A/C against MoR-v3, local and S3 Express.

## H3 — layout versus policy

The difference between RMoW-C and TailCoW is what the single appendable object buys (fewer
objects and footers to open, one append instead of a new object per batch); the difference
between TailCoW and MoR-v3 is what merging the tail buys. We expect the first difference to be
smaller than the second on object stores with cheap PUTs, and to matter mostly for read requests.

*Decided by:* the same comparisons, per request type.

## H4 — old versions and dead bytes

With bounded ranged reads (the vectored Parquet path), reading an old version of an appendable
file costs the same requests and bytes as reading a current version of the same size, and the
dead regions between live row groups add no read bytes. Without a footer cache, an old version's
footer read on S3 costs one aborted open-ended GET more than a current one (the stream reads past
the old footer into newer bytes until the reader seeks).

*Decided by:* `read_old` against `read_full` at equal live size, and the trace's skip-through and
abort counts.

## Known costs going in (from M0 spikes)

- A Parquet footer costs about 1.16 KB per row group at 11 columns and 5.3 KB at 50 columns
  (`FooterBench` sizes). Every RMoW version rewrites its file's whole footer and every open reads
  it, so files must bound their row-group count.
- Stock parquet-java reads a footer with two requests (the 8-byte tail, then the footer) on files
  over 1 MiB; Iceberg fetches files up to 1 MiB whole in one request.

## Added 2026-10-21, before the split-file runs

**RMoW-*-split** = the same region merge on write, with each version's regions written as a new
object (a standalone Parquet file) and its footer as a footer-only object naming the objects its
row groups live in (`ColumnChunk.file_path`), instead of appending both to one object.

### H2 result (local, recorded before H5)

H2 held, as a validating case: under uniform updates every batch touches every row group, and
RMoW writes what copy-on-write writes, 37.5 MB per batch against CoW's 37.6 and MoR-v3's 0.46
(`local-m1-uniform-nocompact`, batch 60). `RunnerSmokeTest` now asserts it, for both layouts.

### H5 — regions in separate objects

Against RMoW on appendable objects, with the same policy:

- **Writes** cost one more request per file a batch changes (a region object and a footer object
  instead of one append), and about the same bytes (each region object adds its own small
  footer).
- **Reads** cost the same GETs within one per file read (a footer object up to 1 MiB is fetched
  whole), and the same bytes.
- **Space**: once versions older than the retained window expire, what remains is within 1.5× of
  the live bytes without compaction, where the appendable object keeps every dead region until a
  rewrite; with compaction, the split files need fewer rewrites for the same bound.
- **Objects**: each version adds one or two objects per file changed, so the stored object count
  grows with batches until expiry; the live object count stays within a small multiple of the
  appendable layout's.

*Decided by:* `batch` requests and bytes, `read_full`/`read_old` GETs and bytes, and the `state`
records' `stored_bytes - reclaimable_bytes` against `live_bytes`, on the local recency and
uniform runs (requests and bytes are what S3 would see, `S3StoreTest`).
