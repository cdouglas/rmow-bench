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

## Added 2026-10-21, before the compaction sweep

Until now compaction was an input: every 10 batches, at thresholds modeled on Iceberg's
`RewriteDataFiles` defaults. The sweep makes it an outcome. Each policy is a check period `P`
(`compactEvery`: 1, 5 or 20 batches, and 10 at the default level) and a threshold level, from
aggressive to lazy:
garbage ratio 0.25, 0.5 or 0.75 (RMoW), delete ratio 0.1, 0.3 or 0.5 (MoR) and 4, 8 or 16 small
row groups (RMoW). Each arm is measured under every policy and under none, and the analysis picks
each arm's cheapest policy for a read rate and a price model. RMoW compaction now has one trigger
and one action per goal: space (rewrite an appendable file; move the live row groups out of a
split file's mostly dead objects) and reads (pack small sealed row groups). It packs only up to
the size of the writer's own row groups (docs/DESIGN.md#compaction).

### H6 — when to compact

- **RMoW reads barely depend on compaction.** At every policy, RMoW's full-read GETs (mean over
  probes) are within 2 of its uncompacted run, in either layout. The read gain reported for
  compacted appendable files (split-vs-append-2026-10-21.md: 10–11 GETs against 17–19) came
  from packing row groups to about twice the writer's size.
- **Appendable RMoW needs compaction for space.** Uncompacted, kept/live grows linearly (about
  8× at batch 60). At `P` = 1 the mean kept/live stays under 1/(1 − g) for garbage ratio g
  (1.33, 2, 4), and compaction writes fall as g rises.
- **Split RMoW needs little.** At g = 0.25 moving row groups out of mostly dead objects brings
  the mean kept/live to 1.3 or less, for under 0.5 MB of compaction writes per batch; at
  g = 0.75 it moves nothing.
- **MoR-v3 and TailCoW trade writes for reads.** Their full-read GETs grow between compactions.
  As `P` falls, their writes per batch (compaction included) rise and their mean full-read GETs
  fall; `P` = 1 writes the most.
- **Costs.** On S3 Express prices with a batch every 5 minutes and compute at $0.04 per
  vCPU-hour, at one full read per batch: split RMoW's cheapest policy costs within 5% of no
  compaction, and MoR-v3's cheapest policy has `P` ≤ 5.

*Decided by:* `analysis/compaction.py` over the `configs/local-compact-*.json` runs: per arm and
policy, bytes, requests and CPU per batch (batch and compaction phases), full-read GETs and bytes
averaged over probes every 3 batches (which sample every phase of each compaction cycle), and
kept/live averaged over steps.

## Added 2026-10-22, before rerunning the uncompacted runs

Until commit 150cc93, time travel planned old RMoW versions without split offsets, so the stock
reader's time-travel reads did not measure stock Iceberg (lab/pages/time-travel.md). The seven
uncompacted configurations that read old versions are rerun with the fix:
`local-m1-recency-exp-nocompact`, `local-m1-uniform-nocompact`,
`local-rg-sweep-recency-exp-rg{1,2,4,8}m` and `local-split-recency-exp-nocompact`.

### H4, revisited: what the rerun should show

- **Old versions read like current ones, under both readers.** Every time-travel read costs
  exactly what the full read of the same table state cost, in every request and byte column, in
  every arm. The one-footer reader's 0.11% fewer bytes for old RMoW versions goes too.
- **Nothing else changes.** Every other record matches the earlier run of its configuration
  exactly (`steps_diff`), except:
  - the RMoW arms' `read_old` records;
  - in the two m1 runs, the MoR and TailCoW arms, whose earlier builds predate counting Puffin's
    `create()` HEAD and run-relative paths (one HEAD per Puffin file, and a few bytes).
- **H3's numbers do not move.** They come from batch phases, full reads and state.

*Decided by:* the `h4_pairs` view and `steps_diff` against the earlier runs.

## Added 2026-10-23, before rerunning the compacted runs that read old versions

The four compacted configurations with time travel are rerun with the split-offset fix:
`local-m1-recency-exp-compact10`, `local-m1-uniform-compact10`,
`local-split-recency-exp-compact10` and `local-split-uniform-compact10`. No hypothesis rides on
them. In a run that compacts, a time-travel read and the full read of its state read different
files (history is recorded before compaction), so they make no paired test.

What the reruns should show against the earlier runs (`steps_diff`):
- **Up to the first compaction (step 10)**, every record matches, except, in the two m1 runs, the
  MoR and TailCoW arms' build differences (Puffin's HEAD, run-relative paths).
- **From step 10 on**, the RMoW arms differ, because RMoW compaction changed after these runs
  (0564114: one trigger per goal, packing up to the writer's row-group size, and split files
  moving row groups out of mostly dead objects). From step 20 on, so do their `read_old` records,
  by the fix.
- **MoR-v3, TailCoW and CoW** match throughout in the split runs: their compaction has not
  changed. In the m1 runs, CoW matches, and MoR and TailCoW differ only in their build
  differences.

*Decided by:* `steps_diff` against the earlier runs.

## Added 2026-10-23, before the -dv ablation

H3 attributed the difference between RMoW-C and TailCoW to the single appendable object. The
local runs showed that most of it comes instead from how RMoW applies updates: it rewrites every
row group an update touches, where TailCoW deletes with DVs (lab/pages/layout-vs-policy.md). The
`-dv` arms separate the two. They keep RMoW's layout and tail policies, but delete rows in a
file's sealed prefix with a DV and append their updated versions to the tail, as TailCoW does.
So:
- **RMoW-X-dv against TailCoW-X is the layout alone**: one appendable object with one footer,
  against data files.
- **RMoW-X against RMoW-X-dv is the update handling alone**: region rewrites against DVs.

**Runs.** `configs/local-dv-{recency-exp,uniform}-nocompact.json` are the 4 MiB uncompacted
settings of `local-rg-sweep-recency-exp-rg4m` and `local-m1-uniform-nocompact`. Arms: MoR-v3, CoW,
TailCoW-B and -C, RMoW-A, -B and -C, and RMoW-A-dv, -B-dv and -C-dv. With version dumps.

### What the ablation should show (recency-skewed updates unless noted)

- **D1: the layout alone costs few bytes.**
  - RMoW-C-dv writes at most 2× TailCoW-C's bytes per batch (RMoW-C writes 9.7×).
  - RMoW-A-dv, whose row groups are all sealed (no tail merges), writes at most 2× MoR-v3's.
- **D2: the update handling carries RMoW's write bytes.** RMoW-C minus RMoW-C-dv accounts for at
  least 80% of RMoW-C minus TailCoW-C in bytes written per batch. The same holds for B.
- **D3: the layout alone keeps most of the read savings.** RMoW-C-dv's full read at batch 60 takes
  at most half TailCoW-C's GETs: one footer and one DV per file instead of one per data file.
- **D4: under uniform updates, DVs avoid copy-on-write.** The RMoW-dv arms write at most 3× MoR-v3's
  bytes per batch, against RMoW's 82× (CoW's).
- **D5: the shared arms match.** Every arm the ablation shares with `local-rg-sweep-recency-exp-rg4m`
  or `local-m1-uniform-nocompact` matches that run record for record (`steps_diff`). Time travel
  costs what the current read of the same state cost in every `-dv` arm, as in every other arm
  (`h4_pairs`).

*Decided by:* `h3_arms` and `h3_diffs`, with the differences extended to the `-dv` arms, and
`steps_diff`.

## Added 2026-10-23, before rerunning the recency -dv ablation with a delete cache

The ablation's reader models load a file's deletion vector once per split (row group), as
Iceberg's readers do: `BaseDeleteLoader` never caches a DV. That confounded D3. A third reader
model, `cached`, loads each data file's deletes once per read, as an engine with a delete cache
would. `configs/local-dv-recency-exp-nocompact.json` is rerun with readers `one-footer`, `cached`
and `stock`.

- **D3, revisited.** Under `cached`, RMoW-C-dv's full read at batch 60 takes at most half
  TailCoW-C's GETs.
- **The cache changes only what it should.** Under `cached`:
  - arms without deletes (RMoW, CoW) read exactly as under `one-footer`;
  - arms with DVs read the same rows with no more GETs.
- **Nothing else changes.** Every `one-footer` and `stock` record, and every write, matches the
  earlier run of the configuration (`steps_diff`).

## Added 2026-10-23, before validating the store simulation

Store latency is simulated two ways, with one model (`org.rmow.bench.io.StoreLatency`): injected
into a run, where each request sleeps its expected latency, and replayed from a run's request
log (`figures.replay`), which sums each phase's requests and schedules its split tasks on its
reader's threads. `configs/sim-validate-s3x.json` runs the S3 Express profile injected on the
in-memory store, logging requests. Its replay must predict its own wall times:

- **S1: one thread.** For every outermost phase on one thread that took at least 20 ms, the
  replay is within 10% of the measured wall time.
- **S2: eight threads.** For full reads under the 8-thread `cached` reader, the replay is within
  20% of the measured wall time.

*Decided by:* `figures.replay` on the run's own log, against `steps.wall_ns`.

## Added 2026-10-23, before rerunning the logged 4 MiB recency setting without payload checks

Two changes since `sim-recency-4m`:
- **Payload checks off.** The payload check of every decoded row (`verifyRows`) is the harness's,
  not an engine's. In a small A/B in scratch (400k rows, 10 batches, `cached` reader), full reads
  took 0.29–0.32× the CPU without it, and batches the same CPU. So the simulated reads of
  lab/pages/store-latency.md are mostly the harness's CPU. The id-and-version oracle stays.
- **Split files are planned per row group.** Each object of a footer-only version is planned on
  its own split offsets, as an appendable file's version is, so a split read runs one task per
  live row group instead of one per object (`RequestLogTest`).

`configs/sim-recency-4m-noverify.json` is `sim-recency-4m` with `verifyRows` false, run with
`--log-requests`. Profiles and batch 60 are as in lab/pages/store-latency.md.

- **N1: requests and bytes are unchanged, except stock reads of split files.** Every record matches
  `sim-recency-4m` in requests and bytes (`steps_diff`), except the split arms' reads under
  `stock`. Those take more GETs, a footer read per split rather than per object. Under
  `one-footer`, `cached` and `cached-p8`, the split arms' requests and bytes do not change.
- **N2: reads lose most of their CPU, batches none.** Each arm's CPU over its one-thread reads
  (full, recent, old) is at most 0.4× the earlier run's. Batch and compaction CPU are within 5%.
- **N3: on one thread, RMoW's read advantage grows.** With `cached`, RMoW-C's simulated full read
  takes at most 0.75× MoR-v3's on S3 Express (it was 0.78×), and at most 0.4× on Azure Blob
  (0.45×).
- **N4: on eight threads, parallelism still erases most of it where requests are cheap.** With
  `cached-p8`, RMoW-C's replayed full read is within 20% of MoR-v3's on S3 Express (it was 1.04×),
  and at most 0.7× on Azure Blob (0.59×).
- **N5: split files read in parallel.** With `cached-p8` on S3 Express, RMoW-C-split's replayed full
  read takes at most 1.5× RMoW-C's (it was 1.9 s against 563 ms).

*Decided by:* `steps_diff` against `sim-recency-4m`, `steps.cpu_ns`, `phase_latency` (one thread)
and `figures.replay` (eight threads).

## Added 2026-10-23, before the scale campaign on havoc

**Question.** Do the simulated latencies hold at Iceberg's defaults (128 MiB row groups, 512 MiB
files)? Latency is part per request and part per byte (and CPU). Scaling the files changes the
balance between the two.

**Setup.** `configs/scale-recency-rg{4,16,64,128}m.json` scale the logged 4 MiB recency setting
with its row group R, so that each batch has the same structure:
- 128k × R initial rows: one file of about four row groups;
- 1,280 × R operations per batch (1% of the initial rows);
- R-MiB row groups and tail seal, and 4R-MiB files.

Everything else is `sim-recency-4m-noverify`'s:
- the six arms;
- the readers `cached`, `cached-p8` and `stock`;
- 60 batches, no compaction, payload checks off, requests logged.

At 128 MiB, a table of one 512 MiB file and its deltas is a few GiB stored in all. It runs in
the container on havoc (`docker/campaigns/scale.json`), each configuration on 10 whole cores of
one NUMA node. CPU there is not comparable with this laptop's, so each prediction compares the
scale points with each other. Batch 60 and the profiles are as in lab/pages/store-latency.md.

- **P1: the structure does not depend on scale.** For every arm and reader, requests per batch
  and per full read at batch 60 at 16, 64 and 128 MiB are within 25% of the 4 MiB point's.
- **P2: bytes scale with the row group.** Bytes written per batch, and bytes read per full read
  at batch 60, are within 25% of R/4 times the 4 MiB point's.
- **P3: CPU scales with bytes.** At 128 MiB, CPU per MiB read (full reads, `cached`) and per MiB
  written (batches) are within 30% of the 4 MiB point's.
- **P4: on one thread, requests stop mattering.**
  - At 128 MiB, RMoW-C's simulated full read with `cached` is within 10% of MoR-v3's on S3
    Express, and at most 0.9× on Azure Blob.
  - RMoW-C's batches are slower than MoR-v3's on every profile: its extra bytes cost more than
    the requests it saves.
- **P5: on eight threads, the largest row group bounds a read.** At 128 MiB, every arm's replayed
  full read with `cached-p8` is within 25% of MoR-v3's on S3 Express.
- **P6: it fits.** Summed over arms, each arm's largest `stored_bytes` at 128 MiB is under 24
  GiB.

*Decided by:* `steps`, `phase_latency` and `figures.replay` over the four runs, with each job's
foreign load from `results/campaigns/scale/` (a job with foreign load decides nothing about CPU).

## Added 2026-10-23, after a three-batch pilot of the 128 MiB setting: P6 revised

A pilot of `scale-recency-rg128m` on this laptop (3 batches, one probe, results in scratch) sized
the campaign:
- each RMoW arm wrote 110 MiB per batch, re-merging the initial load's last, open row group every
  batch (MoR-v3: 5 MiB), at about 11 s of CPU per batch (MoR-v3: 1.3 s);
- a one-thread full read of the 490 MiB table took 12–14 s of CPU;
- the heap peaked at 13.8 GB (`-Xmx24g`, 17.6 GB resident), and the run took 14 minutes.

Over 60 batches that comes to about 7 GiB stored per RMoW arm, so P6 as written (under 24 GiB
summed over arms) will not hold. P6 stands as registered and will be reported against the runs.
- **P6, revised:** at 128 MiB, each RMoW arm's largest `stored_bytes` is under 8 GiB, and the sum
  over arms under 40 GiB.

The campaign's store estimates follow the pilot (`docker/campaigns/scale.json`).

## Added 2026-10-24, before rerunning the scale campaign on clean phases

**Why.** The first scale campaign decided P1, P2 and P6, but not P3–P5: other users' threads ran
on the cores of three of its four jobs (lab/pages/scale.md). Since then two things have changed:
- **Monitoring:** the runner samples each job's foreign load every second, and every phase records
  when it started, so the lab can tell which phases foreign load touched (`phase_load`).
- **Writer footers:** every arm's writer holds a footer for one batch or compaction only (README,
  I/O parity rule 6). Before, RMoW's writer kept its footers across batches, so RMoW's batch
  requests were undercounted.

**Setup.** The same four configurations, `configs/scale-recency-rg{4,16,64,128}m.json` (their
only change is the knob's new name), on the new build, as `docker/campaigns/scale-rerun.json` on
havoc. P3–P5 as registered on 2026-10-23 are decided on this rerun. P1, P2 and P6 stand as the
first campaign decided them; the rerun reports its own values beside them.

**Which CPU counts.** A phase's CPU counts only if the phase is clean: no load sample
overlapping it reached 0.25 CPUs of foreign load (`phase_load.clean`).
- **P3** sums CPU and MiB over clean phases only: full reads under `cached` and batches, at 4
  and at 128 MiB.
- **P4** compares full reads at batch 60 under `cached`, and decides each comparison only if
  both arms' reads are clean. Its batch part uses the mean over clean batches.
- **P5** does the same for `cached-p8` (its split tasks run inside the phase).
- **Undecided cases.** A prediction is not decided by this rerun if a batch-60 read it needs is
  not clean, or if fewer than half of a run's batches are. Its run is then repeated.

**Controls**, which check the code change and the rule, not the format:
- **C1: the footer change touches only writers.**
  - Every probe's requests and bytes, under every reader, equal the first campaign's exactly.
  - So do MoR-v3's batches: it opens each file once per batch.
  - TailCoW-C's batches take at most the first campaign's requests: its merge read can reuse a
    footer its locate scan read.
- **C2: RMoW's writer now reads its footers every batch.**
  - RMoW-B's and RMoW-C's batches take 3–4 more requests than in the first campaign: two live
    files, each footer read as parquet-java reads it (two requests), or fetched whole while the
    file is at most 1 MiB (one).
  - Their split variants take 2 more: two footer-only objects, each fetched whole.
  - Bytes written do not change.
- **C3: clean means clean.** In clean probes, each split arm's full-read CPU under `cached` is
  within 3% of its appendable twin's. The two read the same bytes, and in the first campaign's
  4 MiB job, before its foreign load, they agreed within 1%. If they do not agree, the noise is
  not foreign load, and P3–P5 stay undecided.
