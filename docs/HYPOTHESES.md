# Hypotheses

What the benchmark expects each run to show, written before the run. A result that contradicts a
hypothesis is reported as contradicted. A hypothesis is never edited once registered; a revision
is a new dated entry.

The hypotheses registered from 2026-10-20 to 2026-10-27 (H1–H6, and each campaign's predictions
and controls) are this file as of commit 1145f89 ([on GitHub](https://github.com/cdouglas/rmow-bench/blob/1145f89a36fa60af56aefc9233991f1d03755ae2/docs/HYPOTHESES.md); `git show
1145f89:docs/HYPOTHESES.md`), and the write-ups up to 2026-10-27 cite them there. The commits'
times show each came before the runs it was decided on. What they settled is summarized below.

## The question

Treating a Parquet region (a row group) as the unit of a write adds write latency to cut read
latency. The benchmark measures that trade-off, writes against reads, in latency, requests and
bytes, for each way of encoding an update, on each store and workload.

**The encodings** (the arms new campaigns run; README, Arms):

| Arm | A changed region goes | Its old copy is hidden by |
|---|---|---|
| `mor-v3` | nowhere: updated rows go to the batch's new data file | DVs on rows |
| `tailcow-c` | the same, with small tail files merged by a tiered policy | DVs on rows |
| `rmor-c` | rewritten into the batch's new data file (regional merge-on-read) | a DV covering the old row group |
| `rmor-c-footer` | the same | a new footer appended to its file, without the old row group |
| `rmow-c` | appended to its own object | the object's new footer |

RMoR-C with footers sits between: against RMoR-C it changes only what hides the old copy, and
against RMoW-C only where the rewritten region goes. It needs appendable files, as RMoW-C does.

`rmow-c-split` (a region written as a new object, the old hidden by a footer-only object naming
the objects its row groups live in) was dropped on 2026-10-27: its footers point across objects
through `ColumnChunk.file_path`, which the Parquet spec disowns and no reader assembles. RMoR-C
writes a rewritten region to a new file in a form every reader reads.

**The workloads:** where updates fall (the ingest window, or a Zipf skew over regions), how many
of a region's rows one update statement changes (the update share), how late inserts arrive, and
the row-group size (README, The workload).

**The measures,** per phase (a batch; a full, recent and old read):
- requests (the objects a write creates track its PUTs), bytes read and bytes written, which are
  exact and the same on every store;
- wall time, measured in the cloud and simulated locally from requests, bytes and CPU;
- where the time went: each phase split into its stages (encoding, decoding, footers, deletes,
  other CPU, waiting on the store; `phase_breakdown`).

CPU matters as a bottleneck, not for its own sake.

## Settled before 2026-10-27

From local runs, where requests and bytes are exact (HYPOTHESES.md at 1145f89, EXPERIMENTS.md):
- **The ingest window** (1M rows, 4 MiB row groups): RMoW-C takes 0.21× MoR-v3's requests per
  batch and writes 13× its bytes. Its full read stays flat (16 GETs at batch 10, 19 at batch 60)
  while MoR-v3's grows (32 to 132). Simulated, its batch takes 2.1× MoR-v3's time on S3 Express
  and 0.50× on Azure Blob; its full read 0.69× and 0.33×.
- **Uniform updates** make the region arms copy-on-write: 0.996–0.997× CoW's bytes (H2).
- **Old versions** read like current ones, under every reader, and dead bytes cost reads nothing
  (H4). On S3, an old version's footer probe streams past the version's end until the reader
  closes: a cost of appending, qualitative, not measured.
- **Space:** uncompacted, an appendable file keeps 7.8–8.3× its live bytes by batch 60, so it
  needs compaction (H6).

## Before the calibration runs (registered 2026-10-27)

**What they decide:** whether the simulation's prices hold on the real stores, and the direction
of the trade-off on each. S3 Express and Azure Blob sit on either side of RMoW's break-even
request latency.

**Setup** (docs/S3X_RUNBOOK.md, docs/AZURE_RUNBOOK.md): `configs/s3x-calibration.json` and
`configs/azure-calibration.json`, the five arms (both RMoR-C arms at θ = 0, with the writer that
skips dead row groups). The workload is the ingest window: 200,000 rows, 20 batches of 2,000
operations, 1 MiB row groups, no compaction. One run each, on a machine in the store's zone or
region, from the build the twins ran: build snapshot `20261027T175613Z-1145f89a36fa-dirty`, which
the commit "RMoR-C with footers, a variant that hides old row groups without DVs" holds. The twins
are `s3x-calibration-local-8433ccfd4620/20261027T175847Z` and
`azure-calibration-azurite-a96fd56de2f0/20261027T175614Z` (campaign `twins`).

**Requests and bytes, from the twins** (exact, and the same on both stores; means over batches
1–20 and the full reads after the initial load, under `cached`, RMoR-C's also under
`cached-skip`):

| Arm | A batch: requests | of them PUTs and appends | MB read | MB written | A full read: requests | MB read |
|---|---|---|---|---|---|---|
| MoR-v3 | 31.4 | 2.0 | 0.64 | 0.08 | 46.0 | 7.2 |
| TailCoW-C | 15.1 | 2.0 | 0.62 | 0.13 | 28.0 | 7.0 |
| RMoR-C | 13.7 | 1.4 | 1.56 | 1.02 | 33.0 (26.0) | 9.0 (7.0) |
| RMoR-C with footers | 10.2 | 1.4 | 1.54 | 1.02 | 24.0 | 7.0 |
| RMoW-C | 7.3 | 1.0 | 0.80 | 0.84 | 24.5 | 6.9 |

Under the stock reader a full read takes 104 requests for MoR-v3, 87 for TailCoW-C, 108 for
RMoR-C, 63 for RMoR-C with footers and 70.5 for RMoW-C.

**Measured, not judged:** each store's request latencies by operation and size, its read rate per
MiB, the extra latency per appended part, and what the fork's conditional writes cost against the
store's own (the spikes).

**Predicted, from the twins** (each arm's mean batch and mean full read under `cached`, over
MoR-v3's):

| Arm | S3 Express: a batch | a full read | Azure Blob: a batch | a full read |
|---|---|---|---|---|
| TailCoW-C | 0.69 | 0.89 | 0.50 | 0.66 |
| RMoR-C | 1.54 | 1.03 (`cached-skip` 0.79) | 0.55 | 0.78 (0.62) |
| RMoR-C with footers | 1.43 | 0.76 | 0.54 | 0.58 |
| RMoW-C | 1.37 | 0.80 | 0.36 | 0.59 |

Across three twin runs with identical requests and bytes (EXPERIMENTS.md, Retired runs), the S3
Express ratios moved by up to 0.12 and Azure's by at most 0.02: instructions repeat within 0.4%,
but their pace (cycles per instruction, per run) moved by up to 12%. Every ratio C3 judges here
stayed at least 16% from 1 in all three; TailCoW-C's S3 Express full read ranged from 0.82 to
0.89, and is not judged.

- **C1: the model counts what each store receives** (a control). Every phase's requests and
  bytes equal its twin's. On Azure, the SDK's counts equal its Azurite twin's, but for creates,
  which cost a rename on Azure and a HEAD on Azurite. S3 Express's `CreateSession` is outside the
  model.
- **C2: a phase's time is its CPU plus its requests' latencies.** Priced with the store's
  measured latencies, each one-thread phase's simulated time is within 25% of its wall time.
- **C3: the trade-off falls where the twins put it.** Each ratio in the table lands on the same
  side of 1 in the cloud run's wall times, wherever the prediction is at least 10% from 1. So on
  S3 Express the region arms' batches are slower than MoR-v3's and their full reads faster
  (RMoR-C's only under `cached-skip`); on Azure, both are faster.
- **C4: CPU is the bottleneck only of the region arms' batches on S3 Express.** In
  `phase_breakdown`, CPU is more than half of the region arms' batch wall time on S3 Express
  (RMoR-C's, RMoR-C's with footers and RMoW-C's: 66–74% simulated), and less than a third of every
  phase's on Azure (at most 24%).

## The sweeps (registered 2026-10-29)

**What they decide:** where the region arms' extra write cost is repaid, against MoR-v3, along
the update share, the skew and the row-group size, while a batch changes a minority of the
table's regions. Where most regions change, the region arms are copy-on-write, which is known and
not measured (ROADMAP.md, The base point).

**Setup:** `configs/sweep-*.json`, the campaign `docker/campaigns/sweep.json` on havoc, one run
each from one build snapshot. The five arms (both RMoR-C arms at θ = 0, with the writer that
skips dead row groups), the calibration's five readers, no compaction, payload checks off,
requests logged; probes every 5 batches (a full read, the newest 1% of event time, and the table
10 batches back). The base point (`sweep-base`):
- row groups and tail seal of 16 MiB, the size Durner et al. find cost-optimal for a fetch from
  object storage ([PVLDB 16, 2023](https://vldb.org/pvldb/vol16/p2769-durner.pdf)), and files of
  128 MiB (eight row groups, the calibration's ratio);
- regions of a row group's rows: 513,700, measured on this schema (15.2 MiB on disk; 32 MiB row
  groups hold 1,053,700 rows, 64 MiB ones 2,133,600);
- 50 regions in the initial load (25,685,000 rows), 71 by the end, and 30 batches of 500,000
  operations (70% inserts, 25% updates, 5% deletes);
- a region skew of 1, and statements changing p = 0.03 of a region's live rows (about 15,400),
  the batch's deletes among them.

**The settings,** and where the generator puts their changes (seed 1, batches 16–30):

| Runs | Vary from the base | Regions a batch changes | Distinct regions changed in batches 16–30 |
|---|---|---|---|
| `sweep-base` | nothing | 8.4 of 71 | 42 |
| `sweep-p10`, `sweep-p30` | p = 0.1, 0.3 | 3.6, 1.4 | 34, 15 |
| `sweep-s0`, `sweep-s2` | a skew of 0, 2 | 9.7, 5.8 | 57, 16 |
| `sweep-ingest` | the ingest window | 4.4 | 15 |
| `sweep-ingest-rows` | the ingest window, a row a change (the calibration's workload) | 16.8 | 26 |
| `sweep-rg32m`, `sweep-rg64m` | row groups of 32, 64 MiB; the same log | 10.3 of 35, 7.5 of 17 row groups | |
| `sweep-late99-1`, `sweep-late99-20` | insert lateness with a p99 of 1, 20 batches; the same rows change | 8.4 | 42 |

**Prices:** requests and bytes are exact. Simulated time uses the named `s3x` and `azure`
profiles, as the calibration's predictions did. Havoc has no instruction counters, so CPU is
thread time there: a time ratio within 10% of what a prediction needs decides nothing.

**Predicted** (means over batches 1–30, and over the full reads after the initial load, under
`cached`; RMoR-C's also under `cached-skip`):
- **U1: a region rewrite pays more as p rises** (the user's prediction). From p = 0.03 to 0.1 to
  0.3, each region arm's (RMoW-C, RMoR-C, RMoR-C with footers) bytes written per batch fall, as
  the regions a batch changes fall from 8.4 to 1.4, and its batch time over MoR-v3's is lower at
  0.3 than at 0.03 on both profiles.
- **U2: RMoW more than RMoR** (the user's prediction). At every p, RMoW-C's batch takes fewer
  requests and less time than RMoR-C's and RMoR-C's with footers, on both profiles, and its batch
  time over RMoR-C's is no higher at 0.3 than at 0.03.
- **U3: no region arm writes fewer bytes than MoR-v3, at any p.** A rewrite carries a whole
  region (513,700 rows), where MoR-v3 writes only the changed rows, so a crossover, where there is
  one, is in requests and time.
- **K1: skew moves the region arms' writes, not MoR-v3's, nor the full reads.** At p = 0.03, each
  region arm's bytes written per batch at a skew of 2 are at most 0.8 of those at 0 (statements
  pile onto hot regions: 5.8 regions a batch against 9.7), while MoR-v3's differ by under 10%.
  MoR-v3's and RMoW-C's full reads at the last probe (requests and bytes) differ by under 10%
  across skews 0, 1 and 2. A model of MoR-v3's files on the generator's log puts a DV on 35 of its
  36 files by the last batch at every skew, with the same dead rows; RMoW-C reads only live row
  groups. The expectation that the region arms' read advantage grows as the skew falls needs a
  table of many more files than a run's changes reach, and is not tested here.
- **G1: bigger row groups cost the region arms' writes.** On the same log, each region arm's
  bytes written per batch grow from 16 to 32 to 64 MiB row groups (a rewrite carries a whole row
  group: about 128, 321 and 474 MiB a batch, from the generator and the measured row-group
  sizes), and its batch time over MoR-v3's rises on both profiles, while MoR-v3's bytes written
  differ by under 10%.

**Measured, not judged:**
- where each region arm's batch time crosses MoR-v3's along p on each profile, or that none lies
  between 0.03 and 0.3, and where RMoR-C's crosses TailCoW-C's: the θ the hybrid stage starts
  from (ROADMAP.md, Experiments);
- the arrival window: only event time moves, so it measures the recent-window read;
- the ingest window's two runs, which tie the sweeps to the calibration's workload at this size;
- RMoR-C's full reads across skews, and every arm's reads along p and the row-group size;
- space: each arm's dead bytes at the last batch, uncompacted.

**Limits:** one seed and one run each. The cloud calibration found the `s3x` profile pessimistic
about the region arms' batches (C3 was contradicted on S3 Express, where they beat MoR-v3), so a
crossover on `s3x` likely lies at a higher p than on S3 Express itself. The schema is narrow (11
columns); other schemas are extrapolated by bytes.

## The sweeps: memory and the sorted read (registered 2026-10-29)

**What they decide:** whether what an approach must hold in memory sets it apart, on the write
side and on a read in event-time order (the user's question: MoR may need many row groups open at
once where RMoW streams one). The section above stands; these are added to the same runs.

**What changed in the runs since that section:** the harness's writers stream (requests, bytes,
rows and table state unchanged in 1,939 records of three configurations; instructions within 3%),
every phase records what its readers and writers held (README, What is held), and the cached
reader also reads the table sorted at each probe (`read_sorted`). The sweep configurations set
`initialJitterRows` to 0, so the initial load is in event-time order, which the sorted read needs;
the rows that change are the same, so U1–U3, K1 and G1 stand as registered. The campaign logs
every change of what is held (`--log-held`).

**Measured:** peak rows held (`held_rows`) and peak encoded bytes held (`held_bytes`, and by kind:
column chunks in flight, writer buffers, data waiting for its request, deletes) per phase, exact
and the same on every run. A pilot (16 MiB row groups, 4 regions, 10 batches; not one of the
sweep's runs) showed the shapes below; its table is too small to predict the sweep's sizes from.

**Predicted** (the last probe and the mean batch, at the base point unless named):
- **M1: a sorted read holds MoR-v3's batch files at once.** Every batch file's event times span
  the table (updated rows from anywhere, and inserts), so the merge opens a row group of each and
  holds it to the end: MoR-v3's `read_sorted` holds at least 300 MB at the last probe (30 batch
  files of about 15 MiB), at least 15 times its own `read_full` (one row group). TailCoW-C seals
  each batch file at once (each is about the tail seal), so it holds as much.
- **M2: RMoW-C's sorted read holds less than MoR-v3's, but grows too.** Its rewritten regions keep
  their event times in place, but each batch's tail row group takes a few very late inserts (the
  lognormal's far tail), so its event times reach back across the table and the merge holds it
  open: RMoW-C's `read_sorted` peak is below MoR-v3's at the last probe, and above twice its own
  at the first probe after the initial load. (In the pilot: 0.73 of MoR-v3's at batch 10.)
- **M3: RMoW-C's batch holds more bytes than MoR-v3's.** One append per file per batch keeps every
  region it rewrites until the append, about 8 regions of 15.2 MiB, where MoR-v3's writer holds one
  row group: RMoW-C's mean batch `held_bytes` is at least 4 times MoR-v3's.
- **M4: RMoR-C sorts its whole batch file.** Its writer keeps every row of its batch file (the
  regions it rewrites and the inserts, about 4.7M rows) to sort, where RMoW-C keeps one region at a
  time: RMoR-C's mean batch `held_rows` is at least 5 times RMoW-C's.

**Measured, not judged:** the sorted read of the other arms and settings (in the pilot, RMoR-C's
held one row group, since at 4 regions it rewrote nearly the whole table each batch); how the
arrival window moves RMoW-C's tail row groups' reach; RMoR-C with footers' data file held for its
single create (175 MB in the pilot); deletes held by readers.

**Limits:** the levels are what this harness's writers and readers hold, which model an engine's
(a sort in memory without spilling, one append per file, a merge that opens a row group per run);
another engine could stream differently, so the measure compares the designs as written, not
every implementation of them. A JFR run is still to check that the heap follows the counted levels.

## The base point in the cloud (registered 2026-10-30)

**What they decide:** whether the latency model, priced by request size, holds at the sweeps'
scale on the real stores, where a read is mostly 16 MiB ranges and a region arm's batch rewrites
whole regions of 15.2 MiB; and where the trade-off falls at the base point on each store,
measured. At the calibration's scale (1 MiB row groups) the region arms' batches were predicted
faster than MoR-v3's on Azure; here every region arm's batch is predicted slower on both stores.

**Setup:** `configs/sweep-base.json` (the sweeps' base point, 2026-10-29), run by the deploy's
`bench` step as `sweep-base-s3x` on S3 Express (usw2-az3, from a `c7a.4xlarge`) and then
`sweep-base-azure` on Azure Blob (ADLS Gen2, Standard, westus, from a `D8as_v6`): one run each,
a 16 GB heap, requests and held memory logged (`--log-requests --log-held`). The build is
f51796a, the laptop twin's (its build snapshot `20261030T054758Z-f51796a7d09b-dirty` is dirty in
`bin/matrix.sh` only); its `src/main` and `pom.xml` are havoc's sweeps' (a5e2ee2 on, unchanged
since but for a Javadoc). Each store's spike, from main, runs on the same VM just before. The VMs
allow instruction counts (`perf_event_paranoid` 2, from 2026-10-30), which the calibration's
lacked; each run's manifest says whether the hypervisor gave them. The twins: the laptop's
`sweep-base-f970149711a9/20261030T054758Z`, with instruction counts, and havoc's
`sweep-base-f970149711a9/20261030T055938Z`, whose CPU decides nothing (every phase under foreign
load).

**The model** (`lab/views/65_sized.sql`): a one-thread phase takes its CPU plus each logged
request's expected latency at its size, from a spike: the lognormal mean at each size measured
(64 KiB to 128 MiB), linear between them, flat below, the last slope beyond, a smaller size never
priced above a larger one; an appendable create costs an append of its size plus the create's
overhead; an append past Azure's 100 MiB cap is several. Its rules were drawn from the
calibration's runs, where it put each arm's batches within 25% in 83–100% of phases on S3 Express
and 72–92% on Azure, and reads apart from `stock` in 80% and 89% (ROADMAP.md, A profile priced by
request size). This is its first test on runs it was not drawn from, and its first at sizes past
1 MiB.

**Requests and bytes, from the twin** (exact, and the same on both stores; means over batches 1–30
and the full reads after the initial load, steps 5–30, under `cached`, RMoR-C's also under
`cached-skip`):

| Arm | A batch: requests | of them PUTs and appends | MB read | MB written | A full read: requests | MB read |
|---|---|---|---|---|---|---|
| MoR-v3 | 128.4 | 2.0 | 38.3 | 17.4 | 116.0 | 1,050 |
| TailCoW-C | 128.4 | 2.0 | 38.3 | 17.4 | 116.0 | 1,050 |
| RMoR-C | 118.2 | 2.0 | 459 | 438 | 285.7 (87.2) | 4,096 (973) |
| RMoR-C with footers | 86.2 | 4.6 | 456 | 438 | 76.3 | 973 |
| RMoW-C | 36.7 | 6.2 | 224 | 236 | 77.2 | 968 |

MoR-v3's batch file is about 15 MiB, at the tail seal, so TailCoW-C seals each at once and sends
what MoR-v3 sends (as the sweeps' M1 expects). RMoR-C's batch file is one PUT of 418 MiB on
average (up to 735 MiB); RMoR-C with footers creates the same as an appendable file (3.8 Append
Blocks beyond the first on Azure, `append_splits`), then appends its footers. RMoW-C's appends
average 37 MiB (up to 109 MiB), and it creates 3 files in 30 batches.

**Predicted, from the twin** (each arm's mean batch and mean full read under `cached`, over
MoR-v3's; the twin's work at its arm's pace, `phase_work`, plus its requests priced with the
2026-10-28 spikes, `spike/s3x-spike-20261028T221407Z` and `spike/azure-spike-20261028T233151Z`):

| Arm | S3 Express: a batch | a full read | Azure Blob: a batch | a full read |
|---|---|---|---|---|
| TailCoW-C | 1.02 | 1.00 | 1.01 | 1.00 |
| RMoR-C | 6.59 | 3.85 (`cached-skip` 0.96) | 4.94 | 3.78 (0.95) |
| RMoR-C with footers | 6.23 | 0.87 | 4.75 | 0.87 |
| RMoW-C | 2.98 | 0.85 | 2.21 | 0.87 |

MoR-v3's simulated batch takes 4.5 s on S3 Express and 6.6 s on Azure, its full read 20.6 s and
22.9 s. The requests' share of each simulated phase (C8):

| Arm | S3 Express: a batch | a full read | Azure Blob: a batch | a full read |
|---|---|---|---|---|
| MoR-v3 | 0.19 | 0.35 | 0.44 | 0.42 |
| TailCoW-C | 0.18 | 0.35 | 0.44 | 0.42 |
| RMoR-C | 0.23 | 0.35 (`cached-skip` 0.34) | 0.30 | 0.40 (0.40) |
| RMoR-C with footers | 0.25 | 0.37 | 0.33 | 0.43 |
| RMoW-C | 0.27 | 0.37 | 0.33 | 0.45 |

- **C5: the cloud runs send what the twins count** (a control). Every record's requests and
  bytes, and every one-thread phase's held peaks (`held_*`), equal the laptop twin's, as havoc's
  run's do (all 1,325 records). A parallel read's held peak depends on how its tasks interleave:
  havoc's `cached-p8` reads differ from the laptop's by under 0.6% in 38 records. S3
  Express's `CreateSession` is outside the model; on Azure, an appendable create costs a staged
  Append Blob, an Append Block and a rename, and an append past 100 MiB several Append Blocks.
- **C6: a phase's time is its CPU plus its requests' prices, by size.** Priced with its own VM's
  spike, each arm's mean batch and mean full read under `cached` (RMoR-C's also under
  `cached-skip`), simulated as its thread CPU on the wall clock plus its requests' prices, is
  within 25% of its mean wall time, on both stores.
- **C7: the trade-off falls where the twin puts it.** Each ratio in the first table lands on the
  same side of 1 in the cloud run's mean wall times, wherever the prediction is at least 10% from
  1. So on both stores the region arms' batches are slower than MoR-v3's, the full reads of RMoW-C
  and RMoR-C with footers faster, and RMoR-C's full read under `cached` slower. TailCoW-C's ratios
  and RMoR-C's under `cached-skip` are not judged.
- **C8: at this scale a phase is mostly CPU, on both stores.** In `phase_breakdown`, the store
  (store client CPU and waiting on the store) takes less than half of the wall time of every
  arm's mean batch and mean full read under `cached` (RMoR-C's also under `cached-skip`), on both
  stores: 18–27% of the batches and 34–37% of the full reads simulated on S3 Express, 30–44% and
  40–45% on Azure. The calibration's C4, at 1 MiB row groups, predicted CPU under a third of every
  phase on Azure.

**Measured, not judged:**
- each request's time against its price, by op and size (the cloud run's request log);
- the share of one-thread phases within 25%, per arm and phase;
- the reads under `cached-p8` (parallel, not modelled), `one-footer` and `stock` (whose streamed
  GETs of 64 KiB or less the model prices at half their time), and the sorted read;
- the new VM's spike against the 2026-10-28 spikes.

**Limits:** one run per store to begin with. The model's rules were drawn from the calibration's
runs; its prices past 128 MiB extrapolate the spike's last slope. A PUT past 48 MiB is a multipart
upload on S3 (Iceberg's `S3OutputStream`), whose parts go out while the file is still being
written, where the model prices one request of its size after the CPU; RMoR-C's batch file is
3.5 s of its 29.8 s simulated on S3 Express. A phase's thread CPU includes the client's (TLS, checksums),
which the spike's latencies include too, so it counts twice. CPU on the cloud VMs runs at its own
pace, which moved the calibration's S3 Express ratios by up to 0.12 across runs; the full-read
ratios C7 judges sit 13–15% from 1.

## The base point on a vectorized reader (registered 2026-10-31)

**What it decides:** whether the trade-off the base point measured depends on the Parquet code the
harness reads and writes with. Until 2026-10-31 every arm read through Iceberg's generic row
reader, which builds a Java object per row; a profile of a shortened base point put about 85% of
reading's CPU in that reader's per-value and per-row work, and 8.5% in decompression. Every arm
now reads through Iceberg's vectorized reader (iceberg-arrow), as Spark reads an Iceberg table:
batches of 5,000 rows, deletes applied by position per batch, and the residual compiled to
comparisons on the batch's columns, as Spark compiles a filter (Iceberg's evaluator, which boxes
every value it reads, took about a fifth of a scan's time in a profile). The writer
is still Iceberg's generic writer over parquet-java, as Spark's and Flink's are, but takes strings
as UTF-8 bytes (Avro's `Utf8`), as Spark's rows hold them, where it encoded a Java `String` again
for every value. RMoW keeps the rows it reads back as (id, version) and has them made again for the
encoder, as every MoR-family arm already did (`MadeRows`).

**Setup:** `configs/sweep-base.json` on the laptop, which counts instructions, from a build
snapshot of the change, with requests and held memory logged, against the laptop's run on the old
reader (`sweep-base-f970149711a9/20261030T054758Z`) and the S3 Express run
(`sweep-base-s3x-89a55338cd9a/20261030T205713Z`). S3 Express times are simulated as the model the
S3 Express run confirmed (within 1–11% of every judged mean, C6): the run's work at its arm's pace
(`phase_work`) plus its requests priced with that VM's spike (`spike/s3x-spike-20261030T205211Z`).

**A pilot** (the shortened base point the profile ran: 3 arms, 5 batches, the `cached` reader, on
both builds): the same requests, bytes, table state and held peaks in all 95 records; a full read
took 0.22–0.23 of the instructions, a merge read 0.24, MoR-v3's scan to locate rows 0.40, a batch
0.46–0.76 (MoR-v3's 0.46, RMoW-C's 0.76), and encoding 0.72–0.78 of the CPU.

**Predicted:**
- **V1: the reader changes no request, byte or held peak** (a control). Every record's requests,
  bytes and table state, and every one-thread phase's held peaks, equal the old reader's run.
- **V2: reads cost a quarter of the work.** Each arm's mean full read under `cached` takes at most
  a third of its instructions on the old reader.
- **V3: the trade-off holds and the write side's share grows.** Simulated at S3 Express prices,
  RMoW-C's mean batch over MoR-v3's is higher than the 3.05 S3 Express measured on the old reader,
  and its mean full read over MoR-v3's is between 0.80 and 0.95. (From the old run's breakdown,
  with Parquet's work 2 to 10 times faster: 3.3–3.8 and 0.86–0.89. MoR-v3's batch is mostly
  decoding, so it gains more than RMoW-C's, whose rewritten regions' bytes stay.)

**Measured, not judged:** the batch and its parts (locate, merge-read, write) on the new reader;
the encode CPU the strings' change saves; the store's share of each phase, simulated; the other
readers (`one-footer`, `stock`, `cached-p8`) and the sorted read.

**Limits:** the laptop's core runs at its own pace, which the S3 Express run's cores matched within
a few percent on the old reader. Engines with native readers (Trino, DuckDB, Photon) would decode
faster still; V3's sensitivity is meant to say whether that matters.

## The vectorized base point in the cloud (registered 2026-10-31)

**What they decide:** whether S3 Express measures what the laptop simulated for the vectorized
reader (The base point on a vectorized reader, V3, which held): RMoW-C's batch 4.5 times MoR-v3's
where it was 3.0, and its full read still 0.87 of MoR-v3's. The model was confirmed on the old
reader (C6), where a full read was about a third the store's; on the new reader it is about 70%,
so the requests' prices carry most of a read's time, which C6 tested less.

**Setup:** `configs/sweep-base.json` on S3 Express (usw2-az3, from a `c7a.4xlarge`), as the first
cloud base point (The base point in the cloud): the deploy's `bench` step, one run, a 16 GB heap,
requests and held memory logged, the VM's own spike from main just before. The build is the
commit that brings in the vectorized reader, whose `src/main`, `pom.xml` and `bin/` are the laptop
run's build snapshot's (`20261031T083323Z-ca1985a3d2c9-dirty`). The twin is that laptop run,
`sweep-base-f970149711a9/20261031T083323Z`.

**Predicted, from the twin** (each arm's mean batch and mean full read under `cached`, over
MoR-v3's; the twin's work at its arm's pace plus its requests priced with the first S3 Express VM's
spike, `spike/s3x-spike-20261030T205211Z`; in brackets the requests' share of the simulated phase):

| Arm | A batch | A full read |
|---|---|---|
| MoR-v3 | 2.1 s (0.39) | 10.2 s (0.70) |
| TailCoW-C | 1.00 (0.40) | 1.00 (0.71) |
| RMoR-C | 9.43 (0.34) | 3.83 (0.70); `cached-skip` 1.05 (0.62) |
| RMoR-C with footers | 8.75 (0.38) | 0.87 (0.73) |
| RMoW-C | 4.48 (0.38) | 0.87 (0.73) |

- **V4: the cloud run sends what the twin counts** (a control). Every record's requests, bytes
  and table state, and every one-thread phase's held peaks, equal the twin's, as C5's did.
- **V5: the model holds on the new reader.** Priced with its own VM's spike, each arm's mean
  batch and mean full read under `cached` (RMoR-C's also under `cached-skip`), simulated as its
  thread CPU plus its requests' prices, is within 25% of its mean wall time, as in C6.
- **V6: the trade-off falls where the twin puts it.** Each ratio in the table lands on the same
  side of 1 in the run's mean wall times, wherever the prediction is at least 10% from 1 (as C7);
  and RMoW-C's batch over MoR-v3's is above the 3.05 the first cloud run measured on the old
  reader, its full read between 0.80 and 0.95.
- **V7: reads are mostly the store's, batches mostly CPU.** In `phase_breakdown`, the store
  (store client CPU and waiting on the store) takes more than half the wall time of every arm's
  mean full read under `cached` (RMoR-C's also under `cached-skip`), and less than half of every
  arm's mean batch. On the old reader the store took 32–36% of the reads (C8).

**Measured, not judged:** each request's time against its price; the new spike against the first
VM's; the other readers (`cached-p8`, `one-footer`, `stock`) and the sorted read; the run's
elapsed time (2 h 47 min on the old reader).

**Limits:** one run. C6's limits stand: multipart PUTs past 48 MiB overlap the write, where the
model prices them after it, and a phase's thread CPU counts the client's TLS and checksums that
the spike's latencies include too. TailCoW-C's ratios and RMoR-C's under `cached-skip` are not
judged.

## The vectorized base point on Azure (registered 2026-10-31)

**What they decide:** whether Azure Blob measures what the S3 Express run measured on the
vectorized reader (The vectorized base point in the cloud: V4–V7 held), with the trade-off where
Azure's prices put it. The base point has not run on Azure; the predictions registered for it
(C5–C8) were for the old reader and are not judged. Azure's requests cost more than S3 Express's,
so the store is predicted to take more of every phase there, MoR-v3's write included, and the
region arms' extra write cost to weigh less against it. It is also the latency model's first test
on Azure past the calibration's 1 MiB row groups.

**Setup:** `configs/sweep-base.json` on Azure Blob (ADLS Gen2, Standard, westus, from a
`Standard_D8as_v6`: Zen 4, 8 vCPUs, 32 GiB), as the S3 Express run was made: the deploy's `bench`
step on one VM, one run, a 16 GB heap, requests and held memory logged, at 6f24575 (the vectorized
reader), the contract suites and the VM's own spike, from main, just before. The twins: for the
counts, the laptop's `sweep-base-f970149711a9/20261031T083323Z`; for CPU, the S3 Express run
`sweep-base-s3x-89a55338cd9a/20261031T164030Z`, whose cloud Zen 4 cores took 1.24–1.35 times the
laptop's work in every judged phase.

**Predicted** (each arm's mean write and mean full read after the initial load under `cached`,
over MoR-v3's; RMoR-C's also under `cached-skip`): the S3 Express run's thread CPU plus its
requests (the twin's, V4) priced with the 2026-10-28 Azure spike from a `D8as_v6` in westus
(`spike/azure-spike-20261028T233151Z`). In brackets the requests' share of the simulated phase:

| Arm | A write | A full read |
|---|---|---|
| MoR-v3 | 4.8 s (0.62) | 14.1 s (0.68) |
| TailCoW-C | 1.00 (0.62) | 1.01 (0.67) |
| RMoR-C | 5.65 (0.36) | 3.72 (0.67); `cached-skip` 1.02 (0.60) |
| RMoR-C with footers | 5.39 (0.40) | 0.88 (0.69) |
| RMoW-C | 2.62 (0.38) | 0.90 (0.70) |

At the laptop's pace instead of the S3 Express VM's, the ratios are within 0.12 of these (RMoW-C:
2.53 and 0.90). S3 Express measured RMoW-C's write at 4.18 times MoR-v3's and its full read at
0.87.

- **A1: the Azure run sends what the twin counts** (a control). Every record's requests, bytes and
  table state, and every one-thread phase's held peaks, equal the laptop twin's, as V4's did.
  Azure's own requests (`sdk_*`) differ only where the store does: an appendable create is a
  staged Append Blob, an Append Block and a rename, and an append past 100 MiB is several Append
  Blocks.
- **A2: the model holds on Azure at this scale.** Priced with its own VM's spike, each arm's mean
  write and mean full read under `cached` (RMoR-C's also under `cached-skip`), simulated as its
  thread CPU plus its requests' prices, is within 25% of its mean wall time, as in V5.
- **A3: the trade-off falls where the twin puts it, and the write gap is narrower than on S3
  Express.** Each ratio in the table lands on the same side of 1 in the run's mean wall times,
  wherever the prediction is at least 10% from 1 (as V6): every region arm's write is slower than
  MoR-v3's, RMoR-C's full read under `cached` slower, and the full reads of RMoW-C and RMoR-C with
  footers faster. And RMoW-C's write over MoR-v3's is below the 4.18 S3 Express measured.
- **A4: the store takes most of every read, and of MoR-v3's write.** In `phase_breakdown`, the
  store (store client CPU and waiting on the store) takes more than half the wall time of every
  arm's mean full read under `cached` (RMoR-C's also under `cached-skip`) and of MoR-v3's and
  TailCoW-C's mean write, and less than half of each region arm's mean write. On S3 Express it
  took 58–70% of the full reads and 18–36% of the writes, MoR-v3's included.

**Measured, not judged:** each request's time against its price, by op and size; the new spike
against the 2026-10-28 one; the Azure VM's pace against the S3 Express VM's (the same
instructions); the other readers (`cached-p8`, `one-footer`, `stock`) and the sorted read; the
run's elapsed time (1 h 36 min on S3 Express).

**Limits:** one run. The model's Azure prices were checked only at the calibration's sizes (ranged
GETs to 1 MiB, within 0.70–1.23 of their time); here reads are 16 MiB ranges, and RMoW-C's appends
reach 109 MiB, past the 100 MiB Append Block. TailCoW-C's ratios and RMoR-C's under `cached-skip`
are not judged. The S3 Express run's thread CPU includes its client's TLS and checksums, while
Azure's SDK does much of its I/O on event-loop threads outside the phase's thread, so the predicted
CPU may run high. The VM has 8 vCPUs where the S3 Express VM had 16; only `cached-p8` (not judged)
uses more than two.

## The sweeps in the cloud (registered 2026-11-01)

**What they decide:** the sweeps' claims about time (U1, U2 and G1, registered 2026-10-29),
judged on measured wall time on S3 Express and Azure Blob, with the vectorized reader. They were
registered on simulated profiles whose CPU came from havoc's runs, which decided nothing about CPU
(every job under foreign load), on the old reader. The claims about requests, bytes and memory
(U3, K1, the bytes halves of U1 and G1, M1–M4) are exact, were decided by havoc's runs, and are not
restated: the vectorized reader changes no request or byte (V1).

**Setup:** the ten sweep configurations besides the base point (`configs/sweep-*.json`), each run
once at 6f24575, in a 16 GB heap, with requests and held memory logged. The base point's cloud runs
are the S3 Express run `sweep-base-s3x-89a55338cd9a/20261031T164030Z` and the Azure run under way
(The vectorized base point on Azure).
- **S3 Express:** one `c7a.4xlarge` per configuration in usw2-az3, nine at once (the deploy's
  fleet). AWS's limit of 64 vCPUs refused a tenth, so `sweep-rg64m` runs on `sweep-p30`'s VM after
  it. Each VM runs its spike just before its run. The runs share the bucket, so each measures the
  store under the others' load, as its spike does.
- **Azure Blob:** the base point's `D8as_v6` in westus, the configurations in turn (`sweep-p10`,
  `sweep-p30`, `sweep-rg32m`, `sweep-rg64m`, `sweep-s0`, `sweep-s2`, `sweep-late99-1`,
  `sweep-late99-20`, `sweep-ingest`, `sweep-ingest-rows`), each after a spike of its own.
- **The twins:** each configuration ran first on its S3 Express VM with a local store (the deploy's
  `twin` step): the same build and cores, with instruction counts. Every twin's requests, bytes and
  table state equal havoc's run of its configuration in every record (1,297–1,331 each).

**Predicted,** from the twins: their work at its arm's pace plus their requests priced by size,
with the base point's spikes (`spike/s3x-spike-20261031T163533Z` on S3 Express, from a VM alone on
the bucket; `spike/azure-spike-20261031T234510Z` on Azure); the base point's from its S3 Express
run's work. Each design's mean write over MoR-v3's (over the 30 writes), and RMoW-C's mean full
read under `cached` over MoR-v3's (after the initial load):

S3 Express:

| Run | Varies | MoR-v3's write (s) | RMoW-C | RMoR-C | RMoR-C with footers | RMoW-C's full read |
|---|---|---|---|---|---|---|
| `sweep-base` | the base point | 2.5 | 4.43 | 9.39 | 8.70 | 0.87 |
| `sweep-p10` | p = 0.1 | 1.7 | 3.07 | 7.34 | 6.63 | 0.88 |
| `sweep-p30` | p = 0.3 | 1.1 | 2.30 | 3.60 | 3.37 | 0.89 |
| `sweep-s0` | a skew of 0 | 2.6 | 4.78 | 11.41 | 10.65 | 0.86 |
| `sweep-s2` | a skew of 2 | 2.0 | 3.86 | 5.19 | 4.42 | 0.88 |
| `sweep-rg32m` | 32 MiB row groups | 2.3 | 5.73 | 11.17 | 10.37 | 0.86 |
| `sweep-rg64m` | 64 MiB row groups | 2.3 | 8.97 | 13.97 | 13.19 | 0.86 |
| `sweep-late99-1` | lateness, p99 of 1 batch | 2.4 | 4.48 | 6.89 | 6.09 | 0.86 |
| `sweep-late99-20` | lateness, p99 of 20 batches | 2.4 | 4.52 | 11.02 | 10.30 | 0.86 |
| `sweep-ingest` | the ingest window | 1.2 | 3.60 | 11.94 | 12.16 | 0.88 |
| `sweep-ingest-rows` | the ingest window, a row a change | 1.8 | 6.72 | 11.01 | 11.23 | 0.88 |

Azure Blob:

| Run | Varies | MoR-v3's write (s) | RMoW-C | RMoR-C | RMoR-C with footers | RMoW-C's full read |
|---|---|---|---|---|---|---|
| `sweep-base` | the base point | 4.3 | 2.79 | 5.97 | 5.73 | 0.89 |
| `sweep-p10` | p = 0.1 | 3.0 | 1.89 | 4.72 | 4.40 | 0.91 |
| `sweep-p30` | p = 0.3 | 1.8 | 1.56 | 2.71 | 2.60 | 0.92 |
| `sweep-s0` | a skew of 0 | 4.7 | 2.98 | 7.13 | 6.91 | 0.89 |
| `sweep-s2` | a skew of 2 | 3.7 | 2.30 | 3.24 | 2.83 | 0.91 |
| `sweep-rg32m` | 32 MiB row groups | 4.0 | 3.52 | 6.80 | 6.55 | 0.88 |
| `sweep-rg64m` | 64 MiB row groups | 3.8 | 5.79 | 8.79 | 8.63 | 0.81 |
| `sweep-late99-1` | lateness, p99 of 1 batch | 4.2 | 2.77 | 4.38 | 3.99 | 0.89 |
| `sweep-late99-20` | lateness, p99 of 20 batches | 4.2 | 2.80 | 6.88 | 6.65 | 0.89 |
| `sweep-ingest` | the ingest window | 1.6 | 2.92 | 9.44 | 9.96 | 0.91 |
| `sweep-ingest-rows` | the ingest window, a row a change | 3.0 | 4.62 | 7.40 | 7.82 | 0.90 |

TailCoW-C's write is within 0.01 of MoR-v3's in every run but `sweep-rg64m` (1.13 and 0.95),
whose tail seal of 64 MiB keeps a write's file of about 15 MiB in the tail to be merged. No region design's write comes within 1.5 times MoR-v3's at any
setting, so none crosses it along p by 0.3 (which the generator caps at 0.29 at this size).

- **S1: each cloud run sends what its twin counts** (a control). Every record's requests, bytes and
  table state, and every one-thread phase's held peaks, equal its twin's, as V4's did.
- **U1 (time): a region rewrite pays more as p rises,** as registered. On both stores, each region
  design's (RMoW-C, RMoR-C, RMoR-C with footers) mean write over MoR-v3's is lower at p = 0.3
  (`sweep-p30`) than at p = 0.03 (the base point): about half, by the twins.
- **U2 (time), revised: RMoW-C's write is the fastest region rewrite, but its lead narrows as p
  rises.** On both stores, RMoW-C's mean write takes less time than RMoR-C's and than RMoR-C with
  footers' at p = 0.03, 0.1 and 0.3, as registered. But RMoW-C's write over RMoR-C's is higher at
  p = 0.3 than at 0.03 (by the twins 0.47 to 0.64 on S3 Express, 0.47 to 0.58 on Azure), where U2
  as registered has it no higher. With 1.4 regions a write rewritten instead of 8.4, both designs'
  writes are mostly the batch's inserts, which they encode alike, so the rewrite that sets them
  apart weighs less. If the runs bear this out, U2's second half is contradicted.
- **G1 (time): bigger row groups cost the region designs' writes,** as registered. On both stores,
  each region design's mean write over MoR-v3's rises from 16 to 32 to 64 MiB row groups.

On Azure, U1, U2 and G1 also take the base point's Azure run, which started before this section was
written and had not finished.

**Measured, not judged:** the full reads along every axis; the settings of skew, lateness and the
ingest window; how well the model prices each run (each one-thread mean phase from its own CPU and
its VM's spike); the fleet's spikes, under each other's load, against the base point's alone; the
Azure VM's pace against the S3 Express VMs'.

**Limits:** one run of each configuration, on one seed. The S3 Express runs load one bucket at once;
the Azure runs come one after another over about a day, each priced by its own spike. Predicted CPU
is at the S3 Express VMs' pace, which the Azure VM's may not share; the ratios move little with it
(the base point's, by at most 0.12 between the laptop's pace and the VM's). Where a write's predicted
ratio sits within 10% of another it is compared with, the comparison decides nothing.

## RMoR with the check by range, in the cloud (registered 2026-11-02)

**What it decides:** RMoR's times on S3 Express and Azure Blob with its check of dead row groups
by range (d8a47dd). Every cloud run of RMoR so far (the base point and the sweeps, at 6f24575)
looked up each position of a row group in its deletion vector, which cost its skipping reader and
its writer up to 2.5 s of CPU a phase, so the results pages call its times upper bounds (ROADMAP).
The runs decide whether RMoR's full read, skipping the row groups its deletion vectors cover
whole, now beats MoR's, and where its write stands against RMoW-append's. The check decides the
same row groups as before (`DeletedRangesTest`), so no request or byte changes (R1).

**Setup:** `configs/rmor-*.json`, the sweeps' eleven configurations (the base point and the ten),
each `configs/sweep-<setting>.json` with MoR (`mor-v3`) and RMoR (`rmor-c`) as its only arms and
`rmor-<setting>` as its label, all five readers kept. MoR is each run's anchor: the store drifts
between runs, so times are compared by their ratio over MoR's in the same run. The build is the
commit that registers this section (its code is 505ba07's) on the fork at af875ccfd1, in a 16 GB
heap, with requests and held memory logged. The runs go on the stores as `rmor-<setting>-s3x` and
`rmor-<setting>-azure`, each after its VM's spike; how many VMs is left to the launch (AWS allowed
four `c7a.4xlarge` on 2026-11-01, Azure one `D8as_v6`; deploy/README.md, Quotas).
- **The laptop's runs are the reference:** each configuration once at 505ba07 (2026-11-03 UTC,
  campaign `rmor-rerun`). In every record (508–536 each), MoR's and RMoR's requests, bytes and
  table state equal what the twin of its setting sent at 6f24575: the cloud sweep's twins on their
  VMs, and for the base point the laptop's run of `sweep-base` at 6f24575
  (`sweep-base-f970149711a9/20261102T235620Z`). `rmor-s0`, `rmor-s2` and `rmor-rg32m` ran twice,
  and the two runs of each are alike in every record.
- **The base point under both builds,** every design, on the laptop
  (`sweep-base-f970149711a9/20261102T235620Z` at 6f24575, `/20261103T045636Z` at 505ba07): all
  1,325 records alike. The other four designs' work (user-mode instructions of a write and of a
  full read) is within 0.2%. RMoR's skipping full read takes 43% fewer instructions and its write
  12% fewer. Its read without skipping, which does not check, takes 2% more, within the 2.7% by
  which the two runs of `rmor-s0` differ.

**Predicted,** from the earlier cloud runs of each setting (the cloud sweep's, and the base point's
`sweep-base-s3x-89a55338cd9a/20261031T164030Z` and `sweep-base-azure-0c88452d93b4/20261031T234956Z`):
RMoR's mean write and mean full read, less the work the check no longer does. That work is the drop
in instructions from the setting's twin at 6f24575 to its laptop run at 505ba07, at the pace of
RMoR's phase (median cycles per instruction and clock) in the earlier S3 Express run of the
setting. The Azure VM counts no instructions, so Azure is priced at the same pace. The check took
0.3–2.5 s of a write and 0.0–2.3 s of a full read, least at p = 0.3 and in the two ingest settings,
where fewer row groups are dead whole. MoR's times are its earlier ones, the check not being on its
path. The means are over the 30 writes and the full reads after the initial load, MoR's under
`cached` and RMoR's under `cached-skip`. RMoW-append's (`rmow-c`) write over MoR's is the earlier
run's, for R3.

S3 Express:

| Run | Varies | MoR's write (s) | RMoR's write (s) | over MoR's | RMoW-append's, over MoR's | MoR's full read (s) | RMoR's full read (s) | over MoR's |
|---|---|---|---|---|---|---|---|---|
| `rmor-base` | the base point | 2.6 | 21.0 → 18.9 | 8.01 → 7.21 | 4.18 | 10.7 | 11.3 → 9.1 | 1.06 → 0.85 |
| `rmor-p10` | p = 0.1 | 1.9 | 11.4 → 10.2 | 6.10 → 5.46 | 2.81 | 10.5 | 10.7 → 9.4 | 1.01 → 0.89 |
| `rmor-p30` | p = 0.3 | 1.2 | 4.0 → 3.7 | 3.22 → 2.99 | 2.13 | 10.4 | 9.9 → 9.3 | 0.95 → 0.90 |
| `rmor-s0` | a skew of 0 | 2.9 | 27.6 → 25.2 | 9.50 → 8.65 | 4.47 | 10.9 | 11.3 → 9.3 | 1.03 → 0.85 |
| `rmor-s2` | a skew of 2 | 2.2 | 9.8 → 8.2 | 4.49 → 3.74 | 3.63 | 10.4 | 10.9 → 9.2 | 1.05 → 0.89 |
| `rmor-rg32m` | 32 MiB row groups | 2.6 | 23.8 → 21.5 | 9.17 → 8.29 | 5.32 | 10.4 | 11.0 → 8.7 | 1.06 → 0.84 |
| `rmor-rg64m` | 64 MiB row groups | 2.6 | 29.5 → 27.0 | 11.24 → 10.30 | 8.16 | 10.4 | 11.0 → 8.8 | 1.06 → 0.85 |
| `rmor-late99-1` | lateness, p99 of 1 batch | 2.6 | 15.1 → 13.1 | 5.75 → 4.99 | 4.11 | 10.4 | 10.9 → 8.9 | 1.05 → 0.86 |
| `rmor-late99-20` | lateness, p99 of 20 batches | 2.6 | 24.0 → 21.8 | 9.10 → 8.29 | 4.22 | 10.7 | 11.2 → 9.2 | 1.04 → 0.86 |
| `rmor-ingest` | the ingest window | 1.3 | 12.6 → 12.2 | 9.91 → 9.65 | 3.38 | 10.4 | 9.4 → 9.2 | 0.90 → 0.89 |
| `rmor-ingest-rows` | the ingest window, a row a change | 2.0 | 18.3 → 18.0 | 9.14 → 8.99 | 6.27 | 10.6 | 9.4 → 9.4 | 0.88 → 0.88 |

Azure Blob:

| Run | Varies | MoR's write (s) | RMoR's write (s) | over MoR's | RMoW-append's, over MoR's | MoR's full read (s) | RMoR's full read (s) | over MoR's |
|---|---|---|---|---|---|---|---|---|
| `rmor-base` | the base point | 4.2 | 26.2 → 24.1 | 6.24 → 5.74 | 3.14 | 14.0 | 20.0 → 17.8 | 1.43 → 1.27 |
| `rmor-p10` | p = 0.1 | 3.1 | 14.5 → 13.3 | 4.70 → 4.31 | 2.08 | 14.1 | 17.9 → 16.6 | 1.27 → 1.18 |
| `rmor-p30` | p = 0.3 | 1.9 | 5.1 → 4.8 | 2.73 → 2.58 | 1.68 | 13.4 | 14.7 → 14.1 | 1.09 → 1.05 |
| `rmor-s0` | a skew of 0 | 5.4 | 34.2 → 31.7 | 6.37 → 5.91 | 2.84 | 14.8 | 19.4 → 17.4 | 1.31 → 1.18 |
| `rmor-s2` | a skew of 2 | 3.5 | 12.5 → 10.8 | 3.51 → 3.05 | 2.63 | 13.4 | 16.3 → 14.6 | 1.21 → 1.09 |
| `rmor-rg32m` | 32 MiB row groups | 4.0 | 28.9 → 26.6 | 7.17 → 6.61 | 3.87 | 12.8 | 19.4 → 17.1 | 1.51 → 1.33 |
| `rmor-rg64m` | 64 MiB row groups | 4.8 | 35.6 → 33.1 | 7.46 → 6.94 | 5.08 | 13.0 | 22.9 → 20.7 | 1.76 → 1.59 |
| `rmor-late99-1` | lateness, p99 of 1 batch | 4.2 | 20.2 → 18.2 | 4.77 → 4.30 | 3.11 | 13.6 | 17.4 → 15.4 | 1.28 → 1.14 |
| `rmor-late99-20` | lateness, p99 of 20 batches | 4.2 | 30.9 → 28.8 | 7.38 → 6.87 | 3.18 | 14.0 | 18.3 → 16.4 | 1.31 → 1.17 |
| `rmor-ingest` | the ingest window | 2.1 | 16.1 → 15.8 | 7.52 → 7.37 | 2.50 | 13.5 | 13.9 → 13.8 | 1.03 → 1.02 |
| `rmor-ingest-rows` | the ingest window, a row a change | 3.3 | 23.0 → 22.7 | 6.94 → 6.85 | 4.65 | 13.7 | 14.4 → 14.4 | 1.05 → 1.05 |

- **R1: each cloud run sends what its laptop run counts** (a control). Every record's requests,
  bytes and table state equal its laptop run's, so MoR and RMoR send what they sent in the earlier
  cloud runs of their setting.
- **R2 (time): on S3 Express, RMoR's skipping full read is faster than MoR's; on Azure Blob it is
  not.** On S3 Express, RMoR's mean full read over MoR's is below 0.90 at every setting but
  p = 0.3 (predicted 0.84–0.89), where the runs at 6f24575 had it above 1 at eight settings
  (1.01–1.06). At p = 0.3 it is predicted at 0.90, on the 10% line, so that setting decides
  nothing. On Azure Blob it stays above 1.1 at seven settings (predicted 1.14–1.59); p = 0.3, a
  skew of 2 and the two ingest settings (1.02–1.09) decide nothing.
- **R3 (time): RMoR's write still costs more than RMoW-append's.** On both stores, at every setting,
  RMoR's mean write over MoR's is more than 1.1 times RMoW-append's over MoR's in the earlier run
  of its setting (predicted 1.21–2.86 times on S3 Express, 1.16–2.95 on Azure), except a skew of 2
  on S3 Express (1.03), which decides nothing.

**Measured, not judged:** RMoR's write against RMoW-external's (`rmor-c-footer`; predicted at
0.86–0.98 of its ratio over MoR's, mostly near the 10% line); RMoR's skipping read against the
RMoW designs' (predicted at 0.97–1.01 of RMoW-append's on S3 Express); RMoR's read without skipping,
which does not check; how far each RMoR time falls from its prediction, and each MoR time from the
earlier run's (the stores' drift between the runs); the spikes.

**Limits:** one run of each configuration, on one seed. R3 compares across runs: RMoW-append's
ratio comes from the earlier runs, with all five designs. MoR's write took 1.3–5.7% fewer
instructions in the laptop's runs of two designs than in the twins of five (1.5% at the base point,
both on the laptop; 0.2% between the builds in runs of five), so a ratio over MoR's write carries a
few percent from the designs a run holds. The check's
work on Azure is priced at the S3 Express VMs' pace. Every setting's instruction drop but the base
point's comes from two machines (the twin on its VM, the new run on the laptop; Zen 4 both, on the
same JDK), between which MoR's full reads, which the fix leaves alone, moved by up to 4.2%. Where a
predicted ratio sits within 10% of what it is compared with, the comparison decides nothing.
