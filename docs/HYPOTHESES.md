# Hypotheses

What the benchmark expects each run to show, written before the run. A result that contradicts a
hypothesis is reported as contradicted. A hypothesis is never edited once registered; a revision
is a new dated entry.

The hypotheses registered from 2026-10-20 to 2026-10-27 (H1–H6, and each campaign's predictions
and controls) are this file as of commit 1145f89 ([on GitHub](https://github.com/cdouglas/rmow-bench/blob/1145f89a36fa60af56aefc9233991f1d03755ae2/docs/HYPOTHESES.md); `git show
1145f89:docs/HYPOTHESES.md`), and the write-ups up to 2026-10-27 cite them there. The commits'
times show each came before the runs it was decided on. What they settled is summarized below.

On 2026-11-12 the runs and lab pages of the sections up to "Inserts merged by arrival"
(2026-11-06) were retired, and the tuner's two sections' (2026-11-08) but for its insert
configurations: what they taught is in [LESSONS.md](LESSONS.md), and the pages these sections
cite are in git at bfbdf33. The two workloads' runs of 2026-11-06 live on in the windows
campaign's, which equal them in every record but the windows' reads. The sections stand as
registered.

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

## RMoR with the check by range: the build (registered 2026-11-03)

The runs of R1–R3 (2026-11-02) build at the commit that registers this note, not at 378c967. It
changes only how a run that fails ends: the JVM exits with status 1 and the manifest records the
error, where a failed run had been left alive by its readers' pool threads (a run on the laptop
hung for 4.5 hours after its 8-thread read ran out of memory). A run that finishes sends the same
requests and bytes and does the same work, so the section's predictions and the laptop's
reference runs stand.

## RMoR and RMoW-external with their row groups kept whole (registered 2026-11-04)

**What it decides:** whether RMoR's and RMoW-external's write spikes come from how their writer
lays out a write's new data file, and whether laying it out as RMoW-append does removes them. The
two designs were meant to differ from RMoW-append in two things only: where a rewritten row group
goes (a new data file, not its own object) and what hides its old copy (a deletion vector, or a
footer appended to its file). Their writer (`TailCowArm`) differs in a third. It puts the rows of
every row group a write rewrites, with the write's changes, in one list with the write's inserts,
sorts the list by event time, and lets the Parquet writer cut it into new 16 MiB row groups.
RMoW-append writes each rewritten row group back as one row group of the same rows, and the
inserts as a row group of their own.

Sorting by event time puts the far tail of each write's late inserts among the old regions' rows.
At the base point, every one of the 15 row groups of write 1's file holds some of region 50's rows
(write 1's inserts); its first holds regions 4 and 5 and 58 rows of region 50. A later statement
on a new region changes a few of those rows in nearly every row group of the recent files, and
each costs a whole row group. Writes 3, 4 and 5 rewrote 13, 14 and 18 row groups (192, 206 and
283 MB) only for a new region's rows, most of them for 1–12 changed rows, and each rewritten file
carried its scattered rows into the next. Over the base point's 30 writes on S3 Express
(`sweep-base-s3x-89a55338cd9a/20261031T164030Z`), the 18 writes that change a region inserted
since the load wrote 396–771 MB each in both designs and took RMoW-external 19–37 s. The 12 that
don't wrote 162–313 MB, at most 1.14 times RMoW-append's bytes. RMoW-append wrote about 30 MB for
each region changed, old or new, and took 7.5–13.6 s throughout. This was found on a laptop
replay of the base point's first six writes with the objects kept, whose requests and bytes equal
the S3 Express run's; the row groups each write must rewrite, rebuilt from those files and the op
log, give each write's bytes read back exactly. The replay is not kept in `results/`.

**Setup:** two new arms, `TailCowArm` with its row groups kept whole:
- **The arms:** `rmor-c-kept` (RMoR) and `rmor-c-footer-kept` (RMoW-external). A write's new data
  file holds each row group the write rewrites as one row group of its own: its live rows with
  the write's changes, sorted by event time, as RMoW-append writes a region. The inserts follow
  as a row group of their own, sorted by event time among themselves. The file is encoded by
  RMoW-append's region writer, so a row group of the same rows has the same bytes, and created
  in one request, as now. Finding the rows (`locate`), θ = 0, and what hides the old copies are
  unchanged.
- **The configurations:** `configs/kept-<setting>.json` for the base point, insert lateness with
  a p99 of 1 and of 20 writes, and the ingest window. Each is `configs/sweep-<setting>.json`
  with label `kept-<setting>` and arms MoR (`mor-v3`), RMoR and RMoW-external both as now and
  kept, and RMoW-append (`rmow-c`); TailCoW-C is dropped. At these settings every tail seals at
  once (a write's inserts alone are 11.7 MB, and four times that passes the 16 MiB seal), so no
  design merges its tail.
- **The runs:** on the laptop, one run each, from one build snapshot (`bin/matrix.sh --build`),
  with requests and held memory logged. As a control, the four arms that exist now must send
  the requests and bytes of their earlier runs of the setting in every record: the base point's
  laptop run at 505ba07 (`sweep-base-f970149711a9/20261103T045636Z`); elsewhere, the laptop's
  `rmor-<setting>` at 505ba07 for MoR and RMoR, and the S3 Express sweep for RMoW-external and
  RMoW-append.

**Predicted:**
- **W1 (bytes): the kept arms rewrite what RMoW-append rewrites.** At every write of the four
  runs, each kept arm reads back RMoW-append's bytes within 1% (its merge-read) and writes within
  2% of RMoW-append's bytes. Their mean write's bytes over RMoW-append's fall to at most 1.02.
  Now, for RMoR and RMoW-external alike, that ratio is 1.86 at the base point, 1.26 and 2.18 with
  lateness p99 of 1 and 20 writes, and 3.29 in the ingest window. Their largest write falls to
  within 2% of RMoW-append's largest (292 MB at the base point, against 771 MB now).
- **W2 (work): their writes cost what RMoW-append's do, plus finding the rows.** Over
  RMoW-append's mean write work (user-mode instructions), the kept RMoW-external's is at most 1.25
  and the kept RMoR's at most 1.35, at each setting. No single write costs more than 1.4 and 1.55
  times RMoW-append's same write.
  - **Now:** 1.97 and 2.16 over the mean at the base point (the laptop at 505ba07), and up to
    3.17 and 3.53 for a single write. RMoW-external's mean is 1.40, 2.27 and 3.37 at the other
    three settings.
  - **The basis:** on the base point's 12 writes that change no new region, where the bytes
    nearly match, RMoW-external's write took 1.08–1.31 times RMoW-append's work (1.17 over the
    twelve) and RMoR's 1.19–1.47 (1.29). The excess is their locate, which reads row ids (10–15
    billion instructions a write) where RMoW-append looks ids up in its footers, and RMoR's
    deletion vectors (about 4 billion).
  - **On S3 Express,** where a write's time followed its bytes at 46–49 ms a MB, this would put
    both designs' writes within about 1.5 s of RMoW-append's (about 11 s at the base point,
    against 21 s at 6f24575). That is expected, not judged: no cloud run is planned.
- **W3 (what it costs): the kept RMoW-external's sorted read is RMoW-append's.** The current file's
  row groups each cover a narrow slice of event time. A kept arm's insert row groups, like
  RMoW-append's, each hold a few very late rows that reach back across the table, so a read in
  event-time order opens them early and holds them to the end (M2).
  - **Memory:** at its last probe, the kept RMoW-external's sorted read (`cached`) holds within
    10% of RMoW-append's peak: 367 MB at the base point, and 245, 368 and 349 MB at the other
    three settings. RMoW-external holds 32–83 MB now.
  - **Work:** the sorted read does within 10% of RMoW-append's work (at the base point on the
    laptop, 76.7 billion instructions, against RMoW-external's 50.8 billion now).

**Measured, not judged:**
- the kept RMoR's sorted read (its `cached` reader reads the row groups its deletion vectors
  cover);
- the kept arms' full reads, whose live bytes should be RMoW-append's;
- the kept RMoR's stock full read, whose dead row groups should now pile up at RMoW-append's
  write rate;
- their locate;
- what their writes hold (they no longer sort a whole file; M4);
- their simulated write latency on the S3 Express profile.

**Limits:**
- **One run:** one seed and one run per setting, on the laptop, so time is judged as work.
- **The tail:** the kept arms keep TailCoW's tail unit, a write's whole data file, which any
  write that rewrites a row group seals. At 64 MiB row groups RMoW-append's tail merges and
  theirs would not, so that setting is not run.
- **The writer:** the kept arms need a writer that ends a row group at a chosen row. Iceberg's
  Parquet writer, like parquet-java's, cuts row groups by size alone, so an engine would need
  its own writer for this layout, though the files are plain Parquet that any reader reads. A
  stock writer could come close by ordering a write's rows by the row group they came from,
  inserts last, instead of by event time. That is not tested.
- **Late inserts:** a write could also keep its very late inserts in a small row group of their
  own, which would keep the sorted read cheap in all three designs. That is not tested either.

## Row groups kept whole: where the runs go (registered 2026-11-04)

The four runs of W1–W3 go on havoc, as campaign `kept` (`docker/campaigns/kept.json`), not the
laptop, from the commit that registers this note: the arms are built there, and each run's
manifest names its image. The runs keep everything else of the setup. havoc counts no
instructions (`kernel.perf_event_paranoid` is 4), so its runs decide W1 and W3, which are exact
on any host (bytes, and held memory on one thread). W2 needs the counts: it is decided by runs of
the same four configurations with counters, on havoc if its setting is lowered to 2, or else on
the laptop. Their requests and bytes must equal havoc's in every record, as a control.

The kept arms are written a row group at a time, as RMoW-append is: a row group is read back,
rewritten, encoded and its rows dropped before the next is read, and the inserts come last.
RMoR's file streams to the store as it is encoded. RMoW-external's is held for its single create,
as now. Measured, not judged: the rows their writes hold at once against RMoR's and
RMoW-external's as built, which hold every row of a write's file to sort it (M4).

## The two workloads apart: updates only, and inserts only (registered 2026-11-06)

**What it decides:** the two claims for region merges, each on a workload of its own. Every
campaign so far ran both at once: the base point's writes are 70% inserts (some late), 25% updates
and 5% deletes, so a read's cost mixed the two.
- **(a) Updates:** statements update rows clustered in a key region, on a table laid out by key,
  with no order to keep. An update reads every column of the rows it rewrites, so copying the rest
  of their row group into the write costs only write bytes, and spares readers the deletion
  vectors and the dead rows.
- **(b) Inserts:** inserts only, sorted by event time, some late. A writer that merges late rows
  into the row groups they belong in spares readers from interleaving row groups.

**A fix first: what an update reads.** Until now the MoR family's updates were blind. The locate
read only `id` and `_pos` (`MorArm.locateByScan`), and the new versions were made from an id and
a version (`MadeRows`), never from the old row. So MoR, TailCoW and RMoR below θ never read the old
rows' other columns: at the base point MoR-v3's locate read 36.7 MB a write (the `id` column is
5.9% of a row group's bytes), against RMoW-append's 224 MB merge read.
- **The knob:** `layout.updateRead` `row` makes every arm read whole each row group that holds a
  row it updates, once. The region designs read those row groups whole anyway (their merge read),
  so only the MoR family's writes change: phase `update-read`, after the locate.
- **What it does to earlier runs:** every campaign before this one under-charges the MoR family's
  writes. Their configurations keep their hashes and stay as they ran; which to rerun is open.

**Setup:**
- **Workloads**, from the base point (`kept-base`), with its seed, its table (25.7M rows laid out
  by id, 16 MiB row groups) and 30 writes:
  - **updates only** (`upd-*`): 150,000 changes a write (125,000 updates and 25,000 deletes) as
    statements of 3% of a region's rows (regions of 513,700 rows, skew 1), and no inserts;
  - **inserts only** (`ing-*`): 350,000 a write, sorted by event time, with lateness median 0.05
    writes and p99 5 writes (`ing-base`), 1 write (`ing-late99-1`) or 20 (`ing-late99-20`).
- **Arms, updates:**
  - `upd-base` runs MoR (`mor-v3`), RMoR (`rmor-c-kept`), RMoW-external (`rmor-c-footer-kept`),
    RMoW-append (`rmow-c`), and RMoR with deferred merges (`rmor-density`, θ = 0.06, a delta row
    group per region);
  - **twins of one arm each:** MoR compacted every 10 writes (`upd-base-compact`); MoR with
    Spark's one-pass plan (`upd-base-scan`, `updateRead` `scan`); and the deferred merges at
    θ = 0.25 (`upd-density-p25`) and with one delta row group per write (`upd-density-write`).
- **The deferred merges** (`rmor-density`): DVs until a region's row group has θ of its rows dead
  or changed by the write; then the row group is merged, with the region's rows that wait in
  delta row groups folded in. Below θ, the new versions wait in the write's file: a row group per
  region, or one per write. A delta row group never merges on its own, and there is no cleaner.
- **Arms, inserts:**
  - `ing-base` and `ing-late99-*` run MoR, RMoR and RMoW-append, and `ing-base-compact` runs MoR
    compacted every 10 writes;
  - **they append alike:** at the base point a write's 10 MB of inserts is sealed at once
    (merging four would pass 16 MiB), so tail policy C never merges them. The three arms differ
    only in objects: a file a write, or RMoW's tail object;
  - **(b)'s writer is not built:** no arm merges a late row into the row group it belongs in.
- **The rest as `kept-base`:** locate by scan, writers that skip dead row groups, five readers
  (one-footer, cached with the sorted read, cached-skip, cached-p8, stock), probes every 5
  writes, payload checks off, `--log-requests --log-held`. Campaign `docker/campaigns/split.json`
  on havoc, the user's runs.
- **The model:** `analysis/hybrid_model.py` replays each configuration's op log through each
  policy at 28.6 bytes a row. Against measured runs it reads 5–16% low on bytes (the note of
  2026-11-04), and it says nothing about time. Every prediction below is the model's mean over the
  30 writes, or its state after the 30th.
- **A check at a fifth of the scale** (a scratch run, not in `results/`: 5.1M rows, 4 MiB row
  groups, 30,000 changes a write, 10 writes):
  - every arm's reads matched the oracle;
  - the ratios track the model's: the deferred merges wrote 0.21 of RMoR's bytes (model 0.22);
  - MoR's update read 1.16 times RMoR's after 10 writes (model 1.27 over 30).

**Predicted, updates only:**
- **U1 (what an update reads): MoR's update reads at least as many bytes a write as RMoR's and
  RMoW-append's**, its locate and update read against their locate and merge read (model: 286
  against 225 MB). Its updated rows sit in the base row groups and in the row groups of earlier
  writes' new versions, and it reads both whole. The three region designs read within 5% of one
  another, and the deferred merges within 10% of RMoR (model 232).
- **U2 (writes): RMoR, RMoW-external and RMoW-append each write at least 40 times MoR's bytes a
  write** (model 212 against 3.6 MB). The deferred merges write at most 0.3 of RMoR's at θ = 0.06
  (model 47 MB, 0.22), with either placement, and at most 0.1 at θ = 0.25 (model 9.5 MB, 0.045).
- **U3 (reads): after the 30th write, the region designs' full reads read at most 1.02 times their
  live bytes, and MoR's at least 1.05 times** (model 1.00 against 1.09). **MoR's sorted read holds
  at least 5 times RMoR's bytes at its peak** (model: 17% of the table, 31 row groups open at
  once, against 2% and 1 row group).
- **U4 (where the deltas wait): with a delta row group per region, the deferred merges' sorted
  read holds at most 0.05 of the table at its peak, against at least 0.08 with one per write**
  (model 0.02 against 0.11). **The full read takes at least 1.3 times the requests** after the
  30th write (model 273 against 149; MoR 155, RMoR 69). This is the LSM trade: deltas kept by
  region spare range reads and cost full reads.

**Predicted, inserts only:**
- **I1 (writes): MoR, RMoR and RMoW-append write within 5% of one another's bytes a write**, at
  every lateness (model: 10 MB each).
- **I2 (the newest rows): their reads of the newest 1% of event time are within 1.5 times of one
  another's requests and bytes** at every lateness (model: 2.5 to 3.8 requests, 1.7 to 2.1 times
  live).
- **I3 (what lateness costs readers): after the 30th write, the appending arms' sorted read holds
  at least 0.15 of the table at its peak at every lateness, and MoR compacted every 10 writes at
  most 0.1** (model: 0.21 at p99 1, with 22 row groups open at once; 0.30 at p99 5 and 20, with
  31; compacted, 0.06 at each). Every appended row group spans back to the latest of its late
  rows.

**Measured, not judged:**
- MoR under Spark's one-pass plan: the model puts it at 271 MB a write, but our locate prunes a
  file's row groups by a single id range, so it may read more;
- requests by object layout (a file a write against RMoW's tail object);
- what writes hold.

Work is not judged: havoc has no instruction counters (`perf_event_paranoid` 4), and its jobs
share their cores' siblings with other users' work.

**A calibration of earlier runs (campaign `docker/campaigns/mor-row.json`, measured, not
judged).** Every earlier campaign under-charges the MoR family's writes, and the cloud runs are
not run again. Instead MoR (`mor-v3`) runs alone with `updateRead` `row` on havoc at the eleven
settings of RMoR's rerun in the cloud (`configs/mor-row-*.json`: `configs/rmor-*.json` with MoR
alone and `updateRead` `row`, so the same op logs). Requests and bytes are deterministic, so the
cloud runs' MoR can be priced with what its updates read, from each store's model
(lab/pages/store-latency.md). A control: every record of these runs but the `update-read` phase's
equals the MoR record of the `rmor-*` run on the laptop (2026-11-03); a difference is a change to
MoR's path since then, or a bug.

**Limits:**
- **The runs:** one seed and 30 writes, and the model reads low.
- **The reads probed:** the newest 1% and the whole table. A 1% range elsewhere is extrapolated
  from the model, not measured:
  - updates, by key: MoR 38 requests and 9.0 times live against RMoR's 2.8 and 3.0;
  - appended inserts, by event time at p99 5: 18 requests and 17 times live.
- **(b) is not tested:** the inserts runs measure what lateness costs appending designs and what
  compaction recovers. A writer that merges late rows at once would write 746 MB a write at
  p99 5 in the model, and 148 at p99 1.
- **No tiered merging of the inserts:** the tail's seal is 16 MiB, the row-group size, as in every
  configuration so far (the row-group sweep moved both together). A larger seal would let tail
  policy C merge consecutive writes' inserts into longer runs sorted by event time, in RMoR and
  RMoW-append alike. These runs do not try one.
- **Whole row groups:** `updateRead` `row` reads whole row groups, as Iceberg's reader does. A
  reader of pages would read less (model: MoR 171 MB, RMoR 225).

## Inserts merged by arrival: a larger tail seal (registered 2026-11-06)

**What it decides:** whether merging consecutive writes' inserts as they are written spares a
sorted read most of what lateness costs it, at about twice the write bytes; and whether the
merged runs must be cut into row groups for that.

**Why:** in the inserts-only runs of the split campaign (I1-I3, above), no arm merges inserts. The
tail's seal (`tailSealBytes`) has been the row-group size, 16 MiB, in every configuration so far,
and four writes of 10 MB would pass it, so tail policy C seals each write's inserts at once. Every
appended row group spans back to its latest late row, so the sorted read holds nearly all of them
at once (model: 0.30 of the table at p99 5 and 20, 0.21 at p99 1, after 30 writes).

**Setup:**
- **Configurations:** `ing-seal160`, `ing-seal160-late99-1` and `ing-seal160-late99-20` are
  `ing-base` and its two lateness twins with the seal at 160 MiB (fanout 4). Four writes' units
  of 10 MB merge into one of about 38 MiB, and four of those into one of about 153 MiB, which is
  sealed. Each row is written about three times, and every 16th write rewrites about 150 MB; in
  30 writes there is one unit of the top level.
- **Arms:**
  - **RMoR (`rmor-c`).** Its tail units are data files, and its Parquet writer cuts a merged file
    into row groups at the row-group size. On inserts only, at the 16 MiB seal, it lays out as the
    split campaign's `rmor-c-kept` (a write's inserts are one row group either way), so the
    `ing-*` runs are its baseline.
  - **RMoW-append (`rmow-c`).** A merged unit is one region, so one row group. Its tail file
    also rolls over at the target file size (128 MiB), sealing every open unit, about every 14
    writes, so its units stop at the middle level (about 38 MiB).
  - MoR is left out: the seal does not touch it, and `ing-*` ran it.
- **The rest** as `ing-base`. Campaign `docker/campaigns/seal.json` on havoc, beside the split
  campaign.
- **The model:** `analysis/hybrid_model.py` policies `tier:160` (units cut into row groups) and
  `tier:160:one` (a unit one row group, in a tail file that rolls over), on the configurations'
  op logs. Every prediction below is its mean over the 30 writes, or its state after the 30th.
- **A check at a fifth of the scale** (a scratch run, not in `results/`: 5.1M rows, 4 MiB row
  groups, a seal of 40 MiB and a target file of 32 MiB, 70,000 inserts a write, 20 writes, with
  MoR):
  - every read matched the oracle;
  - RMoR wrote 2.4 times MoR's bytes a write, its largest write 16 times (model 2.1 and 16);
  - RMoW-append wrote 1.6 times, its largest write 4 times (model 1.6 and 4);
  - after the 20th write, the sorted read held 0.05 of the table in RMoR, against 0.25 in MoR and
    in RMoW-append.

**Predicted:**
- **J1 (writes): RMoR writes 1.6 to 2.6 times the appending arms' mean bytes a write in the
  `ing-*` run of the same lateness, and its largest write at least 10 times** (model: 21 against
  10 MB, and 160 MB). **RMoW-append writes 1.3 to 2 times, and its largest write at most 5 times**
  (model: 16 MB, and 40 MB).
- **J2 (RMoR, cut): after the 30th write, RMoR's sorted read holds at most half the share of the
  table that the appending arms' hold in the `ing-*` run of the same lateness** (model: 0.09
  against 0.30 at p99 5 and 20, with 7 row groups open against 31; 0.07 against 0.21 at p99 1),
  **and at p99 5 at most twice the share MoR compacted every 10 writes holds** (model 0.06, with
  4 open).
- **J3 (RMoW-append, one row group a unit): its sorted read holds at least 0.9 times the
  appending arms' share in the `ing-*` run of the same lateness** (model 0.27 against 0.21 at
  p99 1, 0.30 against 0.30 at p99 5 and 20). Each merged unit's one row group spans back to its
  oldest late row, as a write's did.

**Measured, not judged:** the read of the newest 1% of event time, which depends on where the
merge cycle stands at the probe (model after the 30th write: RMoW-append 2.5 to 3.3 times live,
RMoR 1.7 to 2.1; at a fifth of the scale after the 20th, RMoW-append read less than RMoR);
requests; what the merges hold; the full read, all of whose bytes are live in both arms.

**Limits:**
- **The runs:** one seed and 30 writes, so one unit of the top level. A longer run adds a
  sealed unit every 16 writes, and each holds its late rows in its first row group.
- **Not tested:** a merge of each late row into the row group its event time falls in (the model:
  746 MB a write at p99 5); RMoW-append with its merged units cut into row groups and its tail
  kept open past the target file size, which is not built; re-cutting a row group at a late row's
  place.

## Reads of a range across the table, and the work they take (registered 2026-11-07)

**What it decides:** what a read of a narrow range costs where it is not the newest rows. The
reads probed so far are the whole table, its newest 1% of event time and its sorted order. The
newest 1% costs every design alike (0.021 of the table in every `upd-*`, `ing-*` and seal run),
while the model put a 1% range elsewhere at 9 times its live bytes for MoR's updates and 17 times
for appended inserts (2026-11-06, limits). On the laptop the same runs count instructions, which
havoc cannot, so they also give each phase's work for simulated time, write against read.

**The verdicts that led here (judged 2026-11-07, on havoc, campaigns `split` and `seal`):**
- U1–U4 and I1–I3 held, but for two parts. U1's "the region designs within 5% of one another" is
  contradicted: RMoW-append's update reads 220.6 MB a write, RMoR's 240.0 (its locate reads 0.1 MB
  against 19.5). U2's "at least 40 times MoR's bytes" is contradicted at 38.9 times: MoR writes
  5.7 MB a write, 1.7 of them deletion vectors (model 3.6). U3 and U4 are judged on the live
  rows' bytes (754 MB, RMoW-append's full read) and under the reader that skips dead row groups.
- J3 held. J1 and J2 are contradicted in part: RMoR's largest write is 4.0 times the appending
  arms' (47 MB), not 10, and its sorted read holds 0.52 of theirs at p99 1 (not at most half) and
  2.4 times compacted MoR's at p99 5 (not at most twice). Inserts take 33.7 bytes a row, not the
  model's 28.6, so four 4-write units (189 MB) passed the 160 MiB seal and never merged. With
  `--insert-row-bytes 33.7` the model puts the largest write at 40 MB and the held share at 0.12
  (measured 0.135). At p99 5 and 20 RMoR's share is 0.41 of the appending arms'.
- Measured: the sorted read sends the full read's requests and bytes, but on havoc took 1.85–2.2
  times its thread CPU in the appending arms after the 30th write (1.4 at the 5th), 1.43 compacted.

**Setup:**
- **The reads:** `reads.windows` (README), at each probe and under every reader: five windows of
  1% of a column's span, centred at 0.1, 0.3, 0.5, 0.7 and 0.9 of it, each a read of its own
  (`read_window`, with `window_by` and `window_at`), checked against the oracle. Updates read
  windows by `id`, inserts by `ts`: on the updates, event time follows the key; on the inserts,
  ids follow arrival, which a write's files hold together.
- **Configurations:** `configs/win-*.json`, the `split` and `seal` campaigns' with the windows
  added, so the same op logs: `win-upd-base` (MoR, RMoR, RMoW-external, RMoW-append, the deferred
  merges), `win-upd-base-compact`, `win-upd-density-p25`, `win-upd-density-write`, `win-ing-base`,
  `win-ing-base-compact`, `win-ing-late99-1`, `win-ing-late99-20`, and the three `win-ing-seal160*`.
  `upd-base-scan` is left out: its table is MoR's in `upd-base`.
- **Where:** on havoc (campaign `docker/campaigns/windows.json`: requests, bytes, held memory) and
  on the laptop (`bin/matrix.sh`, the same configurations, one at a time: instructions).
- **The model:** `analysis/hybrid_model.py` cuts the runs' five windows (`span_windows`) and prices
  each as its full read: a request per row group and object it touches and per object with a DV,
  bytes of every row group whose statistics overlap the window, against the window's live rows.
  The ingest policies take `--insert-row-bytes 33.7`. It does not count the footers of files
  whose metrics overlap a window but none of whose row groups do.
- **A check at a fifth of the scale** (scratch runs, not in `results/`): every window read matched
  the oracle. On the updates after 10 writes, MoR's windows read 9.9–14.8 MB and 12–23 requests,
  RMoW-append's 3.4–6.8 MB and 2–3. On the inserts at p99 5 after 20 writes, with the seal 40 MiB,
  the appending arm's windows read 23–53 MB, RMoR's 9.8–13.4, RMoW-append's 28–53.

A window's live bytes are its rows times the bytes a live row takes in the arm's full read under
the same reader. Windows are judged under `cached-skip` after the 30th write, as the mean of the
five.

**Predicted:**
- **Q1 (updates, a key range): MoR's windows read at least 3 times the bytes of RMoR's,
  RMoW-external's and RMoW-append's, and at least 10 times RMoW-append's requests** (model: 17
  against 3.3 times the windows' live bytes; 81 requests against 3). Each write's new versions
  span the key range, so every window reads every write's file. **MoR compacted every 10 writes,
  just compacted, reads at most half of MoR's bytes** (model 5.5).
- **Q2 (where the deferred merges' deltas wait): with a delta row group per region, their windows
  read at most 1.3 times RMoR's bytes, at θ = 0.06 and 0.25; with one per write, at least twice**
  (model 3.4 and 3.9 against 3.3; 11). U4's full reads paid for keeping deltas by region; a range
  read is what it buys.
- **Q3 (inserts, an event-time range): at p99 5 and 20 the appending arms' windows read at least
  10 times their live bytes** (model 27): a few rows of every write arrive later than p99, so
  every appended row group spans back over most of the table. **MoR compacted every 10 writes
  reads at most 0.4 of that, RMoR with the 160 MiB seal at most 0.7, and RMoW-append with it at
  least 0.9** (model 6.1, 12 and 28 against 27). **At p99 1 the appending arms read at most half
  of what they read at p99 5** (model 8.6).
- **Q4 (the sorted read's work, the laptop only): after the 30th write, at p99 5 and 20, the
  appending arms' sorted read takes at least 1.5 times the instructions of their full read, and
  MoR compacted every 10 writes less than that ratio** (havoc's thread time: 1.85–2.2 and 1.43).

**Controls:**
- In every record but `read_window`'s, each `win-*` run sends the requests and bytes, and holds
  the one-thread peaks, of its `split` or `seal` run.
- The laptop's run of each configuration and havoc's send the same requests and bytes in every
  record.

**Measured, not judged:** each window apart; requests beyond Q1; the windows under the other
readers (Iceberg's stock reader reads RMoR's dead row groups); every phase's instructions, for the
breakdown of write and read time on S3 Express and Azure Blob (lab/pages/store-latency.md);
RMoR's sorted read with the seal (on havoc 1.89 times its full read, holding 0.41 of the
appending arms' bytes).

**Limits:**
- **The runs:** one seed and 30 writes; five windows of one width.
- **Windows by span:** each window is 1% of the column's span, not of the rows. Ids and event
  times are near uniform here (a write's inserts span a write's event time), so the two differ
  little.
- **The sorted read on the updates** runs under `cached`, which reads RMoR's dead row groups; its
  work there is not RMoR's.
- **Time is simulated** from the laptop's instructions at each arm's pace and the requests priced
  by size, as before (C6 and V5 held within 1–14% on S3 Express).

## Tuning write policy by simulation (registered 2026-11-08)

**What it decides:** whether counts from havoc, priced with a formula for the work, can pick a
write policy for a read mix as well as the laptop's measured work would, so a grid of policies
needs instructions only for its finalists. The windows campaign (Q1–Q4, held) gave the simulated
times of the base policies: on the updates a write takes MoR 4.7 s, the deferred merges 6.2 and
RMoW-append 9.2, and a 1% window 1.40, 0.54 and 0.23 s (S3 Express, the laptop's work). Which
policy wins turns on the read mix, and on knobs no run has varied: the merge threshold, the row
group's size, the seal and how often MoR compacts.

**The tuner** (`analysis/tune.py`): a policy is a configuration and an arm on one workload, the
updates (`win-upd-*`, `tune-upd-*`) or the inserts at lateness p99 5 (`win-ing-base*`,
`win-ing-seal160`, `tune-ing-*`). Its time per write under a mix is its write, plus its
compactions over the writes, plus each read's time times the reads a write; a phase's time is its
work plus its requests priced by size with a spike (the lab's `sized_latency`, which now prices
the windows, as their probe's mean, the sorted read and compactions). The reads are the probes'
of steps 5 to 30: the full read, a window and the newest 1% under `cached-skip`, the sorted read
under `cached`. Requests and bytes come from havoc's runs. The work is predicted from the counts
by a linear formula fitted on the laptop's `windows` runs (each kind of phase: the write's parts,
the compaction and each read; per arm where that predicted better; bytes read and written,
requests, and for the sorted read rows times log2 of the row groups it holds open), at the arm's
pace on the laptop for that phase. Held out one configuration at a time on those runs, its error
on a configuration's and arm's mean is 2.4% for the write (median; at most 12%), 3–5% for the
full, window and newest reads (at most 27–34%, where an arm's only updates configuration is held
out) and 17% for the sorted read (at most 28%).

**The mixes** (reads a write): `writes` (1 window and 0.1 full read), `windows` (10 windows),
`scans` (2 full reads), `sorted` (1 sorted read). **The stores:** the base point's spikes on S3
Express (`spike/s3x-spike-20261031T163533Z`) and Azure Blob (`spike/azure-spike-20261031T234510Z`).

**The grid** (`configs/tune-*.json`, campaign `docker/campaigns/tune.json` on havoc), each a
`win-*` configuration with one knob moved, the windows and readers as in `windows`:
- updates: MoR compacted every 3, 6 and 12 writes (probes every 5 writes fall at every point of
  those cycles; `win-upd-base-compact`'s 10 probes half its reads just after a compaction); the
  deferred merges (deltas by region) at θ 0.12, 0.5 and 1 (θ 1 never merges a row group whose rows
  are not all gone); RMoW-append and the deferred merges at θ 0.06 with row groups of 4, 8 and 32
  MiB;
- inserts: MoR compacted every 3, 6 and 12 writes; RMoR's tail sealed at 256 MiB (fanout 4:
  16-write units, each row rewritten twice), and at 160 MiB with fanouts of 2 (8-write units,
  three times) and 8 (8-write units, once); row groups of 4 MiB for the appending arms (MoR, RMoR,
  RMoW-append) and for RMoR sealed at 256 MiB. A seal of 1024 MiB would do what 256 does in 30
  writes (a 64-write unit cannot form), so it is not run.

**The finalists:** under each workload, store and mix, the tuner's two cheapest policies. Those
the laptop has not run (`tune-*`) run there with the same configurations, for instructions.

**Predicted:**
- **T1 (the formula, held out): on each finalist the laptop runs, the predicted work of each arm's
  write and of its full, window and newest reads is within 15% of the measured (median within
  8%), and of its sorted read within 30%. Its time per write under each mix is within 10%.**
- **T2 (the picks): under each of the 16 workloads, stores and mixes, the tuner's first policy,
  priced with the measured work, costs at most 5% more than the cheapest finalist priced the same
  way.**
- **T3 (the updates' knobs, counts on havoc):**
  - the deferred merges write at most 0.6 of θ 0.06's bytes at θ 0.12 and at most 0.2 at θ 0.5
    and 1 (model 0.47, 0.095, 0.076), and their windows read at most 1.5 times θ 0.06's bytes at
    every θ (model 1.01, 1.24, 1.34): deltas kept by region keep a window cheap without merging;
  - RMoW-append's windows read at most 0.6 of its bytes at 16 MiB with row groups of 4 MiB, and at
    most 0.85 at 8 MiB. A statement changes rows across its whole region, so every row group of
    the region is rewritten at any size: its writes are at most 1.1 times 16 MiB's bytes at 4 and
    8 MiB, and at least 1.5 times at 32 MiB, where a row group holds two regions.
- **T4 (the inserts' knobs, counts on havoc):**
  - RMoR sealed at 256 MiB writes at most 1.5 times its bytes sealed at 160 MiB, and its windows
    read at most 0.85 of theirs (model 1.24 and 0.74); at 160 MiB a fanout of 2 writes at most 1.6
    times and reads at most 0.8 (model 1.41 and 0.65), and a fanout of 8 is within 15% of the
    fanout of 4 on both (model 1.00 and 0.92);
  - with row groups of 4 MiB the appending arms' windows read at most half their bytes at 16 MiB,
    and their sorted read holds at most half: a write's late rows sort first, into its first row
    group, so only that one spans back across the table.

Bytes are judged under `cached-skip`: a write's as the mean of the 30, a window's as the mean of
the five after the 30th write, the sorted read's held bytes after the 30th.

**Measured, not judged:** each mix's ranking and the picks; the time per write over a sweep of
mixes; requests; MoR's compaction intervals.

**Limits:**
- **The runs:** one seed, 30 writes, one table size. The policies that never compact (RMoW-append,
  the deferred merges) keep dead bytes the 30 writes do not price: storage is not in the cost.
- **Time is simulated** at the laptop's pace. The base point's S3 Express VM did the same work in
  1.3–1.6 times the laptop's thread time, alike across designs; writes, which are more work than
  reads, would weigh a little more there.
- **The sorted read** on the updates runs under `cached`, which reads RMoR's and the deferred
  merges' dead row groups.
- **Azure Blob's** block blobs past 256 MiB read slower than their price (RMoR's full reads, 0.66
  of the measured at the base point); no policy here writes a file that large (RMoR's 256 MiB
  seal makes units of about 190 MB).

## The tuner's picks in the cloud (registered 2026-11-08)

**What it decides:** whether the tuner's picks hold on the stores' measured time. T2 held on the
laptop's measured work, but every price so far is the laptop's work at the laptop's pace plus
requests priced by the base point's spikes; no cloud run has timed these policies. And how well
a policy's time per write is priced on S3 Express when the VM's own work and spike price it.

**Setup:** the four configurations of ROADMAP's "The picks in the cloud", seven policies:
- updates: MoR compacted every 12 writes (`tune-upd-mor-c12`, 16 MiB row groups); RMoW-append and
  the deferred merges (θ 0.06) with 4 MiB row groups (`tune-upd-rg4m`);
- inserts: MoR, RMoR with row groups kept and RMoW-append with 4 MiB row groups
  (`tune-ing-rg4m`); RMoR sealed at 256 MiB with 4 MiB row groups (`tune-ing-rg4m-seal256`).

On S3 Express they hold every pick of T2's mixes, or a policy within 0.4% of it (MoR compacted
every 6 writes is the updates' pick under `writes` and `sorted`, at 5.82 s and 13.14 s). On Azure
Blob they hold only the inserts' pick under `windows` (and the updates' within 2.1%: RMoW-append
with 8 MiB row groups); elsewhere the run judges the order of these seven.
Each runs once at be8d6f1, in a 16 GB heap, with requests and held memory logged. Since havoc's
runs (7cabf65), the code has changed only by the fuzzer and its two fixes (RMoR's cleaner above a
share of 1; a dead open tail file leaving the tail policy), neither in these configurations'
settings.
- **S3 Express:** one `c7a.4xlarge` per configuration in usw2-az3, the four at once on one
  bucket. Each VM runs its twin (the configuration on its own disk, with instruction counts),
  then its spike, then its run.
- **Azure Blob:** one `D8as_v6` in westus: a spike, then the four in turn. Azure's VMs count no
  instructions, so it runs no twin.

**Measured time per write:** the tuner's cost with each phase's measured wall time in place of its
price: the mean write over the 30, plus the compactions over the 30 writes, plus each read's mean
over the probes of steps 5 to 30 (the full read, a window as its probe's mean of five, and the
newest 1% under `cached-skip`; the sorted read under `cached`) times the mix's reads a write.

**Predicted** by the tuner (`analysis/tune.py --measured`: the laptop's work at its pace, requests
priced by the base point's spikes), time per write in seconds, the pick among the seven in bold:

S3 Express:

| Workload | Policy (configuration, arm) | writes | windows | scans | sorted |
|---|---|---|---|---|---|
| updates | MoR compacted every 12 writes (`tune-upd-mor-c12`, `mor-v3`) | **5.84** | 10.11 | 20.49 | 13.47 |
| updates | RMoW-append, 4 MiB row groups (`tune-upd-rg4m`, `rmow-c`) | 6.97 | **7.34** | **19.50** | **13.15** |
| updates | the deferred merges, 4 MiB row groups (`tune-upd-rg4m`, `rmor-density`) | 7.56 | 10.44 | 23.31 | 22.93 |
| updates | *runner-up over the pick* | *1.194* | *1.378* | *1.051* | *1.024* |
| inserts | MoR, 4 MiB row groups (`tune-ing-rg4m`, `mor-v3`) | **2.03** | 6.97 | **17.91** | 11.10 |
| inserts | RMoR sealed at 256 MiB, 4 MiB row groups (`tune-ing-rg4m-seal256`, `rmor-c`) | 2.22 | **3.89** | 17.94 | **10.61** |
| inserts | RMoW-append, 4 MiB row groups (`tune-ing-rg4m`, `rmow-c`) | 3.23 | 18.62 | 18.01 | 11.16 |
| inserts | RMoR with row groups kept, 4 MiB (`tune-ing-rg4m`, `rmor-c-kept`) | 3.22 | 19.02 | 18.09 | 11.16 |
| inserts | *runner-up over the pick* | *1.092* | *1.789* | *1.002* | *1.046* |

Azure Blob:

| Workload | Policy (configuration, arm) | writes | windows | scans | sorted |
|---|---|---|---|---|---|
| updates | MoR compacted every 12 writes (`tune-upd-mor-c12`, `mor-v3`) | **8.78** | 16.59 | **27.77** | **18.25** |
| updates | RMoW-append, 4 MiB row groups (`tune-upd-rg4m`, `rmow-c`) | 10.82 | **11.53** | 46.35 | 27.81 |
| updates | the deferred merges, 4 MiB row groups (`tune-upd-rg4m`, `rmor-density`) | 14.20 | 18.98 | 55.89 | 54.36 |
| updates | *runner-up over the pick* | *1.232* | *1.439* | *1.669* | *1.524* |
| inserts | MoR, 4 MiB row groups (`tune-ing-rg4m`, `mor-v3`) | 4.75 | 18.16 | 49.68 | 26.99 |
| inserts | RMoR sealed at 256 MiB, 4 MiB row groups (`tune-ing-rg4m-seal256`, `rmor-c`) | **4.46** | **8.42** | 49.54 | 26.53 |
| inserts | RMoW-append, 4 MiB row groups (`tune-ing-rg4m`, `rmow-c`) | 5.56 | 28.21 | **44.32** | **24.35** |
| inserts | RMoR with row groups kept, 4 MiB (`tune-ing-rg4m`, `rmor-c-kept`) | 5.60 | 29.40 | 44.54 | 24.39 |
| inserts | *runner-up over the pick* | *1.065* | *2.156* | *1.005* | *1.002* |

- **K1: each cloud run sends what havoc's run of its configuration counts** (a control): every
  record's requests, bytes and table state, and every one-thread phase's held peaks; each S3
  Express twin likewise.
- **K2: the picks hold.** On each store, under each workload and mix where the tuner prices the
  runner-up among these policies more than 5% above its pick, the pick's measured time per write is
  at most 5% above the cheapest measured. Judged: on S3 Express the updates under `writes`,
  `windows` and `scans` and the inserts under `writes` and `windows`; on Azure Blob the updates
  under every mix and the inserts under `writes` and `windows`.
- **K3: on S3 Express each policy's time per write under each mix is priced within 20%** by its
  twin's work (on the VM's own cores, at its arm's pace there) and its VM's spike.

**Measured, not judged:** the near ties (on S3 Express the updates under `sorted` and the inserts
under `scans` and `sorted`; on Azure the inserts under `scans` and `sorted`); each phase's measured
time against its price; Azure's prices, from the S3 Express twins' work (both VMs Zen 4) and its
spike; the VMs' pace against the laptop's; the spikes under the fleet's load against the base
point's alone; the laptop-priced predictions above against the twin-priced ones.

**Limits:** one run of each configuration, one seed, 30 writes. The S3 Express runs share one
bucket, each measured under the others' load; the Azure runs come one after another over some
hours, and Azure's read waits moved by seconds between runs before (lab/pages/rmor-rerun.md). The
predictions price CPU at the laptop's pace, and the base point's S3 Express VM did the same work in
1.3–1.6 times the laptop's thread time: where a mix's order turns on a write, which is more work,
against a read, the VMs' pace can move it.

## Copies cut where a write changes, at Iceberg's row-group size (registered 2026-11-09)

**What it decides:** whether RMoR can choose what a write copies at a finer grain than a row group,
without a new reader. Every two-workload run so far used 4–32 MiB row groups on a 1 GB table,
where an update region (513,700 rows) is about one 16 MiB row group, so copying whole row groups
cost little. At Iceberg's 128 MiB a row group holds about eight regions. A write that changes one
reads all eight (MoR, and the deferred merges as built), or copies all eight (RMoR with row groups
kept, RMoW-append).

**The cut:** when RMoR copies a row group, it copies all of it, but as several row groups. Each run
of strides that the write changes, or that holds dead rows, becomes a row group of its own, and so
does each run of strides between them. A run of fewer than four unchanged strides joins the copy
beside it. A stride is the 20,000 rows at which every column starts a new page. At 128 MiB every
column of this table cuts its pages there (checked 2026-11-09, docs/drafts/region-cut-2026-11-09.md).
The old row group is then dead as a whole, and the reader that skips dead row groups (`cached-skip`)
skips it. A later write that changes the same region copies only that region's row group. The plan
is the draft named above; the cut is not built yet.

**The model** (`analysis/hybrid_model.py updates`, 2026-11-09):
- **The op log:** `win-upd-base`'s workload with 8 times the initial rows (205,480,000, about
  6.7 GB), 150,000 changes a write, over 240 writes.
- **The layout:** 128 MiB row groups in 512 MiB files (`--rg-mib 128 --target-mib 512
  --read-every 40`).
- **The reader:** its `rg_*` reader, which reads every live row group its statistics don't prune,
  as `cached-skip` does.
- **The policies:** `mor`, `morc:10`, `copy:file:rg:0.06` (the deferred merges as built), and the
  cut: `copy:split:rg` (θ 0) and `copy:split:rg:0.06`. Over the same workload's first 30 writes
  (its 30-write op log, `--read-every 5`), `copy:file:rg` and `copy:place:rg`: whole row groups
  copied, as RMoR with row groups kept and RMoW-append do.
- **Its rows** are 28.6 bytes; Parquet's at 128 MiB are about 31. So the predictions are ratios
  between designs, or against live bytes.

| Design (arm) | MB written a write: mean, first 40, last 40 | MB read a write: mean, last 40 | 1% window / live bytes, write 240 | Full read / live bytes, write 240 |
|---|---|---|---|---|
| MoR (`mor-v3`) | 3.6 | 1,594; 1,939 | 16.7 | 1.18 |
| MoR compacted every 10 writes | 3.6 | 1,640; 2,069 | 7.0 | 1.15 |
| Deferred merges as built (`rmor-density`, θ 0.06) | 55; 15; 74 | 1,304; 1,413 | 3.3 | 1.03 |
| The cut, θ 0 (`rmor-c-kept` with the cut) | 218; 369; 162 | 231; 173 | 1.01 | 1.00 |
| The cut, θ 0.06 (`rmor-density` with the cut) | 52; 13; 57 | 763; 477 | 1.69 | 1.03 |

Over 30 writes, whole row groups copied (`rmor-c-kept`, `rmow-c`) write 861 MB a write, against the
deferred merges' 19 MB and MoR's 3.6. Their windows read 3.3 times their live bytes.

Priced on S3 Express with the laptop's CPU per MB (a scratch pricing of the model, not a lab view),
the cut at θ 0 takes 7.0 s a write over writes 201–240. That is as fast as the footer re-cut the
model also ran, against 16.4 s for the deferred merges as built and 19.4 s for MoR. Its 1% window
takes 0.56 s at write 240, against 1.79 and 10.5 s.

**Setup:**
- **Configurations:** `configs/d128-upd-*.json`.
  - **Common to all:** `win-upd-base`'s workload with 205,480,000 initial rows, so the op logs are
    the model's; 128 MiB row groups in 512 MiB files.
  - **Reads:** every 40 writes, a full read, the newest 1% and five 1% windows by id, under
    `cached-skip` alone. No time-travel read: `travelBack` is past the last write.
  - **One setting a configuration,** so they run apart: `d128-upd-mor` (MoR),
    `d128-upd-compact` (MoR compacted every 10 writes), `d128-upd-density` (the deferred merges as
    built, θ 0.06, deltas by region), each over 240 writes; `d128-upd-copies` (RMoR with row groups
    kept, RMoW-append) over 30 writes, read every 10.
- **After the cut is built:** `d128-upd-cut` (RMoR with row groups kept, cut) and
  `d128-upd-density-cut` (the deferred merges, cut), over the same 240 writes.
- **Where:** on havoc (campaign `docker/campaigns/density128.json`), with requests and held
  memory logged. No laptop twins: these are byte claims, which need no instruction counts.

**How it is judged:**
- **When:** after the 240th write (the 30th for `d128-upd-copies`), under `cached-skip`.
- **A window:** the mean of the five.
- **A write's reads:** its update and merge reads (`update-read`, `merge-read`; the model counts
  the locate apart).
- **A write's writes:** everything it puts (`write`, `delete-write`).
- **Live bytes:** a window's rows times the bytes a live row takes in that arm's full read.

**Predicted:**
- **D1 (whole row groups read, as built): MoR, compacted MoR and the deferred merges each read
  at least 1 GB a write over the 240 writes, and at least 15 times what they write** (model 1.59,
  1.64 and 1.30 GB, against 3.6, 3.6 and 55 MB written). **After the 240th write, MoR's windows
  read at least 8 times their live bytes, compacted MoR's at least 4 times and the deferred
  merges' at least 2.5 times** (model 16.7, 7.0 and 3.3).
- **D2 (whole row groups copied, 30 writes): RMoR with row groups kept and RMoW-append write at
  least 30 times the deferred merges' bytes a write** (model 861 against 19 MB, 45 times), **and
  their windows after the 30th write read at least 2.5 times their live bytes** (model 3.3).
- **D3 (the cut, θ 0):**
  - **Over writes 201–240, a write reads at most 0.2 of what the deferred merges as built read**
    (model 173 against 1,413 MB, 0.12), **and writes at most 3 times their bytes** (162 against
    74, 2.2).
  - **Its first 40 writes write at least 1.8 times its last 40** (369 against 162, 2.3): the copies
    of whole 128 MiB row groups end once the regions in them are cut out.
  - **After the 240th write, its windows read at most 1.3 times their live bytes, and its full read
    at most 1.05 times** (model 1.01 and 1.00).
- **D4 (the cut, θ 0.06):**
  - **Over writes 201–240, a write reads at most 0.5 of the deferred merges' bytes** (477 against
    1,413, 0.34), **and writes 0.5 to 1.2 times theirs** (57 against 74, 0.77).
  - **After the 240th write, its windows read at most 2.2 times their live bytes** (model 1.69).

**Controls:**
- **The op logs:** every 240-write run replays the model's op log, and `d128-upd-copies` the
  30-write one (the SHA-256 of `oplog.bin`).
- **The build:** with the cut off, every arm sends what it sent before the build, in requests and
  bytes. Checked on the laptop on `win-upd-base` (`steps_diff`).

**Measured, not judged:**
- requests: at 16 MiB the model counted 3–14 for a window where RMoR and the deferred merges sent
  25–65;
- each design's row groups and files after 240 writes (the model: 756 row groups for the cut,
  540 for the deferred merges, 288 for MoR);
- held memory;
- time, simulated on S3 Express and Azure Blob from requests by size and the laptop's CPU per MB,
  as the forecasts were.

**Limits:**
- **One seed, one workload.** A statement changes 3% of a region's rows, spread evenly, so every
  stride of the region changes and the cut follows regions. The density of the deletion vectors
  has nothing finer to find here; a workload of sparse changes would.
- **A narrow table.** 128 MiB is about 4.3 million of these 11-column rows. A wide table reaches
  128 MiB in fewer rows, and pays for small row groups in a request a column.
- **Cold pieces never merge back.** The cut only splits.
- **No page-skipping reader.** It is planned separately (the draft above).
- **No instruction counts on havoc.** Time is simulated, as in the forecasts.

## The two workloads in the cloud (registered 2026-11-10)

**What it decides:** whether the workloads page's trade-offs hold on the stores' measured time.
Every number on that page is simulated: the laptop's work at its pace, plus havoc's requests
priced by the registered latency tests. The cloud runs so far timed the calibration, the base
point, the sweeps, RMoR's rerun and the tuner's four picks, never these designs side by side on
the two workloads. The page's claims: on the updates the region designs write in about twice MoR's time
and read a range 2 to 4 times as fast, even against MoR compacted; on the inserts, compacting MoR
or sealing RMoR's tail reads a range 2 to 3 times as fast as appending, for under half a second
more a write.

**Setup:** five configurations, `configs/wl-*.json`: the windows campaign's main points (2026-11-07)
with the two reader changes of 6db916a, which every design takes. The locate prunes row groups by
each changed key (`layout.locate` `keys`), and the sorted read skips dead row groups. Each reads
through the skipping reader alone (`cached-skip`, the sorted read too) every 5 writes, and reads
no old version (`travelBack` 1000). The table and workloads are those of 2026-11-06: 25.7 million
rows laid out by id in 16 MiB row groups, and 30 writes.
- **Updates** (150,000 changes a write as statements of 3% of a region's rows, a sixth of them
  deletes; five windows of 1% by id at each probe): `wl-upd-base` runs MoR (`mor-v3`), RMoR
  (`rmor-c-kept`), RMoW-external (`rmor-c-footer-kept`), RMoW-append (`rmow-c`) and the deferred
  merges (`rmor-density`, θ = 0.06); `wl-upd-compact` runs MoR compacted every 10 writes.
- **Inserts** (350,000 rows a write, late by a median of 0.05 writes and a p99 of 5; five windows
  of 1% by event time): `wl-ing-base` runs MoR appending (and RMoR and RMoW-append, not judged);
  `wl-ing-compact` runs MoR compacted every 10 writes; `wl-ing-seal160` runs RMoR (`rmor-c`) and
  RMoW-append (`rmow-c`) with the tail sealed at 160 MiB.

Each runs once at 0942497, in a 16 GB heap, with requests and held memory logged.
- **S3 Express:** one `c7a.4xlarge` per configuration in usw2-az3, on one bucket. Four run at
  once (the account's 64 vCPUs), and `wl-ing-compact` starts once `wl-upd-compact`'s VM is down.
  Each VM runs its twin (the configuration on its own disk, with instruction counts), then its
  spike, then its run.
- **Azure Blob:** one `D8as_v6` in westus: a spike, then the five in turn, shortest first. Azure's
  VMs count no instructions, so it runs no twin.

**The references** (campaign `workloads-ref`): havoc's runs at b49a57f for requests, bytes and held
memory, and the laptop's at 0942497 for instructions. 0942497 adds only the fuzzer's
`layout.eagerBytes`, which these leave unset, and every record's requests and bytes agree between
the two builds in all five configurations. The laptop's first runs (2026-11-10, 20:42–21:31 UTC)
ran beside another session's model checks at 3.0–3.2 GHz against 4.6, and are excluded; the
reruns ran at 4.49–4.59 GHz.

**Measured latency:** as the workloads page has it. A write's mean over the 30, with the
compactions spread over them; each read's mean over the probes of steps 5 to 30. Each phase's
time is its wall time on its VM.

**Forecast** (lab/pages/workloads-cloud.md: the laptop's work at its pace, havoc's requests priced
by the registered spikes of 2026-10-31), in seconds:

S3 Express:

| Workload | Design (configuration, arm) | a write | a 1% window | a full scan | the sorted read |
|---|---|---|---|---|---|
| updates | MoR (`wl-upd-base`, `mor-v3`) | 4.15 | 0.92 | 8.30 | 9.55 |
| updates | MoR compacted (`wl-upd-compact`, `mor-v3`) | 4.01 | 0.47 | 8.02 | 8.66 |
| updates | RMoR (`wl-upd-base`, `rmor-c-kept`) | 9.82 | 0.33 | 7.21 | 7.53 |
| updates | RMoW-external (`wl-upd-base`, `rmor-c-footer-kept`) | 9.66 | 0.25 | 7.02 | 7.28 |
| updates | RMoW-append (`wl-upd-base`, `rmow-c`) | 9.24 | 0.24 | 6.99 | 7.30 |
| updates | deferred merges (`wl-upd-base`, `rmor-density`) | 5.45 | 0.42 | 8.15 | 8.57 |
| inserts | appending (`wl-ing-base`, `mor-v3`) | 0.49 | 1.92 | 8.84 | 10.82 |
| inserts | MoR compacted (`wl-ing-compact`, `mor-v3`) | 0.96 | 0.70 | 8.77 | 9.83 |
| inserts | RMoR sealed (`wl-ing-seal160`, `rmor-c`) | 0.85 | 0.91 | 8.80 | 9.99 |
| inserts | RMoW-append sealed (`wl-ing-seal160`, `rmow-c`) | 0.86 | 1.88 | 8.76 | 10.19 |

Azure Blob:

| Workload | Design (configuration, arm) | a write | a 1% window | a full scan | the sorted read |
|---|---|---|---|---|---|
| updates | MoR (`wl-upd-base`, `mor-v3`) | 6.53 | 1.99 | 11.50 | 12.76 |
| updates | MoR compacted (`wl-upd-compact`, `mor-v3`) | 5.77 | 0.72 | 10.33 | 10.98 |
| updates | RMoR (`wl-upd-base`, `rmor-c-kept`) | 11.01 | 0.49 | 9.29 | 9.61 |
| updates | RMoW-external (`wl-upd-base`, `rmor-c-footer-kept`) | 11.15 | 0.34 | 9.04 | 9.30 |
| updates | RMoW-append (`wl-upd-base`, `rmow-c`) | 10.21 | 0.30 | 8.98 | 9.28 |
| updates | deferred merges (`wl-upd-base`, `rmor-density`) | 7.27 | 0.68 | 11.12 | 11.54 |
| inserts | appending (`wl-ing-base`, `mor-v3`) | 0.50 | 2.84 | 11.74 | 13.72 |
| inserts | MoR compacted (`wl-ing-compact`, `mor-v3`) | 1.04 | 0.95 | 11.30 | 12.37 |
| inserts | RMoR sealed (`wl-ing-seal160`, `rmor-c`) | 0.91 | 1.20 | 11.27 | 12.46 |
| inserts | RMoW-append sealed (`wl-ing-seal160`, `rmow-c`) | 0.97 | 2.12 | 10.90 | 12.34 |

**What the reader changes moved**, against the windows campaign's runs as the workloads page
prices them:
- **Writes:** the locate by key takes 12–16% off MoR's write and 12–17% off the deferred merges',
  3–8% off RMoR's and RMoW-external's, and nothing off RMoW-append's.
- **The sorted read:** skipping dead row groups brings RMoR's to 0.31 of its time and the
  deferred merges' to 0.53. The page priced the sorted read under `cached`.
- **Everything else:** within 1.5% on the updates' other reads, and within 2.5% on the inserts.

- **L1: each cloud run sends what havoc's run of its configuration counts** (a control): every
  record's requests, bytes and table state, and every one-thread phase's held peaks; each S3
  Express twin likewise.
- **L2: on the updates, the region designs trade a slower write for a faster range read.** On each
  store:
  - RMoR, RMoW-external and RMoW-append each write slower than MoR and than MoR compacted
    (forecast 2.2–2.5 times on S3 Express, 1.6–1.9 on Azure);
  - each of them reads a 1% window faster than both (1.4–3.9 and 1.5–6.7 times);
  - MoR compacted reads a window faster than MoR (2.0 and 2.8 times);
  - the deferred merges read a window faster than MoR (2.2 and 2.9 times), and write faster than
    each region design (1.7–1.8 and 1.4–1.5 times).
- **L3: on the inserts, merging late rows trades a little write time for a faster range read.**
  On each store, MoR compacted and RMoR sealed each:
  - read a 1% window faster than appending (2.1–2.7 times on S3 Express, 2.4–3.0 on Azure);
  - write slower than it (1.7–2.1 times), but by less than a second a write (forecast
    0.36–0.53 s).
- **L4: on S3 Express each design's write and each of its reads is priced within 15%** by its
  twin's work (on the VM's own cores, at its arm's pace there) and its VM's spike. In the tuner's
  cloud run these came within 0–12% (K3 judged whole mixes at 20%).

**Measured, not judged:**
- **The near pairs:**
  - the full scans and sorted reads, where the region designs' forecast is 11–28% faster than
    either MoR's on the updates, and the designs are within 11% of each other on the inserts;
  - MoR compacted's write against MoR's (0.97 and 0.88 times);
  - the deferred merges' write against MoR's and MoR compacted's (1.11–1.36 times), and their
    window against MoR compacted's (1.07–1.11);
  - RMoW-append sealed's window against appending's (1.02 and 1.34).
- **Each operation's measured time against its forecast**, on each store, and so the VMs' pace
  against the laptop's.
- **Azure's prices**, from the S3 Express twins' work (both VMs Zen 4) and its spike. In the
  tuner's cloud run they came within 0.82–1.20 of the measured time.
- **Latency by snapshot over the 30 writes.** On the page, MoR's write and range read grow as its
  deletes pile up, and RMoW-append's stay level.
- **Where the measured time goes:** decoding, encoding, the store, and pauses.
- **The other two designs of `wl-ing-base`.**
- **The spikes under the fleet's load.**

**Limits:**
- **Scale:** one run of each configuration, one seed, 30 writes.
- **Shared and sequential runs:** the S3 Express runs share one bucket, each measured under the
  others' load, and the fifth runs after the first four. The Azure runs come one after another over
  three to four hours, and Azure's read waits have moved by seconds between runs before
  (lab/pages/rmor-rerun.md).
- **Short reads:** the window reads of the region designs and of MoR compacted take a fraction of
  a second, so a few requests' first bytes decide them.
- **The forecast's pace:** it prices CPU at the laptop's pace. The region designs' writes are
  55–64% CPU in the forecast, against MoR's 26–41%, so the VMs' pace moves their writes more than
  MoR's.

## Every design on both workloads, at Iceberg's sizes (registered 2026-11-12)

**What it decides:** whether each design's mechanism, defined once and run on both workloads at
Iceberg's 128 MiB row groups, spares readers what it costs writers. The runs so far did not
exercise the mechanisms:
- **The inserts never merged a region.** A region was a range of ids, and every insert takes a new
  id, so no write rewrote one. The region designs differed there only by a tail merge.
- **On the updates at 16 MiB, a region (513,700 rows) is about one row group,** so copying part of
  a row group (the cut, RMoR-density) did not matter. At 128 MiB it does (D1–D4, above).

**What is new (built 2026-11-12):**
- **The event-time merge** (`layout.inserts` `event-time`, `EventTimeRegions`). The region designs
  put each insert into the live row group whose event-time range holds it: in a gap, the one
  before; before the first, the first. They rewrite that row group as they rewrite one an update
  changes, so no two row groups overlap in event time.
  - A row group grown past the rows of a full one (the initial load's largest) is cut, never
    between two rows of one event time. The newest, which rows on time join, is cut into full row
    groups and the rest. An older one, which only late rows grow, is cut once it holds two full
    ones' rows, into equal parts.
  - RMoR-density merges a row group once the rows that join it, those waiting included, reach θ of
    its rows. Until then they wait in delta row groups, as updated rows do.
  - The writer finds the row groups from its index of them, and reads the footer of each file
    whose event-time bounds overlap them (phase `locate`), checking each placement against the
    footer's statistics.
  - Moot without inserts. Refused with a workload that also updates or deletes, and with
    compaction (not built for them).
- **MoR (compacted) as Iceberg plans it** (`compactEvery` 1, `compactMinInputFiles` 5). After every
  write, the small files (under 0.75 of the target size) and those with 30% of their rows deleted
  form one group. It is rewritten when it holds at least 5 files, or more than one and over the
  target size, or a file with 30% deleted: `BinPackRewriteFilePlanner`'s defaults (its groups reach
  100 GB). Before, MoR compacted every 10 writes, any two small files.
- **Range reads at every write** (`reads.rangeEvery` 1): the newest 1% and the five 1% windows
  after every write, and the full read and the sorted read every 10.

**The model** (`analysis/event_time_model.py`, the inserts; its docstring has the check against
the harness at a small scale, which it matched within 10%):
- **At 128 MiB** (3.54 million rows a row group, 58 in the initial load, 30.1 bytes a row), the
  event-time merge with every late row merged at once (θ 0: RMoR, RMoW-external, RMoW-append)
  rewrites 2.3 GB a write, about 20 row groups, where MoR writes 10.5 MB.
- **Why:** lateness is lognormal, median 0.05 writes and p99 5, with a long tail: of a write's
  350,000 rows about 1,300 come more than 10 writes late and 21 more than 100. Each reaches back to
  another old row group, which the write rewrites whole. Over 240 writes that is 552 GB.
- **RMoR-density** (θ 0.06) writes 64 MB a write, 15 GB over 240 writes.
- **The updates** as D1–D4 measured them: the cut 172 MB a write, RMoR-density cut 56.6, whole
  copies 906.

**Setup:**
- **Configurations:** `configs/w128-{upd,ing}-{mor,compact,rmor,external,append,density}.json`,
  one design each.
  - **The table** of D1–D4: 205,480,000 initial rows in event-time order, 128 MiB row groups in
    512 MiB files, seed 1. Updates: D1–D4's 150,000 changes a write. Inserts: 350,000 a write,
    lateness median 0.05 writes and p99 5.
  - **The designs:** MoR (`mor-v3`), MoR (compacted) as above, RMoR (`rmor-c-kept`), RMoW-external
    (`rmor-c-footer-kept`), RMoW-append (`rmow-c`) and RMoR-density (`rmor-density`, θ 0.06, a
    delta row group per region). The cut on RMoR, RMoW-external and RMoR-density (it is not built
    for RMoW-append). The event-time merge on the four region designs.
  - **240 writes,** except where every write copies whole 128 MiB row groups: RMoW-append on the
    updates (906 MB a write, D2), and RMoR, RMoW-external and RMoW-append on the inserts (2.3 GB
    in the model). These run the first 30 writes of the same op log; 240 would store 217 and 552 GB.
  - **Reads,** under `cached-skip` with the sorted read: the newest 1% and five 1% windows (by id on
    the updates, by event time on the inserts) after every write; the full read and the sorted read
    every 10 writes. Payload checks off.
- **Where:** on havoc (campaign `docker/campaigns/w128.json`, the user's runs), with requests and
  held memory logged. As for D1–D4, the claims are bytes, which need no laptop twins.

**How it is judged:**
- **When:** after the last write (the 240th, or the 30th), under `cached-skip`.
- **A window:** the mean of the five, or the one named.
- **A write's bytes:** `write` and `delete-write`, and `compact` for MoR (compacted), a mean over
  the run's writes.
- **Live bytes:** as D1–D4.

**Predicted:**
- **W1 (inserts, merging every late row): over their 30 writes, RMoR, RMoW-external and RMoW-append
  write at least 100 times MoR's bytes a write** (model 2,317 against 10.5 MB, 220 times), **and
  RMoR-density over its 240 at most 10 times** (64 MB, 6.1 times).
- **W2 (inserts, what readers get): after the 240th write, RMoR-density's window at 0.9 of the
  event-time span, among the inserted rows, reads at most 3 times its live bytes and at most a
  third of MoR's bytes, and its sorted read holds at most 0.4 GB where MoR's holds at least 1 GB.**
  - MoR's window reads the batch files written after its rows, about 80 of 10.5 MB, whose bounds
    all reach back past it: about 10 times its 87 MB live.
  - RMoR-density's reads one or two row groups and the delta row groups waiting for them: 1.2 to
    2.5 times live.
  - Every MoR batch file's most belated rows reach back to the table's start, so its sorted read
    opens all 240 at once (2.5 GB). RMoR-density's opens a row group at a time, and its delta row
    groups.
- **W3 (updates, D1–D4 again, now with RMoW-external): after the 240th write, RMoR's,
  RMoW-external's and RMoR-density's windows read at most 1.3, 1.3 and 2.2 times their live bytes,
  and MoR's at least 8 times** (D3 and D4 measured 1.16 and 1.60, D1 14.4). **RMoW-external writes
  within 5% of RMoR's bytes a write:** the same copies, with a footer appended where RMoR writes a
  DV.

**Controls:**
- **The op logs:** each run replays its workload's 240-write op log, or its first 30 writes (the
  SHA-256 of `oplog.bin` against a 30-write log of the same seed).
- **The build:** with the new knobs off, every arm sends what it sent before. Checked on the laptop
  on `wl-upd-compact` and `wl-ing-base` against their runs at 0942497.
- **Range reads change no write:** `SuiteKnobsTest` checks every write record equal with and
  without `rangeEvery`.

**Measured, not judged:**
- MoR (compacted) under Iceberg's rule on both workloads: how often it compacts, what it writes,
  and its windows;
- the θ 0 designs' reads on the inserts: over 30 writes the inserted rows are 5% of the table, so
  only the newest 1% and the sorted read fall among them;
- requests, the newest 1%, held memory, row groups and files;
- time, simulated on S3 Express and Azure Blob as before.

**Limits:**
- **One seed, one lateness.** No watermark drops late rows, as a stream processor past its allowed
  lateness would; the θ 0 cost on the inserts comes from that tail.
- **30 writes** for the designs that copy whole row groups every write, so no steady state.
- **What is not built:** the event-time merge with updates or deletes, or with compaction; the cut
  for RMoW-append.
- **The writer's index:** the region designs find their row groups from an index of them, as RMoW
  keeps its footers; an engine without one would read more footers.
