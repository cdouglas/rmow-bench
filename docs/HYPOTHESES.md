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
