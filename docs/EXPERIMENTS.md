# Experiments: a map

Where to start. This page says which runs stand, what each experiment that made them varied and
asked, what it found, and where the write-ups are. It covers the 95 runs that stand as of
2026-11-18, all on the two workloads (updates only, inserts only) on the atomicio fork's build,
and the JMH micro-benchmarks.

Everything run before (the mixed workload, the base point and its sweeps, the calibrations, the
RMoR rerun, the tuner's update policies and its cloud check) was retired on 2026-11-12, with its
lab pages ([Retired runs](#retired-runs)). What it taught and decided is in
[LESSONS.md](LESSONS.md). This file as it stood then, with every retired experiment's row, its
findings and the verdicts of H1–H6, C1–C8, V1–V7, A1–A4, R1–R3, W1–W3, T1–T4 and K1–K3 (2026-11-08), is
`git show bfbdf33:docs/EXPERIMENTS.md`; the retired pages are under `lab/pages/` at the same commit.
On 2026-11-16 the 16 MiB workloads pages, which the 128 MiB results replaced, were retired with the
runs only they read; what they taught is in LESSONS.md (The 16 MiB workloads).

For the trade-off, start with the [results at 128 MiB](../lab/pages/workloads-128.md) (simulated:
every design at Iceberg's 128 MiB row groups on both workloads, its write latency against its
reads, its latency and storage as snapshots are added, where the time goes, and compaction). How
the simulation compares with the cloud is a calibration page,
[simulation-cloud.md](../lab/pages/simulation-cloud.md), on the 16 MiB workloads' main points run
in the cloud on 2026-11-11: the only measured check of the simulation until a cloud run at 128 MiB.

## Where things are

| Document | What it is for |
|---|---|
| [README.md](../README.md) | The question, the five designs, the workloads, and how to build and run. |
| [docs/HARNESS.md](HARNESS.md) | Running, measuring, checking: flags and outputs, the fuzzer, the phases, correctness, what is held, the I/O parity rules, the code's layout, compatibility (the README's reference sections until 2026-11-18). |
| [docs/CONFIG.md](CONFIG.md) | Every arm and every knob of a configuration. |
| [docs/STORES.md](STORES.md) | The S3 and Azure stores, and appends through the atomicio fork. |
| [docs/LESSONS.md](LESSONS.md) | What the retired experiments taught and decided: the measurement method, the stores, the designs dropped, and the results that carry over. |
| [docs/HYPOTHESES.md](HYPOTHESES.md) | The question, and every hypothesis, registered before the runs that decide it. |
| [docs/ROADMAP.md](ROADMAP.md) | The outstanding work, and the decisions that shaped it. |
| [docs/DESIGN.md](DESIGN.md) | The format-level findings: writer rules, footer order, split files, compaction. |
| [docs/PLAN.md](PLAN.md) | The plan as approved on 2026-10-20. Its default matrix is not what ran. |
| `lab/` | The results database and pages (labbook). `lab serve` shows the pages as a site, whose front page lists the campaigns and every run. |
| `results/` | The raw runs, one directory per configuration (`<label>-<hash>/<start>/`). Not in git. |
| `docs/measurements/` | The footer micro-benchmarks' JMH output (2026-10-20), the source of the footer cost in docs/CONFIG.md (`maxRowGroupsPerFile`). |

## The designs

Results pages name the designs for a reader, not as the code does. Each standing run holds some of
these arms:

| Design | Arm | A changed region goes | Its old copy is hidden by |
|---|---|---|---|
| MoR | `mor-v3` | nowhere: updated rows go to the write's new data file | deletion vectors on rows |
| MoR compacted | `mor-v3` with `layout.compactEvery` | the same; every N writes Iceberg's bin-pack rewrites the small files and those with at least 30% of their rows deleted (on these workloads, mostly the files the recent writes added), not the whole table | the same, until compacted |
| RMoR | `rmor-c-kept` | each rewritten row group into the write's new data file, as one row group of its own | a deletion vector covering the old row group, which the skipping reader skips |
| RMoW-external | `rmor-c-footer-kept` | the same | a footer appended to the old file, without the old row group |
| RMoW-append | `rmow-c` | appended to its own object | the object's new footer |
| RMoR-density ("the deferred merges" in the hypotheses and earlier records) | `rmor-density` | deletion vectors, until θ of a region's row group is dead or changed (`layout.defer`); then merged with the rows that wait in delta row groups (by region or by write) | deletion vectors, then a DV covering the row group |
| the cut | `rmor-c-kept` or `rmor-density` with `layout.cut` | as above, but a copied row group is cut where the write changes it, so only changed runs are copied | as above |

On the inserts, RMoR is `rmor-c` with its tail sealed at 160 MiB (inserts merged by arrival into
data files cut into row groups) and RMoW-append is `rmow-c` sealed the same way; "appending" is
MoR, which writes each write's inserts as a file. The 128 MiB suite (HYPOTHESES.md W1–W5,
campaign `w128`, run 2026-11-13/14) runs every design above on both workloads, the inserts placed
by the event-time merge (`layout.inserts` `event-time`): each into the row group its event time
falls in, so no two row groups overlap in event time. On the inserts, RMoR, RMoW-external and
RMoW-append are projected rather than run ([inserts-128.md](../lab/pages/inserts-128.md)): merging
every late row at once would write 2.3 GB a write. Every design reads through the reader that skips
dead row groups (`cached-skip`; the windows campaign's runs also through the other readers). A write
finds its rows by scanning the key column of the files whose key range may hold a changed key
(`layout.locate`): in the 16 MiB `workloads-ref` and `workloads-cloud` runs only the row groups whose
statistics may hold one (`keys`), in the windows campaign's and the 128 MiB `w128` runs every row
group between the write's smallest and largest changed key (`scan`). The 128 MiB updates ran again
under `keys` (campaign `w128-keys`), and again with reads that only count their rows (campaign
`w128-keys-2`, docs/HARNESS.md, Correctness), from which the results page times them. The changes to stock Iceberg's readers and writers are
[Appendix A](../lab/pages/reader-changes.md).

The other arms in the code (`mor-v2-*`, `cow`, `tailcow-*`, `rmow-a`, `rmow-b`, the `-split`
arms, and `rmor-c` and `rmor-c-footer` on the updates) appear in no standing run. docs/CONFIG.md, Arms
describes them; LESSONS.md says why each was dropped.

## The two workloads

Every standing run but the 128 MiB ones replays one of these, seed 1, from the same initial load:

| | updates only (`*-upd-*`) | inserts only (`*-ing-*`) |
|---|---|---|
| initial load | 25,685,000 rows of 11 columns (about 1 GB), 16 MiB row groups in 128 MiB files, zstd | the same |
| writes | 30, after 3 warm-up writes on a 1M-row table | the same |
| a write | 150,000 changes: 5/6 updates, 1/6 deletes | 350,000 inserts, by event time |
| where they fall | clustered by region (regions of 513,700 rows, Zipf skew 1); each statement changes 3% of its region | late rows: median 0.05 writes, p99 5 (1 and 20 in the lateness twins) |
| an update | reads the rows it rewrites (`updateRead` `row`) | none |
| reads, every 5 writes | a full scan, the newest 1%, five 1% windows by key, a sorted read | the same, windows by event time |

`verifyRows` is off: verification runs check each row's id and version, not its payload. Runs before
2026-11-17 checked them inside their reads (docs/HARNESS.md, Correctness). Each configuration ran once a host:
havoc for requests, bytes and held memory, the laptop for instructions, and a cloud VM for wall
time. The 128 MiB campaigns (`d128-upd-*`, `w128-*`) take both workloads on 8 times the rows
(205,480,000, about 6.2 GB) in 128 MiB row groups and 512 MiB files, over 240 writes: `d128` with
reads every 40, `w128` with the range reads after every write and the full and sorted reads every
10.

## The experiments that stand

| Experiment | Question | Varies | Designs | Runs | Write-up |
|---|---|---|---|---|---|
| **reads of a range** (`configs/win-*`, campaign `windows`, havoc and the laptop) | Q1–Q4 of 2026-11-07: what a 1% window by key (updates) or event time (inserts) costs away from the newest rows, and each phase's instructions | the two workloads' configurations of 2026-11-06 (`split`, `seal`; retired, which these equal in every record but the windows') but MoR under Spark's one-pass plan, with `reads.windows`: on the updates MoR, compacted every 10 writes, the deferred merges at θ 0.06 and 0.25 and by write; on the inserts lateness p99 1, 5 and 20, compacted, and the 160 MiB seal | MoR, RMoR, RMoW-external, RMoW-append, the deferred merges | 22, 2026-11-08 at 479edb1: 11 on havoc (10–87 min, 3 cores each) and the same 11 on the laptop (4–33 min) | Q1–Q4 held; the laptop's runs equal havoc's in all 10,197 records. The laptop's runs train the work formula (`analysis/tune.py`) that [simulation-cloud.md](../lab/pages/simulation-cloud.md)'s forecast prices with. |
| **the two workloads' main points** (`configs/wl-*`, campaign `workloads-ref`) | The references for the cloud run (L1–L4 of 2026-11-10) | the `windows` main points with the locate by each changed key and the sorted read skipping dead row groups, read through `cached-skip` alone: on the updates MoR, MoR compacted every 10 writes, RMoR, RMoW-external, RMoW-append and the deferred merges; on the inserts MoR appending and compacted, RMoR and RMoW-append sealed at 160 MiB | as listed | 10: 5 on havoc (2026-11-10 at b49a57f, 7–61 min) and 5 on the laptop at 0942497 (2–18 min; the first five, beside another session's model checks, retired) | The references [simulation-cloud.md](../lab/pages/simulation-cloud.md) simulates and compares with the cloud runs. |
| **the two workloads in the cloud** (`configs/wl-*`, campaign `workloads-cloud`) | L1–L4 of 2026-11-10: do the workloads page's trade-offs hold on the stores' measured time? | the store | as `workloads-ref` | 21, 2026-11-11 at 0942497, once each: 5 on S3 Express (a `c7a.4xlarge` each in usw2-az3, four at once on one bucket, 4–33 min), each after its twin on the VM's disk and its spike; 5 on Azure Blob (one `D8as_v6` in westus, in turn after one spike, 5–47 min) | L1–L4 held. On the updates the region designs took 2.3–2.4 times as long as MoR and MoR compacted to write on S3 Express (1.5–2.2 on Azure Blob) and read a 1% range 1.4–3.9 times as fast (1.5–6.4); on the inserts MoR compacted and RMoR sealed read a range 2.1–2.7 times as fast as appending (2.1–2.6) for 0.41–0.47 s more a write (0.45–0.64). From the first five writes to the last five MoR's update write grew 1.84 times on S3 Express (2.40 on Azure Blob), and from the 5th write to the 30th its range read 3.11 times (3.93); the region designs' writes 1.10–1.23 times and their range reads 0.90–1.51. The region designs' update writes are 64–71% CPU on S3 Express (MoR's 41%). Priced from the cloud runs (the twin's work, the VM's spike), within 4.0% and 12.3% of measured (7.7% on S3 Express as judged, before the fix of 2026-11-16 that prices a writer's footer read as the two GETs it is). [simulation-cloud.md](../lab/pages/simulation-cloud.md) (L1, L4: the simulation against the cloud); the measured page (L2, L3) was retired on 2026-11-16 with the 16 MiB workloads (LESSONS.md). |
| **copies cut where a write changes, at 128 MiB** (`configs/d128-upd-*`, campaigns `density128` and `density128-cut`, on havoc) | D1–D4 of 2026-11-09: at Iceberg's 128 MiB row groups, where a row group holds about eight update regions, do the designs as built read or copy whole row groups, and does copying a row group as several, cut where the write changes it (`layout.cut`, a68eda3), let RMoR copy only what changed? | one setting a configuration: MoR, MoR compacted every 10 writes, the deferred merges (θ 0.06, deltas by region), the whole copies, the cut (θ 0) and the deferred merges cut | MoR, RMoR kept, RMoW-append, the deferred merges | 6 on havoc, 3 cores each: 4 at 2025e7c from 2026-11-10 07:45 UTC (90–171 min), 2 at a68eda3 from 10:52 UTC (142–150 min), requests and held memory logged; before them, the cut's control on the laptop (all 2,350 records equal) and a fuzz round on havoc (1,274 seeds, no wrong result) | D1, D2 and D4 held; D3 contradicted by one part of five. The cut (θ 0) reads 173 MB a write over writes 201–240, 0.12 of the deferred merges' 1,454, and its windows read 1.16 times their live bytes (3.21 for the deferred merges, 14.4 for MoR); but it writes 172 MB a write, 3.04 times the deferred merges' 56.6. Simulated on S3 Express, a write takes the cut 12.9 s against 21.4 for the deferred merges and 25.8 for MoR, a 1% window 1.86 s against 2.90 and 11.1. The model left out the locate: the cut sends about 1,070 requests a write to find its rows. On Azure Blob the cut's write is 25.3 s against 27.4, and its full scan the slowest (86 s against 63). [region-cut.md](../lab/pages/region-cut.md). |
| **every design at 128 MiB** (`configs/w128-*`, campaign `w128`, on havoc) | W1–W5 of 2026-11-12 (revised the same day): with each design defined once, at Iceberg's 128 MiB row groups, does it spare readers what it costs writers, on both workloads; and is RMoR-density an alternative to MoR (compacted) on the inserts? | the design and the workload: on the updates MoR, MoR (compacted) as Iceberg plans its bin-pack after every write, RMoR, RMoW-external, RMoW-append (30 writes) and RMoR-density (θ 0.06, cut); on the inserts, placed by the event-time merge, MoR, MoR (compacted) and RMoR-density; range reads after every write | as listed | 9 on havoc, 3 cores each, four at once: the updates at dd15dc9 from 2026-11-13 19:50 UTC (53–322 min), the inserts at 14727a8 from 2026-11-14 03:50 UTC (192–271 min; their first start failed hashing the 3 GB op log in one array, which 14727a8 streams, changing no byte). Other users' load shared every job's cores and the container counts no instructions: requests and bytes only | W1 (RMoR-density's half) to W5 held, at the last write as registered (at earlier read points RMoR-density's window at 0.9 on the inserts often read past W2's bound). On the updates the windows read 1.16 (RMoR), 1.06 (RMoW-external), 1.60 (RMoR-density), 3.0 (RMoW-append, 30 writes), 8.7 (MoR compacted) and 14.4 (MoR) times their live bytes after the last write, and RMoW-external writes 0.1% more than RMoR (231 MB a write); every record the runs share with D1–D4's is equal. On the inserts RMoR-density writes 85.0 MB a write, 7.2 times MoR's 11.8 and 1.17 times MoR (compacted)'s 72.8; its windows read 1.63 times live (MoR compacted 10.9, MoR 20.7) and its sorted read holds 0.15 GB (1.14, 2.93). Its price is requests: 260 a write (MoR 1, compacted 5), 362 a window (17, 300), over 4,592 row groups (85, 298). MoR (compacted) under Iceberg's rule repacks the initial load's part-full last file with the batch files on the updates, so for its first 40 writes its windows read more than MoR's. The inserts' model missed RMoR-density's writes (0.74 of measured, bound 20%; reads and the other designs' writes within bounds), so its projection of RMoR, RMoW-external and RMoW-append awaits a revision. [suite-128.md](../lab/pages/suite-128.md) (Decisions), [model-128.md](../lab/pages/model-128.md) (Calibration); timed by its laptop twins (next row) in [workloads-128.md](../lab/pages/workloads-128.md) (Results). |
| **the 128 MiB suite's laptop twins** (`configs/w128-*`, campaign `w128-twins`, the laptop) | none: the work havoc's container cannot count (user-mode instructions), from which the results page prices each phase's time | nothing: `w128`'s nine configurations | as `w128` | 9, 2026-11-15/16 at 7590d4a (build snapshot 20261115T071921Z-7590d4a6d299), one at a time with a 12g heap (RMoW-external's, which ran out of 12g, with 18g), 19–109 min; RMoW-append's and the inserts' four retired 2026-11-18 for `w128-twins-2`'s (Retired runs) | Every record equals havoc's run of its configuration. Simulated on S3 Express (Azure Blob in the page; repriced 2026-11-16, when a writer's footer read, two GETs, had been priced as one), on the updates RMoR writes in 18.9 s on average against MoR's 25.8 and reads a 1% range in 1.7 s against 6.5. RMoW-external is the fastest at both, 16.1 s and 1.0 s: its appended footers narrow its files' statistics to the row groups they still list, so after the 240th write its five range reads read 82 files and no deletion vectors where RMoR's read 147 files and 666 deletion vectors; and its write's scan for the rows to change decodes less than RMoR's (3.4 s of the twin's CPU a write against 5.3), reading 149 MB of the key column against 172 and decoding each MB faster (23 ms against 31; the designs with deletion vectors decode their scans more slowly as the vectors accumulate, RMoW-external, with none, does not). RMoR-density takes 20.3 s and 2.3 s, its writes slowing from 8.0 s over the first 10 to 21.7 s over the last 10: 11.1 s of the rise is that scan, which reads the key column of every row group in the write's range of keys (`locate` `scan`), as its cut copies multiply its row groups from 58 to 2,480. RMoR's scan grows too (2.8 to 11.4 s a write), offset by its copies shrinking (379 to 170 MB a write); RMoR's and RMoW-external's writes hold steady. MoR's writes slow from 8.6 s over the first 10 to 33.3 s over the last 10, each reading more to find the rows it changes (138 MB at the first, 2,098 MB at the 240th), and its range read takes 6.7 times as long after the 240th as after the first. On Azure Blob RMoR's full scans end slower than MoR's (87 s against 78 after 240 writes), as it reads the table in pieces of 1 to 16 MB, which Azure Blob serves more slowly. On the inserts RMoR-density reads a range in 3.2 s against MoR's 10.1, for writes of 4.7 s against 0.45 and full scans of 80 s against 67 (4,592 row groups). Iceberg's compaction halves MoR's range read after 240 writes (11.2 s to 5.7 on the updates, 16.7 to 8.5 on the inserts) for writes of 31.8 s and 8.1 s on average. These are the times under `locate` `scan`; since 2026-11-17 the results page times the updates from `w128-keys` (below), and since 2026-11-18 RMoW-append and the inserts from `w128-twins-2` (below), whose runs replaced these four's (Retired runs). [workloads-128.md](../lab/pages/workloads-128.md) (Results). |
| **writers that prune by key at 128 MiB** (`configs/w128k-upd-*`, campaign `w128-keys`, the laptop) | K1–K3 of 2026-11-16: how much of the region designs' write at 128 MiB is finding the rows it changes, and does pruning row groups by each changed key change the trade-off? | `layout.locate` `keys`, on `w128`'s five update configurations whose writers find their rows by reading the table | MoR, MoR (compacted), RMoR, RMoW-external, RMoR-density | 5, 2026-11-17 at 202bc66 (build snapshot 20261117T005411Z-202bc66bc369, the twins' jar and libraries), one at a time under the laptop lock with requests logged, 51–99 min | Every record but the writes' locate equals havoc's `scan` run, and every write reads the same files' footers and deletion vectors. Simulated on S3 Express from each run's own requests and work, writes take RMoR 11.9 s on average (18.9 under `scan`), RMoW-external 10.7 (16.1), RMoR-density 13.6 (20.3), MoR 22.6 (25.8) and MoR (compacted), writes alone, 20.7 (23.5). RMoR-density's writes slow from 6.8 s over the first 10 to 10.8 over the last 10 (8.0 to 21.7 under `scan`): what is left of finding its rows is per file, two footer requests and a deletion vector for each file that may hold a changed key (317 and 157 requests a write over the last 10, against 35 for the key column), as its table grows from 12 data files to 166. MoR gains from bytes, not requests: its locate sends 0.96 of its `scan` requests but reads 0.51 of the bytes, skipping the key columns of the initial load's 128 MiB row groups. MoR's full scans ran 6.4% more instructions than in its twin, with the same requests, bytes and code (0.2% at the scan before the first write, 6.6% from the 10th write on): the JIT compiled the benchmark's check of each row it returns into the scan's loop in a slower form (diagnostic `diag-mor`, below). Until 2026-11-18 the results page timed the updates from these runs, writes, reads and compactions alike (since, from `w128-keys-2`, below): at each run's own pace the same reads took 5.3% less to 5.1% more time than in the twins (MoR's full scans the most), and MoR (compacted)'s compactions 15% more, with the same instructions (the pace of the same work moves between runs), so its write with compactions took 30.1 s on average (29.0 from the twin's). [suite-128.md](../lab/pages/suite-128.md) (Decisions: K1–K3), [workloads-128.md](../lab/pages/workloads-128.md) (Results), [Appendix A](../lab/pages/reader-changes.md). |
| **MoR's full scans under `keys`: a diagnostic** (`configs/diag-mor-*`, campaign `diag-mor`, the laptop) | none (registered as a diagnostic, 2026-11-17): why MoR's full scans in `w128-keys` ran 6.4% more instructions than in its twin | `layout.locate` `keys` and `scan` on MoR's update configuration, 20 writes each, with the JIT's compilation log and the GC log | MoR | 2, 2026-11-17 from build snapshot 20261117T005411Z-202bc66bc369 (logs in its `logs/diag`), one after the other, 9 min each | Every record equals the first 20 writes of its 240-write run, and the offset repeats: a full scan under `keys` runs 20.2 G more instructions after the 10th write and 20.5 G after the 20th (-0.2 G before the first), about 98 a row. The cause is the benchmark's own check of the rows a read returns (`Scans.hash` into the id-and-version oracle, inside the read's decode): at the first write the JIT recompiles the loop that hands each batch to it; under `scan` it calls the check through a virtual call, under `keys` it inlines it, outgrows its inlining budget and leaves three calls a row (`TableHash.add`, `RowBatch.version`, `ArrowBuf.addr`). The reader's own work does not differ. [suite-128.md](../lab/pages/suite-128.md) (Decisions: the diagnostic under K1–K3). |
| **What the oracle cost a read** (`configs/diag-mor-*`, campaign `oracle-out`, the laptop) | none (registered as a measurement, 2026-11-17) | the oracle out of measured reads (branch `oracle-out`: reads count their rows; `--verify` checks every row apart) | MoR | 2 measured, 2026-11-17 from build snapshot 20261117T203716Z-2722f89e4829-dirty, and their 2 verification runs (excluded) | Every record of each measured run equals the diagnostic's run and its verification run, which passed. Without the oracle MoR's full scans run the same instructions under `keys` and `scan` (702.5 G and 702.4 G over the 20 writes): the offset was the oracle's. Compiled as under `scan` the oracle was 0.6% to 2.9% of a read's instructions (29 a row in a full scan, 59 in a sorted read); as under `keys`, 7.7% of a full scan (95 a row). Writes hashed no rows (within 0.1%). A spot check of the fuzzer's calibration on the branch (20 seeds a mutant) stayed clean with no mutant and caught the three it ran at the 2026-11-08 rates, one of them (DVs on the wrong rows, counts right) by the hash alone. [suite-128.md](../lab/pages/suite-128.md) (Decisions: the diagnostic under K1–K3). |
| **the updates at 128 MiB, measured without the row check** (`configs/w128k-upd-*`, campaign `w128-keys-2`, the laptop; verification runs on havoc, campaign `w128-keys-verify`) | none: the runs the results page times the updates from, measured with reads that only count their rows (docs/HARNESS.md, Correctness) | nothing: `w128-keys`' five configurations | MoR, MoR (compacted), RMoR, RMoW-external, RMoR-density | 5 measured, 2026-11-17/18 at 6daa887 (build snapshot 20261117T225741Z-6daa88784286, the fork at a32bc97), one at a time under the laptop lock with requests logged (12g heaps, RMoW-external's 18g), 55–99 min; and a verification run of each on havoc (excluded), 3–6 h each on two cores | Every record of each run equals its `w128-keys` run and its verification run (steps_diff), and the verification runs passed, checking every row. Simulated on S3 Express, writes take RMoR 11.8 s on average, RMoW-external 11.2, RMoR-density 13.7 and MoR 22.6, and MoR (compacted)'s write with its compactions 29.1; RMoR-density's writes slow from 6.9 s over the first 10 to 10.9 over the last 10. Against the `w128-keys` runs, the reads ran up to 7.9% fewer instructions (MoR's full scans) and at most 0.7% more; the writes and compactions ran the same instructions to within 2.2% (RMoR's writes the most), yet took from 7.1% less to 9.1% more time: RMoW-external's rerun ran at a clock 5% slower throughout (4.43 GHz, its phases' median, against 4.67). The planted bugs' calibration at 6daa887 on havoc (`results/fuzz/cal-6daa887-*`, 46 pairs on the three profiles) passed, catching the same number of seeds as at 9ff9176 in every pair but the nondeterministic mutant's (16 and 5 of 240, against 14 and 6). [workloads-128.md](../lab/pages/workloads-128.md) (Results: the updates' times, Limits), [Appendix A](../lab/pages/reader-changes.md). |
| **RMoW-append's and the inserts' laptop twins, measured without the row check** (`configs/w128-upd-append.json`, `configs/w128-ing-*`, campaign `w128-twins-2`, the laptop) | none (registered as a measurement, 2026-11-18): the work the results page times RMoW-append and the inserts from, with reads that only count their rows (docs/HARNESS.md, Correctness) | nothing: four of `w128-twins`' configurations | RMoW-append, MoR, MoR (compacted), RMoR-density | 4, 2026-11-18 at 6daa887 (build snapshot 20261117T225741Z-6daa88784286, the fork at a32bc97), one at a time under the laptop lock with a 12g heap and no request log, 21–90 min; no verification runs of their own: each equals havoc's `w128` run, whose reads checked every row | Every record equals havoc's run of its configuration and the twin it replaced (deleted 2026-11-18; Retired runs, with what the check cost the reads and how the pace moved). Simulated on S3 Express, on the inserts RMoR-density reads a range in 3.3 s against MoR's 10.1, for writes of 5.1 s against 0.46 and full scans of 81 s against 67, and Iceberg's compaction halves MoR's range read after 240 writes (16.8 s to 8.5) for writes of 8.3 s on average. RMoW-append's writes take 37.4 s over its 30 writes, and its range reads 1.69 s. RMoR-density's and RMoW-append's reruns ran at a slower pace than the twins they replaced (the same write instructions in 17.7% and 11.7% more time), so their times rose where their work did not. | [workloads-128.md](../lab/pages/workloads-128.md) (Results). |
| **micro** (`results/micro/*.json`, JMH) | What a footer costs per row group | row groups 10–10k × 11 or 50 columns; parse, convert, merge and serialize | n/a | 2 files, 2026-10-20 | [micro.md](../lab/pages/micro.md); `docs/measurements/footer-*-2026-10-20.txt`. The source of docs/CONFIG.md's "about 1.2 KB and 30 µs per row group". |
| **prices** (campaign `prices`) | none: the latency tests the simulated pages price requests with | S3 Express (from the base point's `c7a.4xlarge` in usw2-az3) and Azure Blob (a `D8as_v6` in westus) | n/a | 2 spikes, 2026-10-31 at 6f24575 | `analysis/tune.py` `SPIKES`, and region-cut's prices. The cloud runs' own spikes are in `workloads-cloud`. |

## The hypotheses that stand

Every hypothesis is in [HYPOTHESES.md](HYPOTHESES.md) as registered. Those below are decided on
runs that stand; the verdicts of the retired runs' hypotheses are summarized in
[LESSONS.md](LESSONS.md) and listed in full in this file at bfbdf33.

| | Claim, in short | Status |
|---|---|---|
| U1–U4 (2026-11-06) | Updates only: MoR's update reads at least the region designs'; they write at least 40 times MoR's bytes, the deferred merges at most 0.3 of RMoR's; full and sorted reads favour the region designs; deltas kept by region hold less in a sorted read and take more requests in a full read | **Held** (havoc, campaign `split`, retired; the `windows` runs equal it in every record but the windows'), **but for two parts:** the region designs' update reads differ by 8.8%, not 5% (RMoW-append 220.6 MB a write, RMoR 240.0), and they write 38.9 times MoR's bytes, not 40 (MoR 5.7 MB, 1.7 of them deletion vectors; model 3.6). The deferred merges wrote 0.22 of RMoR's (model 0.22) and 0.058 at θ 0.25. Full reads 1.00 of the live rows' bytes and MoR 1.18; MoR's sorted read held 136 MB against 16; deltas by region 0.024 of the table against 0.116 by write, with 2.3 times the full read's requests. On updates the region designs keep 9.8 times the live rows after 30 writes (RMoW-append all of it inside its live objects), MoR 1.25. |
| I1–I3 (2026-11-06) | Inserts only: the three designs write alike; the newest 1% costs them alike; late rows make the appending arms' sorted read hold at least 0.15 of the table, MoR compacted at most 0.1 | **Held** (as U1–U4): 11.7–11.8 MB a write each; 3–4 requests and 0.021 of the table for the newest 1%; 0.23, 0.33 and 0.33 held at p99 1, 5 and 20, compacted 0.057. |
| J1–J3 (2026-11-06) | A 160 MiB tail seal: RMoR writes 1.6–2.6 times the appending arms' bytes with a largest write at least 10 times, and its sorted read holds at most half their share (at most twice compacted MoR's at p99 5); RMoW-append, one row group a unit, holds at least 0.9 of it | **J3 held** (havoc, campaign `seal`, retired; the `windows` runs equal it): 1.00, 1.27 and 1.00 of the appending arms' share. **J1 in part:** RMoR wrote 1.70 times and RMoW-append 1.60 (both held), but RMoR's largest write was 4.0 times (47 MB), not 10. **J2 in part:** 0.41 of the appending arms' share at p99 5 and 20 (held), 0.52 at p99 1 and 2.37 times compacted MoR's at p99 5 (contradicted). Inserts take 33.7 bytes a row, not the model's 28.6, so four 4-write units (189 MB) passed the seal and never merged. |
| Q1–Q4 (2026-11-07) | Reads of a 1% window away from the newest rows: MoR's by key at least 3 times the region designs' bytes; deltas by region within 1.3 times RMoR's; appended inserts at least 10 times their live bytes by event time, less compacted or with the seal cut into row groups; the sorted read's instructions at least 1.5 times the full read's | **Held** (campaign `windows`, havoc and the laptop, 2026-11-08). **Q1:** MoR's windows read 5.1–5.3 times the region designs' bytes and 31 times RMoW-append's requests (80 against 2.6); MoR just compacted 0.31 of MoR's bytes. **Q2:** deltas by region 1.08 and 1.20 times RMoR's bytes at θ 0.06 and 0.25; by write 3.39. **Q3:** appended, 29.2 and 29.0 times the windows' live bytes at p99 5 and 20; compacted 0.21 of that, RMoR with the seal 0.44 at both, RMoW-append with it 1.00 at both; at p99 1, 0.32 of p99 5's. **Q4:** the sorted read takes 2.57–2.59 times the full read's instructions at p99 5 and 2.34–2.35 at p99 20; compacted MoR 1.67. |
| D1–D4 (2026-11-09) | At 128 MiB row groups: MoR, compacted MoR and the deferred merges read at least 1 GB a write and 15 times what they write, windows at least 8, 4 and 2.5 times live; whole copies write at least 30 times the deferred merges' bytes; the cut (θ 0) reads at most 0.2 of the deferred merges, writes at most 3 times their bytes, warms up (first 40 writes at least 1.8 times the last 40), windows at most 1.3 and full reads 1.05 times live; cut at θ 0.06, reads at most 0.5 and writes 0.5–1.2 times the deferred merges', windows at most 2.2 | **D1, D2, D4 held; D3 contradicted by its writes** (havoc, campaigns `density128` and `density128-cut`, 2026-11-10). **D1:** 1,667, 1,709 and 1,363 MB read a write, 125, 132 and 23 times written; windows 14.4, 8.1 and 3.2. **D2:** 906 MB a write, 42 times the deferred merges' 21.8 over the same 30 writes; windows 3.04 and 3.02. **D3:** reads 0.12, windows 1.16, full read 1.01, warm-up 2.26 held; writes 3.04 times (at most 3). **D4:** reads 0.32, writes 1.10, windows 1.60. Every op log is the model's. |
| L1–L4 (2026-11-10) | The two workloads in the cloud: each run sends what havoc's run of its configuration counts; on the updates the region designs write slower than MoR and MoR compacted and read a 1% window faster than both, MoR compacted and the deferred merges read one faster than MoR, and the deferred merges write faster than the region designs; on the inserts MoR compacted and RMoR sealed read a window faster than appending and write slower, by under 1 s; on S3 Express each write and read priced within 15% by its twin's work and its VM's spike | **Held** (S3 Express and Azure Blob, 2026-11-11, at 0942497). **L1:** the 15 runs (5 a store and 5 twins) equal havoc's in every record, one-thread held peaks too. **L2:** the region designs' writes 2.3–2.4 and 1.5–2.2 times MoR's and MoR compacted's, their windows 1.4–3.9 and 1.5–6.4 times as fast; MoR compacted's window 1.9 and 2.6 times as fast as MoR's, the deferred merges' 2.1 and 2.6, and their writes 1.7–1.8 and 1.3–1.5 times as fast as the region designs'. **L3:** windows 2.1–2.7 and 2.1–2.6 times as fast, writes 0.41–0.47 and 0.45–0.64 s more. **L4:** priced 0.92–1.03 of measured (within 4.0% since a writer's footer read is priced as two GETs, 2026-11-16). |
| W1–W5 (2026-11-12, revised the same day) | At 128 MiB: on the inserts RMoR-density writes at most 10 times MoR's bytes (RMoR, RMoW-external and RMoW-append at least 100 times: not run, projected), its window at 0.9 reads at most 3 times live and a third of MoR's bytes, its sorted read holds at most 0.4 GB where MoR's holds at least 1; on the updates RMoR's, RMoW-external's and RMoR-density's windows read at most 1.3, 1.3 and 2.2 times live and MoR's at least 8, and RMoW-external writes within 5% of RMoR's bytes; on the inserts RMoR-density writes 0.75–1.25 times MoR (compacted)'s bytes, its windows read at most half of theirs and its sorted read holds at most a third. The projection stands only if the model comes within 20% of each run's writes and 1.5 times its reads | **Held** (havoc, campaign `w128`, 2026-11-13/14), W1's first half not judged. **W1:** 7.2 times. **W2:** 1.75 times live, 158 MB against 1,025; 0.15 GB against 2.93. **W3:** 1.16, 1.06, 1.60 and 14.4; +0.1%. **W4:** 1.17. **W5:** 0.15 and 0.13. Judged at the last write, as registered, which is a low point for RMoR-density: at 13 of the 23 read points before it, its window at 0.9 on the inserts read more than 3 times live (from one to the next it swings between 1.8 and 4.3), and on the updates its windows came under 2.2 only after write 180. **The model's check: missed** by RMoR-density's writes, model 0.74 of measured (its merges rewrite 53 MB of stored rows a write; the run's 73); the other eight checks held (writes 0.89 and 0.94, windows 0.87–1.36, sorted reads 0.71–0.89). |
| K1–K3 (2026-11-16) | At 128 MiB with writers that prune row groups by each changed key: every record but the writes' locate equals `w128`'s and the reads' instructions come within 2% of the twins'; over the last 10 writes the locate sends at most 0.55 of its `scan` requests for RMoR and RMoR-density, 0.35 for RMoW-external and at least 0.85 for MoR; on S3 Express RMoR, RMoW-external and RMoR-density write at least 20% faster, MoR at most 5%, each region design in at most 0.6 of MoR's time, and RMoR-density's last 10 writes take at most twice its first 10 | **K2 held; K1 and K3 contradicted in part** (campaign `w128-keys`, the laptop; [suite-128.md](../lab/pages/suite-128.md)). K1: every record matches but the locate's, and the same footers and deletion vectors; MoR's full read ran 6.4% more instructions than its twin's and its newest 1% 2.1% (the rest within 1.2%). K2: 0.42, 0.42, 0.26 and 0.98. K3: RMoR 37% faster, RMoW-external 34%, RMoR-density 33%, and MoR 12% (contradicted: the locate's work follows its bytes, which fell by half, not its requests, by which the forecast scaled it); 0.53, 0.47 and 0.60 of MoR's time; RMoR-density's last 10 writes 1.57 times its first 10. |

## Retired runs

Outdated runs are deleted, not kept: `results/` is not in git, so this section records what went
and why. What their data showed, where it still matters, is in [LESSONS.md](LESSONS.md); the
entries before 2026-11-12, with their numbers, are this file at bfbdf33.

**Deleted 2026-11-18: four of the 128 MiB suite's laptop twins** (4 runs, 4.4 GB): RMoW-append's
update twin and the three inserts twins of campaign `w128-twins`
(`w128-upd-append-ffaf06363184/20261115T071922Z`, `w128-ing-mor-79aaf8e840cb/20261115T135516Z`,
`w128-ing-compact-706ce269d387/20261115T151958Z`, `w128-ing-density-b9539b01d08f/20261115T162954Z`,
at 7590d4a), whose reads checked each row they returned against the benchmark's hash of ids and
versions (docs/HARNESS.md, Correctness). The user decided to rerun them without the check and delete them
once the reruns checked out. Campaign `w128-twins-2` replaced them: the same configurations from
the 6daa887 snapshot, each equal in every record to the twin it replaced and to havoc's run of its
configuration. Against the retired twins the reruns' reads ran 0.2% to 6.2% fewer instructions
(RMoW-append's sorted reads the most, the inserts' full scans 2.7% to 3.0%), and their writes and
compactions the same to within 1.1%. The pace moved more than between `w128-keys` and
`w128-keys-2`: RMoR-density's inserts rerun and RMoW-append's ran at a lower clock (4.47 and
4.31 GHz, their writes' median, against 4.68 and 4.60) and with more cycles an instruction (their
writes 12% and 5% more), so their writes took 17.7% and 11.7% more time than in the retired twins,
and their reads 4.5% to 8.6% more; MoR's and MoR (compacted)'s phases took from 5.3% less to 3.9%
more. What the retired twins timed, simulated on S3 Express as priced on 2026-11-16: on the inserts
RMoR-density read a range in 3.2 s against MoR's 10.1, for writes of 4.7 s against 0.45 and full
scans of 80 s against 67, and Iceberg's compaction halved MoR's range read after 240 writes (16.7 s
to 8.5) for writes of 8.1 s on average; RMoW-append's writes took 34.9 s over its 30 writes, and its
range reads 1.65 s. The other five twins of `w128-twins`, of the update
configurations the results page times from `w128-keys-2`, stay: the hypotheses page (K1–K3) and
Appendix A compare with them.

**Deleted 2026-11-16: the 16 MiB workloads pages, and the runs only they read** (36 runs and 4
build snapshots, 1.7 GB). The 128 MiB results ([workloads-128.md](../lab/pages/workloads-128.md)) replaced the pages:
at 16 MiB an update region is about one row group, so copying whole row groups cost about what
copying the region would, and no insert was merged into the row group its event time falls in. What
the pages taught is in LESSONS.md (The 16 MiB workloads); the pages are in git before the commit
that deleted them (`git log --diff-filter=D -- lab/pages/workloads.md`). What went:
- **Pages** `workloads.md` (simulated: the designs, compaction, RMoR-density's θ, row groups of 4
  to 32 MiB, the inserts' merges and lateness) and `workloads-cloud.md` (the main points measured
  in the cloud, L2 and L3).
- **The update curves** (campaign `workloads-curves`, 20: `wlc-upd-*` on havoc and the laptop,
  2026-11-11) and **the inserts' policies** (campaign `tune`, 16: `tune-ing-*` on havoc and the
  laptop, 2026-11-08), which only the workloads page read; their configurations stay in
  `configs/`.
- **Build snapshots** only those runs used: `20261108T070603Z-79ad4e7163f8`,
  `20261108T094702Z-79ad4e7163f8-dirty`, `20261111T052006Z-2e3152dcdfaf-dirty` and
  `20261111T075032Z-2e3152dcdfaf-dirty`.

What stays of the 16 MiB runs is what the simulation's cloud check reads
([simulation-cloud.md](../lab/pages/simulation-cloud.md), Calibration): `workloads-ref` (the
references), `workloads-cloud` (the cloud runs, their twins and spikes) and `windows` (the work
formula's training runs), 53 runs. They go when a cloud run at 128 MiB replaces that check.

**Deleted 2026-11-12: every run before the two workloads' current settings** (the lab refresh),
239 runs and 70 GiB, with the lab pages that read them. The pages went first: they recorded runs
whose MoR skipped the update read (until 2026-11-06, under-charging MoR's writes in every
campaign), whose readers have since changed (the vectorized reader on 2026-10-31, RMoR's check of
dead row groups by range on 2026-11-02, the locate by key and the sorted read's skip on
2026-11-10), or whose workload (the mixed batches, the base point and its sweeps) no current
question uses. What went:
- **The mixed workload, local** (2026-10-23 to 26): campaigns `m1` (4), `rg-sweep` (4),
  `split-vs-append` (3), `compaction` (11), `sim` (3, and the excluded havoc pipeline check),
  `cpu-measure` (2), and the in-memory `smoke` runs (2, excluded). Pages `run.md`'s m1 view (the
  page now browses the standing runs), `sweep.md`, `layouts.md`, `layout-vs-policy.md`,
  `time-travel.md`, `compaction.md`, `summary.md`, `tradeoffs.md`, `store-latency.md`,
  `cpu-measure.md`.
- **Iceberg's defaults on havoc and the laptop:** campaigns `scale`, `scale-rerun` and
  `scale-laptop` (4 each). Page `scale.md`.
- **The calibration** (2026-10-27/28): campaigns `twins` (2), `s3x` and `cloud` (3 runs on each
  store and their spikes). Page `cloud.md`.
- **The base point and its sweeps** (2026-10-30 to 11-04): campaigns `sweep` (11, havoc),
  `base-point` (6, and the unfinished laptop run of 2026-11-02), `cloud-sweep` (50: twins, runs on
  both stores, spikes), `rmor-rerun` (40), `mor-row` (11). Pages `base-point.md`,
  `base-point-model.md`, `cloud-sweep.md`, `cloud-sweep-model.md`, `rmor-rerun.md`,
  `vectorized-reader.md`. The base point's two Oct-31 spikes stay (campaign `prices`).
- **The two workloads before the windows** (2026-11-04/06): campaigns `kept` (4), `split` (9),
  `seal` (3). The `windows` runs equal `split`'s and `seal`'s in every record but the windows'
  (5,997 records a host), and carry their verdicts; `upd-base-scan` (MoR under Spark's one-pass
  plan) has no rerun.
- **The tuner's update policies and its cloud check** (2026-11-08/09): `tune-upd-*` (10, havoc
  and the laptop, and one excluded laptop run), replaced by `workloads-curves`; campaign
  `tune-cloud` (17: four runs and twins a store, spikes). Pages `tune.md` and `tune-cloud.md`.
  `analysis/tune.py` stays: the workloads page prices with it.
- **Laptop runs excluded for their clock and rerun:** 7 of `workloads-ref` (beside other
  sessions' model checks, 3.0–3.6 GHz against 4.5–4.6), 7 of `workloads-curves` (3.70–4.44 GHz),
  2 of `tune`'s inserts (beside a lab build). Their requests and bytes equalled havoc's.
- **Build snapshots** only those runs used (18, 1.2 GB), from `20261023T182632Z-55938debe2e4-dirty`
  to `20261110T204202Z-09424970f97e`; the havoc job records of `kept`, `mor-row`, `pipeline`,
  `scale`, `scale-rerun`, `seal`, `split` and `sweep` (`results/campaigns/`); and the 2026-10-21
  analysis outputs `results/compaction-sweep/`, `results/split-comparison/` and
  `results/sweep-rg-recency-exp/`, with their static reports in `docs/measurements/`.

**Earlier deletions** (each with what its data showed, in this file at bfbdf33):

| Deleted | What | Why |
|---|---|---|
| 2026-10-22 | seven early m1 runs | uncommitted code; MoR-v2-file's deletes not file-scoped |
| 2026-10-23 | the seven uncompacted runs that read old versions | time travel planned old RMoW versions without split offsets |
| 2026-10-23 | the dv ablation's three runs, with the `-dv` arms | conclusions recorded (DESIGN.md) |
| 2026-10-23 | the four compacted runs with time travel | before the split-offset fix and the compaction rework |
| 2026-10-26 | the 24 runs the core rerun replaced | writer footers kept across batches, no `cached` readers, payload check on |
| 2026-10-26 | two failed Azurite twins | `--store-root` replaced their Azure root |
| 2026-10-27 | the calibration's twins on the release build, then on six, five and four arms | the fork became the only build; the calibration's arms changed |
| 2026-10-28 | the cloud stand-ins and emulator spikes | replaced by the cloud runs |
| 2026-11-11 | the catalog's Azure append spike and seal probe | moved to catalog-bench, unchanged |

## What is next

The outstanding work is [ROADMAP.md](ROADMAP.md). Next, from the runs that stand: the density
hypothesis where density can choose, planned in
[drafts/density-plan-2026-11-16.md](drafts/density-plan-2026-11-16.md) (step 1, writers that prune
by key, done 2026-11-17; next a workload of sparse changes, then cold pieces merged back by a
compaction, a reader that skips pages and part copies); then a cloud run at 128 MiB, reading at
every snapshot, deferred until those are settled. Also open: a second
seed, to show the request and byte findings are not one op log's; wider tables (every run has 11
columns); GCS once the fork appends there; split files on standard S3.
