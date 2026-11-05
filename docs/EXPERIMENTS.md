# Experiments: a map

Where to start. This page says what has been run, what each experiment varied and asked, what it
found, and where the details are. It covers the 25 canonical runs and the JMH micro-benchmarks as
of 2026-10-23.

For the trade-offs in one frame, start with [lab/pages/tradeoffs.md](../lab/pages/tradeoffs.md)
(When RMoW pays). It shows what each arm spends on a batch and a read, and the request latency
above which RMoW pays, and how the workload moves that latency.
[lab/pages/summary.md](../lab/pages/summary.md) collects every number a write-up quotes.

## Where things are

| Document | What it is for |
|---|---|
| [README.md](../README.md) | What RMoW is, how to build and run, and the reference tables of arms and knobs. |
| [docs/PLAN.md](PLAN.md) | The plan as approved on 2026-10-20, with milestones. Its default matrix is not what ran ([below](#what-every-experiment-shares)). |
| [docs/DESIGN.md](DESIGN.md) | The format-level findings: writer rules, footer order, split files, compaction. |
| [docs/HYPOTHESES.md](HYPOTHESES.md) | The question (the write and read trade-off), what is settled, and the hypotheses of the runs to come, each written down before them: the calibration's C1–C4 (registered 2026-10-27). The earlier H1–H6 and campaign predictions are [the file at 1145f89](https://github.com/cdouglas/rmow-bench/blob/1145f89a36fa60af56aefc9233991f1d03755ae2/docs/HYPOTHESES.md). |
| [docs/ROADMAP.md](ROADMAP.md) | The outstanding work, and the decisions that shaped it. |
| `docs/measurements/` | One write-up per experiment. Write-ups from 2026-10-22 on are lab pages (`lab/pages/`), whose tables and numbers are computed from the runs. |
| `lab/` | The results database and pages (labbook). `lab jupyter` opens them as notebooks and `lab serve` as a site. `lab/pages/experiments.md` regenerates this page's parameter tables from the runs. |
| `results/` | The raw runs, one directory per configuration (`<label>-<hash>/<start>/`). Not in git. |

## The arms

All arms replay the same op log from the same initial-load file. The ids are the configuration's:
`rmow-b` is the plan's and the hypotheses' RMoW-B.

| Arm | Family | Inserts | Updates and deletes |
|---|---|---|---|
| `mor-v2-file` | merge-on-read | a new data file per batch | a position-delete file per touched data file per batch |
| `mor-v2-partition` | merge-on-read | the same | one position-delete file per batch, applied by sequence number |
| `mor-v3` | merge-on-read | the same | deletion vectors in one Puffin file per batch |
| `cow` | copy-on-write | a new data file per batch | every touched data file rewritten whole |
| `tailcow-b`, `tailcow-c` | copy-on-write tail | merged into open tail **data files** by policy B or C | deletion vectors, except in tail files being merged |
| `rmor-c` | regional merge-on-read | as `tailcow-c` | every row group (of a sealed file) or open tail file a batch changes enough of (`rmorRewriteShare`, θ) rewritten: a row group's rows go into the batch's new data file, sorted by event time together with the batch's inserts and the other rewritten rows and cut into new row groups, so a row group no longer keeps its rows (docs/HYPOTHESES.md, 2026-11-04), and a DV covers the old row group; an open tail file is rewritten whole and takes its place. Below θ, deletion vectors. |
| `rmor-c-footer` | regional merge-on-read, footers | as `tailcow-c`, in files the store can extend | as `rmor-c` at θ = 0, each old row group hidden by a footer appended to its file instead of a DV (built 2026-10-27) |
| `rmow-a`, `rmow-b`, `rmow-c` | RMoW, appendable | a row group appended to the tail **object**, merged with open tail row groups by policy A, B or C | every touched row group rewritten as a new region of its object |
| `rmow-b-split`, `rmow-c-split` | RMoW, split | as `rmow-*`, but each version's regions go to a new object and its footer to a footer-only object | the same |
| `rmow-*-dv` | RMoW with DVs (ablation, removed) | as `rmow-*` | deletion vectors on each object's stable prefix (its sealed row groups); open tail row groups were still rewritten. Removed after the dv ablation ([DESIGN.md](DESIGN.md#deletion-vectors-on-an-appendable-file-an-ablation-removed)). |

The tail policies (`TailPolicy`), shared by TailCoW and RMoW:
- **A** never merges. Each batch's inserts stay their own unit (the plan called this "append-CoW").
- **B** re-merges the one open tail with every batch until it reaches `tailSealBytes`.
- **C** is tiered: `tierFanout` (4) units of one level merge into one unit a level up.

Each pair of families isolates one choice:
- **RMoW against TailCoW** isolates the layout: one appendable object against separate data files.
- **TailCoW against MoR-v3** isolates the tail-merge policy.
- **RMoR-C against TailCoW-C** isolates update handling: regions rewritten, against DVs on rows.
- **RMoR-C against RMoW-C** isolates where a rewritten region goes: a new data file with a DV on
  the old, against appended to its object with a new footer.
- **RMoW against RMoW-split** isolates append: the same policy and footer, with history kept in
  one object or spread over objects that can be deleted.

## What every experiment shares

Every canonical run used the local store, seed 1, and one run per configuration. **Workload:**

| | |
|---|---|
| initial load | 1M rows |
| batches | 60 batches of 10k operations |
| operation mix | 70% inserts, 25% updates, 5% deletes |
| update and delete keys | recency-exponential with a mean of 2 batches, unless an experiment says uniform |
| insert lateness | median 0.05 batches, p99 5 batches |

**Layout:**

| | |
|---|---|
| codec | zstd |
| files | 32 MiB target |
| row groups and tail seal | 4 MiB unless swept |
| tiers and footers | tier fanout 4, at most 128 row groups per file |
| writes | sorted writes; rows located by scan (not the oracle); footer order logical |

**Reads** depend on the experiment:

| | m1, rg-sweep, split-vs-append | compaction sweep |
|---|---|---|
| probes | every 10 batches | every 3 batches |
| time travel | 20 batches back | none |
| readers | `one-footer` (primary) and `stock` | `one-footer` only |
| recent-window read | newest 1% of event time | the same |

The [plan's default matrix](PLAN.md#default-experiment-matrix) was larger than what ran. It called
for 5M rows, 200 batches, 16 MiB row groups, and three repetitions plus two more seeds. None of
the local experiments repeats a run, so the Repetitions page has nothing to compare yet.

**Builds.** Runs before 2026-10-26 ran on Iceberg 1.12.0 as released. From 2026-10-26 the atomicio
fork's build is the only one, and every store appends through its `SupportsAtomicOperations`
(README, Appends). On a 7-arm local configuration the two builds sent the same requests and bytes,
except one thing: MoR-v3's and TailCoW-C's DV files are 18 bytes larger, since Puffin's
`created-by` names the build (`1.12.0-atomicio-SNAPSHOT`). The config hash changed too. The
configuration lost `store.append` and `store.adlsReads`, so a configuration rerun on the fork has
a new hash under the same label.

## The experiments

| Experiment | Question | Varies | Arms | Runs | Write-up |
|---|---|---|---|---|---|
| **m1** (`configs/local-m1-*`) | Every arm under recency-skewed and uniform updates, with and without compaction. | key distribution (recency, uniform) × compaction every 0 or 10 batches | the 9 MoR, CoW, TailCoW and RMoW arms (all but split and DV) | 4, rerun 2026-10-25 | H2 re-decided in [lab/pages/summary.md](../lab/pages/summary.md); the lab's run page shows every run. The first H2 result is in [HYPOTHESES.md](HYPOTHESES.md#h2-result-local-recorded-before-h5). |
| **rg-sweep** (`configs/local-rg-sweep-*`) | How row-group size trades RMoW's writes against its reads. | `rowGroupBytes` = `tailSealBytes` = 1, 2, 4, 8 MiB; recency keys, no compaction | the same 9 | 4, rerun 2026-10-25 | [lab/pages/sweep.md](../lab/pages/sweep.md); the first runs' report is [rg-sweep-recency-2026-10-21.md](measurements/rg-sweep-recency-2026-10-21.md) |
| **split-vs-append** (`configs/local-split-*`) | Appendable against split RMoW files (H5). | recency, no compaction; recency, compaction every 10; uniform, compaction every 10 | MoR-v3, TailCoW-C, RMoW-B and -C in both layouts (plus CoW for uniform) | 3, rerun 2026-10-25 | [lab/pages/layouts.md](../lab/pages/layouts.md); the first runs' report is [split-vs-append-2026-10-21.md](measurements/split-vs-append-2026-10-21.md) |
| **compaction** (`configs/local-compact-*`) | When to compact, as an outcome (H6). | check period `P` = none, 1, 5, 10, 20 × threshold level (table below) | the split comparison's six | 11, rerun 2026-10-25 | [lab/pages/compaction.md](../lab/pages/compaction.md); the first runs' report is [compaction-sweep-2026-10-21.md](measurements/compaction-sweep-2026-10-21.md) |
| **micro** (`results/micro/*.json`, JMH) | What a footer costs per row group. | row groups 10–10k × 11 or 50 columns; parse, convert, merge and serialize | n/a | 2 files | `docs/measurements/footer-*-2026-10-20.txt`. The source of the README's "about 1.2 KB and 30 µs per row group". |
| **layout vs policy** (no new runs) | H3: is RMoW against TailCoW (layout) a smaller difference than TailCoW against MoR-v3 (tail policy), and mostly in read requests? | read from the uncompacted rg-sweep, split and m1 uniform runs | MoR-v3, TailCoW-B and -C, RMoW-B and -C in both layouts | 6 of the above | [lab/pages/layout-vs-policy.md](../lab/pages/layout-vs-policy.md) |
| **time travel** (reruns) | H4: does an old version read like the current one, and do dead bytes cost reads? | the 7 uncompacted configurations that read old versions, rerun 2026-10-22/23 with the split-offset fix: each time-travel read against the full read of the same state | all that ran | 7 of the above, rerun | [lab/pages/time-travel.md](../lab/pages/time-travel.md) |
| **store latency** (`configs/sim-*`, and the existing runs) | What would each arm take on a given store? Latency simulated from what each phase records: CPU, requests and bytes. | endive's store profiles (S3 Express, S3, Azure), and a sweep of request latency × throughput; a validation run with latency injected; a 4 MiB recency run with its requests logged | MoR-v3, TailCoW-C, RMoW-B and -C, both layouts | 3 (and the 4 MiB recency and uniform runs) | [lab/pages/store-latency.md](../lab/pages/store-latency.md) |
| **s3x calibration, local twin** (`configs/s3x-calibration-local.json`) | The requests and bytes the first S3 Express run must reproduce exactly, and its runtime. | the S3 Express calibration's settings (200k rows, 20 batches, 1 MiB row groups) on the local store, with a JIT warmup and five readers (one-footer, cached on 1 and 8 threads, cached-skip, stock) and the writer that skips dead row groups; rerun 2026-10-27 with the five arms new campaigns run (campaign `twins`) | MoR-v3, TailCoW-C, RMoR-C, RMoR-C with footers and RMoW-C | 1 | None: a baseline. 1.2 minutes, 10,222 requests. |
| **azure calibration, Azurite twin** (`configs/azure-calibration-azurite.json`) | The requests the first Azure run must send, through ADLSFileIO and the Azure SDK, and the predictions it is checked against (docs/AZURE_RUNBOOK.md). | the same settings on Azurite, 2026-10-27, with the five arms (campaign `twins`) | the same | 1 | The runbook's predictions. 2.2 minutes; every phase sent the model's 10,222 requests (RMoR-C with footers' creates aside, three requests each there), and its model requests and bytes equal the local twin's in all 862 records. |
| **cloud stand-ins** (`configs/standin-*.json`, spikes on emulators) | Not an experiment: inputs for the cloud calibration's charts until the cloud runs return, so the analysis runs end to end beforehand. | the local twin with the `s3x` or `azure` profile injected (measured wall time is its CPU and the injected waits), and the spikes on versitygw and Azurite at 3 reps | the calibration's five | 3 + 3, and 2 spikes, 2026-10-28; deleted | Replaced by the cloud runs, then deleted (Retired runs). The model check prices them with the injected profile: measured over modeled is about 1.01 at the median in every phase. |
| **cloud calibration** (`configs/s3x-calibration.json`, `configs/azure-calibration.json`) | C1–C4: does each store receive what the model counts, does the latency model hold with the stores' measured latencies, and does the trade-off fall where the twins put it? | the store: S3 Express in usw2-az3 from a `c7a.2xlarge`, Azure Blob (ADLS Gen2, Standard) in westus from a `D8as_v6`; the calibration's settings on the registered build 9a69028, requests logged; each store's spike from main | the calibration's five | 3 + 3, and 2 spikes, 2026-10-28 (campaign `cloud`) | [lab/pages/cloud.md](../lab/pages/cloud.md). C1 held on both; C2 contradicted (the spikes' profiles misprice small GETs); C3 contradicted for S3 Express's batches and RMoR-C with footers' batch on Azure; C4 held on S3 Express, contradicted on Azure. |
| **scale** (`configs/scale-recency-*`, on havoc) | Do the 4 MiB recency setting's requests, bytes, CPU and simulated latency hold at Iceberg's defaults (P1–P6)? | row group R = tail seal = 4, 16, 64, 128 MiB, with rows, operations per batch and files scaled by R; recency, no compaction, payload checks off, requests logged | MoR-v3, TailCoW-C, RMoW-B and -C in both layouts | 4, 4 in the rerun, 4 on the laptop | [lab/pages/scale.md](../lab/pages/scale.md). CPU decided on the laptop, in instructions: on havoc, foreign load and then noise beyond it left it undecided. |
| **cpu measure** (`configs/cal-cpu-recency-4m.json`) | How to measure CPU so that one phase can decide a 10% question (M1–M4)? | the measure: thread CPU time against each thread's user-mode instructions and cycles (`perf_event_open`); the 4 MiB scale setting, all six arms, `cached` and `cached-p8`, run twice | all six of the scale campaign | 2 | [lab/pages/cpu-measure.md](../lab/pages/cpu-measure.md) |
| **dv ablation** (`configs/local-dv-*`) | H3's attribution: how much of RMoW against TailCoW is the layout, and how much the update handling (region rewrites against DVs)? | key distribution (recency, uniform); 4 MiB, no compaction | MoR-v3, CoW, TailCoW-B and -C, RMoW-A, -B and -C, RMoW-A-dv, -B-dv and -C-dv | 3, deleted | Conclusions in [DESIGN.md](DESIGN.md#deletion-vectors-on-an-appendable-file-an-ablation-removed); numbers under Retired runs. |
| **sweeps** (`configs/sweep-*`, on havoc) | U1–U3, K1, G1: where the region arms' extra write cost is repaid against MoR-v3 while a batch changes a minority of the regions; M1–M4 of 2026-10-29: what the arms hold in memory, and a sorted read. | from a base point of 16 MiB row groups (`sweep-base`), one axis at a time: update share p = 0.03, 0.1, 0.3; region skew 0, 1, 2; row groups 16, 32, 64 MiB; insert lateness with a p99 of 1, 5 and 20 batches; the ingest window, by statement and a row a change. No compaction, payload checks off, requests and held memory logged | MoR-v3, TailCoW-C, RMoR-C, RMoR-C with footers, RMoW-C | 11 on havoc (2026-10-30, campaign `sweep`), every job under foreign load, so their CPU decides nothing; run again in the cloud on the vectorized reader (below) | Their requests, bytes and held peaks stand; their times are the cloud sweeps'. |
| **base point in the cloud** (`configs/sweep-base.json`) | C5–C8: does the model priced by request size hold at the sweeps' scale on the real stores, and where does the trade-off fall there? | S3 Express in usw2-az3 from a `c7a.4xlarge`, on f51796a, requests and held memory logged, the VM's spike just before; its twins on the laptop (with counters) and havoc | the sweeps' five | 1 and a spike, 2026-10-30; Azure not run yet | The model and the controls: [lab/pages/base-point-model.md](../lab/pages/base-point-model.md), with the vectorized reader's rerun (campaign `base-point`). 2 h 47 min. |
| **vectorized reader** (`configs/sweep-base.json`) | V1–V7: does the trade-off depend on the Parquet code the arms read and write with? Since 2026-10-31 every arm reads through Iceberg's vectorized reader, and the writer takes strings as UTF-8 bytes. | the reader: the laptop's base point on the new reader against its run on the old one; then S3 Express on the new reader (6f24575) | the sweeps' five | 1 on the laptop, 2026-10-31 (40 min, against 2 h 6 min on the old reader), and 1 on S3 Express with its spike the same day (1 h 36 min, against 2 h 47 min) | The results: [lab/pages/base-point.md](../lab/pages/base-point.md); the decision: [lab/pages/vectorized-reader.md](../lab/pages/vectorized-reader.md); the model: [lab/pages/base-point-model.md](../lab/pages/base-point-model.md). V1–V7 held. |
| **sweeps in the cloud** (`configs/sweep-*`) | S1, U1, U2 (revised), G1 of 2026-11-01: the sweeps' claims about time, measured on S3 Express and Azure Blob on the vectorized reader | the sweeps' ten configurations besides the base point, at 6f24575, each run first as a twin with a local store on its S3 Express VM (instruction counts), then on the store after its VM's spike; on S3 Express nine `c7a.4xlarge` at once (AWS's limit of 64 vCPUs refused a tenth; `sweep-rg64m` ran on `sweep-p30`'s VM after it), on Azure one `D8as_v6` in turn | the sweeps' five | 10 twins, 10 runs on each store and their spikes, 2026-11-01 (campaign `cloud-sweep`; twins 23–55 min, S3 Express runs 59–108 min, Azure 79–150 min, 20.5 h in all) | Every cloud run sent what its twin counts, and every twin what havoc's run counts. S1, U1, G1 held; U2 held as revised, so its second half as registered on 2026-10-29 is contradicted: RMoW-C's write is the fastest region rewrite at every p, but its lead over RMoR-C narrows as p rises. No region design's write catches MoR-v3's (RMoW-C's 2.1–8.2 times on S3 Express, 1.7–5.1 on Azure); RMoW-C repays its write after 1.2 and 0.9 full reads per write at p = 0.3, against about six at the base point. The model put every S3 Express mean within 25%, and 89% of Azure's: RMoR-C's full reads there take up to twice their price (block blobs). RMoR-C's times in these runs and the base point's include its skipping reader's and writer's check of dead row groups position by position, about 2.2 s a full read and 2.1 s a write at the base point on S3 Express (the laptop's runs under both builds), since replaced by a range check (2026-11-02, ROADMAP), so they are upper bounds; since 2026-11-04 the results pages take RMoR-C's times from its rerun with the range check (R1–R3, below), each over the MoR-v3 of its own run. [lab/pages/cloud-sweep.md](../lab/pages/cloud-sweep.md), [lab/pages/cloud-sweep-model.md](../lab/pages/cloud-sweep-model.md). |
| **base point on Azure** (`configs/sweep-base.json`) | A1–A4: does Azure Blob measure what S3 Express did on the vectorized reader, with the trade-off where Azure's prices put it? | Azure Blob (ADLS Gen2, Standard, westus) from a `D8as_v6`, on 6f24575, requests and held memory logged, the contract suites and the VM's spike just before; twins: the laptop's (counts) and the S3 Express run (CPU) | the sweeps' five | 1 and a spike, 2026-11-01 (2 h 18 min); the hypervisor gives no instruction counts | A1, A3, A4 held; A2 contradicted by RMoR-C's full reads alone (its write files past 256 MiB are block blobs that read 1.6 times slower). RMoW-C's write 3.14 times MoR-v3's (4.18 on S3 Express), its full read 0.89; it repays a write past about six full reads per write on both stores. [lab/pages/base-point.md](../lab/pages/base-point.md), [lab/pages/base-point-model.md](../lab/pages/base-point-model.md). |
| **RMoR rerun** (`configs/rmor-*`) | R1–R3 of 2026-11-02: RMoR's times with its check of dead row groups by range, on S3 Express and Azure Blob | the sweeps' eleven configurations (the base point and the ten), MoR and RMoR alone, at 505ba07; on the laptop, the base point with every design under both builds | MoR-v3, RMoR-C | on the laptop, 11 reference runs (`rmor-s0`, `rmor-s2` and `rmor-rg32m` twice) and the base point under both builds, 2026-11-03 UTC (campaign `rmor-rerun`; 11–33 min each, the base point 55–58 min); in the cloud at d30544a (505ba07's code), one VM per store running the eleven in turn after its spike: on S3 Express one `c7a.4xlarge`, 2026-11-03 18:54 to 2026-11-04 02:32 UTC (27–52 min each); on Azure Blob one `D8as_v6`, 2026-11-03 18:57 to 2026-11-04 06:14 UTC (37–83 min each) | Every laptop run sends what the twins at 6f24575 sent, in every record, and each repeat what its first run did. The check by range cuts RMoR-C's skipping full read by 0.3–47% of its instructions and its write by 2–22% (the least in the two ingest settings), and moves no other design's work by more than 0.2%. On S3 Express every run sent what its laptop run counts (R1 held). RMoR's skipping full read is now 0.88–0.92 of MoR's at every setting (1.01–1.06 at eight settings before), but not below 0.90 at p = 0.1 and a skew of 2, so R2 is contradicted there and holds at the other eight. Its write is still 3.1–10.9 times MoR's, 1.30–2.93 times RMoW-append's ratio (R3 held). The check cost less than predicted: a read is now its old read less the check, which ran at 16–18 billion instructions a second where the prediction priced it at its phase's 13–15, so a read saved 72–89% of the predicted time and a write about half. On Azure Blob, too, every run sent what its laptop run counts (R1 held). RMoR's skipping full read is 0.96–1.31 of MoR's (1.03–1.76 before; predicted 1.02–1.59): above 1.1 at six settings, faster than MoR's at p = 0.3 and the ingest window, and 1.09 at p = 0.1, where R2 is contradicted. The check took the same time as on S3 Express (within 0.06 s a read; both VMs Zen 4), but the time a read waits on Azure's store moved between the runs by −5.2 to +0.9 s (MoR's by ±1.0 s; on S3 Express by at most 0.04 s), more than the check took at six settings; at p = 0.1, with the earlier run's waits, the ratio would be 1.19. Its write is 2.7–8.5 times MoR's, 1.17–3.41 times RMoW-append's ratio (R3 held at all eleven). [lab/pages/rmor-rerun.md](../lab/pages/rmor-rerun.md). |

The compaction sweep's threshold levels:

| level | RMoW garbage ratio | MoR delete ratio | RMoW small row groups |
|---|---|---|---|
| aggressive | 0.25 | 0.1 | 4 |
| default | 0.5 | 0.3 | 8 |
| lazy | 0.75 | 0.5 | 16 |

### Every run, by what it varies

| experiment | configuration | keys | row group = seal | compact every | thresholds | arms |
|---|---|---|---|---|---|---|
| m1 | local-m1-recency-exp-nocompact | recency | 4 MiB | never | — | 9 |
| m1 | local-m1-recency-exp-compact10 | recency | 4 MiB | 10 | default | 9 |
| m1 | local-m1-uniform-nocompact | uniform | 4 MiB | never | — | 9 |
| m1 | local-m1-uniform-compact10 | uniform | 4 MiB | 10 | default | 9 |
| rg-sweep | local-rg-sweep-recency-exp-rg1m | recency | 1 MiB | never | — | 9 |
| rg-sweep | local-rg-sweep-recency-exp-rg2m | recency | 2 MiB | never | — | 9 |
| rg-sweep | local-rg-sweep-recency-exp-rg4m | recency | 4 MiB | never | — | 9 |
| rg-sweep | local-rg-sweep-recency-exp-rg8m | recency | 8 MiB | never | — | 9 |
| split-vs-append | local-split-recency-exp-nocompact | recency | 4 MiB | never | — | 6 |
| split-vs-append | local-split-recency-exp-compact10 | recency | 4 MiB | 10 | default | 6 |
| split-vs-append | local-split-uniform-compact10 | uniform | 4 MiB | 10 | default | 7 |
| compaction | local-compact-none | recency | 4 MiB | never | — | 6 |
| compaction | local-compact-{aggr,default,lazy}-p1 | recency | 4 MiB | 1 | aggressive, default, lazy | 6 |
| compaction | local-compact-{aggr,default,lazy}-p5 | recency | 4 MiB | 5 | aggressive, default, lazy | 6 |
| compaction | local-compact-default-p10 | recency | 4 MiB | 10 | default | 6 |
| compaction | local-compact-{aggr,default,lazy}-p20 | recency | 4 MiB | 20 | aggressive, default, lazy | 6 |
| s3x calibration | s3x-calibration-local | recency | 1 MiB | never | — | 5 |
| azure calibration | azure-calibration-azurite | recency | 1 MiB | never | — | 5 |
| store latency | sim-validate-s3x | recency | 1 MiB | 10 | default | 5 |
| store latency | sim-recency-4m | recency | 4 MiB | never | — | 6 |
| store latency | sim-recency-4m-noverify | recency | 4 MiB | never | — | 6 |
| scale | scale-recency-rg4m | recency | 4 MiB | never | — | 6 |
| scale | scale-recency-rg16m | recency | 16 MiB | never | — | 6 |
| scale | scale-recency-rg64m | recency | 64 MiB | never | — | 6 |
| scale | scale-recency-rg128m | recency | 128 MiB | never | — | 6 |
| scale rerun | scale-recency-rg{4,16,64,128}m, from 247f054 | recency | 4–128 MiB | never | — | 6 |
| cpu measure | cal-cpu-recency-4m (twice) | recency | 4 MiB | never | — | 6 |
| scale, laptop | scale-recency-rg{4,16,64,128}m, from 9f868af | recency | 4–128 MiB | never | — | 6 |
| cloud calibration | s3x-calibration, azure-calibration (three runs each, on the stores) | recency | 1 MiB | never | — | 5 |
| sweeps | sweep-{base,p10,p30,s0,s2,rg32m,rg64m,late99-1,late99-20} and sweep-ingest{,-rows} (havoc, a5e2ee2) | regions (ingest: recency) | 16–64 MiB | never | — | 5 |
| base point | sweep-base on the laptop (f51796a) and S3 Express (f51796a) | regions | 16 MiB | never | — | 5 |
| vectorized reader | sweep-base on the laptop (ca1985a with the change, committed as 6f24575) and S3 Express (6f24575) | regions | 16 MiB | never | — | 5 |
| base point, Azure | sweep-base on Azure (6f24575) | regions | 16 MiB | never | — | 5 |
| sweeps in the cloud | sweep-{p10,p30,s0,s2,rg32m,rg64m,late99-1,late99-20} and sweep-ingest{,-rows}: twins on the S3 Express VMs, and runs on S3 Express and Azure (6f24575) | regions (ingest: recency) | 16–64 MiB | never | — | 5 |

"Thresholds" are only used when compaction runs, so "—" marks runs that never compact (their
configurations carry the default values). The lab's Experiments page builds the same table from
the runs' manifests. Rebuild it after new runs with `lab ingest && lab build`, or open
`lab/pages/experiments.md` in `lab jupyter`.

### Retired runs

Outdated runs are deleted, not kept: `results/` is not in git, so this section records what went
and why.

**Deleted 2026-10-22: seven early m1 runs** (retired on 2026-10-21 as `superseded-…-<reason>`).
All ran before the first commit, from uncommitted code. What their data showed:
- **`unmerged`** (`local-m1-recency-exp`, finished) and **`v2bounds`** (`nocompact`, finished;
  `compact10`, stopped at step 1). `mor-v2-file` ended with 807 position-delete files for 61 data
  files: Iceberg did not treat its deletes as file-scoped, so they were never merged.
  `RunnerSmokeTest` now asserts at most one live delete file per data file, and `MorV2Arm` writes
  full `file_path` bounds.
- **`partial`** (`local-m1-uniform`): stopped at step 21, with 231 delete files for 22 data files.
- **`onereader`** (`nocompact`, finished; `compact10`, stopped at step 24): a single reader, before
  reader models.
- **`twopass`** (`nocompact`): readers `stock` and `cached`; stopped at step 50.

**Deleted 2026-10-23: the seven earlier runs of the uncompacted configurations that read old
versions** (2026-10-21):
- `local-m1-recency-exp-nocompact` and `local-m1-uniform-nocompact`;
- `local-rg-sweep-recency-exp-rg{1,2,4,8}m`;
- `local-split-recency-exp-nocompact`.

Reruns from cfe40b2 replace them. Three build snapshots went with them:
`20261021T044313Z-4b682c91d403-dirty` and `20261021T162327Z-45b4c692f26f-dirty` (only those runs
used them), and `20261022T224501Z-cfe40b2f65bf` (a rerun stream a power loss cut off). What their
data showed:
- **Time travel planned old RMoW versions without split offsets.** Against the full read of the
  same state:
  - the stock reader's time-travel reads took 62% fewer GETs: 3,938 against 10,248 over 100 pairs
    on appendable files, and 199 against 239 over 10 on split files;
  - the one-footer reader's took the same GETs, and 0.11% fewer bytes.

  The MoR, CoW and TailCoW arms matched in all 190 pairs per reader.
- **Otherwise the reruns match them record for record** (`steps_diff`), except:
  - the RMoW arms' `read_old` records;
  - in the two m1 runs, the MoR and TailCoW arms, whose builds predate counting Puffin's HEAD and
    run-relative paths.

  That is what docs/HYPOTHESES.md at 1145f89 registered on 2026-10-22: no difference outside those.

**Deleted 2026-10-23: the dv ablation's three runs**, with the `-dv` arms themselves (docs/DESIGN.md):
- `local-dv-recency-exp-nocompact` twice (readers `one-footer` and `stock`; then also `cached`);
- `local-dv-uniform-nocompact`.

They came from build snapshots `20261023T075127Z`, `…075131Z` and `…175254Z`
(`-55938debe2e4-dirty`), deleted with them. What they showed (batch 60, 4 MiB, no compaction):

| | recency | uniform |
|---|---|---|
| bytes per batch, RMoW-A-dv / MoR-v3 | 0.97 | 1.04 |
| bytes per batch, RMoW-B-dv / TailCoW-B | 0.90 | 0.95 |
| bytes per batch, RMoW-C-dv / TailCoW-C | 3.6 (RMoW-C: 9.7) | 1.35 |
| update handling's share of RMoW's extra bytes over TailCoW, B / C | 107% / 70% | |
| full-read GETs, RMoW-C-dv / TailCoW-C, `one-footer` | 42 / 62 | 54 / 67 |
| full-read GETs, RMoW-C-dv / TailCoW-C, `cached` | 24 / 52 | |
| full-read GETs under `cached` (`one-footer`): MoR-v3, TailCoW-B, RMoW-A-dv, RMoW-B-dv | 132 (142), 26 (39), 75 (144), 19 (32) | |

- Against the predictions registered on 2026-10-23: D1 contradicted for C (3.6×), held for A; D2
  held for B, contradicted for C; D3 contradicted under the per-split readers (0.68) and held
  under `cached` (0.46); D4 held for A and C, contradicted for B (4.79× MoR-v3, policy B's own
  tail re-merging: TailCoW-B 5.05×); D5 held. The `cached` reader's predictions all held: the
  same rows, no more GETs, arms without deletes identical, nothing else changed.
- Every arm the runs shared with earlier runs matched them record for record (4,883 records),
  and every time-travel pair was exact (200). Every dumped version read back in pyarrow and
  DuckDB (420 recency, 508 uniform).

**Deleted 2026-10-23: the four earlier compacted runs with time travel** (2026-10-21):
- `local-m1-recency-exp-compact10` and `local-m1-uniform-compact10`, from 763423b and 4b682c9;
- `local-split-recency-exp-compact10` and `local-split-uniform-compact10`, from build snapshot
  `20261021T163850Z-45b4c692f26f-dirty`, deleted with them.

Reruns from 17476f3 replace them. The earlier runs predate both the split-offset fix and the
compaction rework (0564114). What their data showed (recency, 4 MiB, at batch 60):
- **Appendable RMoW.** Its old rewrite packed row groups to about twice the writer's size, so it
  read the table in 10–11 GETs against 17–18 now. It also wrote 6.0–6.4 MB per batch against
  5.2–5.4.
- **Split RMoW.** It never reached the old trigger, so it had no compaction writes, and kept
  1.41–1.52× its live bytes against 1.28–1.29× now.

These are the numbers in split-vs-append-2026-10-21.md's compaction tables, which the compaction
sweep had already superseded. The lab's Layouts page now shows the reruns.

The reruns match them as registered in docs/HYPOTHESES.md at 1145f89 (2026-10-23), with no difference
outside the expected kinds:
- the RMoW arms from step 10, the first compaction, on;
- the m1 runs' MoR and TailCoW build differences.

| configuration | records | differing | RMoW from step 10 | m1 build differences | other |
|---|---|---|---|---|---|
| m1-recency-exp-compact10 | 3,172 | 1,800 | 668 | 1,132 | 0 |
| m1-uniform-compact10 | 3,175 | 1,778 | 708 | 1,070 | 0 |
| split-recency-exp-compact10 | 2,085 | 555 | 555 | 0 | 0 |
| split-uniform-compact10 | 2,430 | 475 | 475 | 0 | 0 |

The in-memory `smoke` run tests the harness. The lab marks it excluded and leaves it out of every
result (`lab/annotations.toml`).

**Deleted 2026-10-26: the 24 runs the core rerun replaced** (2026-10-21 to 23), 5.4 GB:
- `local-m1-*` (4), `local-rg-sweep-recency-exp-rg{1,2,4,8}m` (4, with their version dumps),
  `local-split-*` (3) and `local-compact-*` (11);
- `sim-validate-s3x` (`30a4ea0533b8`) and `sim-recency-4m-noverify` (`f412f1efff2d`).

The reruns from b6c2605 replace them (docs/HYPOTHESES.md at 1145f89, 2026-10-25). Twelve build snapshots
went with them, which only they used: `20261021T1922{59,302,305}Z-aac3bdff873a-dirty`,
`20261022T224505Z-cfe40b2f65bf`, `20261023T0422{19,24,28}Z-cfe40b2f65bf-dirty`,
`20261023T045645Z-cfe40b2f65bf-dirty`, `20261023T06175{1,5}Z-17476f3567dc`,
`20261023T181429Z-55938debe2e4-dirty` and `20261023T195500Z-967db793bc7f-dirty`.

What their data showed, against the reruns:
- **Writes, reads, bytes.**
  - Every MoR-v2 and MoR-v3 batch, and every probe under `one-footer` and `stock` outside the
    split arms, matches its rerun exactly.
  - RMoW's batches took one footer read fewer per live file, because their writer kept its
    footers across batches.
  - CoW's and TailCoW's batches took more requests where their merge reads re-read a footer.
  - Their write-ups are the static reports in `docs/measurements/` (2026-10-21) and the lab
    pages as they stood before 2026-10-26.
- **Payload check:** `sim-recency-4m-noverify-f412f1efff2d` was half of store-latency's
  comparison with the check, now a record in that section (N1–N5, every number).
- **CPU** was thread CPU with the payload check on (off in the noverify run): reads took 3.2–3.5×
  the CPU they take without the check.

**Deleted 2026-10-26: two failed Azurite twins**, `azure-calibration-exact-azurite`
(`20261026T055119Z`) and `azure-calibration-iceberg-azurite` (`20261026T055158Z`), with their build
snapshot `20261026T055118Z-9713a7c52d0c-dirty`, which only they used. Each failed before its
first step and left a manifest and an op log. `bin/matrix.sh`'s `--store-root` had replaced their
Azure root with a local path, which the runner now applies to local stores only. Their reruns,
`20261026T055317Z` and `20261026T055554Z`, were the twins docs/AZURE_RUNBOOK.md read, until the
next entry.

**Deleted 2026-10-27: the calibration's twins on the release build,** replaced by reruns on the
fork build (campaign `twins-fork`), when the fork became the only build:
- `azure-calibration-exact-azurite-fa06fdb7634c/20261026T055317Z` and
  `azure-calibration-iceberg-azurite-bbc35d39598f/20261026T055554Z` (campaign `azure-twins`), with
  their build snapshot `20261026T055316Z-9713a7c52d0c-dirty`, which only they used;
- `s3x-calibration-local-7c1a2744ce06/20261023T072437Z`, with its build snapshot
  `20261023T072437Z-17476f3567dc-dirty`, which only it used.

What they showed, kept in docs/AZURE_RUNBOOK.md:
- **Exact reads** sent the model's requests and bytes in every phase. The fork's reads do too,
  and their predictions agree within 3%: RMoW-C ÷ MoR-v3 was 0.37 for a batch and 0.59 for a full
  read, now 0.38 and 0.59.
- **Iceberg's reads as shipped** sent 47.1 and 79.5 GETs and 7.4 and 122.9 MB for MoR-v3's batch
  and full read, and 11.6 and 49.0 GETs and 24.1 and 157.9 MB for RMoW-C's. Simulated, RMoW-C ÷
  MoR-v3 was 0.54 and 0.91 (draft Z6, in HYPOTHESES.md at 1145f89). It needs a build before 2026-10-26 to rerun.
- **The local twin** took 1.8 minutes and 10,446 requests, with three readers and before the
  writers read footers per transaction; its rerun takes 11,706.

**Deleted 2026-10-27: the calibration's six-arm twins on the fork build** (campaign
`twins-fork`), replaced by the five-arm twins (campaign `twins-five`) when the calibration
dropped MoR-v2-file, RMoW-A and RMoW-B and took RMoR-C and RMoW-C-split:
- `azure-calibration-azurite-02879401d0c8/20261027T041701Z` and
  `s3x-calibration-local-424bf5d32c80/20261027T042004Z`, with their build snapshot
  `20261027T041701Z-63b1703b36df-dirty`, which only they used.

What they showed, kept here and in docs/AZURE_RUNBOOK.md:
- **Requests.** Every phase on Azurite sent the model's 11,706 requests, and the two twins' model
  requests and bytes matched in all 922 records. The five-arm twins match them in every record of
  the arms both ran (MoR-v3, TailCoW-C, RMoW-C).
- **Simulated on `azure`** (ms, a batch and a full read under `cached`, CPU on the wall clock):
  MoR-v2-file 1,998 and 2,568, MoR-v3 1,558 and 2,569, TailCoW-C 784 and 1,698, RMoW-A 895 and
  1,972, RMoW-B 460 and 1,392, RMoW-C 572 and 1,511. RMoW-C ÷ MoR-v3 was 0.37 and 0.59 (0.38 and
  0.59 with CPU on the thread clock, as the runbook first quoted them).

**Deleted 2026-10-27: the calibration's five- and four-arm twins** (campaigns `twins-five` and
`twins-four`): the five-arm pair replaced by the four-arm one when RMoW-C-split was dropped, and
that by the current pair (campaign `twins`) when RMoR-C with footers joined:
- `azure-calibration-azurite-fa985c0f0fe1/20261027T073122Z` and
  `s3x-calibration-local-7596607f5035/20261027T073342Z`, with their build snapshot
  `20261027T073121Z-d6d6f0b44523-dirty`;
- `azure-calibration-azurite-0ba3de47df6e/20261027T172447Z` and
  `s3x-calibration-local-5c721acefc31/20261027T172650Z`, with their build snapshot
  `20261027T172446Z-1145f89a36fa-dirty`. Each snapshot was used only by its pair.

What they showed:
- **Requests and bytes.** Each pair matched within itself (842 and 676 records), and the arms the
  twins share matched in every record, so neither the variant nor dropping RMoW-C-split changed
  another arm. Azurite sent the model's 10,136 and 8,665 requests, with no retries. RMoW-C-split's
  batch took 5.4 GETs and HEADs, 2.0 PUTs, 3.26 MB read and 0.86 MB written, its full read 21.0
  GETs and 6.9 MB.
- **Simulated** (ms, a batch and a full read under `cached`, CPU on the wall clock; RMoR-C under
  `cached-skip` in brackets). Five arms, on `azure`: MoR-v3 1,554 and 2,549, TailCoW-C 780 and
  1,684, RMoR-C 850 and 2,006 (1,580), RMoW-C 560 and 1,510, RMoW-C-split 586 and 1,354; on
  `s3x`: 134 and 369, 92 and 305, 197 and 390 (311), 172 and 290, 191 and 272. Four arms, on
  `azure`: 1,561 and 2,567, 784 and 1,689, 876 and 2,020 (1,587), 574 and 1,504; on `s3x`: 136 and
  389, 92 and 319, 215 and 386 (298), 177 and 280.
- **The CPU's pace varies by run.** With identical requests and bytes, the S3 Express ratios to
  MoR-v3 moved by up to 0.12 between these and the current twins (RMoR-C's batch, 1.47 to 1.59),
  Azure's by at most 0.02. Instructions repeated within 0.4%; cycles per instruction, taken per
  run, moved by up to 12%.

**Deleted 2026-10-28: the cloud stand-ins** (campaign `standins`), once the cloud runs replaced
them in lab/pages/cloud.md:
- `standin-s3x-d1bce4a9b22a` and `standin-azure-6ffadad54e64`, three runs each: the calibration's
  local twin with the `s3x` or `azure` profile injected (`configs/standin-*.json`, kept), so that
  measured wall time was the run's CPU and the injected waits;
- `spike/s3x-spike-20261028T165827Z` and `spike/azure-spike-20261028T165903Z`: the spikes on
  versitygw and Azurite at 3 reps, which checked the code, not the stores;
- their three build snapshots, which only they used: `20261028T165318Z`, `…170541Z` and
  `…171750Z-8b8f5392e7c3-dirty`.

What their data showed:
- **The analysis ran end to end** before the cloud runs: every chart and table of the cloud page.
- **The model check reproduced the injected profile:** measured over modeled was 1.016 at the
  median in every kind of phase on `s3x`, 1.002 on `azure`.
- **Their ratios were the twins' predictions** (each arm's batch and full read over MoR-v3's,
  `cached`): on `s3x` RMoW-C 1.27 and 0.80, RMoR-C 1.53 and 1.01, RMoR-C with footers 1.37 and
  0.77, TailCoW-C 0.67 and 0.85; on `azure` 0.37 and 0.63, 0.56 and 0.81, 0.46 and 0.62, 0.51 and
  0.70. The cloud runs measured on S3 Express 0.72 and 0.73, 0.99 and 0.93, 0.85 and 0.71, 0.60 and
  0.77 (lab/pages/cloud.md).

## Findings so far

**The cloud calibration (2026-10-28,** [lab/pages/cloud.md](../lab/pages/cloud.md)**).** Three runs
of the calibration on S3 Express (usw2-az3) and on Azure Blob (westus), each from a Zen 4 VM beside
the store, on the registered build:
- **C1 held.** Every run equals its twin in requests and bytes in all 862 records, and the SDKs'
  counts equal the model's (S3 Express) or the Azurite twin's (Azure), but for Azure's creates.
- **The stores are not their profiles.** Azure's GETs take 9.5 ms (profile 38), the fork's appends
  13.8 ms (88) and its reads 5.7 ms/MiB (25): Azure sits much nearer RMoW's break-even than
  assumed. On S3 Express GETs take 6.2 ms (2.5) and the fork's appends 8.7 ms (13). The fork's
  atomic create on Azure (a staged Append Blob and a rename) takes 112 ms.
- **On S3 Express the region arms' batches beat MoR-v3's** (RMoW-C 0.72×, predicted 1.37×; RMoR-C
  with footers 0.85×; RMoR-C 0.99×). They are 56–67% CPU, at the VM's pace, and their appends are
  cheaper than assumed. Full reads land as predicted (RMoW-C 0.73×). On Azure every judged ratio
  lands where predicted, nearer 1, but RMoR-C with footers' batch (1.12×, predicted 0.54×).
- **C2 is contradicted by the profile, not the model.** CPU and waiting on the store account for
  all but 0.6% of the one-thread phases' time, but the spikes' profiles (latency at 64 KiB plus a
  slope) misprice the calibration's small GETs: by +31% to +45% on S3 Express, by −43% for Azure's
  larger ones.
- **H4's abort, measured.** Under the stock reader an old footer's open-ended GET declares the
  rest of an appendable object (379 MB on S3 Express, 191 MB on Azure, against 7 MB read). S3
  Express aborts it at no visible cost; on Azure RMoW-C's time-travel reads take 3.7 s against at
  most 1.1 s for the other arms. The bounded readers avoid it.

**CPU on the wall clock (2026-10-27).** The laptop's thread CPU clock runs 11.1% fast against its
wall clock (WSL2 on Hyper-V; manifest `host.thread_cpu_per_wall`, 1.1116 on the boot every
counted run used), and `phase_work` turned instructions into time through thread CPU time. The
lab now divides by the ratio (`run_clock`): every laptop run's modeled CPU fell by 10.0%, and its
simulated latencies by 0.1–10% (median 2–3% on Azure and S3, 6.5% on S3 Express). The lab pages
recompute. Below, the store-latency and scale figures are updated where they moved, with the old
value beside; the compaction figures moved by at most a point (MoR-v3's saving on S3 Standard 41%
to 42%), and the trade-offs page's break-evens fell 7–10% (lab/pages/tradeoffs.md). No verdict
changed: P4's read stays 0.82× on S3 Express and 0.70× on Azure Blob, P5's range 0.91–1.01×, H6's
MoR-v3 saving 4%.

All are local runs: requests and bytes are exact, latency is not measured.

**The core experiments were rerun on 2026-10-25 on the current build** (docs/HYPOTHESES.md at 1145f89):
writer footers held per transaction, the `cached` reader models, split files planned per row
group, the payload check off, and CPU in instructions. The bullets from m1 to store latency
below give the reruns' figures; lab/pages/summary.md collects every number a write-up quotes.
Against the runs they replaced (controls K1–K3):
- MoR-v2 and MoR-v3 batches match exactly, and so does every probe under the readers that did
  not change.
- RMoW's batches take one footer read more per live file. Before, RMoW's writer kept its footers
  across batches, which an Iceberg writer cannot (README, I/O parity rule 6).
- CoW's and TailCoW's batches take fewer requests where their merge reads reuse a footer their
  locate scan read. K1 grouped CoW with MoR-v3 and missed this: contradicted for CoW, same bytes.

- **Uniform updates (m1, H2).** RMoW writes what copy-on-write writes: 0.996–0.997× CoW's bytes
  per batch, against MoR-v3's 0.012×.
- **Row-group size (rg-sweep).** The balance for this workload is near 2–4 MiB. No size changes
  the order against MoR-v3:
  - RMoW-B and -C write 11–18× MoR-v3's bytes per batch, in 15–26% of its requests;
  - they read the table in 8–37% of its GETs.
- **Split files (split-vs-append).**
  - They read the same, and write the same bytes within 0.2%.
  - They take 1.28 more PUT-class requests per batch, one per changed file, but 1.73 fewer GETs,
    since a footer-only object is fetched whole in one request where an appendable footer takes
    two. On balance that is 0.45 fewer requests per batch.
  - Once older versions expire they keep 1.4–1.5× their live bytes, where an uncompacted
    appendable file keeps 7.8–8.3×.
- **When to compact (compaction).**
  - The store decides whether MoR-v3 should compact. On S3 Standard it is 21–41% cheaper at up
    to one full read per batch (every 5 batches) and 47–50% at ten (every batch). On S3 Express
    it is at most 8% cheaper.
  - Space is the only reason to compact RMoW. It pays only when storage weighs enough (a day
    between batches at this table size). There appendable files save 48–71% at the sweep's
    most aggressive policy, split files 5–13%.
  - Split files need little compaction, and it is cheap.
- **Layout against tail policy (H3).**
  - In request counts, RMoW against TailCoW is the smaller difference, and mostly in reads
    (2–8 MiB).
  - In bytes it is the larger: RMoW writes 3.3–5.3 MB more per batch, from rewriting the row
    groups that updates touch. So on S3 Express it is the larger cost at up to one full read per
    batch.
  - Append itself saves 1.28 PUT-class requests per batch and costs 1.73 GETs. Net, that is
    0.45 more requests, though still 1.4 µ$ saved per batch on S3 Express. Most of the rest
    comes from how RMoW applies updates (no locate scan, no DVs) and from one footer per file.
- **Time travel (H4).**
  - Under every reader model, an old version costs exactly what the current read of the same
    state costs, in every request and byte column: all 1,200 pairs, 440 of them over RMoW
    files.
  - Dead bytes are never read: appendable files read 1.000–1.002× their live bytes.
  - The earlier runs planned old RMoW versions without split offsets, so their stock-reader
    comparison measured the harness. The reruns fix that, and otherwise match them record for
    record, as registered (Retired runs).
  - The trace cannot show S3's extra abort per old footer.
- **Layout against update handling (the dv ablation).**
  - The single appendable object, with TailCoW's DVs, writes 0.9–1.0× its counterpart's bytes
    for policies A and B, and 3.6× for C. The hybrid still rewrote open tail row groups that
    updates hit.
  - Rewriting the row groups updates touch carries 70% (C) to all (B) of RMoW's extra bytes
    over TailCoW.
  - The layout alone saves most of the write requests: locating rows in two logical files
    instead of many data files.
  - With deletes loaded once per file per read (the `cached` reader), the layout alone carries
    most of the read savings too: 24 GETs at batch 60 against TailCoW-C's 52 (RMoW-C: 19).
  - Both older reader models load a file's DV once per split (row group), as Iceberg's readers
    do. That inflates every DV arm's reads, MoR-v3's and TailCoW's included.

- **Store latency, simulated (the 4 MiB recency run, rerun on the instruction measure; CPU on
  the wall clock since 2026-10-27).**
  - On S3 Express, RMoW-C's batches take 2.1× MoR-v3's time (2.2× before the correction), mostly its own CPU re-encoding
    rewritten regions. Its 9.5 requests against 45 do not make up for it at S3 Express's request
    latencies.
  - Where requests are dear (Azure Blob), RMoW-C's batches take 0.50× (0.52×). At 100 MiB/s the
    crossover lies near 20 ms per request.
  - On one thread, RMoW-C's full read at batch 60 takes 0.69× MoR-v3's on S3 Express and 0.33×
    on Azure Blob (0.34×). On eight threads, 0.86× and 0.49× (0.50×).
  - The replay of a request log predicts a run with the same latency injected: 280 of 281
    one-thread phases within 10%, and 24 of 25 eight-thread full reads within 20% (S1 and S2
    contradicted as written, one phase each).
- **Store latency without the payload check (`sim-recency-4m-noverify`, 2026-10-23).** The run
  is `sim-recency-4m` with `verifyRows` off. Split files are now planned per row group. N1–N5
  all held:
  - Requests and bytes match `sim-recency-4m` in 2,248 of 2,277 records. The other 29 are the
    split arms' stock reads, which re-read the version's footer per split: 206 GETs against 154
    for RMoW-B-split's full reads, and slightly more bytes.
  - Reads take 0.29–0.31× the CPU, and batches 0.97–1.02×: the payload check was about 70% of a
    read.
  - On one thread (`cached`, batch 60), RMoW-C's full read takes 0.67× MoR-v3's on S3 Express
    (1,319 against 1,982 ms; 0.78× with the check) and 0.33× on Azure Blob (0.45×).
  - On 8 threads (`cached-p8`), RMoW-C's still matches MoR-v3's on S3 Express (380 against
    376 ms), and is 0.54× on Azure Blob (672 against 1,256).
  - Split files now read in parallel: RMoW-C-split takes 347 ms against RMoW-C's 380 on S3
    Express (1,882 before).
  - lab/pages/store-latency.md still shows the run with the check.
  - The run's build planned every split object per row group, even one of at most 1 MiB, which a
    footer-cache reader fetches whole: each of its splits fetched it again. The fix, one task for
    such an object (as `readAll` already plans small files), came after the run, and changes none
    of its records: its footer-cache reads match `sim-recency-4m` exactly, so it had no such
    object. A compacting run does (`sim-validate-s3x`'s split objects under 1 MiB), and
    `RequestLogTest` now pins it.
  - Appendable RMoW's GETs hit objects of 24 appended parts on average (up to 47). At an extra
    0.1 ms per part a full read costs 3% more; the S4 spike would measure the real cost.

- **At Iceberg's defaults (the scale campaign and its rerun, havoc, 2026-10-23/24).** Four runs
  at row groups of 4, 16, 64 and 128 MiB, everything else scaled with them, then the same four on
  the build that judges CPU per phase and reads RMoW's footers every batch (lab/pages/scale.md):
  - **The small settings flattered MoR-v3's requests.** Its per-batch data files (0.16 MiB at
    4 MiB) cross the 1 MiB whole-file threshold between 16 and 64 MiB, after which each costs
    three requests in a locate scan and two in a full read. MoR-v3 issues 94 requests per batch at
    128 MiB against 43 at 4 MiB. RMoW-C stays at 9.6–9.7 in the rerun (6 in the first campaign,
    whose writer kept its footers), and falls from 0.22× to 0.10× of MoR-v3's. TailCoW-C's
    batches grow 1.5× for the same reason.
  - **RMoW-C's bytes grow faster than the row group.** It writes 1.30× its scaled 4 MiB bytes per
    batch at 128 MiB: the same live bytes sit in 10 row groups instead of 13, so each rewritten
    region is larger. It writes 22× MoR-v3's bytes per batch at 128 MiB (15× at 4 MiB).
  - **Storage.** Uncompacted, RMoW-C stores 10.8× its live bytes at 128 MiB (7.3 GiB for
    0.67), RMoW-B 8.1 GiB; 32.6 GiB over the six arms.
  - **CPU decides nothing, in either campaign.**
    - **First campaign:** other users' threads ran on the three larger jobs' cores (1.6–2.9 CPUs
      on average). Split arms, which read exactly what their appendable twins read, differed from
      them by 0.85–1.23× in read CPU.
    - **Rerun:** 67–95% of each run's batches were clean (under 0.25 CPUs of foreign load
      throughout). Even so, in clean probes the twins differed by 0.94–1.09×. Reads with more of
      the JVM's other work (collection, compilation) running during them tended to take more.
      So the control C3 failed, and P3–P5 stay undecided.
    - **At face value, over clean phases:** P3 and P5 would hold with room to spare. P4's read
      half would fail (0.83 on S3 Express), but those reads were not clean.
  - **The rerun's other controls.** It reproduced every probe and every MoR-v3 batch of the first
    campaign exactly (C1). Each RMoW batch now reads one footer per live file; the counts
    registered for that assumed two live files, so C2 failed as stated.
  - **On the instruction measure (laptop, 2026-10-25).** The four configurations ran again on
    this laptop, whose kernel allows counters. Every request and byte matched havoc's rerun (C1).
    Each comparison was judged against its run's twins: 2.8% for full reads at 128 MiB, 0.4% for
    batch means, 2.0% on eight threads.
    - **P3 held, in work.** From 4 to 128 MiB, instructions per MiB read change by 0.98–1.00×
      and per MiB written by 0.75–1.09×.
    - **P4 contradicted.** On one thread, RMoW-C's full read at batch 60 takes 0.82× MoR-v3's
      on S3 Express, against 0.9–1.1 predicted, and 0.70× on Azure Blob, as predicted.
      RMoW-C's batches take 2.2–5.7× MoR-v3's on every store where it can run (2.3–5.7× before
      CPU went on the wall clock, 2026-10-27).
    - **P5 held.** On eight threads every arm's full read is within 0.91–1.01× of MoR-v3's on
      S3 Express.
  - **Verdicts.** P1 contradicted (MoR-v3 and TailCoW-C from 64 MiB). P2 contradicted for
    RMoW-C's batches from 64 MiB. P3 held (in work). P4 contradicted (S3 Express read). P5 held.
    P6, and the revised P6's per-arm bound, contradicted (RMoW-B 8.06 GiB); its sum held.

- **Measuring CPU (`cal-cpu-recency-4m`, twice, 2026-10-25).** Thread CPU time is not a measure
  of work on this laptop. The same read repeated back to back took up to twice the time, and
  twin reads differed by up to 19% (lab/pages/cpu-measure.md).
  - **Instructions are.** Each thread's user-mode instructions put twin reads within 0.17% and
    twin batches within a 0.14% standard deviation (M1 held). The same phase in two runs counts
    within 1.3% (M2 held).
  - **Time needs a pace, and pace is noisy.** Modeled time (instructions × the arm's median
    cycles per instruction ÷ the clock) put twins up to 3.9% apart (M3, 2%, contradicted). Read
    CPI varies by 3–7% across probes (M4, 5%, contradicted).
  - **Pace differs by arm where the code differs.** RMoW-C's batches run at 1.12–1.13× MoR-v3's
    cycles per instruction, so one pace for all arms would not do.
  - **Already ruled out** in scratch: a heap collection before each phase (worse), the cheapest
    of three runs, a pre-touched heap, user-mode time.

## The hypotheses

H1–H6 are HYPOTHESES.md as of commit 1145f89; the refocused file (2026-10-27) carries
what they settled, the calibration's C1–C4, decided by the cloud runs of 2026-10-28, the
sweeps' (2026-10-29), the base point's in the cloud, C5–C8 (2026-10-30), and the vectorized
reader's, V1–V7 (2026-10-31), the vectorized base point on Azure's, A1–A4, and the sweeps in the
cloud, S1 and U1, U2 (revised) and G1 restated for measured time (both 2026-11-01), and RMoR's
rerun with the check by range, R1–R3 (2026-11-02). The sweeps' M1–M4,
on memory, are not the cpu measure's M1–M4.

| | Claim, in short | Status |
|---|---|---|
| H1 | Recency-skewed updates: RMoW-C's full read stays flat, its writes within 3× MoR-v3's | **Open.** Decided on S3 Express, where only the calibration has run (2026-10-28), which H1 was not registered against. |
| H2 | Uniform updates: RMoW's writes approach copy-on-write | **Held** (local, m1; on the reruns, 0.996–0.997× CoW's bytes). |
| H3 | RMoW-C against TailCoW is the single-object effect | **Mixed.** Smaller, and mostly in reads, in request counts; larger, and mostly write bytes, on S3 Express prices at up to one full read per batch. The dv ablation puts 70% to all of the extra bytes on the update handling, and most of the read savings on the single object (with a delete cache). |
| H4 | Old versions read like current ones; dead bytes cost nothing | **Held** for reads, under every reader model (on the reruns, all 1,200 pairs), and for dead bytes. The extra abort: on S3 Express the declared GET bytes show it, at no visible cost; on Azure it costs RMoW-C's time travel under the stock reader 3.7 s against 1.1 s (the cloud runs, 2026-10-28). |
| H5 | Split files: one more request per changed file, the same reads, kept bytes within 1.5× | **Mostly held.** One more PUT-class request per changed file, the same bytes and reads; kept 1.41× for RMoW-B, 1.52× for RMoW-C. **Contradicted** for requests in all: on the reruns split files take 0.45 fewer per batch, since their writer fetches a footer-only object in one GET where an appendable footer takes two. |
| C1 | The model counts what each store receives | **Held** on S3 Express and Azure (the cloud runs, 2026-10-28). |
| C2 | A phase's time is its CPU plus its requests' latencies, within 25%, priced with the measured latencies | **Contradicted** as registered: the spikes' profiles misprice small GETs. The structure holds (0.6% unexplained). |
| C3 | The trade-off falls where the twins put it | **Contradicted** for S3 Express's batches (the region arms beat MoR-v3) and RMoR-C with footers' batch on Azure; held for every judged full read. |
| C4 | CPU is the bottleneck only of the region arms' batches on S3 Express | **Held** on S3 Express (56–67%), **contradicted** on Azure (RMoW-C's batches 39%, more than a third). |
| U1 | A region rewrite pays more as the update share p rises: the region arms' batch time over MoR-v3's is lower at p = 0.3 than at 0.03 | **Held** on both stores (the cloud sweep, 2026-11-01): from p = 0.03 to 0.3, RMoW-C's write over MoR-v3's went from 4.18 to 2.13 on S3 Express and 3.14 to 1.68 on Azure; RMoR-C's from 8.01 to 3.22 and 6.24 to 2.73. |
| U2 | RMoW-C's batch takes fewer requests and less time than both RMoR-C arms' at every p, and its time over RMoR-C's is no higher at 0.3 than at 0.03 | **First half held** (time) on both stores at p = 0.03, 0.1 and 0.3; the requests are havoc's, not yet judged. **Second half contradicted**, as the revision registered from the twins predicted: RMoW-C's write over RMoR-C's rose from 0.52 to 0.66 on S3 Express and 0.50 to 0.61 on Azure, since at p = 0.3 both write mostly the batch's inserts. |
| U3 | No region arm writes fewer bytes than MoR-v3, at any p | **Open:** havoc's sweeps decide it (bytes are exact); not yet judged. |
| K1 | Skew moves the region arms' writes, not MoR-v3's, nor the full reads | **Open:** the cloud sweep (bytes from havoc's, not yet judged). |
| G1 | Bigger row groups cost the region arms' writes | **Held** for time on both stores: RMoW-C's write over MoR-v3's at 16, 32 and 64 MiB was 4.18, 5.32 and 8.16 on S3 Express, 3.14, 3.87 and 5.08 on Azure (RMoR-C's 32-to-64 step on Azure narrowly, 7.17 to 7.46). Bytes from havoc's, not yet judged. |
| S1 | Each cloud run of the sweeps sends what its twin counts (a control) | **Held:** all 22 cloud runs (the base point's two included), every record and every one-thread held peak. |
| M1–M4 (memory) | A sorted read holds MoR-v3's batch files at once; RMoW-C's holds less, but grows too; RMoW-C's batch holds more bytes than MoR-v3's; RMoR-C sorts its whole batch file | **Open:** havoc's sweeps decide it (held peaks are exact on one thread); not yet judged. |
| C5 | The base point's cloud runs send what the twins count (a control) | **Held** on S3 Express (2026-10-30): all 1,325 records' requests and bytes, and all 1,230 one-thread held peaks, equal the laptop twin's. On Azure, A1–A4 (the vectorized reader) took its place. |
| C6 | Priced by request size, each arm's mean batch and full read is within 25% of its wall time | **Held** on S3 Express: every judged mean within 1–11%. On Azure, A1–A4 (the vectorized reader) took its place. |
| C7 | At the base point the trade-off falls where the laptop twin puts it: the region arms' batches slower than MoR-v3's on both stores | **Held** on S3 Express: over MoR-v3's, RMoR-C's batch 6.33 (predicted 6.59), with footers 6.33 (6.23), RMoW-C's 3.05 (2.98); full reads 3.78 (3.85), 0.85 (0.87) and 0.85 (0.85). On Azure, A1–A4 (the vectorized reader) took its place. |
| C8 | At the base point a phase is mostly CPU: the store takes under half of every mean batch and full read | **Held** on S3 Express: the store took 13–28% of the batches and 32–36% of the full reads. On Azure, A1–A4 (the vectorized reader) took its place. |
| V1 | The vectorized reader changes no request, byte or held peak (a control) | **Held** (the laptop, 2026-10-31): all 1,325 records and all 1,230 one-thread held peaks equal the old reader's run. |
| V2 | On the vectorized reader a full read takes at most a third of its old instructions | **Held:** 0.26–0.28 under `cached`. |
| V3 | At S3 Express prices RMoW-C's batch over MoR-v3's grows past the 3.05 measured on the old reader, its full read stays between 0.80 and 0.95 of MoR-v3's | **Held:** 4.48 and 0.87, simulated from the laptop. MoR-v3's batch was mostly decoding, which the new reader cut most; 61% of RMoW-C's batch CPU is now parquet-java's encoding. |
| V4 | On S3 Express the vectorized reader sends what its laptop twin counts (a control) | **Held** (2026-10-31): all 1,325 records and all 1,230 one-thread held peaks. |
| V5 | On the vectorized reader the model priced by request size puts each judged mean within 25% of its wall time | **Held:** within 2–14%, the most for RMoR-C's batch (1.14), whose multipart PUT overlaps its writing. |
| V6 | On S3 Express the trade-off falls where the twin puts it; RMoW-C's batch over MoR-v3's above 3.05, its full read between 0.80 and 0.95 | **Held:** RMoW-C's batch 4.18 times MoR-v3's (predicted 4.48), its full read 0.87 (0.87); RMoR-C's 8.01 (9.43) and 3.82 (3.83), with footers 8.19 (8.75) and 0.87 (0.87). |
| V7 | On the vectorized reader full reads are mostly the store's time, batches mostly CPU | **Held:** the store took 58–70% of the full reads and 18–36% of the batches. |
| A1 | On Azure the vectorized base point sends what the laptop twin counts (a control) | **Held** (2026-11-01): all 1,325 records and all 1,230 one-thread held peaks. |
| A2 | On Azure the model priced by request size puts each judged mean within 25% of its wall time | **Contradicted** by RMoR-C's full reads alone (0.69 of measured under both readers): its write files past 256 MiB are block blobs of 4 MiB blocks, from which a 16 MiB range takes 228 ms at the median against about 140 ms from any other file; the spike reads only blobs written in one request. Every other mean within 2–12%. |
| A3 | On Azure the trade-off falls where the twin puts it, RMoW-C's write gap below S3 Express's 4.18 | **Held:** RMoW-C's write 3.14 times MoR-v3's (predicted 2.62), its full read 0.89 (0.90); RMoR-C's 6.24 (5.65) and 5.20 (3.72), with footers 6.54 (5.39) and 0.89 (0.88). |
| A4 | On Azure the store takes most of every full read and of MoR-v3's write, less than half of the region designs' writes | **Held:** 75–81% of the full reads, 60–63% of MoR-v3's and TailCoW-C's writes, 35–47% of the region designs'. |
| R1 | Each cloud run of RMoR's rerun sends what its laptop run counts (a control) | **Held** on both stores (2026-11-04): none of 5,839 records differs, on S3 Express or on Azure Blob. |
| R2 | With the check by range, RMoR's skipping full read beats MoR's on S3 Express (below 0.90 of it), not on Azure | **Contradicted on S3 Express at p = 0.1 and a skew of 2** (0.913 and 0.918 of MoR's), held at the other eight settings (0.88–0.89; predicted 0.84–0.89); p = 0.3 decides nothing (0.92). Faster than MoR's at every setting, but the check cost less than predicted. **Contradicted on Azure Blob at p = 0.1** (1.092, not above 1.1; predicted 1.18), where the time RMoR's read waits on the store fell 1.23 s from the earlier run (with the earlier run's waits, 1.19); held at the other six decided settings (1.14–1.31; predicted 1.14–1.59); p = 0.3, a skew of 2 and the two ingest settings decide nothing (0.96–1.08). |
| R3 | RMoR's write still costs more than RMoW-append's, by ratio over MoR's | **Held on S3 Express** (2026-11-04): 1.30–2.93 times at the ten decided settings (predicted 1.21–2.86); a skew of 2 decides nothing (1.09). **Held on Azure Blob** (2026-11-04): 1.17–3.41 times at all eleven settings (predicted 1.16–2.95). |
| W1–W3 | RMoR and RMoW-external with their row groups kept whole, as RMoW-append keeps them: the same bytes as RMoW-append's write (within 2%); its work plus their locate (at most 1.25 and 1.35 times); a sorted read that holds what RMoW-append's holds | **W1 held** (havoc, campaign `kept`, 2026-11-05): at every write of the four settings both kept arms read back RMoW-append's bytes within 0.01% and wrote them within 0.03%; their largest write is RMoW-append's (292 MB at the base point, against 771 MB as built). **W3 held for memory:** the kept RMoW-external's sorted read held exactly RMoW-append's peak at every setting (245–368 MB; as built 32–83 MB). **W2 and W3's work are open:** havoc counts no instructions. The control held: the four existing arms sent their earlier runs' requests and bytes in all 3,764 records. |
| H6 | When to compact | **Mixed** (unchanged on the reruns). The read half held for RMoW-B, not RMoW-C; the space and write-for-read halves held; the costs half held for split RMoW, not for MoR-v3 (its cheapest on S3 Express is every 10 batches, 4%). |

## What is next

The outstanding work is tracked in [ROADMAP.md](ROADMAP.md), with the decisions of 2026-10-26:
the arms new campaigns run, the workload axes, and SAO for every store. The list below is as of 2026-10-25.

The goal is a blog post on appending regions to Parquet: compacting regions, and when an
appendable object beats Iceberg's merge-on-read. ORC is out of scope (decided 2026-10-25): it
would need its own arms and reader checks.

- **Now: the core experiments rerun on the current build** (docs/HYPOTHESES.md at 1145f89, 2026-10-25):
  the 22 `local-*` configurations and the two `sim-*` ones. They fix the gaps below: writer
  footers, the `cached` reader models, split planning, the payload check, and CPU in
  instructions. Controls K1–K3 check them against the old runs, and H2–H6 are re-decided on them.
- **Then: the write-ups on the reruns.**
  - The pinned pages (layout-vs-policy, time-travel, store-latency) move to the reruns.
  - The campaign pages (run, sweep, layouts) follow once the old runs are marked superseded.
  - The compaction sweep, until now only a static report, gets a lab page.
  - A summary page holds every number the post quotes.
- **Done: the cloud calibration** on S3 Express and Azure (2026-10-28,
  [lab/pages/cloud.md](../lab/pages/cloud.md)). A profile that prices each logged request at its
  size from the spikes (lab/views/65_sized.sql, 2026-10-29) fits the calibration's requests better
  than the spikes' latency-plus-slope; the base point's cloud runs are its first test.
- **Done: the sweeps on havoc** (2026-10-30), every job under foreign load, so their CPU decides
  nothing; **the base point on S3 Express** (2026-10-30), where C5–C8 held; and **the vectorized
  reader** (2026-10-31), where V1–V3 held on the laptop. **Done:** the base point on S3
  Express on the new reader (2026-10-31), where V4–V7 held ([lab/pages/base-point.md](../lab/pages/base-point.md), the results;
  [lab/pages/base-point-model.md](../lab/pages/base-point-model.md), the model). **Done:** the base point on Azure and the sweeps in the cloud (2026-11-01; [lab/pages/cloud-sweep.md](../lab/pages/cloud-sweep.md), the results; [lab/pages/cloud-sweep-model.md](../lab/pages/cloud-sweep-model.md), the model).
- **Done: RMoR with the check by range in the cloud** (registered 2026-11-02, R1–R3):
  `configs/rmor-*.json` on S3 Express and Azure Blob, the laptop's runs at 505ba07 as their
  reference, both done 2026-11-04 (R1 and R3 held on both; R2 contradicted at p = 0.1 and a skew
  of 2 on S3 Express and at p = 0.1 on Azure Blob;
  [lab/pages/rmor-rerun.md](../lab/pages/rmor-rerun.md)). Since 2026-11-04 the results pages
  ([base-point](../lab/pages/base-point.md), [cloud-sweep](../lab/pages/cloud-sweep.md)) take
  RMoR's times from its runs, each over the MoR of its own run, and the model's pages keep the
  earlier runs they price.
- **Both calibrations run the five arms new campaigns run** (MoR-v3, TailCoW-C, RMoR-C, RMoR-C
  with footers, RMoW-C) since 2026-10-27, RMoR-C with the writer that skips dead row groups and a `cached-skip` reader beside
  the others, and their spikes time GETs by size for the read rate per MiB.
- **Optional, to come back to:**
  - a second seed, to show the request and byte findings are not one op log's;
  - a CDC-like workload with late arrivals;
  - a server core's pace on havoc. P3–P5 were decided on this laptop; havoc could check them once
    an admin allows counters (`sysctl -w kernel.perf_event_paranoid=2`; it is 4);
  - split files on standard S3.
- **Gaps in the old runs**, which the rerun closes:
  - no `cached` reader model in runs before 2026-10-23, so MoR-v3's and TailCoW's reads load a
    file's DV once per split;
  - split files read as one task per object before `sim-recency-4m-noverify`;
  - RMoW's writer keeping its footers across batches before 2026-10-24, so its batches take 3–4
    requests fewer than a rerun;
  - the payload check on, and CPU in thread time.
- **Gaps that remain:** no repetitions or second seeds (above), and no write-up of m1 beyond H2.
