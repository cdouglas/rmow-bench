# Experiments: a map

Where to start. This page says which runs stand, what each experiment that made them varied and
asked, what it found, and where the write-ups are. It covers the 99 runs that stand as of
2026-11-12, all on the two workloads (updates only, inserts only) on the atomicio fork's build,
and the JMH micro-benchmarks.

Everything run before (the mixed workload, the base point and its sweeps, the calibrations, the
RMoR rerun, the tuner's update policies and its cloud check) was retired on 2026-11-12, with its
lab pages ([Retired runs](#retired-runs)). What it taught and decided is in
[LESSONS.md](LESSONS.md). This file as it stood then, with every retired experiment's row, its
findings and the verdicts of H1–H6, C1–C8, V1–V7, A1–A4, R1–R3, W1–W3, T1–T4 and K1–K3, is
`git show bfbdf33:docs/EXPERIMENTS.md`; the retired pages are under `lab/pages/` at the same commit.

For the trade-off, start with the [workloads page](../lab/pages/workloads.md) (simulated: each
design's write latency against its reads, its latency and storage as snapshots are added, then
compaction and each knob apart), then [the same main points measured in the
cloud](../lab/pages/workloads-cloud.md). How the simulation compares with the cloud is a
calibration page, [simulation-cloud.md](../lab/pages/simulation-cloud.md).

## Where things are

| Document | What it is for |
|---|---|
| [README.md](../README.md) | What RMoW is, how to build and run, and the reference tables of arms and knobs. |
| [docs/LESSONS.md](LESSONS.md) | What the retired experiments taught and decided: the measurement method, the stores, the designs dropped, and the results that carry over. |
| [docs/HYPOTHESES.md](HYPOTHESES.md) | The question, and every hypothesis, registered before the runs that decide it. |
| [docs/ROADMAP.md](ROADMAP.md) | The outstanding work, and the decisions that shaped it. |
| [docs/DESIGN.md](DESIGN.md) | The format-level findings: writer rules, footer order, split files, compaction. |
| [docs/PLAN.md](PLAN.md) | The plan as approved on 2026-10-20. Its default matrix is not what ran. |
| `lab/` | The results database and pages (labbook). `lab serve` shows the pages as a site, whose front page lists the campaigns and every run. |
| `results/` | The raw runs, one directory per configuration (`<label>-<hash>/<start>/`). Not in git. |
| `docs/measurements/` | The footer micro-benchmarks' JMH output (2026-10-20), the source of the README's footer cost. |

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
MoR, which writes each write's inserts as a file. The next suite (registered 2026-11-12,
HYPOTHESES.md W1–W3, not yet run) runs every design above on both workloads, the inserts placed by
the event-time merge (`layout.inserts` `event-time`): each into the row group its event time falls
in, so no two row groups overlap in event time. Every design finds a write's rows by each
changed key (`layout.locate` `keys`) and reads through the reader that skips dead row groups
(`cached-skip`), except the windows and tune campaigns' runs, which locate by scan and also read
through the other readers. The changes to stock Iceberg's readers and writers are
[Appendix A](../lab/pages/reader-changes.md).

The other arms in the code (`mor-v2-*`, `cow`, `tailcow-*`, `rmow-a`, `rmow-b`, the `-split`
arms, and `rmor-c` and `rmor-c-footer` on the updates) appear in no standing run. README, Arms
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

`verifyRows` is off (the id-and-version oracle stays on). Each configuration ran once a host:
havoc for requests, bytes and held memory, the laptop for instructions, and a cloud VM for wall
time. The 128 MiB campaigns (`d128-upd-*`) take the updates on 8 times the rows (205,480,000,
about 6.2 GB) in 128 MiB row groups and 512 MiB files, over 240 writes with reads every 40.

## The experiments that stand

| Experiment | Question | Varies | Designs | Runs | Write-up |
|---|---|---|---|---|---|
| **reads of a range** (`configs/win-*`, campaign `windows`, havoc and the laptop) | Q1–Q4 of 2026-11-07: what a 1% window by key (updates) or event time (inserts) costs away from the newest rows, and each phase's instructions | the two workloads' configurations of 2026-11-06 (`split`, `seal`; retired, which these equal in every record but the windows') but MoR under Spark's one-pass plan, with `reads.windows`: on the updates MoR, compacted every 10 writes, the deferred merges at θ 0.06 and 0.25 and by write; on the inserts lateness p99 1, 5 and 20, compacted, and the 160 MiB seal | MoR, RMoR, RMoW-external, RMoW-append, the deferred merges | 22, 2026-11-08 at 479edb1: 11 on havoc (10–87 min, 3 cores each) and the same 11 on the laptop (4–33 min) | Q1–Q4 held; the laptop's runs equal havoc's in all 10,197 records. The laptop's runs train the work formula the simulated pages price with (`analysis/tune.py`), and the inserts' runs give the workloads page its lateness twins. |
| **the inserts' policies** (`configs/tune-ing-*`, campaign `tune`) | T4 of 2026-11-08 (the tuner's insert configurations; its update ones and T1–T3 are retired) | one knob a configuration: MoR compacted every 3, 6 and 12 writes; RMoR's seal at 256 MiB, and at 160 MiB with fanouts 2 and 8; 4 MiB row groups, with and without the 256 MiB seal | MoR, RMoR, RMoW-append | 16: 8 on havoc, 2026-11-08 at 7cabf65, and the same 8 on the laptop (two first beside the lab's build, excluded, rerun) | The workloads page's insert curves. The 256 MiB seal writes 1.24 times and reads 0.74 of the 160 MiB one; with 4 MiB row groups RMoR's and RMoW-append's writers still keep a write's inserts as one row group. |
| **the two workloads' main points** (`configs/wl-*`, campaign `workloads-ref`) | The references for the cloud run (L1–L4 of 2026-11-10) | the `windows` main points with the locate by each changed key and the sorted read skipping dead row groups, read through `cached-skip` alone: on the updates MoR, MoR compacted every 10 writes, RMoR, RMoW-external, RMoW-append and the deferred merges; on the inserts MoR appending and compacted, RMoR and RMoW-append sealed at 160 MiB | as listed | 10: 5 on havoc (2026-11-10 at b49a57f, 7–61 min) and 5 on the laptop at 0942497 (2–18 min; the first five, beside another session's model checks, retired) | The workloads page's main points. |
| **the update curves** (`configs/wlc-upd-*`, campaign `workloads-curves`) | none: the workloads page's update curves with the main points' reader changes | MoR compacted every 3, 6 and 12 writes; the deferred merges at θ 1, 0.5, 0.25 and 0.12; row groups of 4, 8 and 32 MiB | MoR; the deferred merges; RMoW-append and the deferred merges at the row-group sizes | 20: 10 on havoc, 2026-11-11 at 2e3152d (7–24 min), and the same 10 on the laptop (seven first beside other sessions' jobs, retired, rerun at 4.52–4.64 GHz) | Every laptop run equals havoc's in requests, bytes and held memory. [workloads.md](../lab/pages/workloads.md). |
| **the two workloads in the cloud** (`configs/wl-*`, campaign `workloads-cloud`) | L1–L4 of 2026-11-10: do the workloads page's trade-offs hold on the stores' measured time? | the store | as `workloads-ref` | 21, 2026-11-11 at 0942497, once each: 5 on S3 Express (a `c7a.4xlarge` each in usw2-az3, four at once on one bucket, 4–33 min), each after its twin on the VM's disk and its spike; 5 on Azure Blob (one `D8as_v6` in westus, in turn after one spike, 5–47 min) | L1–L4 held. On the updates the region designs took 2.3–2.4 times as long as MoR and MoR compacted to write on S3 Express (1.5–2.2 on Azure Blob) and read a 1% range 1.4–3.9 times as fast (1.5–6.4); on the inserts MoR compacted and RMoR sealed read a range 2.1–2.7 times as fast as appending (2.1–2.6) for 0.41–0.47 s more a write (0.45–0.64). From the first five writes to the last five MoR's update write grew 1.84 times on S3 Express (2.40 on Azure Blob), and from the 5th write to the 30th its range read 3.11 times (3.93); the region designs' writes 1.10–1.23 times and their range reads 0.90–1.51. The region designs' update writes are 64–71% CPU on S3 Express (MoR's 41%). Priced from the cloud runs (the twin's work, the VM's spike), within 7.7% and 12.3% of measured. [workloads-cloud.md](../lab/pages/workloads-cloud.md) (L2, L3); [simulation-cloud.md](../lab/pages/simulation-cloud.md) (L1, L4: the simulation against the cloud). |
| **copies cut where a write changes, at 128 MiB** (`configs/d128-upd-*`, campaigns `density128` and `density128-cut`, on havoc) | D1–D4 of 2026-11-09: at Iceberg's 128 MiB row groups, where a row group holds about eight update regions, do the designs as built read or copy whole row groups, and does copying a row group as several, cut where the write changes it (`layout.cut`, a68eda3), let RMoR copy only what changed? | one setting a configuration: MoR, MoR compacted every 10 writes, the deferred merges (θ 0.06, deltas by region), the whole copies, the cut (θ 0) and the deferred merges cut | MoR, RMoR kept, RMoW-append, the deferred merges | 6 on havoc, 3 cores each: 4 at 2025e7c from 2026-11-10 07:45 UTC (90–171 min), 2 at a68eda3 from 10:52 UTC (142–150 min), requests and held memory logged; before them, the cut's control on the laptop (all 2,350 records equal) and a fuzz round on havoc (1,274 seeds, no wrong result) | D1, D2 and D4 held; D3 contradicted by one part of five. The cut (θ 0) reads 173 MB a write over writes 201–240, 0.12 of the deferred merges' 1,454, and its windows read 1.16 times their live bytes (3.21 for the deferred merges, 14.4 for MoR); but it writes 172 MB a write, 3.04 times the deferred merges' 56.6. Simulated on S3 Express, a write takes the cut 12.9 s against 21.4 for the deferred merges and 25.8 for MoR, a 1% window 1.86 s against 2.90 and 11.1. The model left out the locate: the cut sends about 1,070 requests a write to find its rows. On Azure Blob the cut's write is 25.3 s against 27.4, and its full scan the slowest (86 s against 63). [region-cut.md](../lab/pages/region-cut.md). |
| **micro** (`results/micro/*.json`, JMH) | What a footer costs per row group | row groups 10–10k × 11 or 50 columns; parse, convert, merge and serialize | n/a | 2 files, 2026-10-20 | [micro.md](../lab/pages/micro.md); `docs/measurements/footer-*-2026-10-20.txt`. The source of the README's "about 1.2 KB and 30 µs per row group". |
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
| L1–L4 (2026-11-10) | The two workloads in the cloud: each run sends what havoc's run of its configuration counts; on the updates the region designs write slower than MoR and MoR compacted and read a 1% window faster than both, MoR compacted and the deferred merges read one faster than MoR, and the deferred merges write faster than the region designs; on the inserts MoR compacted and RMoR sealed read a window faster than appending and write slower, by under 1 s; on S3 Express each write and read priced within 15% by its twin's work and its VM's spike | **Held** (S3 Express and Azure Blob, 2026-11-11, at 0942497). **L1:** the 15 runs (5 a store and 5 twins) equal havoc's in every record, one-thread held peaks too. **L2:** the region designs' writes 2.3–2.4 and 1.5–2.2 times MoR's and MoR compacted's, their windows 1.4–3.9 and 1.5–6.4 times as fast; MoR compacted's window 1.9 and 2.6 times as fast as MoR's, the deferred merges' 2.1 and 2.6, and their writes 1.7–1.8 and 1.3–1.5 times as fast as the region designs'. **L3:** windows 2.1–2.7 and 2.1–2.6 times as fast, writes 0.41–0.47 and 0.45–0.64 s more. **L4:** priced 0.92–1.03 of measured. |

## Retired runs

Outdated runs are deleted, not kept: `results/` is not in git, so this section records what went
and why. What their data showed, where it still matters, is in [LESSONS.md](LESSONS.md); the
entries before 2026-11-12, with their numbers, are this file at bfbdf33.

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

The outstanding work is [ROADMAP.md](ROADMAP.md). Open, from the runs that stand: a second seed,
to show the request and byte findings are not one op log's; wider tables (every run has 11
columns); the density hypothesis where density can choose (a workload of sparse changes, cold
pieces merged back, and the locate's requests); a reader that skips pages, not just row groups;
GCS once the fork appends there; split files on standard S3.
