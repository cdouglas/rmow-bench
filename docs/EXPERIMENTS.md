# Experiments: a map

Where to start. This page says what has been run, what each experiment varied and asked, what it
found, and where the details are. It covers the 22 canonical runs and the JMH micro-benchmarks as
of 2026-10-22.

## Where things are

| Document | What it is for |
|---|---|
| [README.md](../README.md) | What RMoW is, how to build and run, and the reference tables of arms and knobs. |
| [docs/PLAN.md](PLAN.md) | The plan as approved on 2026-10-20, with milestones. Its default matrix is not what ran ([below](#what-every-experiment-shares)). |
| [docs/DESIGN.md](DESIGN.md) | The format-level findings: writer rules, footer order, split files, compaction. |
| [docs/HYPOTHESES.md](HYPOTHESES.md) | H1–H6, each written down before the runs that decide it. |
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

## The experiments

| Experiment | Question | Varies | Arms | Runs | Write-up |
|---|---|---|---|---|---|
| **m1** (`configs/local-m1-*`) | Every arm under recency-skewed and uniform updates, with and without compaction. | key distribution (recency, uniform) × compaction every 0 or 10 batches | the 9 MoR, CoW, TailCoW and RMoW arms (all but split and DV) | 4 | None. The H2 result is in [HYPOTHESES.md](HYPOTHESES.md#h2-result-local-recorded-before-h5); the lab's run page shows every run. |
| **rg-sweep** (`configs/local-rg-sweep-*`) | How row-group size trades RMoW's writes against its reads. | `rowGroupBytes` = `tailSealBytes` = 1, 2, 4, 8 MiB; recency keys, no compaction | the same 9 | 4 | [rg-sweep-recency-2026-10-21.md](measurements/rg-sweep-recency-2026-10-21.md) |
| **split-vs-append** (`configs/local-split-*`) | Appendable against split RMoW files (H5). | recency, no compaction; recency, compaction every 10; uniform, compaction every 10 | MoR-v3, TailCoW-C, RMoW-B and -C in both layouts (plus CoW for uniform) | 3 | [split-vs-append-2026-10-21.md](measurements/split-vs-append-2026-10-21.md). Its compaction numbers are superseded by the sweep. |
| **compaction** (`configs/local-compact-*`) | When to compact, as an outcome (H6). | check period `P` = none, 1, 5, 10, 20 × threshold level (table below) | the split comparison's six | 11 | [compaction-sweep-2026-10-21.md](measurements/compaction-sweep-2026-10-21.md) |
| **micro** (`results/micro/*.json`, JMH) | What a footer costs per row group. | row groups 10–10k × 11 or 50 columns; parse, convert, merge and serialize | n/a | 2 files | `docs/measurements/footer-*-2026-10-20.txt`. The source of the README's "about 1.2 KB and 30 µs per row group". |
| **layout vs policy** (no new runs) | H3: is RMoW against TailCoW (layout) a smaller difference than TailCoW against MoR-v3 (tail policy), and mostly in read requests? | read from the uncompacted rg-sweep, split and m1 uniform runs | MoR-v3, TailCoW-B and -C, RMoW-B and -C in both layouts | 6 of the above | [lab/pages/layout-vs-policy.md](../lab/pages/layout-vs-policy.md) |
| **time travel** (reruns) | H4: does an old version read like the current one, and do dead bytes cost reads? | the 7 uncompacted configurations that read old versions, rerun 2026-10-22/23 with the split-offset fix: each time-travel read against the full read of the same state | all that ran | 7 of the above, rerun | [lab/pages/time-travel.md](../lab/pages/time-travel.md) |
| **dv ablation** (`configs/local-dv-*`) | H3's attribution: how much of RMoW against TailCoW is the layout, and how much the update handling (region rewrites against DVs)? | key distribution (recency, uniform); 4 MiB, no compaction | MoR-v3, CoW, TailCoW-B and -C, RMoW-A, -B and -C, RMoW-A-dv, -B-dv and -C-dv | 3, deleted | Conclusions in [DESIGN.md](DESIGN.md#deletion-vectors-on-an-appendable-file-an-ablation-removed); numbers under Retired runs. |

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

  That is what docs/HYPOTHESES.md registered on 2026-10-22: no difference outside those.

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

The reruns match them as registered in docs/HYPOTHESES.md (2026-10-23), with no difference
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

## Findings so far

All are local runs: requests and bytes are exact, latency is not measured.

- **Uniform updates (m1, H2).** RMoW writes what copy-on-write writes: 37.5 MB per batch, against
  CoW's 37.6 and MoR-v3's 0.46.
- **Row-group size (rg-sweep).** The balance for this workload is near 2–4 MiB. No size changes
  the order against MoR-v3:
  - RMoW-B and -C write 10–18× MoR-v3's bytes per batch, in 7–18% of its requests;
  - they read the table in 8–37% of its GETs.
- **Split files (split-vs-append).** They read the same and write the same bytes, at one more
  request per changed file. Once older versions expire they keep 1.4–1.5× their live bytes, where
  an uncompacted appendable file keeps 7.8–8.3×.
- **When to compact (compaction).**
  - The store decides whether MoR-v3 should compact: 20–33% cheaper on S3 Standard, at most 7%
    on S3 Express.
  - Space is the only reason to compact RMoW. It pays only when storage weighs enough (a day
    between batches at this table size).
  - Split files need little compaction, and it is cheap.
- **Layout against tail policy (H3).**
  - In request counts, RMoW against TailCoW is the smaller difference, and mostly in reads
    (2–8 MiB).
  - In bytes it is the larger: RMoW writes 3.3–5.3 MB more per batch, from rewriting the row
    groups that updates touch. So on S3 Express it is the larger cost at up to one full read per
    batch.
  - Append itself is worth 1.28 requests per batch. Most of the rest comes from how RMoW applies
    updates (no locate scan, no DVs) and from one footer per file.
- **Time travel (H4).**
  - Under both readers, an old version costs exactly what the current read of the same state
    costs, in every request and byte column: all 600 pairs, 220 of them over RMoW files.
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


## The hypotheses

| | Claim, in short | Status |
|---|---|---|
| H1 | Recency-skewed updates: RMoW-C's full read stays flat, its writes within 3× MoR-v3's | **Open.** Decided on S3 Express, which has not run. |
| H2 | Uniform updates: RMoW's writes approach copy-on-write | **Held** (local, m1). |
| H3 | RMoW-C against TailCoW is the single-object effect | **Mixed.** Smaller, and mostly in reads, in request counts; larger, and mostly write bytes, on S3 Express prices at up to one full read per batch. The dv ablation puts 70% to all of the extra bytes on the update handling, and most of the read savings on the single object (with a delete cache). |
| H4 | Old versions read like current ones; dead bytes cost nothing | **Held** for reads, under both readers (after the rerun), and for dead bytes. The extra abort on S3 is **open**: the trace cannot show it. |
| H5 | Split files: one more request per changed file, the same reads, kept bytes within 1.5× | **Held**, except that RMoW-C kept 1.52×. |
| H6 | When to compact | **Mixed.** The read half held for RMoW-B, not RMoW-C; the space and write-for-read halves held; costs half held. |

## What is next

- **S3 Express (milestone M3).** It starts with `configs/s3x-calibration.json`, which needs
  credentials, a directory bucket and an instance in its zone. It is what decides H1 and gives the
  first latencies. It is also where H4's abort can be measured, from aborted connections or the
  GET bytes the SDK declares. Split files can also run on standard S3.
- **Gaps in what ran:**
  - no repetitions or second seeds;
  - no write-up of m1 beyond H2;
  - only runs from 2026-10-23 on can have the `cached` reader model, which loads each file's
    deletes once per read. Earlier runs' readers load a file's DV once per split (row group), as
    Iceberg's readers do, which inflates MoR-v3's and TailCoW's reads;
