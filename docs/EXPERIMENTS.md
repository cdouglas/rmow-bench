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
| `rmow-*-dv` | RMoW with DVs (ablation) | as `rmow-*` | deletion vectors on each object's stable prefix. **In no experiment so far.** |

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
| **time travel** (no new runs) | H4: does an old version read like the current one, and do dead bytes cost reads? | read from the 7 uncompacted runs: each time-travel read against the full read of the same state | all that ran | 7 of the above | [lab/pages/time-travel.md](../lab/pages/time-travel.md) |

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

Seven early m1 runs were retired by renaming their directories `superseded-…-<reason>`:
- `v2bounds` (2 runs)
- `onereader` (2 runs)
- `twopass`
- `unmerged`
- `partial`

The in-memory `smoke` run tests the harness. The lab marks all eight and leaves them out of every
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
  - Under the one-footer reader, an old version costs exactly the current read's GETs, in all 110
    RMoW pairs. Dead bytes are never read: appendable files read 1.000–1.002× their live bytes.
  - Under the stock reader, these runs' harness planned old RMoW versions without split offsets,
    so that comparison measures the harness. The harness is fixed; the runs predate the fix.
  - The trace cannot show S3's extra abort per old footer.

## The hypotheses

| | Claim, in short | Status |
|---|---|---|
| H1 | Recency-skewed updates: RMoW-C's full read stays flat, its writes within 3× MoR-v3's | **Open.** Decided on S3 Express, which has not run. |
| H2 | Uniform updates: RMoW's writes approach copy-on-write | **Held** (local, m1). |
| H3 | RMoW-C against TailCoW is the single-object effect | **Mixed.** Smaller, and mostly in reads, in request counts; larger, and mostly write bytes, on S3 Express prices at up to one full read per batch. Not the single object: append is 1.28 requests per batch. |
| H4 | Old versions read like current ones; dead bytes cost nothing | **Held** for reads and dead bytes, under the one-footer reader. The extra abort on S3 is **open**: the trace cannot show it. |
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
  - the `-dv` ablation is in no experiment. `rmow-c-dv` against `tailcow-c` would isolate the
    single object, which H3's comparison could not;
  - every run so far planned old RMoW versions without split offsets, which biases the stock
    reader's `read_old` (lab/pages/time-travel.md). The harness is fixed, and
    `RunnerSmokeTest.timeTravelReadsLikeTheCurrentRead` pins it. A rerun of the uncompacted
    configurations would make the stock-reader pairs a test.
