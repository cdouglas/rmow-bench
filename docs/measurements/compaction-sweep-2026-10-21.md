# When to compact: a compaction-policy sweep (2026-10-21)

## Setup

- **Configurations:** `configs/local-compact-*.json`. The workload is `local-m1`'s recency
  workload: 1M initial rows, then 60 batches of 10k operations (70% inserts, 25% updates, 5%
  deletes), with update and delete keys exponential in age at a mean of 2 batches.
- **Arms:** those of the split comparison: MoR-v3, TailCoW-C, and RMoW-B and RMoW-C on appendable
  and on split files.
- **Policies:** eleven in all. One is no compaction. Nine cross check periods `P` (`compactEvery`)
  of 1, 5 and 20 batches with the three threshold levels below. The last is `P` = 10 at the
  default level.

| level | RMoW garbage ratio | MoR delete ratio | RMoW small row groups |
|---|---|---|---|
| aggressive | 0.25 | 0.1 | 4 |
| default | 0.5 | 0.3 | 8 |
| lazy | 0.75 | 0.5 | 16 |

MoR's small-file fraction stays at Iceberg's 0.75. RMoW compaction is the one in
[DESIGN.md](../DESIGN.md#compaction):
- for space, an appendable file is rewritten, while a split file has the live row groups of its
  mostly dead objects moved out;
- for reads, small sealed row groups are packed, up to the writer's row-group size.

How it was measured:
- **Probes** run every 3 batches. Each reads the full table and the newest 1% of event time,
  under the one-footer reader. Probes every 3 batches sample every phase of a 1-, 5-, 10- or
  20-batch compaction cycle. Time travel is not read.
- **Runs:** local store, three runs at a time, from one jar: build snapshots
  `results/builds/20261021T1922{59,302,305}Z-aac3bdff873a-dirty`.
- **Analysis:** `analysis/compaction.py results/compaction-sweep results/local-compact-*/<start>`
  writes every policy's numbers, `compaction-frontier.png` and `compaction-cost.png`.
- **Units:**
  - Writes are per batch, means over the 60 batches, compaction included.
  - Reads are means over the probes.
  - Kept/live is the mean over steps of what remains once every older version expires, against
    live bytes. For MoR, live bytes include deleted rows inside live files.

## What compaction costs and buys

MoR-v3 and TailCoW-C did the same I/O at every threshold level for a given `P`. The small-file rule
fired before any delete ratio did, so only `P` matters for them:

| `P` | MoR-v3: MB written | compaction MB | full-read GETs | full-read MB | writer GETs | TailCoW-C: MB written | compaction MB | full-read GETs |
|---|---|---|---|---|---|---|---|---|
| none | 0.39 | 0 | 85.0 | 41.4 | 43.3 | 0.53 | 0 | 44.8 |
| 20 | 0.81 | 0.43 | 45.2 | 39.3 | 34.4 | 0.94 | 0.42 | 34.4 |
| 10 | 1.11 | 0.76 | 36.3 | 38.7 | 22.2 | 1.24 | 0.72 | 32.5 |
| 5 | 1.75 | 1.42 | 31.8 | 38.5 | 15.8 | 1.57 | 1.06 | 31.9 |
| 1 | 6.99 | 6.66 | 26.7 | 38.3 | 13.5 | 1.72 | 1.21 | 31.0 |

Between compactions MoR-v3's full read grows by 2 GETs per batch: at `P` = 20 it climbs from 29 to
59–65 and drops back at each compaction. Writer GETs, mostly the `locate` scans that find the rows
a batch changes, fall with compaction too. At `P` = 1 MoR rewrites its growing compacted file every
batch until that file passes the small-file size, which is where its 6.7 MB per batch of compaction
goes.

The RMoW arms are given as MB written (of which compaction), full-read GETs, and kept/live:

| policy | RMoW-B | RMoW-C | RMoW-B-split | RMoW-C-split |
|---|---|---|---|---|
| none | 5.55 (0) · 15.3 · 5.07 | 5.16 (0) · 19.1 · 4.82 | 5.55 (0) · 15.3 · 1.37 | 5.17 (0) · 19.0 · 1.40 |
| lazy P=1 | 6.80 (1.47) · 15.6 · 2.02 | 6.77 (1.47) · 16.8 · 2.02 | as none | as none |
| default P=20 | 6.41 (0.86) · 15.0 · 2.55 | 6.24 (0.86) · 17.4 · 2.53 | 5.60 (0.05) · 15.3 · 1.27 | 5.31 (0.14) · 19.0 · 1.31 |
| default P=10 | 6.74 (1.59) · 15.6 · 1.97 | 7.02 (1.59) · 16.8 · 2.00 | 5.60 (0.05) · 15.3 · 1.25 | 5.31 (0.14) · 19.0 · 1.25 |
| default P=1 | 8.83 (3.54) · 15.2 · 1.57 | 8.92 (3.54) · 15.4 · 1.57 | 5.65 (0.10) · 15.3 · 1.24 | 5.37 (0.20) · 19.0 · 1.22 |
| aggressive P=20 | 6.97 (1.41) · 15.0 · 2.31 | 6.80 (1.41) · 17.4 · 2.30 | 5.73 (0.18) · 15.3 · 1.23 | 5.77 (0.36) · 16.9 · 1.26 |
| aggressive P=5 | 9.42 (4.20) · 15.5 · 1.31 | 9.56 (4.20) · 16.0 · 1.32 | 5.78 (0.23) · 15.3 · 1.19 | 5.74 (0.31) · 16.6 · 1.19 |
| aggressive P=1 | 14.91 (9.53) · 14.6 · 1.07 | 14.91 (9.53) · 14.6 · 1.07 | 5.85 (0.30) · 15.3 · 1.17 | 5.80 (0.37) · 16.5 · 1.18 |

## The cheapest policy

The cost per batch is:
- **writes:** PUTs × put + GETs × get + GB written × upload + GB read × retrieval + CPU × compute;
- **reads:** q × the same terms for one full read;
- **storage:** kept GB × storage per GB-month × the batch interval.

Prices:
- **Stores:** US East list prices. S3 Standard: $0.005 per 1,000 PUTs, $0.0004 per 1,000 GETs,
  free in-region transfer, $0.023 per GB-month. S3 Express One Zone since its April 2025 cut:
  $0.00113 and $0.00003 per 1,000, $0.0032 per GB uploaded, $0.0006 per GB retrieved, $0.11 per
  GB-month.
- **Compute:** $0.04 per vCPU-hour, an assumption.

The batch interval only weighs storage against the rest. At this table size (about 55 MB), a day's
interval weighs storage as a 5-minute one would for a table 288 times as large.

Policies of one arm that did the same I/O count as one, with their CPU averaged. A full read's
CPU is the arm's mean over its runs: the split arms read identically under every policy, yet
their read CPU varied by 6% between runs. Runs that did the same I/O still differ by up to 2.0% in
cost, from CPU alone, so smaller savings are reported as no compaction. Appendable RMoW cannot run
on S3 Standard, which has no append.

Each cell is the cost per batch in µ$, with the saving against no compaction:

| store | full reads per batch | batch interval | MoR v3 DV | TailCoW-C | RMoW-B | RMoW-C | RMoW-B-split | RMoW-C-split |
|---|---|---|---|---|---|---|---|---|
| S3 Standard | 0.1 | 5 min | **any level P=5**: 25 (30%) | **any level P=1**: 24 (5%) | n/a (no append) | n/a (no append) | none: 23 | none: 25 |
| S3 Standard | 1 | 5 min | **any level P=5**: 67 (31%) | **any level P=1**: 66 (9%) | n/a (no append) | n/a (no append) | none: 58 | none: 61 |
| S3 Standard | 10 | 5 min | **any level P=1**: 474 (33%) | **any level P=1**: 485 (10%) | n/a (no append) | n/a (no append) | none: 402 | **aggressive P=1**: 410 (2%) |
| S3 Standard | 0.1 | 1 day | **any level P=5**: 54 (20%) | **any level P=1**: 53 (4%) | n/a (no append) | n/a (no append) | **aggressive P=1**: 58 (7%) | **aggressive P=1**: 60 (8%) |
| S3 Standard | 1 | 1 day | **any level P=5**: 96 (25%) | **any level P=1**: 95 (7%) | n/a (no append) | n/a (no append) | **aggressive P=1**: 92 (5%) | **aggressive P=1**: 95 (6%) |
| S3 Standard | 10 | 1 day | **any level P=1**: 503 (32%) | **any level P=1**: 514 (10%) | n/a (no append) | n/a (no append) | none: 441 (≈ aggressive P=1, 1%) | **aggressive P=1**: 444 (3%) |
| S3 Express | 0.1 | 5 min | none: 15 | none: 13 | none: 36 | none: 35 | none: 35 | none: 35 |
| S3 Express | 1 | 5 min | **default P=10**: 69 (2%) | none: 67 | none: 85 | none: 84 | none: 85 | none: 85 |
| S3 Express | 10 | 5 min | **any level P=5**: 593 (5%) | none: 601 (≈ default P=10, 1%) | none: 581 | none: 580 | none: 583 | none: 584 |
| S3 Express | 0.1 | 1 day | **default P=10**: 157 (7%) | none: 158 (≈ any level P=20, 1%) | **aggressive P=1**: 226 (70%) | **aggressive P=1**: 226 (69%) | **aggressive P=1**: 198 (11%) | **aggressive P=1**: 199 (13%) |
| S3 Express | 1 | 1 day | **default P=10**: 209 (7%) | none: 211 (≈ default P=10, 1%) | **aggressive P=1**: 276 (66%) | **aggressive P=1**: 275 (64%) | **aggressive P=1**: 248 (9%) | **aggressive P=1**: 249 (11%) |
| S3 Express | 10 | 1 day | **any level P=5**: 732 (6%) | none: 745 (≈ default P=10, 2%) | **aggressive P=1**: 770 (41%) | **aggressive P=1**: 769 (39%) | **aggressive P=1**: 745 (3%) | **aggressive P=1**: 747 (4%) |

MoR's `default P=10` is just its 10-batch period, since its thresholds never fired.

## Findings

- **The store decides whether MoR should compact.**
  - On S3 Standard, a GET costs 13 times what it costs on S3 Express, and bytes are free. There,
    compacting every 5 batches saves MoR-v3 20–31% at up to 1 full read per batch, and
    compacting every batch saves 32–33% at 10.
  - On S3 Express, reads are charged by the byte, and compaction leaves those nearly unchanged (41
    to 38 MB per full read), so it saves MoR-v3 at most 7%.
  - TailCoW's tail merge already does much of compaction's work, so it gains less: 4–10% on
    Standard, nothing on Express.
- **Space is the only reason to compact RMoW, and it pays only once storage weighs enough.** At
  5-minute batches on this table, storage is at most 7% of any arm's cost (3% from one full read
  per batch up), and no RMoW compaction pays. The 2% at 10 full reads per batch is at the noise
  floor.
- **At a day's storage weight, appendable files need the most aggressive policy tested.**
  - Their cheapest policy is garbage 0.25 at `P` = 1, which cuts their cost by 39–70% on S3
    Express.
  - Uncompacted, they keep 4.8–5.1× their live bytes on average and 7.8–8.3× at batch 60.
  - That optimum is at the edge of the sweep, so a lower threshold may do better.
- **Split files need little compaction, and it is cheap.**
  - Moving row groups out of objects at least 25% dead holds their kept/live at 1.17–1.26, against
    1.37–1.40 uncompacted. It costs 0.18–0.37 MB of compaction writes per batch, 3–7% of their
    batch writes.
  - It saves 3–13% at a day's weight and nothing at 5 minutes.
  - At each layout's cheapest policy on S3 Express, split files cost 9–12% less than appendable
    ones at a day's weight with up to 1 full read per batch. At 10 reads per batch the gap is 3%,
    and at 5 minutes they cost the same.
- **At 5-minute batches, full reads dominate the cost from one read per batch up.** They are
  42–89% of it at one read per batch and 88–99% at ten. A read's CPU (about 2.9 s, so 32 µ$ at the
  assumed price) and, on Express, its retrieval (about 23 µ$ for 38 MB) do not depend on
  compaction. Only its GETs do, and they matter only at S3 Standard's price.
- **RMoW reads at least as well uncompacted as MoR does at its most frequent compaction.**
  RMoW-B's full read takes 15 GETs at every policy, against MoR-v3's 27 at `P` = 1 and 85
  uncompacted.

### H6 (docs/HYPOTHESES.md)

- **RMoW reads barely depend on compaction: held for B, not for C.**
  - RMoW-B read 14.6–15.6 GETs against 15.3 uncompacted, in either layout.
  - RMoW-C dropped by more than 2 under most policies, by up to 4.6 (to 14.6 against 19.1, at
    aggressive `P` = 1). Its split files dropped by 2.1–2.6 under the aggressive policies, which
    pack from 4 small row groups up, and not at all under the others. When the tail file rolls over, tiered
    merging leaves small row groups sealed, and rewrites and packing merge them.
  - The second half held. With packing stopped at the writer's size, RMoW-B at the default `P` =
    10 reads 18 GETs at batch 60 (17 uncompacted). The rewrite in split-vs-append-2026-10-21.md
    gave it 10.
- **Appendable files need compaction for space: held.**
  - Uncompacted, kept/live reached 7.8–8.3× at batch 60.
  - At `P` = 1, mean kept/live stayed at 1.07, 1.57 and 2.02 for garbage ratios 0.25, 0.5 and 0.75,
    under the bounds 1.33, 2 and 4.
  - Compaction writes fell from 9.5 to 3.5 to 1.5 MB per batch as the ratio rose.
- **Split files need little: held.** At garbage 0.25, mean kept/live was 1.17–1.26, for 0.18–0.37
  MB of compaction writes per batch. At 0.75 nothing moved.
- **MoR-v3 and TailCoW trade writes for reads: held.**
  - As `P` fell from none through 20, 10 and 5 to 1, both arms' writes rose monotonically and
    their mean full-read GETs fell.
  - `P` = 1 wrote the most.
  - MoR-v3's GETs grow linearly between compactions.
- **Costs (S3 Express, 5-minute batches, 1 full read per batch): half held.**
  - Split RMoW's cheapest policy is no compaction.
  - MoR-v3's cheapest has `P` = 10, not 5 or less, and saves only 2%: `P` = 5 costs 1% more than no
    compaction. The prediction was right for S3 Standard (`P` = 5, 31%), not for Express.

## Limits

- **Scale.** The table is about 55 MB. The batch interval stands in for table size only as far as
  storage goes. A larger table also has more files to read and larger files to rewrite.
- **Retention.** Kept bytes assume every older version expires at once. With a retention window,
  the objects a rewrite replaces, and a split file's reclaimable objects, stay stored until they
  leave it. That raises storage most for aggressive policies.
- **CPU.** CPU is thread CPU time, measured locally with three runs at a time. It is noisy, so
  differences under 2% decide nothing. Latency is not measured locally. It matters most where
  compaction cuts GETs: MoR's full read goes from 85 GETs to 27–45.
- **Not modeled.** Metadata and commit costs (manifests, the catalog) are left out for every arm.
- **Grid.** The sweep did not vary MoR's small-file fraction, the only MoR threshold that fired.
  For RMoW at a day's weight, the cheapest policy is the grid's most aggressive one.
