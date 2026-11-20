# Configuration: arms and knobs

Every arm the code runs, and every knob of a configuration (`configs/*.json`). Moved from the
README on 2026-11-18; a citation "README, Arms", "README, Knobs" or "README, The workload" means
the section of that name here. The README names the five designs the results compare; this page
maps them to arms and settings.

## Arms

### The designs

| Design | Arm | Settings that make it the design |
|---|---|---|
| **MoR** | `mor-v3` | none |
| **MoR (compacted)** | `mor-v3` | `compactEvery` (1 at 128 MiB) with `compactMinInputFiles` (5): Iceberg's bin-pack, as its rule plans it after every write |
| **RMoR** | `rmor-c-kept` | `cut` (at 128 MiB) |
| **RMoW-external** | `rmor-c-footer-kept` | `cut` (at 128 MiB) |
| **RMoW-append** | `rmow-c` | none (it copies whole row groups) |
| **RMoR-density** | `rmor-density` | `defer` (θ 0.06, deltas by region) and `cut` (at 128 MiB) |

What each does:
- **`mor-v3`:** a new data file per batch for inserts and the new versions of updated rows; the
  replaced and deleted rows marked by deletion vectors in a Puffin file per batch, a data file's DV
  replaced by one merged with its predecessor.
- **`rmor-c-kept`:** a batch copies each row group it changes, with its changes, into the batch's
  new data file, as one row group of the same rows (sorted by `ts` within itself; with `cut`,
  several), and a DV covers the old row group whole, so a reader that skips dead row groups skips
  it. A file with no live row left leaves the table. The format is Iceberg's, and no store needs to
  append. Written a row group at a time: each is read, rewritten, encoded and dropped before the
  next is read, and the file streams to the store.
- **`rmor-c-footer-kept`:** as `rmor-c-kept`, but the old row group is hidden by a footer appended
  to its file, listing the file's other row groups, instead of a DV. Its data files are created as
  objects the store can extend, and each append is a new version of the data file (length, split
  offsets, metrics). No delete files. The batch's new file is held for its single create.
- **`rmow-c`:** inserts are appended to the tail object as a row group, merged with open tail row
  groups by tail policy C (below); every row group a batch changes is rewritten as a region
  appended to its own object, followed by a new footer that lists the object's live row groups.
  Every version of the object is its prefix up to that version's footer.
- **`rmor-density`:** `rmor-c-kept` that defers each copy (`layout.defer`). A batch merges a row
  group it changes once the row group's dead rows and the batch's changes to it reach θ
  (`defer.share`) of its rows, folding in the rows of the region that wait in delta row groups
  (found by id range; each delta row group read whole once a batch, DVs hiding the rows taken).
  Below θ, DVs, and the new versions wait in delta row groups of the batch's file: one per region,
  the base row group they left (`defer.deltas` `region`), or one per batch (`write`). A delta row
  group never merges on its own, except by the merge compaction (`layout.merge`, below). With θ
  above 1 it never copies: MoR with its new versions in a delta row group per region, which with
  the merge compaction is the control **MoR (deltas by key range)** (density plan, step 3).

On the inserts, where a region design puts a new row is set by `layout.inserts` (below): at
128 MiB, the event-time merge.

### Other arms

The code runs more arms, which no standing run uses (docs/EXPERIMENTS.md says which retired
experiments did; docs/LESSONS.md what they taught). They stay for ablations and the fuzzer.

| Arm | Inserts | Updates and deletes |
|---|---|---|
| `mor-v2-file` | a new data file per batch | Parquet position-delete files, one per touched data file per batch, never merged |
| `mor-v2-partition` | the same | one position-delete file per batch, matched to every older data file by sequence number (as Iceberg 1.12 plans them) |
| `cow` | a new data file per batch | every touched data file is rewritten whole |
| `tailcow-b`, `tailcow-c` | merged into open tail *data files* by policy B or C | DVs, except in tail files being merged |
| `rmor-c` | as `tailcow-c` | a batch rewrites each region it changes when it changes at least θ (`rmorRewriteShare`) of the region's live rows: a sealed file's row group goes, with the batch's changes, into the batch's new data file, sorted with all the file's rows by `ts` and cut into new row groups, and a DV covers the old row group; an open tail file is rewritten whole. Below θ, DVs. θ = 0 rewrites every region a batch changes; above 1 it is `tailcow-c` |
| `rmor-c-footer` | as `rmor-c`, its data files created as objects the store can extend | `rmor-c`'s rewrites at θ = 0, each old row group hidden by a footer appended to its file instead of a DV |
| `rmor-c-clean` | as `rmor-c-kept` | `rmor-c-kept` with a rewrite threshold and a cleaner (`layout.clean`): a batch rewrites a row group only when it changes at least `clean.rewriteShare` of its live rows, and otherwise puts DVs on the rows it changes. Then a cleaner (phase `clean`) carries forward the live rows of row groups under `clean.low` of their rows live and unchanged for `clean.age` batches, ranked by (1 − u) · age / (1 + u), within `clean.budget` of the batch's own rows; a cold file under `clean.drain` live has all its rows carried and leaves the table (an unregistered draft: docs/drafts/strides-and-cleaner-2026-11-04.md) |
| `rmow-a`, `rmow-b` | as `rmow-c`, by tail policy A or B | as `rmow-c` |
| `rmow-a-split`, `rmow-b-split`, `rmow-c-split` | as `rmow-*`, but each version's regions go to a new object, and its footer to a footer-only object naming the objects its row groups live in (`file_path`) | the same row-group copy-on-write; no append, so standard S3 and GCS can run them, and whole objects become deletable as versions expire ([DESIGN.md](DESIGN.md#regions-in-separate-objects-split-files)) |

The tail policies (`TailPolicy`) are:
- **A:** never merge.
- **B:** re-merge the one open tail until it reaches `tailSealBytes`.
- **C:** tiered: `tierFanout` open units of one level merge into one unit a level up.

## Knobs (`layout` in the config)

| Knob | Meaning |
|---|---|
| `codec` | The Parquet codec. The default is zstd. |
| `rowGroupBytes` | The row-group target of data files and the initial load, as Parquet counts it (buffered bytes), so row groups are smaller on disk: about a third of a 1 MiB target for this schema, three quarters of a 4 MiB one, and 15.2 MiB of a 16 MiB one (the default). The 128 MiB runs set 134,217,728, Iceberg's default. RMoW-append's compaction packs row groups up to the size the writer's come out at on disk. |
| `targetFileBytes` | The size at which data files, and an appendable file's live bytes, roll over. |
| `sortWrites` | Sort every write by `ts` (on by default). |
| `tailSealBytes` | The compressed size at which an open tail unit is sealed (policies B and C). An RMoW unit is a row group of inserts; a TailCoW unit is a batch's data file, updated versions included. |
| `tierFanout` | How many units of one level merge into one (policy C). |
| `maxRowGroupsPerFile` | The RMoW tail rolls over at this many row groups. Footers cost about 1.2 KB and 30 µs per row group. |
| `footerOrder` | Where RMoW lists a row group that replaces another. `logical` (the default) puts it in the replaced one's place, so footer order, which is row order, stays id order and a sorted file stays sorted. `offset` lists it last, keeping the footer in file-offset order. The Parquet spec requires neither, and the I/O is the same ([DESIGN.md](DESIGN.md#footer-order)). |
| `writerFooters` | What every arm's writer holds of the footers it reads (HARNESS.md, I/O parity rule 6). `transaction` (the default): a file version's footer is read once per batch or compaction and reused within it. `open`: every open reads it again, as parquet-java does. Nothing carries over from one transaction to the next. |
| `footerPrefetchBytes` | Writers (locate scans, merge reads, compaction) prefetch this tail on a file's first footer probe, so a footer costs 1 request instead of 2. 0 (the default) means stock parquet-java. Each reader model sets its own (`reads.readers`). |
| `locate` | `oracle` (the default) takes row positions from an in-memory index, at no cost. `scan` finds them the way a MERGE would, in a timed `locate` phase checked against the index. The MoR family scans `id` and `_pos` of every file whose `id` bounds may hold a target, with row groups pruned to the targets' one `id` range; RMoW-append looks the ids up in its footers' row-group statistics. `keys` (since 2026-11-10) is `scan` with the MoR family's row groups pruned by each target, as files are: a writer reads only the row groups whose `id` statistics, in the footer it holds under `writerFooters` `transaction`, may hold one. Iceberg's filters stop pruning by a list of values past 200 of them (`IN_PREDICATE_LIMIT`), so a MERGE of thousands of changes prunes at best to their range; `keys` is an enhancement an engine could make (lab page reader-changes, Writers). Files of at most 1 MiB, fetched whole, are pruned as under `scan`. The 128 MiB results use `keys`. |
| `updateRead` | What a write reads of the rows it updates; a new version is its old row with some columns set. Absent (or `id`): nothing beyond the locate's `id` and `_pos`, so the MoR family's new versions are made blind. `row`: every column of each row group holding a row the batch updates, deletes applied, once: the row groups and files a write rewrites are read whole anyway (`merge-read`), the others in phase `update-read`, after the locate, as Spark plans a copy-on-write MERGE (find the rows by key, then read). `scan`: MoR (v2, v3) reads every column in its locate scan, one pass, as Spark plans a MERGE on a merge-on-read table; the arms that rewrite regions plan as under `row`. RMoW and CoW read the row groups or files they change whole under any setting. Left out of the configuration's hash when absent. |
| `writerSkipsDeadRowGroups` | The MoR family's writers (their scans to locate rows, merge reads and compaction) read no row group the file's deletes cover whole, nor any file whose DV covers every row, as the `skipDeadRowGroups` reader does. Off (the default) is Iceberg's MERGE scan, which fetches such row groups and drops their rows. Only the region designs that leave DVs over whole row groups have them; configurations that report RMoR set it. |
| `cut` | The arms that keep row groups whole (`rmor-c-kept`, `rmor-c-footer-kept`, `rmor-c-clean`, `rmor-density`): a row group a batch rewrites is written as several row groups, cut where the batch changes it, an object (absent: one row group; left out of the configuration's hash when absent). Its rows are counted in strides of `strideRows` (20,000, Iceberg's page row limit, where every column of these rows starts a page) from its first row; a stride is flagged when the batch changes a row in it or the file's DV holds a dead row there (`at` `changed`, the only kind). Each run of flagged strides is a row group, and so is each run between them; an unflagged run shorter than `minStrides` (4) joins the flagged run beside it. Rows folded in from delta row groups go to the piece whose rows' ids span them, or the nearest. The old row group is dead whole, so the skipping reader and writers skip it (docs/HYPOTHESES.md, 2026-11-09, D3–D4; docs/drafts/region-cut-2026-11-09.md). |
| `defer` | `rmor-density`'s settings, an object (absent: the defaults; left out of the configuration's hash when absent): `share` (θ, 0.06), `deltas` (`region` or `write`), and `deltaFiles` (since 2026-11-20; absent or false: no, and left out of the hash): whether delta row groups go to files of their own, a write's (`<batch>-deltas`) apart from the row groups it copies and its inserts, and the merge compaction's apart from the row groups it merges back, so each file's id bounds in the manifest cover only its own rows and a read by id skips the copies' files outside its range. |
| `inserts` | Where the region designs put inserts (since 2026-11-12). Absent (or `tail`): into the tail, as the tail policy merges it (and left out of the configuration's hash). `event-time`: each into the live row group whose event-time range holds it (in a gap, the one before; before the first, the first), which the batch rewrites as it rewrites one an update changes, so no two row groups overlap in event time (`EventTimeRegions`; docs/HYPOTHESES.md, 2026-11-12). A row group grown past the rows of the initial load's largest is cut, never between two rows of one event time: the newest, which rows on time join, into full row groups and the rest; an older one once it holds two full ones' rows, into equal parts. RMoW-append keeps a cut row group's pieces in its place, and puts the newest piece in the tail file. `rmor-density` merges a row group once the rows that join it, those waiting included, reach θ of its rows, and until then they wait in delta row groups. The writer finds the row groups from its index of them and reads the footers of the files whose event-time bounds overlap them (`locate`, under `scan` or `keys`). Read by `rmow-*` (not split), `rmor-c-kept`, `rmor-c-footer-kept` and `rmor-density`; refused by the other region arms; ignored by the MoR family and `cow`. Moot without inserts; refused with a workload that also updates or deletes, and with compaction. Needs an initial load in event-time order (`initialJitterRows` 0). |
| `compactEvery` | Check the compaction triggers every this many batches (0 means never). MoR-family arms bin-pack files smaller than `compactSmallFileFraction` of the target and files whose delete ratio is at least `compactDeleteRatio`, as Iceberg's `RewriteDataFiles` does. RMoW triggers on space and on reads separately: it rewrites an appendable file, or moves the live row groups out of a split file's objects, whose dead bytes are at least `compactGarbageRatio` of their length; and it packs small sealed row groups once a file holds `compactSmallRowGroups` of them ([DESIGN.md](DESIGN.md#compaction)). |
| `merge` | `rmor-density`'s merge compaction (density plan step 3, docs/drafts/density-plan-2026-11-16.md; since 2026-11-18), an object (absent: none, and `rmor-density` compacts as the MoR family does; left out of the configuration's hash when absent; ignored by the other arms). It is the arm's compaction, run every `compactEvery` batches as phase `compact`, a transaction of its own: it drains each file with fewer than `drain` (0.5) of its rows live, none of its live row groups changed for `cold` (20) batches; merges back, in id order, runs of adjacent row groups that no batch changed for `cold` batches into row groups of about `rowGroupBytes` (32 MiB) on disk, where a run holds two and one under `small` (0.5) of that, one under half live, or one of a drained file (a run of one moves as it is), folding in the rows waiting for them in delta row groups; and merges each region's delta row groups (a drained file's move as they are): under a `cap` (3, since 2026-11-19, after bLSM), leveled, at most one a level 1..cap, a batch's merged into level 1 and a merge past its level's size, u·T^level (u the batch's rows, T the top level's rows over u to the power 1/cap, at least 2: bLSM's ratio), taking in the next level's and going on up, the top level unbounded; with `cap` 0, in tiers, `fanout` (4) of one tier (a batch's are 0) into one a tier up, the oldest first. Its row groups go to files of its own (`merge-<batch>-<n>`), rolled over at the target size (with `defer.deltaFiles`, its delta row groups in files apart from the others); DVs cover the old row groups whole, and a file with no live row left leaves the table. It plans from the manifest (sizes, record counts, DV cardinalities, checked against the DVs the arm wrote) and a history the arm keeps beside the table of the row groups it wrote (rows, id range, tier, region, last change), and reads the footer and DV of each file it reads from, once, checking each row group against the footer. |
| `compactMinInputFiles` | The MoR family's compaction as Iceberg's `RewriteDataFiles` plans a bin-pack, for a schedule that checks it after every write (`compactEvery` 1; since 2026-11-12). The files the two rules above pick form one group (Iceberg's reach 100 GB), rewritten into files of up to the target size only when it holds at least this many files (Iceberg's `min-input-files`, default 5) and more than one, or more than one and over the target size, or a file with `compactDeleteRatio` of its rows deleted; otherwise nothing is rewritten. Absent: groups of up to the target size, each rewritten unless it is one file with few deletes (and left out of the configuration's hash). |
| `rmorRewriteShare` | `rmor-c`'s θ: a batch rewrites a region it changes (a sealed file's row group, or an open tail file) when it updates or deletes at least this share of the region's live rows, counted per batch, and otherwise puts DVs on the rows. 0 (the default) rewrites every region a batch changes; above 1 none, which is `tailcow-c`. |
| `clean` | `rmor-c-clean`'s settings, an object (absent: the defaults; left out of the configuration's hash when absent): `rewriteShare` (0.25), `low` (0.5), `age` (2 batches), `budget` (0.5), `drain` (0.5), and `examine` (0.1: files with at least this share of their rows dead have their footers read to judge their row groups). |

## Reads (`reads` in the config)

| Knob | Meaning |
|---|---|
| `every` | Probe the table after every this many batches: a full read, the newest 1% (`read_recent`), a time-travel read (`read_old`, `travelBack` batches back), the windows and, under a reader with `sortedRead`, the sorted read. |
| `rangeEvery` | Also read the newest 1% (`read_recent`) and the windows (`read_window`) after every this many batches, between the probes (since 2026-11-12; absent: only at the probes, and then left out of the configuration's hash). The range reads change no write's request or byte (`SuiteKnobsTest`). |
| `windows` | Windows read at each probe under every reader (`read_window`; docs/HYPOTHESES.md, 2026-11-07): for each column in `by` (`id`, `ts`), `count` windows (default 5) of `fraction` (default 0.01) of the column's span at the probe, centred at (i + 1/2) / `count` of it, each a read of its own. The newest-rows read covers the end of the event time, these the rest. Off when absent, and then left out of the configuration's hash. |
| `readers` | The reader models every probe runs under, on the same table state. The first is primary; the default is `one-footer`, then `stock`. `one-footer` opens each file at one version and reads that version's one footer once, in one request (a 64 KiB prefetch), then only the row groups it needs. `stock` is stock Iceberg today: one split per row group (`FileScanTask.split`), each split re-reading the footer with two requests. Each model sets `footerCache`, `footerPrefetchBytes`, `splitTargetBytes` and `parallelism` (the threads a read's split tasks run on). With `deleteCache`, it loads each data file's deletes once per read rather than once per split, as an engine with a delete cache would. With `skipDeadRowGroups` (it needs `footerCache`), it plans no split for a row group the file's deletes cover whole, nor any for a file whose DV covers every row, checking the deletes, loaded once, against the footer it reads; Iceberg's reader fetches such row groups and drops their rows one by one. A file of at most 1 MiB, fetched whole, is skipped only whole. With `sortedRead`, the model also reads the table in event-time order at each probe (`read_sorted`), on one thread: a merge of the table's row groups, each a sorted run, opened when the merge reaches its smallest `ts`, so it holds every row group whose event times overlap. A file of at most 1 MiB is decoded and sorted in memory. It needs `footerCache`, and every run sorted: sorted writes and an initial load in event-time order (`initialJitterRows` 0); a row out of order stops the run. Under `skipDeadRowGroups` it leaves out each row group the file's deletes cover whole, as the full read does. The 128 MiB runs read under `cached-skip` alone (footer and delete caches, skipping dead row groups, sorted reads). |
| `recentFraction`, `travelBack` | The newest-rows read's share of event time (0.01), and how many batches back the time-travel read goes. |

## Latency and warm-up

- **`warmup`** runs a small op log on the in-memory store before the measured run, so the JIT has
  compiled the paths first. It changes no request or byte (`RunnerSmokeTest`).
- **`store.latency`** injects a simulated store's latency into every measured request: median
  latencies by kind (GET and HEAD, PUT and DELETE, append) with their lognormal sigmas, an extra
  GET latency per appended part, transfer times per MiB, and `maxAppendMib`, the store's cap on an
  append (100 for Azure; 0, the default, for none), past which each part costs an append. Each
  request sleeps its expected latency, so a phase's wall time is what it would take on that store
  (`StoreLatency`; `RequestLogTest` checks the injection). The results price requests in the lab
  instead (HARNESS.md, Requests, bytes and work).
- **`verifyRows`** (default true): a verification run also checks every decoded row's payload
  (HARNESS.md, Correctness).

## The workload (`workload` in the config)

Every arm replays one seeded op log (`OpLogGenerator`): an initial load, then batches of inserts,
updates and deletes on disjoint rows. Ids grow with insertion, and writes keep rows in roughly id
order, so a range of ids stands for a region of the table. The manifest records the log's SHA-256.

| Knob | Meaning |
|---|---|
| `seed`, `initialRows`, `batches`, `opsPerBatch` | The log's seed and size. |
| `insertFrac`, `updateFrac`, `deleteFrac` | Each batch's operations, by share. The rows updated and deleted per batch are fixed. |
| `keyDist` | How updates and deletes pick rows. `RECENCY_EXP` (the ingest window): a row's age in rows, newest first, is exponential with mean `recencyMeanBatches` batches of inserts. `UNIFORM`: every live row alike. `REGIONS`: by region, each with a heat drawn once from the seed, `U^-regionSkew` for a uniform U, so regions ranked by heat follow Zipf's law with exponent `regionSkew`. A region is picked with weight heat times live rows (as of the batch's start), then a row in it. Hot regions can be anywhere and stay hot; a skew of 0 is uniform over live rows. |
| `regionRows` | Consecutive ids per region (default 100,000). A region stands for a part of a row group: a 16 MiB one holds 513,700 rows of this schema, the 128 MiB runs' regions; a 128 MiB row group holds about eight. The initial load's row groups vary by a few percent, so a region need not line up with one. |
| `regionSkew` | `REGIONS`' Zipf exponent (default 1). On 1M rows and 60 batches of 10,000 operations in regions of 10,521 rows (135 by the end), the hottest tenth of the regions took 12% of the updates at 0, 54% at 1 and 89% at 2. |
| `updateShare` | The share p of a region's live rows one statement changes. 0 (the default) makes each update and delete a row of its own. Above 0, a statement takes the region of a row the key distribution picks, under any `keyDist`, and changes p of its live rows (at least one), picked uniformly among those the batch has not touched. The batch's deletes are drawn uniformly from the rows its statements changed, and the rest are updated, as a MERGE updates some of the rows it matches and deletes others, so a batch's deletes fall in the regions its updates do. The batch's last statement takes what is left of its updates and deletes, so p counts only up to those per batch over `regionRows`. The 128 MiB updates set 0.03. |
| `tsStepMicros` | Event time (`ts`) between consecutive inserts, in microseconds (default 1,000). |
| `lateMedianBatches`, `lateP99Batches` | Insert lateness in event time (`ts`), lognormal, in batches. |
| `initialJitterRows` | How far, in rows of event time, an initial-load row's `ts` may run ahead of its id's (default 100). At 0 the initial load is in event-time order, which a sorted read needs. The rows that change are the same at any value. |

Logs without regions, the ingest window or uniform keys at an update share of 0, are what they
were before regions existed (`OpLogGeneratorTest` pins three runs' hashes).
