# The density hypothesis where density can choose: a plan (draft, 2026-11-16)

The user's hypothesis (2026-11-09): a writer can use the deletion vectors (DVs) on a row group as a
measure of how much of it is dead or changed, and choose to copy at a finer grain than the file.
It can copy whole row groups or parts of them (runs of pages, which every column starts together
every 20,000 rows: a *stride*). It can also merge small, sparse or cold pieces back together, even
across files, cutting where a reader skips cheaply (the user, 2026-11-05). MoR recovers its reads
with compaction on a static rule; RMoR-density tries to recover most of what compaction achieves
for I/O, at a lower cost, by copying where the density says it pays. This plan covers the work left
before that can be judged. Nothing in it is registered or built until the user agrees each step. The
cloud run at 128 MiB waits for it (ROADMAP).

The work is about patterns a Parquet reader or writer *could* use, if they prove useful (the user,
2026-11-16). Results pages show every design with the patterns; Appendix A calls out each change to
the stock reader, as a *pattern* (information a reader could use with Parquet as it is) or a *format
change* (what a stock reader must understand), and which designs need appends (ROADMAP).

## What the runs so far say

- **The workload never let density choose.** In the updates every statement changes 3% of a
  region's rows, spread over the region, so it touches every stride of it. The choice is then
  whether to copy a region now or later, never which part of it. The 2026-11-09 model found copying
  at once better than deferring at stride grain for that reason. RMoR-density, as built, only
  chooses *when* to copy (θ, the share of a row group dead or changed): its θ moved its write
  little at 16 MiB (4.8–5.4 s from θ 1 to 0.06, docs/LESSONS.md).
- **The cut works, and fragments the table.** Cutting a copied row group where the write changes
  it (`layout.cut`, D3/D4) lets RMoR, RMoW-external and RMoR-density copy little and read close to
  their live bytes (windows 1.06–1.60 times live after 240 writes, W3). But their tables grow from
  58 row groups to 766 (RMoW-external's live ones), 2,480 (RMoR-density) and 4,267 (RMoR, dead ones
  in live files included). Every piece costs:
  - footer entries;
  - a request in a full scan, which Azure Blob serves slowly at 1–16 MB (RMoR's full scan 87 s
    against MoR's 78 after 240 writes);
  - under `locate` `scan`, a key-column read in every write whose key range spans it.
- **The scan for a write's rows hides the copies' cost.** At 128 MiB RMoR-density's scan grows from
  2.2 s to 13.3 s a write over the 240 writes on S3 Express, most of what its write slows by
  (under `scan`). With `keys` its write takes 13.6 s on average against 20.3 (2026-11-17,
  suite-128.md, K3), and what is left of the scan is per file: two footer GETs, and a DV loaded
  before pruning.
- **Density is already cumulative.** A v3 data file has one DV, and each write that deletes more of
  its rows writes the union, so the DV on a row group sums every commit's deletes and updates. θ
  counts its dead rows in the row group plus the rows waiting for the region in delta row groups.
  What the DV does not say is which dead rows were updated and where their new versions wait: the
  build finds those through an index by region, which works because a region is a range of ids.

## The design space

Two choices: what a write copies of a row group it changes, and when.

| What is copied | Built? | What hides the old rows | Reads need | Kind |
|---|---|---|---|---|
| the whole row group (RMoR, RMoW-external, RMoW-append) | yes | a DV over it, or a footer without it | nothing new | pattern (RMoR); appends (RMoW) |
| the row group as pieces cut at the changed runs (`layout.cut`) | yes | the same | nothing new | the same |
| **only the changed runs**, the rest left in place (a *part copy*) | no | a DV over the copied runs (RMoR's form), or a footer that re-cuts the old row group by reference (RMoW-append's form) | a reader that skips pages, or it reads the dead runs | pattern (RMoR's form); format change and appends (the re-cut) |
| pieces of a fixed size, whatever changed (`copy:shrink:rg` in the model) | no | as the cut | nothing new | pattern |

And **when:** at once (θ 0), or once the row group's DV and waiting changes reach θ
(RMoR-density). Plus the repair: **merging small, sparse or cold pieces back**, which bounds the
fragmentation the cut and part copies cause.

**Statistics after a cut stay sound where they bound.** The cut writes each piece as a new row
group, so its statistics are exact. What goes stale is what still covers rows a reader must skip:
an RMoR file's metrics in the manifest (they include the row groups its DV hides; RMoW-external
recomputes its file's metrics from the footer it appends, which is why its reads open fewer
files), and in a part copy the old row group's footer statistics and page index, which still cover
the runs copied out. A min/max bound, a dictionary or a bloom filter that covers more than the live
rows stays correct: a reader treats it as "may hold", reads a row group or a page whose matching
rows have moved, and the DV drops them. Readers already face this with every DV. What must stay
exact is what a reader takes as a count or a position: a row group's row count and the offset
index's first row of each page (the DV's positions depend on them), a null count wherever a reader
compares it with the row count (a stale count can make a row group look all null), the exact-bound
flags, and any answer from metadata alone (count, min, max), which a reader must not take while
DVs apply. RMoR's form changes no count or position. A re-cut by reference changes both, so its
footer must recompute them (or drop the statistics): one more reason it is a format change.

## Steps, in order

1. **Locate by key at 128 MiB** (K1–K3; done 2026-11-17, lab/pages/suite-128.md). It took most of
   the scan's growth out of the writes: RMoR-density's writes now slow from 6.8 s over the first 10
   to 10.8 over the last 10 (8.0 to 21.7 under `scan`), what is left being each file's footer and
   deletion vector. The results page uses these runs; Appendix A shows what pruning by key takes
   off.
2. **A sparse workload with hot regions, with no new code.** `updateShare` 0 makes each change a
   row of its own, in a region picked by heat (`REGIONS`, skew 1; decided: hot regions, or density
   has little to work with). A write's 150,000 changes then fall thinly over most row groups and
   densely over the hot ones. By rough arithmetic, a hot row group reaches θ 0.06 within a few
   writes, and the coldest not in 240. Copying what changed would copy most of the table every
   write; never copying leaves MoR's reads; θ copies the hot row groups and leaves the cold under
   DVs. Changes this sparse touch every stride of a hot row group, so the cut copies it whole: here
   *when* decides, where in the clustered workload *what* does. The two workloads together cover
   the hypothesis.
   - Model it first (`analysis/hybrid_model.py`: check it handles `updateShare` 0). Include the scan
     and the per-file footers and DVs: the model's omission of the scan is what contradicted D3.
     Done 2026-11-18 ([Step 2: the model](#step-2-the-model-2026-11-18)).
   - Register, then run MoR, MoR (compacted), RMoR and RMoR-density at two or three θ at 128 MiB
     under `keys`, on the laptop under the lock, or on havoc with laptop twins.
3. **Merging pieces back, as a compaction** (decided). A compaction, not the writer, so its cost
   shows apart from the write, as MoR (compacted)'s does. Unlike MoR's static rule (Iceberg's
   bin-pack: small files, or 30% deleted), it is planned from RMoR-density's own signal: after every
   write, it rewrites adjacent pieces that are small, sparse (much of them dead) or cold (unchanged
   for N writes) into row groups of the writer's size, and drains files mostly dead, within a budget
   of bytes. It is also what deletes regions: storage is reclaimed only once nothing live is left in
   a file.
   - **A start:** the cleaner built on 2026-11-05 (`rmor-c-clean`, `layout.clean`;
     docs/drafts/strides-and-cleaner-2026-11-04.md, never registered or run) carries forward the
     live rows of row groups under a share live and unchanged for some writes, ranked by the
     log-structured file systems' cost-benefit rule, within a budget, and drains cold files. It runs
     in the write (phase `clean`) on whole row groups. The step moves its policy into a compaction
     phase, extends it to the cut's pieces and RMoR-density, and keeps contiguity in its measure
     (an update of every other stride is not worth carrying forward, the user, 2026-11-05).
   - Measured by the row groups and files it leaves, what it writes, and the reads, against the
     cut alone and against MoR (compacted). Model, register, build, fuzz (`bin/fuzz.sh`, new
     mutants), run.
4. **A reader that skips pages.** It reads only the strides of a row group that its DV leaves live.
   Step 0 is done (docs/drafts/region-cut-2026-11-09.md): no change to Iceberg, its own row-group
   loop with parquet-java's `RowRanges`, one more GET a file for the page index. A pattern: it goes
   to every design with partly dead row groups, MoR included, and the results pages show only runs
   with it (decided); Appendix A shows what it takes off. Part copies need it. Build, fuzz with a
   reader model of its own, then rerun the configurations it changes.
5. **Part copies,** RMoR's form first (a new file holding the changed runs, a DV over them in the
   old row group): a pattern, no appends, readable by any reader and fast through step 4. Then
   RMoW-append's re-cut by reference (in scope, decided): the model's best write (2026-11-09), but a
   format change (a footer naming row groups inside another's pages, its counts and positions
   recomputed) that needs appends. RMoR-density needs no appends at all, though its gains rely on
   skipping row groups, a pattern.
6. **Then the cloud run** at 128 MiB, reading at every snapshot (`reads.rangeEvery` 1), on the
   designs and settings these steps settle.

**Later:**
- **Unsorted data** (row lineage, after more data; decided). Without keys clustered by position,
  a merge cannot find the waiting new versions by a region's id range. Iceberg v3's row lineage
  carries an updated row's `_row_id` forward, and a row's id is its original file's first row id
  plus its position until a rewrite stores it; an update reads every column of the row anyway, so
  `_row_id` costs nothing to read there. Delta row groups written in `_row_id` order would carry
  min/max statistics on it that point back to the row groups their rows came from, with no map of
  our own.
- **Fixed-size cuts:** the model runs out of memory on them at 240 writes until it frees the rows of
  copies it has replaced, and they answer a narrower question than steps 3–5.
- Hot-region isolation (ROADMAP) is what the cut does for changed runs; step 3 decides whether hot
  pieces stay apart.

## Step 2: the model (2026-11-18)

**The op log:** the `w128k-upd-*` configurations' workload with `updateShare` 0, so each change is
a row of its own (`REGIONS`, skew 1; 150,000 changes a write over 240 writes on 205,480,000 rows;
SHA-256 16b5639b...). It was generated outside a run by `OpLogGenerator` from the 6daa887 build
snapshot, with a launcher that reproduces the clustered log (22ced251) byte for byte.

**The model** (`analysis/hybrid_model.py updates --rg-mib 128 --target-mib 512 --read-every 40`)
now counts the locate's requests under `keys`: two for each candidate file's footer, one for its
deletion vector, and one for the id column of each candidate row group. On the clustered log, with
RMoR-density as run (`copy:split:rg:0.06`), it counts 28 a write over the first 10 writes and 541
over the last 10. The `w128-keys-2` run measured 28.2 and 510: footers 337 against 317, deletion
vectors 167 against 157, id columns 37 against 35. Its rows are 28.6 bytes (Parquet's about 31),
so its numbers are ratios between designs; nothing here is priced yet.

**How the changes fall** over the model's 49 row groups of 4,256,000 rows:
- Every write changes rows in every row group. The hottest takes 10% of the changes (about 15,000
  a write), the median about 2,200 and the coldest about 520.
- By the 240th write, 5% to 29% of a row group's rows have changed (median 11%).
- θ 0.06 is first reached at write 3, by half the row groups at write 133, and by 48 of 49 at
  write 228. θ 0.15 is reached by 15 row groups, θ 0.25 by 3.

**What that means in this workload:**
- Every design's write reads the whole table, about 6.1 to 7.1 GB of the model's rows a write.
  `updateRead` `row` reads every row group holding a row the write updates, and every row group
  holds one. The designs differ in what they write, in the locate's requests, and in their reads.
- Copying at once (RMoR, θ 0) copies the whole table every write, 1.6 TB over 240 writes. It is
  left to the model, without a run.
- The cut copies whole row groups: changes this sparse touch nearly every stride.

| After 240 writes (model) | MoR | MoR compacted every 5 writes | RMoR-density θ 0.02 | θ 0.06 (as run) | θ 0.15 |
|---|---|---|---|---|---|
| MB written a write (mean) | 3.6 | 3.6, and 40 compacted | 188 | 53 | 12 |
| Locate requests a write, last 10 writes | 1,025 | 104 | 1,178 | 3,579 | 9,704 |
| A 1% window's bytes, against its live bytes | 18.3 | 6.9 | 3.3 | 3.4 | 3.6 |
| A 1% window's requests | 723 | 9 | 56 | 198 | 766 |
| A full scan's requests | 794 | 82 | 1,102 | 3,329 | 9,596 |
| Row groups | 289 | 55 | 971 | 2,848 | 9,091 |

- **RMoR-density's range reads come closest to their live bytes at every θ:** 3.3 to 3.6 times,
  against MoR's 18 and MoR compacted's 6.9. A 1% window reads the 128 MiB row group holding it
  (about 2% of the table) and that row group's deltas.
- **But its deltas pile up.** As built (`defer.deltas` `region`), each write puts a row group of
  new versions for every base row group it changes, and here it changes all of them: 49 a write, of
  about 2,500 rows each. They are folded in only when their row group is copied. At θ 0.06 the
  table holds 2,848 row groups after 240 writes. The locate reads each one's id column every write,
  3,579 requests against MoR's 1,025 and MoR compacted's 104, and a full scan sends 3,329.
- **θ trades one cost for the other.** At θ 0.15 the deltas pile up further (9,091 row groups). At
  θ 0.02 the copies come often enough to fold them (971), for 188 MB a write.
- **MoR compacted is the design to beat on requests.** Iceberg's bin-pack every 5 writes keeps 55
  row groups, for 40 MB a write of compaction and windows of 6.9 times their live bytes.
- **For step 2:** run as it is, RMoR-density sends 6 to 120 times MoR compacted's requests, at
  any θ. Merging pieces back (step 3) is what would fold the deltas
  without copying their base row groups. **Open (the user):** run step 2 as it is, measuring the
  pile-up, or bring step 3 forward and run RMoR-density with it.

## Decided (the user, 2026-11-16/17)

- The sparse workload has hot regions (`REGIONS`).
- Pieces are merged back by a compaction, planned from RMoR-density's density signal, not a static
  rule.
- The page-skipping reader is a pattern: results pages show only runs with it; Appendix A splits
  patterns from format changes and says which designs need appends.
- RMoW-append's re-cut by reference is in scope, as a format change that needs appends.
- The 128 MiB results switch to the `keys` runs once judged.
- Row lineage for unsorted data waits for more data.

## Size

- Steps 1–2: configurations and a model check, a day of laptop runs (about 1–2 h a 240-write
  run, one at a time).
- Steps 3–5: each a build, a fuzz round and its calibration, and a campaign: two to four sessions
  each.
- Memory: a model process at 205 million rows takes about 9 GB and the op-log generator about 10;
  run at most two models at once, and the generator alone (docs/LESSONS.md, Machines). On the
  sparse log (2026-11-18), θ 0.02 and the clustered log needed more than 7 GB (under 13 GB and
  11 GB), and the generator took 5.6 GB at `-Xmx6g`.
