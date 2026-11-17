# The density hypothesis where density can choose: a plan (draft, 2026-11-16)

The user's hypothesis (2026-11-09): a writer can use the deletion vectors (DVs) on a row group as a
measure of how much of it is dead or changed, and choose to copy at a finer grain than the file.
It can copy whole row groups or parts of them (runs of pages, which every column starts together
every 20,000 rows: a *stride*). It can also merge small cold pieces back, cutting where a reader
skips cheaply. This plan covers the work left before that hypothesis can be judged. Nothing in it
is built or registered until the user agrees. The cloud run at 128 MiB waits for it (ROADMAP).

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
  (workloads-128.md, section 2). With `keys` the forecast is 13.3 s a write on average against 20.3
  (HYPOTHESES.md, 2026-11-16, running). After `keys`, what is left of the scan is per file: two
  footer GETs, and a DV loaded before pruning.

## The design space

Two choices: what a write copies of a row group it changes, and when.

| What is copied | Built? | What hides the old rows | Reads need |
|---|---|---|---|
| the whole row group (RMoR, RMoW-external, RMoW-append) | yes | a DV over it, or a footer without it | nothing new |
| the row group as pieces cut at the changed runs (`layout.cut`) | yes | the same | nothing new |
| **only the changed runs**, the rest left in place (a *part copy*) | no | a DV over the copied runs (RMoR's form), or a footer that re-cuts the old row group by reference (RMoW-append's form) | a reader that skips pages, or it reads the dead runs |
| pieces of a fixed size, whatever changed (`copy:shrink:rg` in the model) | no | as the cut | nothing new |

And **when:** at once (θ 0), or once the row group's DV and waiting changes reach θ
(RMoR-density). Plus the repair: **merging cold pieces back** into one row group once they stop
changing, which bounds the fragmentation the cut and part copies cause.

## Steps, in order

1. **Locate by key at 128 MiB** (running; K1–K3). It takes the scan's growth out of the writes,
   so the steps below judge copies and reads.
2. **A sparse workload, with no new code.** The generator already has it: `updateShare` 0 makes
   each change a row of its own, in a region picked by heat (`REGIONS`, skew 1) or anywhere
   (`UNIFORM`). A write's 150,000 changes then fall thinly over many row groups, and densely only in
   the hot ones. Copying what changed would copy most of the table every write. Never copying
   leaves MoR's reads. θ can copy the hot row groups and leave the cold under DVs: this is where
   density chooses.
   - Model it first (`analysis/hybrid_model.py`: check it handles `updateShare` 0). Include the scan
     and the per-file footers and DVs: the model's omission of the scan is what contradicted D3.
   - Register, then run the designs as built (MoR, MoR compacted, RMoR, RMoR-density at two or
     three θ) at 128 MiB under `keys`, on the laptop under the lock, or on havoc with laptop twins.
3. **Merging cold pieces back.** Inside a row group's old key range, merge adjacent pieces that no
   write has changed for N writes, up to a row group's size, in the write or in a compaction. Knobs:
   N, the smallest piece kept, a budget of bytes a write. Measured by the row groups it leaves and
   what it costs writes, against the cut alone. Model, register, build (an estimated 200–300 lines
   with tests in `TailCowArm`, `LayoutSpec` and `Cut`), fuzz (`bin/fuzz.sh`, new mutants), run.
4. **A reader that skips pages.** It reads only the strides of a row group that its DV leaves live.
   Step 0 is done (docs/drafts/region-cut-2026-11-09.md): no change to Iceberg, its own row-group
   loop with parquet-java's `RowRanges`, one more GET a file for the page index. It goes to every
   design that has partly dead row groups, MoR included, as every reader change has
   (Appendix A). It is what part copies need. Build, fuzz with a reader model of its own, then
   rerun the configurations it changes.
5. **Part copies,** in RMoR's form first (a new file holding the changed runs, a DV over them in the
   old row group): the least data written of any form, and readable only through step 4. RMoW-append's
   re-cut by reference is the model's best write (2026-11-09), but needs a footer that names row
   groups inside another's pages: a format question before a build one.
6. **Then the cloud run** at 128 MiB, reading at every snapshot (`reads.rangeEvery` 1), on the
   designs and settings these steps settle.

Fixed-size cuts wait: the model runs out of memory on them at 240 writes until it frees the rows
of copies it has replaced. They answer a narrower question than steps 3–5. Hot-region isolation (ROADMAP)
is what the cut already does for changed runs; step 3 decides whether hot pieces stay apart.

## Questions for the user

- **The sparse workload's shape:** `updateShare` 0 under `REGIONS` skew 1 (hot and cold row
  groups), or `UNIFORM` too (no hot ones)? Keep 150,000 changes a write?

CD: We want some hot regions, or else RMoR-density doesn't have much to work with.
CD: For it to be effective, should we merge roaring bitmaps to identify dense
CD: regions across commits? This isn't quite the same as compaction maps since
CD: we're not tracking which rows were updated and which were deleted, but it could
CD: be a proxy for it? If we maintained a "conflict map" that recorded which of the
CD: inserts were updates, that would help for unsorted data? Or is that update tracking
CD: part of our build?

- **Who merges cold pieces:** the writer, within a budget each write, or a compaction (as Iceberg
  would schedule one)? Fair comparison asks the same of MoR's compaction.



- **The page-skipping reader:** an engine could build it, so it goes to every design. Should
  results pages show designs with and without it, or only with?

CD: Only with. We should call out the changes we made to the stock reader. We'll
CD: want to distinguish perf changes- information a reader could use, like skipping
CD: deleted data- from format changes- information that's a breaking change from
CD: Parquet. So far most of our changes are only breaking in the sense that a
CD: "footer" is at an offset in the blob, row groups don't need to be ordered, etc.

- **RMoW-append's re-cut by reference:** in scope, given that it changes what a stock reader must
  understand?

CD: Yes, though we want to distinguish which features require append. RMoR-density
CD: doesn't require append support, though its gains rely on skipping row groups (a
CD: change we made).

- **The results page:** once K1–K3 are judged, should the 128 MiB results use the `keys` runs, with
  the `scan` runs moved to Appendix A as what the change takes off?

CD: yes

## Size

- Steps 1–2: configurations and a model check, a day of laptop runs (about 1–2 h a 240-write
  run, one at a time).
- Steps 3–5: each a build, a fuzz round and its calibration, and a campaign: two to four sessions
  each.
- Memory: a model process at 205 million rows takes about 9 GB and the op-log generator about 10;
  run at most two models at once, and the generator alone (docs/LESSONS.md, Machines).
