# Copies cut where a write changes: a plan (draft, 2026-11-09)

The hypotheses are D1–D4 in [docs/HYPOTHESES.md](../HYPOTHESES.md) (2026-11-09). D1 and D2 test the
designs as built at 128 MiB row groups and run now. D3 and D4 need the cut, which this draft plans.
Nothing here is built until the user agrees the plan.

**Built (2026-11-10, the user agreed the plan).** Where it differs from the plan below:
- **The knob** is an object, `layout.cut`: `at` (`changed`, the only kind), `minStrides` (4) and
  `strideRows` (20,000). Absent means one row group, as before, and keeps every configuration's
  hash. `strideRows` is smaller only in the fuzzer, whose row groups hold 270 to 35,000 rows.
- **Which arms:** every arm that keeps row groups whole, `rmor-c-footer-kept` and
  `rmor-c-clean` too. It changes only how the batch's file cuts a rewritten row group.
- **Folded rows** go to the piece whose live rows' ids span them, or the nearest. The writer no
  longer knows the position they left, and ids follow position in this workload, as in the model.
- **The planted bugs:** a piece drops the row just before a cut; a folded row that no piece's ids
  span is dropped. A folded row placed in the neighbouring piece, as planned, is no bug: placement
  changes the layout, never the rows, so no check could catch it.
- **The code:** `arms/Cut.java` (the pieces), `TailCowArm.cutRows`, `LayoutSpec.CutSpec`,
  `CutTest`; the fuzzer draws the cut from a stream of its own, so every other draw of a seed is
  as before.
- **Configurations:** `configs/d128-upd-cut.json`, `configs/d128-upd-density-cut.json`, campaign
  `docker/campaigns/density128-cut.json`.

## Why

At Iceberg's 128 MiB row groups a row group of this table holds about eight update regions. The
designs as built either read all eight when a write changes one (MoR, compacted MoR, and the
deferred merges that `rmor-density` builds), or copy all eight (RMoR with row groups kept,
RMoW-append).

The layout model (`analysis/hybrid_model.py`, 240 writes on an 8× table) says that cutting copies
at what a write changes, in RMoR's form (new file, and a deletion vector over the old row group),
reaches the footer re-cut's steady state with only the reader we have (`cached-skip`). Over writes
201–240 it forecasts 7.0 s a write on S3 Express, against 16.4 s for the deferred merges as built
and 19.4 s for MoR. A 1% window takes 0.56 s at write 240, against 1.79 and 10.5 s. The re-cut
keeps row groups by reference in the old footer, which needs a format extension unless it keeps
only a prefix; the cut needs none. The price is a warm-up: the first 40 writes write 369 MB a write,
against 162 MB in the last 40.

## The cut

`layout.cut` (new), for the arms that keep row groups whole (`rmor-c-kept`, `rmor-density`):
- `whole` (the default): as built. A rewritten row group is written as one row group.
- `changed`: a rewritten row group is written as several row groups.
  - **What changed:** strides of `strideRows` (20,000) rows of the old row group, counted from its
    first row, flagged when the write changes a row in them or the old deletion vector holds a dead
    row there.
  - **The pieces:** each run of flagged strides becomes a row group, and so does each run of
    unflagged strides between them. A run of fewer than `cutMinStrides` (4) unflagged strides joins
    the flagged run beside it. A row group none of whose strides is flagged (a deferred merge
    triggered by deltas alone) stays whole.
  - **What goes in each piece:** each row is placed by its position in the old row group: its live
    rows, the new versions of the rows the write changes, and, with deferred merges, the rows
    folded in from delta row groups, placed by the position of the version they replace.

**Why strides:** a stride is where every column starts a new page (`write.parquet.page-row-limit`,
20,000). The cut does not need page alignment, since RMoR writes the pieces anew. But strides are
the model's unit, so the forecasts apply. A later reader that skips pages (below) would also find
the pieces' strides where the old row group had them.

**The old row group:** nothing changes. It is rewritten whole, so the write's deletion vector
already covers every row, and `cached-skip` and `writerSkipsDeadRowGroups` skip it.

## Where it goes

`TailCowArm.writeKept` writes each rewritten row group with one `KeptFile.add(rows, false, region)`.
With the cut it calls `add` once a piece, in the old row group's order.

- **The flags:** the write's changed positions in the row group come from `dvPositions` for the
  file, and the dead rows from the deletion vector the arm already holds (`dead`).
- **Regions:** deferred merges key the rows waiting in delta row groups by region, which is the
  base row group (`regionOf(file, first row)`), carried to the row group its merge writes. A merge
  folds in everything waiting for its region (`fold`), so after a cut nothing waits for the old
  region. Each piece gets a region of its own (the first may keep the old one), and later deltas
  wait for the piece whose id range holds them. To check while building: that nothing else
  assumes a region is one row group of the initial load's size (`rewrites`, `deltas`, the
  cleaner, which stays off).
- **The batch's file:** it holds more, smaller row groups. Its footer grows by about 1.2 KB a row
  group; the model puts the table at 756 row groups after 240 writes, against 49 at the start.

## Checks

- **Unit:** the cut's pieces from (rows, changed positions, dead positions): a row group shorter
  than a stride; a partial last stride; every stride flagged (one piece); none (whole); short
  unflagged runs joining their neighbours.
- **The build control** (D's control): with `cut` `whole`, every arm sends what it sent before the
  build, in requests and bytes. Checked by running `win-upd-base` on the laptop under the build
  before and the build after (`bin/matrix.sh --build`, `steps_diff`).
- **The model at 1×:** `win-upd-base` with `cut` `changed`, 16 MiB, 30 writes on the laptop
  (about 35 min). Its bytes written and read a write should be within 10% of the model's
  `copy:split:rg` on the same op log (`--rg-mib 16`), as the model's were at 16 MiB before.
- **The fuzzer:** `bin/fuzz.sh` draws `cut` for the kept arms. Two planted bugs that it must
  catch, added to `bin/fuzz-mutants.py`: a piece that drops its last row at a cut, and a folded
  row placed in the neighbouring piece. Run on havoc before the D runs (`docker/run.sh fuzz`).
- **Correctness:** every read is checked by the id-and-version oracle, as always.

## Runs

- **Now (D1, D2):** campaign `density128` on havoc: `d128-upd-mor`, `d128-upd-compact`,
  `d128-upd-density` (240 writes) and `d128-upd-copies` (30 writes). Each is one job, with 3 cores
  and a 32 GB heap.
- **After the build (D3, D4):** `d128-upd-cut` (`rmor-c-kept`, cut) and `d128-upd-density-cut`
  (`rmor-density`, cut), as campaign `density128-cut`.
- **Then:** a lab page: write against read, simulated on S3 Express and Azure Blob, with D1–D4's
  verdicts. Its table compares against the model's.

## Size

- **The cut:** about 150–250 lines in `TailCowArm` and `LayoutSpec`, with the unit tests about as
  long.
- **The control and the 1× check:** about an hour on the laptop.
- **The D3/D4 runs:** each about as long as `d128-upd-density`.

## Later, not in this plan

- **A page-skipping reader** (a third reader model beside stock and `cached-skip`). Step 0 is
  below. The model gives it most to MoR and compacted MoR (writes 3.5 s, windows 0.67 s on S3
  Express); the cut does not need it.
- **Cuts of a fixed size** (`copy:shrink:rg` in the model: 16 MiB pieces whatever changed). It had
  the best windows at 30 writes (1.16 s) but runs the model out of memory at 240 writes. The model
  must first free the row ids of copies it has killed.
- **Merging cold pieces back:** the inverse of the cut, once a region stops changing.
- **A sparse workload,** in which a statement changes a few rows of each page. Only there can the
  deletion vectors' density choose differently from "copy what changed".

## Step 0 of the page-skipping reader (done, 2026-11-09)

A throwaway check (the session's scratch `PageCheck.java`, in package `org.apache.iceberg.parquet`):
the bench's writer settings (zstd, 128 MiB row groups), 9 million rows, 280 MB, read through a
`RangeReadable` stream that logs each request.

- **Pages line up:** all 11 columns start a page every 20,000 rows in every row group, 215 pages a
  128 MiB row group (4.29 million rows).
- **Dictionary pages:** only version, l1, s1 and s2 have one (35 B to 2.3 KB); id and ts are plain.
- **Page indexes:** in a file Iceberg writes, all row groups' sit together before the footer, about
  86 KB a 128 MiB row group (181 KB for the file). The readers' 64 KiB footer prefetch does not
  cover them: one more GET a file. In an RMoW-append object each region's sit after its own row
  group (docs/DESIGN.md), so one GET a region read in part.
- **A partial read** (`ParquetFileReader.readFilteredRowGroup` with `RowRanges` of two runs, 6% of
  a row group): 23 requests, 7.9 MB. One read of the offset indexes, then one range a column a run.
  The dictionary pages merged with neighbouring ranges here; otherwise each costs a small range.
  The id column alone took 3 requests and 262 KB.
- **Counts and positions:** the page store reports the selected rows (253,518), every column's
  value count matches, and the row group's first row position is correct.
- **Decoding:** Iceberg's vectorized readers, unchanged, decoded every selected row correctly
  (id and a payload column checked), with batches cut at each run's end. (The bench's
  `ParquetBatches` sets Arrow's null check off; a reader without that fails on the first value.)

So a reader that skips pages needs no change to Iceberg:
- its own row-group loop, beside `VectorizedParquetReader`'s, in package `org.apache.iceberg.parquet`
  to reuse `ReadConf`;
- `RowRanges` from the deletion vector's live strides and the page index's admitted pages;
- batches cut at the end of each run;
- the page index read in one GET a region.
