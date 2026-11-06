# Strides and a cleaner: an unregistered draft (2026-11-04)

This draft was in `docs/HYPOTHESES.md` until 2026-11-06. It was never committed there, so it was
never registered. It moved here when the two workloads apart (2026-11-05) were registered: Variant
A (strides, virtual row groups) was not built, and the user set it aside. Variant B
(`rmor-c-clean`) is built, with configurations `configs/clean-*.json`, but has not run. To run
either, register its hypotheses as a new dated section of `docs/HYPOTHESES.md` first, with the
model's numbers checked again: they are from 2026-11-04.

## The unit of a rewrite follows the updates: strides and a cleaner (drafted 2026-11-04)

*A draft for review; it is registered when committed, before any run of its arms.*

**What it decides:** whether letting the writer choose the unit it rewrites, finer where updates
land and coarser where they don't, keeps the region designs' reads while cutting their writes.
That is database cracking driven by updates instead of queries. W1–W3 keep a row group whole;
two variants go further:
- **Variant A (strides):** RMoW-append rewrites only the strides of a row group that hold a
  changed row, and its footer splits the row group around them.
- **Variant B (a cleaner):** RMoR rewrites a row group only when a write changes enough of it, and
  otherwise leaves deletion vectors. A cleaner later carries the live rows of sparse, cold row
  groups forward, and empties files that are mostly dead.

**The unit, measured:** a stride is a run of rows at which every column starts a new page.
Iceberg's 20,000-row page limit cuts every column of our rows at the same rows. A 16 MiB row group
of the base point's rows, written with the shared writer settings, has 27 strides, and all 11
columns start their pages at rows 0, 20,000, 40,000 and so on (its page index). Each page is
compressed on its own, so a stride can be read or skipped without decompressing its neighbours.
Four columns (version, l1, s1, s2) are dictionary-encoded, and their pages need the row group's
dictionary page; they hold about 9% of the bytes. ORC's 10,000-row index groups sit inside its
compression chunks, so this unit is Parquet's.

**Contiguity:** rewriting every other stride would leave a row group in pieces, each a row group
of its own in RMoW's footer and a separate read. Both variants measure contiguity the same way:
- **Variant A:** between two strides a write rewrites in one file, it also rewrites the live
  strides in the gap when there are at most `gap` of them (`layout.strideGap`, 1).
- **Variant B's cleaner:** it judges spans, a file's live units separated by at most `gap` dead
  ones, by their occupancy: live rows over the rows of the span's whole extent.

**The model:** `analysis/layout_model.py` replays a setting's op log through each policy and
counts rows per unit, at 28.6 bytes a row (a 16 MiB row group of the initial load). It ignores
encodings, requests and deletion vectors' own bytes. Whole row groups and MoR read 5–16% low
against measured runs:

| Setting | RMoW-append's mean write (MB) | MoR's |
|---|---|---|
| The base point | 219 (measured 236) | 14 (measured 16) |
| The ingest window | 76 (measured 90) | 14 |

Every prediction below is the model's.

### Variant A: strides, on RMoW-append

**Setup:**
- **The arm:** `rmow-c-stride`, `RmowArm` with strides. For each row group a write changes, it
  reads the page index once, then reads back only the changed strides' pages (parquet-java's read
  of a row group restricted to row ranges). It rewrites those strides, plus gaps of at most one
  live stride, as one appended row group per contiguous run.
- **Virtual row groups:** the new footer lists the untouched strides of a split row group as
  virtual row groups over the old pages:
  - the seven plain-encoded columns are referenced where they sit;
  - each dictionary-encoded column's dictionary page and pages are copied into the append (the
    dictionary covers more values than the copied pages use, which is still valid);
  - statistics come from the page index, and the old bloom filters are kept (false positives
    only).
- **The rest:** inserts are unchanged.
- **The runs:** the four settings of W1–W3 (the base point, lateness p99 of 1 and 20 writes, the
  ingest window), each with MoR, RMoW-append, and `rmow-c-stride` at a gap of 0 and of 1.
  Requests, bytes and held memory are exact on any host; work needs instruction counts (the note
  of 2026-11-04 on where W1–W3 run).

**Predicted:**
- **T0 (a control): virtual row groups read alike in every reader.** Every version the arm writes
  reads the same rows in Iceberg's reader (the runner checks every probe against the oracle),
  pyarrow and DuckDB (`analysis/interop_check.py` on dumped versions).
- **T1 (bytes): `rmow-c-stride` writes at most 0.65 times RMoW-append's mean bytes** at the base
  point and both lateness settings (model: 126 against 219 MB, 0.57), and at most 0.95 times in
  the ingest window (67 against 76, 0.88). There, updates fall on the newest rows, whose row
  groups hold few others. It reads back at most 0.65 times as much at the base point (116
  against 210 MB).
- **T2 (what it costs): its footers list at most 1.6 times RMoW-append's row groups after 30
  writes** at the base point (model: 125 against 84 at a gap of 1, 134 at a gap of 0), and the
  same number within 5% in the ingest window (83 against 84).
- **T3 (reads): its full read reads RMoW-append's bytes within 2%** (the same live rows, no dead
  bytes in either), in at most 1.6 times its requests at the base point.

**Measured, not judged:** the gap's effect (the model puts 83 live fragments at a gap of 0 against
73 at a gap of 1, for 1 MB more a write); the dictionary columns' copied bytes; footer bytes;
write work; the sorted read.

### Variant B: a cleaner, on RMoR

**Setup:**
- **The arm:** `rmor-c-clean`, RMoR with its row groups kept whole (W1–W3's `rmor-c-kept`),
  working in whole row groups: Iceberg's reader cannot skip pages, and our skipping reader
  (`cached-skip`) skips whole row groups.
- **Rewrites:** a write rewrites a row group only when it changes at least a quarter of its live
  rows (`layout.cleanRewriteShare`, 0.25). Below that, the changed rows get deletion vectors and
  their new versions join the write's inserts.
- **The cleaner:** after each write it carries forward the live rows of row groups whose occupancy
  is below one half (`layout.cleanLow`) and that no write has changed for two writes
  (`layout.cleanAge`).
  - **Ranking:** by the cost-benefit rule of log-structured file systems, (1 − u) · age / (1 + u),
    where age comes from each row group's history of changes.
  - **Budget:** at most half the write's own rows (`layout.cleanBudget`): bLSM's gear, pacing
    cleaning to the write so that no write stalls behind it.
  - **Draining:** a file left under half live, and quiet for two writes, has all its remaining
    rows carried (`layout.cleanDrain`), and leaves the table.
  - **Placement:** carried rows go into row groups of their own, kept apart from the write's own
    rows, and deletion vectors cover the old row groups whole.
- **A churn workload** (`churn`), where MoR leaves files sparse: the ingest window with 30%
  inserts, 60% updates and 10% deletes, aimed at the newest rows (a recency mean of one write's
  inserts). In the model, MoR's file of each write ends about a fifth live (29 of 30 under half),
  and a full read opens every one.
- **The runs:** `churn` and the base point, each with MoR, MoR with Iceberg's bin-packing
  compaction every 10 writes (a twin configuration with `compactEvery` 10), RMoW-append and
  `rmor-c-clean`, read by the `cached` and `cached-skip` readers (`configs/clean-*.json`); probes
  after every write on `churn`, so reads between compactions count, and after every fifth at the
  base point.

**Predicted:**
- **B1 (reads, churn): over the probes, `rmor-c-clean`'s full read reads at most 1.05 times its
  live bytes, and opens at most 0.6 times the files MoR's opens.** Model: 1.01 and 10.1 files
  against MoR's 1.19 and 21.5. MoR with compaction reads 1.07 and opens 11.2.
- **B2 (writes, churn): `rmor-c-clean`'s mean write writes at most twice MoR's bytes, and its
  largest less than MoR's largest compaction.** Model: 22 against MoR's 13 MB a write, at most 43
  MB against a 98 MB compaction. Over the run, it writes no more than 1.2 times what MoR with
  compaction writes in its writes and compactions together (model: 660 against 598 MB).
- **B3 (a control on the threshold, base point): `rmor-c-clean` writes and reads as MoR, within
  2% of its bytes.** No write changes a quarter of a row group's rows (each statement changes 3%
  of a region), and no row group falls under half live in 30 writes, so it never rewrites or
  cleans.

**Measured, not judged:** files opened against MoR with compaction (the model puts them close,
10.1 against 11.2); its requests; the cleaner's share of each write; held memory.

**Limits (both variants):**
- **The model:** the predictions are a model of the policies, which reads 5–16% low on bytes and
  says nothing about time.
- **The runs:** one seed and 30 writes. MoR's sparse files grow with every write on `churn`, so a
  longer run would widen B1.
- **Stride-level RMoR waits:** it needs a reader that skips the pages a deletion vector covers
  whole. The model puts it at 1.01 times live bytes with page skipping, and 1.18 times without.
- **History:** Variant A ignores the history of changes, which Variant B uses for age. A writer
  that splits only where updates recur, and merges strides back where they stopped (the inverse
  of cracking), is the next step, and not tested here.
