# Appendable against split RMoW files (2026-10-21)

Configurations `configs/local-split-*.json`: the `local-m1` workloads (1M initial rows, 60
batches of 10k operations at 70% inserts, 25% updates and 5% deletes), 4 MiB row-group target
and tail seal, MoR-v3, TailCoW-C, and RMoW-B and RMoW-C on both layouts:

- **appendable:** one object per file;
- **split:** each version's regions in a new object, its footer in a footer-only one
  ([DESIGN.md](../DESIGN.md#regions-in-separate-objects-split-files)).

Local store; requests and bytes are what S3 receives (`S3StoreTest`). Values are at batch 60:
per-batch means over the 60 batches, and reads at the last probe under the one-footer reader.
**Kept** is what remains once every older version expires (stored minus reclaimable bytes).
Tables from `analysis/layouts.py`, which decides H5 (docs/HYPOTHESES.md).

## Recency-skewed updates, no compaction

| arm | write MB/batch | requests/batch | full read GETs | full read MB | old read GETs | stored/live | kept/live | objects | live objects | footer KB |
|---|---|---|---|---|---|---|---|---|---|---|
| mor-v3 | 0.393 | 45.3 | 142 | 50.6 | 102 | 1.09 | 1.05 | 121 | 94 | |
| tailcow-c | 0.533 | 23.6 | 62 | 47.2 | 49 | 1.34 | 1.01 | 121 | 29 | |
| rmow-b | 5.54 | 3.58 | 17 | 43.9 | 16 | 8.31 | 8.31 | 2 | 2 | 20.2 |
| rmow-b-split | 5.55 | 4.87 | 17 | 43.8 | 16 | 8.31 | 1.41 | 156 | 8 | 24.1 |
| rmow-c | 5.16 | 6.00 | 19 | 43.9 | 20 | 7.78 | 7.78 | 2 | 2 | 22.6 |
| rmow-c-split | 5.17 | 7.28 | 19 | 43.8 | 20 | 7.79 | 1.52 | 156 | 11 | 27.0 |

## Recency-skewed updates, compaction every 10 batches

Compaction on each layout rewrites a file whose garbage (bytes it no longer uses, inside objects
the current version still uses) reaches half its retained bytes.

| arm | write MB/batch | compaction MB/batch | requests/batch | full read GETs | old read GETs | stored/live | kept/live | objects | live objects |
|---|---|---|---|---|---|---|---|---|---|
| mor-v3 | 0.350 | 0.763 | 21.8 | 29 | 49 | 2.22 | 1.00 | 127 | 3 |
| tailcow-c | 0.518 | 0.716 | 17.6 | 29 | 36 | 2.39 | 1.00 | 127 | 4 |
| rmow-b | 6.39 | 1.59 | 3.90 | 10 | 10 | 11.7 | 1.69 | 9 | 2 |
| rmow-b-split | 5.55 | 0 | 4.87 | 17 | 16 | 8.31 | 1.41 | 156 | 8 |
| rmow-c | 6.01 | 1.59 | 5.32 | 11 | 13 | 11.2 | 1.69 | 9 | 2 |
| rmow-c-split | 5.17 | 0 | 7.28 | 19 | 20 | 7.79 | 1.52 | 156 | 11 |

## Uniform updates, compaction every 10 batches (the validating case)

Every batch touches every row group, so region merge on write must write what copy-on-write
writes, in either layout. Over batches 1–9 (before the first compaction) each RMoW arm wrote
0.999–1.000× CoW's bytes; MoR-v3 wrote 0.011×.

| arm | write MB/batch | compaction MB/batch | requests/batch | full read GETs | old read GETs | stored/live | kept/live | objects | live objects | footer KB |
|---|---|---|---|---|---|---|---|---|---|---|
| mor-v3 | 0.428 | 1.01 | 35.3 | 30 | 49 | 2.43 | 1.00 | 127 | 3 | |
| cow | 37.5 | 0.784 | 48.5 | 17 | 25 | 53.2 | 1.00 | 447 | 2 | |
| tailcow-c | 0.656 | 0.983 | 32.0 | 30 | 38 | 2.68 | 1.00 | 127 | 3 | |
| rmow-b | 37.5 | 3.84 | 10.4 | 10 | 10 | 57.4 | 1.00 | 13 | 2 | 11.7 |
| rmow-b-split | 37.5 | 0 | 16.9 | 17 | 16 | 52.1 | 1.00 | 214 | 4 | 24.7 |
| rmow-c | 37.5 | 3.84 | 12.0 | 11 | 13 | 57.4 | 1.00 | 13 | 2 | 12.9 |
| rmow-c-split | 37.5 | 0 | 21.3 | 25 | 23 | 52.1 | 1.00 | 214 | 4 | 36.3 |

Every row group is replaced every batch, so once older versions expire every layout keeps just its
live bytes. The appendable arms got there by compacting six times (230 MB each, 3.8 MB per
batch); the split arms needed no rewrite.

## Findings

- **Same reads, same bytes written, one more request per changed file.** Split files write within
  0.2% of the appendable bytes (each region object's own footer). They take 1.3 more requests per
  batch: a region object and a footer object per changed file, instead of one append. Full and
  time-travel reads cost the same GETs and bytes.
- **Expiry reclaims what append cannot.** Without compaction, an appendable file keeps every
  replaced region: 7.8–8.3× the live bytes at batch 60, growing linearly. Once older versions
  expire, a split file keeps 1.4–1.5×, flat from the first batches. What remains is replaced row
  groups inside objects still in use, chiefly the large initial file.
- **Compaction becomes optional.** With compaction every 10 batches, the appendable arms
  compacted 6 times each (95 MB, 1.6 MB per batch) to reach 1.69× kept. The split arms never
  reached the garbage trigger: they kept 1.41–1.52× with no rewrite, and wrote 30–32% fewer bytes
  per batch, compaction included (5.55 against 7.98 MB for B, 5.17 against 7.60 for C). Appendable files read with fewer GETs after compaction (10–11 against 17–19), because
  compaction packs their row groups, not because of the layout.
- **Objects.** A split file adds one or two objects per version, so 156 objects were stored by
  batch 60, against 2 appendable objects (MoR-v3: 121). The current version reads from 8–11 of
  them, against 2.
- **The footer grows by the object names.** Each column chunk of a row group in another object
  carries its `file_path`, about 20% more footer here (24 against 20 KB).
- **Uniform updates validate it.** RMoW writes what CoW writes in both layouts. The split
  layout's expiry leaves only live bytes without the 3.8 MB per batch of compaction the
  appendable one needs, at 6–9 more requests per batch (every file changes every batch).
- **H5:** writes, reads and object counts as predicted. Kept bytes were within 1.5× for RMoW-B
  (1.41) and just over for RMoW-C (1.52).
- **Interop:** all 316 versions of the uncompacted run, the split files' footer-only versions
  included, read back (`analysis/interop_check.py`).
