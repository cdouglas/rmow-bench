# Row-group size sweep, recency-skewed updates (2026-10-21)

Configurations `configs/local-rg-sweep-recency-exp-rg{1,2,4,8}m.json`: the `local-m1` recency
workload (1M initial rows, 60 batches of 10k operations at 70% inserts, 25% updates and 5% deletes,
update and delete keys exponential in age with a mean of 2 batches), compaction off, with
`rowGroupBytes` and `tailSealBytes` both set to 1, 2, 4 and 8 MiB. Local store, build snapshot
`results/builds/20261021T044313Z-4b682c91d403-dirty`; `analysis/sweep.py` over the four runs.
Reads are the last probe's, under the one-footer reader.

The two knobs count different bytes. Parquet's row-group target counts buffered bytes (mostly
open, uncompressed pages), while the tail seal counts compressed bytes. `rg_mb_median` is the
median compressed size of an arm's live row groups at the end; `new_rg_mb_median` counts only
those written after the initial load (taken from the version dumps).

| arm | rg_mib | write_mb_per_batch | requests_per_batch | read_full_gets | read_recent_gets | stored_over_live | footer_kb | read_old_gets | row_groups | rg_mb_median | new_rg_mb_median |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mor-v3 | 1 | 0.393 | 47 | 313 | 5 | 1.09 |  | 273 | 156 |  |  |
| mor-v3 | 2 | 0.393 | 45.5 | 178 | 5 | 1.09 |  | 138 | 89 |  |  |
| mor-v3 | 4 | 0.393 | 45.3 | 142 | 5 | 1.09 |  | 102 | 71 |  |  |
| mor-v3 | 8 | 0.393 | 45.2 | 130 | 5 | 1.09 |  | 90 | 65 |  |  |
| rmow-a | 1 | 3.8 | 16.8 | 159 | 4 | 5.83 | 187 | 139 | 156 | 0.331 | 0.214 |
| rmow-a | 2 | 3.92 | 15.6 | 91 | 4 | 6.04 | 107 | 71 | 89 | 0.215 | 0.214 |
| rmow-a | 4 | 4.27 | 15.4 | 73 | 4 | 6.53 | 86.5 | 53 | 71 | 0.214 | 0.214 |
| rmow-a | 8 | 5.08 | 15.3 | 67 | 4 | 7.64 | 79.6 | 47 | 65 | 0.214 | 0.214 |
| rmow-b | 1 | 4.26 | 6.9 | 112 | 3 | 6.48 | 131 | 108 | 109 | 0.332 | 1.06 |
| rmow-b | 2 | 4.69 | 4.48 | 38 | 2 | 7.12 | 44.9 | 36 | 36 | 1.09 | 2.11 |
| rmow-b | 4 | 5.54 | 3.58 | 17 | 2 | 8.31 | 20.2 | 16 | 15 | 3.09 | 4.22 |
| rmow-b | 8 | 7.12 | 3.18 | 10 | 2 | 10.5 | 11.8 | 9 | 8 | 7.38 | 5.08 |
| rmow-c | 1 | 4.16 | 8.42 | 117 | 4 | 6.34 | 137 | 112 | 114 | 0.332 | 0.844 |
| rmow-c | 2 | 4.28 | 7.55 | 49 | 3 | 6.55 | 57.9 | 44 | 47 | 1.09 | 0.843 |
| rmow-c | 4 | 5.16 | 6 | 19 | 2 | 7.78 | 22.6 | 20 | 17 | 3.09 | 3.38 |
| rmow-c | 8 | 5.97 | 5.87 | 13 | 2 | 8.89 | 15.5 | 14 | 11 | 3.38 | 3.38 |
| tailcow-c | 1 | 0.393 | 47 | 313 | 5 | 1.09 |  | 273 | 156 |  |  |
| tailcow-c | 2 | 0.533 | 23.7 | 98 | 1 | 1.34 |  | 85 | 44 |  |  |
| tailcow-c | 4 | 0.533 | 23.6 | 62 | 1 | 1.34 |  | 49 | 26 |  |  |
| tailcow-c | 8 | 0.649 | 18.4 | 27 | 1 | 1.55 |  | 22 | 11 |  |  |

## Findings

- **Smaller row groups barely cut RMoW's write cost.** From 4 to 1 MiB, RMoW-B writes 5.5 →
  4.3 MB per batch (−23%), against mor-v3's 0.39 at every size. The full read meanwhile rises
  from 17 to 112 GETs (6.6×), and the footer from 20 to 131 KB. Updates fall on the last few
  batches' rows, so each batch rewrites that window whatever the row-group size; smaller row
  groups only trim the rounding at its edges. RMoW-A never merges its tail, so its recent row
  groups are each batch's inserts (0.21 MB) at every setting: it writes the least, and reads
  the most.
- **Larger row groups cost RMoW writes and space.** At 8 MiB, RMoW-B writes 7.1 MB per batch and
  stores 10.5× its live bytes. Full reads fall to 10 GETs, against mor-v3's 130.
- **The balance is near 2–4 MiB (Parquet's target, about 1–3 MB compressed) for this workload.**
  Below it, reads and footers grow much faster than writes shrink. Above it, writes and dead
  bytes grow. No setting changes the order against MoR-v3:
  - RMoW writes 10–18× its bytes per batch;
  - in 7–18% of its requests (B and C), or 34–36% (A);
  - and reads the table in 8–37% of its GETs (B and C), or about 51% (A).
- **TailCoW-C degenerates to MoR-v3 at 1 MiB.** Its units are whole batch files (inserts plus
  updated versions, about 0.3 MB), and four of them pass a 1 MiB seal, so every unit is sealed on
  arrival and never merges. RMoW's units hold inserts only.
- **Interop.** Every version of every RMoW object in the four runs (948 versions, 267 of them
  with footers out of offset order) reads back in pyarrow 26.0.0 and DuckDB 1.5.6
  (`analysis/interop_check.py`).
