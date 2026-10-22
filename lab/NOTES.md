# Notes on rmow-bench results

What anyone (or Claude) reading these results must know. Keep it short and current.

- Units: `steps.jsonl` counts requests and bytes per arm, step (batch) and phase. Pages report MB
  as 1e6 bytes and row-group sizes in MiB (`rg_mib`).
- Deterministic metrics: requests and bytes are a function of the op log, the configuration and
  the code, so repetitions of one configuration must match exactly (`steps_diff`, the
  Repetitions page). Wall and CPU time are not reproducible on local and in-memory stores and are
  not reported.
- The batch phase includes the locate and merge reads; compaction is amortized separately
  (`compact_mb_per_batch`).
- Reads are probes every `reads_every` batches, under each reader model; summaries use the last
  probe under the one-footer reader. Runs before reader models have only `stock` (or `cached`).
- `footer_bytes` and `row_groups_out_of_order` are -1 where unknown (non-RMoW arms).
- Superseded runs (directories renamed `superseded-*`, tagged with the suffix) and the in-memory
  smoke run are excluded from `canonical_runs`.
