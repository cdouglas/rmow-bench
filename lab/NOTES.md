# Notes on rmow-bench results

What anyone (or Claude) reading these results must know. Keep it short and current.

- Units: `steps.jsonl` counts requests and bytes per arm, step (batch) and phase. Pages report MB
  as 1e6 bytes and row-group sizes in MiB (`rg_mib`).
- Deterministic metrics: requests and bytes are a function of the op log, the configuration and
  the code, so repetitions of one configuration must match exactly (`steps_diff`, the
  Repetitions page). Wall and CPU time are not reproducible on local and in-memory stores and are
  not reported.
- CPU: runs from 2026-10-24 with counters carry each phase's user-mode `instructions` and
  `cycles` (and `pool_*` for a read's pool threads; split tasks carry theirs in `requests`).
  Instructions are the steady measure; thread CPU time (`cpu_ns`, `pool_cpu_ns`) moves with the
  clock. `phase_work.work_ms` is a phase's time for the latency model (lab/pages/cpu-measure.md).
- Where the time went: runs from 2026-10-26 carry each phase's wall and CPU time by kind of work,
  exclusive of one another (`io`, `encode`, `decode`, `footer`, `deletes` as `<kind>_ns` and
  `<kind>_cpu_ns`; `pool_*` for a read's pool), and the request log each request's `start_ns`,
  `dur_ns` and `cpu_ns`. `phase_breakdown` turns a phase into components that add up to its
  `wall_ms`. They are times, so locally they are as noisy as `cpu_ns`: for the cloud runs, and for
  orientation locally. Thread CPU time ran 11% fast against wall time on the laptop (WSL2), so
  `cpu_ns` exceeds `wall_ns` in most busy phases of every local run; runs record the ratio
  (manifest `host.thread_cpu_per_wall`), and `phase_breakdown` corrects for it.
- Builds: runs from 2026-10-26 are on the atomicio fork (README, Appends). They send the same
  requests and bytes as the release build, but MoR-v3's and TailCoW-C's DV files are 18 bytes
  larger (Puffin's `created-by`). The configuration lost `store.append` and `store.adlsReads`, so a
  rerun has a new `config_hash` under the same label.
- The batch phase includes the locate and merge reads; compaction is amortized separately
  (`compact_mb_per_batch`).
- Reads are probes every `reads_every` batches, under each reader model; summaries use the last
  probe under the one-footer reader. Runs before reader models have only `stock` (or `cached`).
- `footer_bytes` and `row_groups_out_of_order` are -1 where unknown (non-RMoW arms).
- Superseded runs (directories renamed `superseded-*`, tagged with the suffix) and the in-memory
  smoke run are excluded from `canonical_runs`.
