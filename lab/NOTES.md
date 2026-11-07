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
  exclusive of one another (`io`, `encode`, `decode`, `footer`, `deletes`, and from 2026-10-29
  `rows`, the generator's rows for a writer, as `<kind>_ns` and `<kind>_cpu_ns`; `pool_*` for a
  read's pool), and the request log each request's `start_ns`,
  `dur_ns` and `cpu_ns`. `phase_breakdown` turns a phase into components that add up to its
  `wall_ms`. They are times, so locally they are as noisy as `cpu_ns`: for the cloud runs, and for
  orientation locally. Thread CPU time ran 11% fast against wall time on the laptop (WSL2), so
  `cpu_ns` exceeds `wall_ns` in most busy phases of every local run; runs record the ratio
  (manifest `host.thread_cpu_per_wall`), and `phase_breakdown` and `phase_work` correct for it
  (`run_clock`; since 2026-10-27, which cut every laptop run's `work_ms` by 10.0%).
- Held: runs from 2026-10-29 carry each phase's peak of what its readers and writers hold
  (`held_rows`, `held_bytes`, `held_<kind>`; README, What is held), as exact as requests and
  bytes on one thread. A parallel reader's peak (`cached-p8`) depends on how its tasks
  interleave: two laptop runs of `rmor-rg32m`, `rmor-s0` and `rmor-s2` differ in 3 to 9 of 19
  of its probes, and the `mor-row` runs differ from the `rmor-*` runs' MoR there and nowhere else. With `--log-held` a run also writes every change to `held.jsonl`, which is not yet a lab
  source.
- Builds: runs from 2026-10-26 are on the atomicio fork (README, Appends). They send the same
  requests and bytes as the release build, but MoR-v3's and TailCoW-C's DV files are 18 bytes
  larger (Puffin's `created-by`). The configuration lost `store.append` and `store.adlsReads`, so a
  rerun has a new `config_hash` under the same label.
- The batch phase includes the locate and merge reads; compaction is amortized separately
  (`compact_mb_per_batch`).
- Reads are probes every `reads_every` batches, under each reader model; summaries use the last
  probe under the one-footer reader. Runs before reader models have only `stock` (or `cached`).
- `footer_bytes` and `row_groups_out_of_order` are -1 where unknown (non-RMoW arms).
- The cloud calibration (lab/pages/cloud.md, views/60_cloud.sql): latency is measured wall time
  only on the cloud runs (campaign `cloud`); elsewhere it is not reported. `sent_requests` is what each phase sent the store by file: the SDK's counts
  where a run has them (DVs take the model's; the rest went to Parquet), else the model's.
  `file_requests` and `stored_by_part` add up exactly to the steps' requests, bytes and stored
  bytes. Spikes (`spike/<label>-<UTC>`, fact `spike_requests`) on an emulator check the code, not
  the store; `measured_profiles` fits a store profile to each: a latency at the smallest size
  and a slope. `sized_requests(run, spike)` and `sized_latency(run, spike)` (views/65_sized.sql)
  price each logged request at its own size from a spike's medians instead, so they need a run
  that logged its requests; `request_ops` is the request log as ops and sizes.
- Names: the results pages (base-point, cloud-sweep, reader-changes) name the designs for a reader,
  not as the code does (`figures.DESIGNS`): MoR is `mor-v3`, RMoR is `rmor-c` read by the
  `cached-skip` reader, RMoW-external is `rmor-c-footer`, RMoW-append is `rmow-c`. TailCoW
  (`tailcow-c`) and RMoR without skipping (`rmor-c` under `cached`) are in their appendix only.
  Other pages keep the arms' labels (`arm_meta`). They split a phase's time into
  `figures.TIME_PARTS` (`bp_parts(..., fine=True)`): "rows and planning" is `phase_breakdown`'s
  other CPU, which includes the harness making the rows a writer encodes (`rows_cpu_ns`) and RMoR's
  check of row groups against its deletion vectors: in every run before 2026-11-02, the cloud
  runs included, a lookup per row position (`Scans.coversAll`, since replaced by
  `DeletedRanges`, a range check on the vector's bitmap).
- Superseded runs (directories renamed `superseded-*`, tagged with the suffix) and the in-memory
  smoke run are excluded from `canonical_runs`.
