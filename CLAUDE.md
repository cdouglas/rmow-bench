# CLAUDE.md

rmow-bench measures **region-merge-on-write (RMoW)** for Parquet against Iceberg
**merge-on-read (MoR)**. RMoW appends a merged region and a new footer to the same object; MoR
writes new data files plus deletes. Every arm replays the same seeded op log, and the benchmark
records requests, bytes and CPU per arm, step and phase.

## Start here

- **[docs/EXPERIMENTS.md](docs/EXPERIMENTS.md)** is the map. It covers:
  - what has been run, and what each experiment varied and found;
  - the arms and tail policies in one table;
  - where each hypothesis stands.

  Read it first, and keep it current (see [Adding an experiment](#adding-an-experiment)).
- **[README.md](README.md)** has build and run, the phases, the I/O parity rules, the arms and
  every config knob (`layout.*`, `reads.*`).
- **[docs/ROADMAP.md](docs/ROADMAP.md)** is the outstanding work and what was decided about it.
  Keep it current as items land.
- **[docs/HYPOTHESES.md](docs/HYPOTHESES.md)** holds the question (the write and read trade-off),
  what is settled, and the hypotheses of the runs to come; the earlier H1–H6 and each campaign's
  predictions are the file as of commit 1145f89, which the older write-ups cite.
  **[docs/DESIGN.md](docs/DESIGN.md)** holds the format-level findings and the writer's rules.
  **[docs/PLAN.md](docs/PLAN.md)** is the original plan, whose default matrix was not what ran.
- **Write-ups**, one per experiment, are lab pages (`lab/pages/<name>.md`), so the data stay in
  context: their tables, charts and quoted numbers are computed from the runs. Those before
  2026-10-22 are `docs/measurements/<experiment>-<date>.md`.

## Build, test, run

Java 17 and Maven, and the atomicio fork's Iceberg 1.12.0 (`1.12.0-atomicio-SNAPSHOT`) in the
local Maven repository: every store appends through its `SupportsAtomicOperations` (README,
Appends). parquet-java 1.17.1 comes from Maven Central.

```bash
mvn -q package -DskipTests                     # target/rmow-bench-*.jar + target/lib
mvn test                                       # unit, compatibility and end-to-end smoke tests
bin/run.sh configs/smoke.json                  # one configuration; prints the run directory
bin/matrix.sh --build configs/local-compact-*.json   # several, from a build snapshot
```

- **`bin/matrix.sh`** snapshots the build into `results/builds/<UTC>-<sha>[-dirty]/` (jar,
  configs, and the diff if the tree is dirty), so you can keep editing and rebuilding while runs
  go. Each run's manifest names its snapshot.
- **Flags:** `--dump-versions` keeps every version of the appendable objects for
  `analysis/interop_check.py`, `--keep-store` keeps the objects, `--log-requests` writes every
  request to `requests.jsonl` for the latency replay, and `--log-held` every change of what the
  phases hold to `held.jsonl` (README, What is held). `store.latency` in a config injects a
  simulated store's latency (README).
- **Never edit `bin/matrix.sh` while it runs:** bash reads the script as it goes.
- **Campaigns too large for the laptop** (the production-sized `scale`) run on havoc, in a
  container: `docker/run.sh build`, then `start <campaign>` ([docker/README.md](docker/README.md)).
  They are the user's runs. A session prepares the campaign (configs, `docker/campaigns/*.json`,
  hypotheses) and reads the results once they are copied back into `results/`. Each job has
  whole idle cores, and its foreign load is sampled every second; only phases it did not touch
  decide CPU (`phase_load` in the lab, docker/README.md).
- **Simulation runs turn `verifyRows` off.** The payload check costs about 70% of a read's CPU,
  and CPU counts toward simulated latency. The id-and-version oracle stays on.
- **Duration:** a `local-*` configuration has taken 8–34 minutes, longest with all 9 arms
  (`elapsed_s` in each manifest). The README's S3 section says what
  `S3StoreTest` needs (versitygw in Docker; skipped without it), and its Azure section what
  `AzureStoreTest` needs (Azurite in Docker).
- **Tests that pin results:** `RunnerSmokeTest` asserts H2 for both layouts.
  `AppendableParquetTest`, `SplitFileTest` and `VersionDumpTest` cover reader compatibility.

## Results

- A run writes `results/<label>-<config hash>/<UTC start>/`:
  - `manifest.json`: the full config, git commit and dirty flag, versions, host, and the op log's
    SHA-256;
  - `oplog.bin`;
  - `steps.jsonl`: one record per arm, step and phase;
  - `versions/` with `--dump-versions`.
- `results/` is not in git. **Never edit results.** Outdated runs (replaced by a rerun, or
  retired) are deleted rather than kept, when the user decides. Deleting is permanent, so first
  record in `docs/EXPERIMENTS.md` (Retired runs) what goes and why, and carry over anything a
  write-up still needs from it.

## Analysis

- **labbook** (`lab`, with the labbook skill) is the way to answer questions about results.
  `lab/lab.toml` describes `results/` to it. Use the `labbook` skill:
  - `lab ingest` after new runs;
  - `lab runs` and `lab sql "…"` for answers, giving the query with the numbers;
  - pages in `lab/pages/*.md`, built with `lab build --strict`.
- **Tables available in `lab sql`:**
  - `steps` (every JSONL record);
  - `runs` and `canonical_runs`, with promoted config columns such as `rg_bytes`,
    `compact_every` and `key_dist`;
  - `run_params` (every config key);
  - `arm_summary` (`analysis/sweep.py`'s per-run, per-arm summary, ported to SQL and checked equal
    on 429 values);
  - `arm_meta` (labels, families, plot encoding);
  - `rg_meta` (row-group sizes from the version dumps' footers);
  - `micro` (JMH);
  - `steps_diff(a, b)` (`compare_runs.py`);
  - `requests` (the request logs), `store_profiles`, `run_readers`, `phase_latency` and
    `store_sweep(...)` for simulated store latency; `figures.replay(run, profile)` replays a
    logged run with parallel reads (lab/pages/store-latency.md).
- **`analysis/*.py`** are the scripts behind the existing write-ups. They run in the project venv,
  `.venv/bin/python analysis/<script>.py`. If you change one that a lab view ports
  (`sweep.summarize` → `arm_summary`), change the view too and check that both agree.

## Rules for measurements

- **Local runs report requests, bytes and CPU, never measured latency.** WSL2 and the page cache
  make local wall time meaningless. Latency is simulated from requests, bytes and CPU, with a
  named store profile (lab/pages/store-latency.md), and reported as simulated; a cloud run is
  needed only to check the simulation's parameters.
- **Requests and bytes are deterministic** in the op log, config and code. Two runs of one config
  must match exactly (`steps_diff`, or `analysis/compare_runs.py`). A mismatch is a bug, not
  noise.
- **CPU is measured in instructions** where the kernel allows counters (`ThreadCounters`;
  `kernel.perf_event_paranoid` at most 2). Thread CPU time moves with the clock: the same read
  repeated back to back took up to twice the time on this laptop. A phase's work is its
  user-mode instructions; its time for simulated latency is that work at its arm's pace
  (`phase_work.work_ms`, lab/pages/cpu-measure.md). Without counters, thread CPU time under 10%
  decides nothing for one phase.
- **The I/O parity rules in the README** guard against biases found in Iceberg's code. A change
  to any arm's read or write path must keep them, and the tests that check them must pass.
- **Hypotheses come before the runs that decide them.** Add each one to `docs/HYPOTHESES.md` in a
  new dated section, before running. Never edit an earlier one in place; record a revision as a
  new section. Report a contradicted hypothesis as contradicted. Keep them few, and about the
  question: writes against reads in latency, requests and bytes, and where each phase's time
  went. What a run only measures (a store's latencies, a control's counts) is
  listed as measured, not judged.

## Adding an experiment

1. State the question and hypothesis in `docs/HYPOTHESES.md` (new dated section).
2. Add `configs/<label>.json`, one per setting. The label names the setting, as in
   `local-compact-aggr-p5`.
3. Run with `bin/matrix.sh --build configs/<label>*.json`, in the background. It takes a while.
4. Run `lab ingest`, and add a campaign for the runs in `lab/annotations.toml`
   (`lab annotate 'local-<label>-*' --campaign <name>`).
5. Write the write-up as a lab page, `lab/pages/<name>.md` (the labbook skill), not a plain
   markdown report, and add it to `lab/myst.yml`'s toc. Follow `layout-vs-policy.md` and
   `time-travel.md`:
   - setup (configs, workload, arms, build snapshot), with the runs pinned by id;
   - tables and charts computed from the runs, each chart with its table;
   - findings, with every number they quote computed (`{eval}`);
   - the hypothesis verdict, and limits.

   Queries worth reusing go in `lab/views/`. Build with `lab build --strict` and look at the
   charts in `lab/_build/charts/`.
6. Update `docs/EXPERIMENTS.md`. Its run table and findings are written by hand; the lab's
   Experiments page regenerates the run table from the runs to check against. Update the README's
   Status too.

## Working here

- **Other sessions may work in this repository at the same time.** Check `git status` before
  editing, and commit only the files you changed (`git commit -- <paths>`).
- **The lab's pages may be open in JupyterLab** (`lab jupyter`), whose autosave can overwrite
  edits made underneath it. Before editing a page, ask whether it is open.
- **Commits:** the history is linear on `main`; never merge. Subjects are a sentence-case summary
  (`Split files: an object per version instead of appends`), with a prose body saying what
  changed and why. Commit only when the user asks.
