# CLAUDE.md

rmow-bench measures **region-merge-on-write (RMoW)** for Parquet against Iceberg
**merge-on-read (MoR)**. RMoW appends a merged region and a new footer to the same object; MoR
writes new data files plus deletes. Every arm replays the same seeded op log, and the benchmark
records requests, bytes and CPU per arm, step and phase.

## Start here

- **[docs/EXPERIMENTS.md](docs/EXPERIMENTS.md)** is the map. It covers:
  - the runs that stand, and what each experiment varied and found;
  - the designs and the two workloads;
  - where each standing hypothesis stands, and the runs retired (Retired runs).

  Read it first, and keep it current (see [Adding an experiment](#adding-an-experiment)).
- **[docs/LESSONS.md](docs/LESSONS.md)** holds what the retired experiments taught and decided:
  why their runs no longer stand, the measurement method (work in instructions, latency priced by
  request size, the stores' surprises), and the results that carry over. Their pages and the full
  record are in git at bfbdf33.
- **[README.md](README.md)** has the question, the five designs, the workloads and how to build
  and run. The reference moved to docs/ on 2026-11-18: **[docs/HARNESS.md](docs/HARNESS.md)**
  (flags and outputs, the fuzzer, the phases, correctness, what is held, the I/O parity rules, the
  code's layout, compatibility), **[docs/CONFIG.md](docs/CONFIG.md)** (every arm and every knob,
  `layout.*`, `reads.*`, `workload.*`) and **[docs/STORES.md](docs/STORES.md)** (the S3 and Azure
  stores, appends through the fork). Older documents cite those sections as "README, <section>".
  Keep the README focused on the five designs; detail goes to docs/.
- **[docs/ROADMAP.md](docs/ROADMAP.md)** is the outstanding work and what was decided about it.
  Keep it current as items land.
- **[docs/HYPOTHESES.md](docs/HYPOTHESES.md)** holds the question (the write and read trade-off),
  what is settled, and the hypotheses of the runs to come; the earlier H1–H6 and each campaign's
  predictions are the file as of commit 1145f89, which the older write-ups cite.
  **[docs/DESIGN.md](docs/DESIGN.md)** holds the format-level findings and the writer's rules.
  **[docs/PLAN.md](docs/PLAN.md)** is the original plan, whose default matrix was not what ran.
- **Write-ups**, one per experiment, are lab pages (`lab/pages/<name>.md`), so the data stay in
  context: their tables, charts and quoted numbers are computed from the runs. A retired
  experiment's page is deleted with its runs, once docs/LESSONS.md says what it taught.

## Build, test, run

Java 17 and Maven, and the atomicio fork's Iceberg 1.12.0 (`1.12.0-atomicio-SNAPSHOT`) in the
local Maven repository: every store appends through its `SupportsAtomicOperations` (docs/STORES.md,
Appends). parquet-java 1.17.1 comes from Maven Central.

```bash
mvn -q package -DskipTests                     # target/rmow-bench-*.jar + target/lib
mvn test                                       # unit, compatibility and end-to-end smoke tests
bin/run.sh configs/smoke.json                  # one configuration; prints the run directory
bin/matrix.sh --build configs/w128k-upd-*.json     # several, from a build snapshot
```

- **Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED`** on Java 17: every arm reads
  Parquet through Iceberg's vectorized reader (docs/HARNESS.md, What is measured). `bin/run.sh`,
  `bin/matrix.sh`, `docker/campaign.py` and surefire pass it; a JVM started any other way needs it.
- **`bin/matrix.sh`** snapshots the build into `results/builds/<UTC>-<sha>[-dirty]/` (jar,
  configs, and the diff if the tree is dirty), so you can keep editing and rebuilding while runs
  go. Each run's manifest names its snapshot.
- **Flags:** `--dump-versions` keeps every version of the appendable objects for
  `analysis/interop_check.py`, `--keep-store` keeps the objects, `--log-requests` writes every
  request to `requests.jsonl` for the latency replay, and `--log-held` every change of what the
  phases hold to `held.jsonl` (docs/HARNESS.md, What is held). `store.latency` in a config
  injects a simulated store's latency (docs/CONFIG.md).
- **Never edit `bin/matrix.sh` while it runs:** bash reads the script as it goes.
- **Campaigns too large for the laptop** (the production-sized `scale`) run on havoc, in a
  container: `docker/run.sh build`, then `start <campaign>` ([docker/README.md](docker/README.md)).
  They are the user's runs. A session prepares the campaign (configs, `docker/campaigns/*.json`,
  hypotheses) and reads the results once they are copied back into `results/`. Each job has
  whole idle cores, and its foreign load is sampled every second; only phases it did not touch
  decide CPU (`phase_load` in the lab, docker/README.md).
- **Measured runs check no rows; verification runs check every row.** A run measures by default:
  its reads only count their rows against the reference model. `--verify` makes a verification
  run of the same config, whose reads hash every row's (id, version) (and, with `verifyRows`, its
  payload) against the model; it issues the same requests and bytes, so `steps_diff` against it
  vouches for the measured run, and the lab excludes it. Never put a check of correctness, or any
  other per-row work of the harness's, inside a measured phase (docs/HARNESS.md, Correctness).
- **Duration:** a `local-*` configuration has taken 8–34 minutes, longest with all 9 arms
  (`elapsed_s` in each manifest); a 128 MiB one 55–99 minutes on the laptop. docs/STORES.md says
  what `S3StoreTest` needs (versitygw in Docker; skipped without it) and what `AzureStoreTest`
  needs (Azurite in Docker).
- **Fuzzing:** `bin/fuzz.sh` runs random small configurations of every arm and reader through
  the runner, with checks beyond a run's own (docs/HARNESS.md, Fuzzing); `bin/fuzz-mutants.py` checks it
  catches a set of planted bugs. Rerun both after changing an arm, a reader or the I/O layer, and
  keep the mutants' anchors current. On havoc: `docker/run.sh fuzz <name>`.
- **Tests that pin results:** `RunnerSmokeTest` asserts H2 for both layouts.
  `AppendableParquetTest`, `SplitFileTest` and `VersionDumpTest` cover reader compatibility.

## Results

- A run writes `results/<label>-<config hash>/<UTC start>/`:
  - `manifest.json`: the full config, git commit and dirty flag, versions, host, and the op log's
    SHA-256. A run that fails records its error as `failed` (the lab's status `failed`) and exits
    with status 1;
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
    logged run with parallel reads (docs/LESSONS.md, Pricing latency).
- **`analysis/*.py`** are the scripts behind the existing write-ups. They run in the project venv,
  `.venv/bin/python analysis/<script>.py`. If you change one that a lab view ports
  (`sweep.summarize` → `arm_summary`), change the view too and check that both agree.

## Rules for measurements

- **Local runs report requests, bytes and CPU, never measured latency.** WSL2 and the page cache
  make local wall time meaningless. Latency is simulated from requests, bytes and CPU, each
  request priced at its size by a store's latency test (`sized_latency`, lab/views/65_sized.sql;
  docs/LESSONS.md, Pricing latency), and reported as simulated; a cloud run is needed only to
  check the simulation's parameters.
- **Requests and bytes are deterministic** in the op log, config and code. Two runs of one config
  must match exactly (`steps_diff`, or `analysis/compare_runs.py`). A mismatch is a bug, not
  noise.
- **CPU is measured in instructions** where the kernel allows counters (`ThreadCounters`;
  `kernel.perf_event_paranoid` at most 2). Thread CPU time moves with the clock: the same read
  repeated back to back took up to twice the time on this laptop. A phase's work is its
  user-mode instructions; its time for simulated latency is that work at its arm's pace
  (`phase_work.work_ms`; docs/LESSONS.md, Measuring work). Without counters, thread CPU time
  under 10% decides nothing for one phase.
- **The I/O parity rules in docs/HARNESS.md** guard against biases found in Iceberg's code. A change
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
   `wl-upd-compact`.
3. Run with `bin/matrix.sh --build configs/<label>*.json`, in the background. It takes a while.
4. Run `lab ingest`, and add a campaign for the runs in `lab/annotations.toml`
   (`lab annotate '<label>-*' --campaign <name>`).
5. Write the write-up as a lab page, `lab/pages/<name>.md` (the labbook skill), not a plain
   markdown report, and add it to `lab/myst.yml`'s toc. Follow `workloads-128.md` (results) and
   `region-cut.md` (a decision):
   - setup (configs, workload, arms, build snapshot), with the runs pinned by id;
   - tables and charts computed from the runs, each chart with its table;
   - findings, with every number they quote computed (`{eval}`);
   - the hypothesis verdict, and limits.

   Queries worth reusing go in `lab/views/`. Build with `lab build --strict` and look at the
   charts in `lab/_build/charts/`.
6. Update `docs/EXPERIMENTS.md`. Its run table and findings are written by hand; check its run
   counts against the campaigns table on the lab's front page. Update the README's Status too, briefly.

## Working here

- **Other sessions may work in this repository at the same time.** Check `git status` before
  editing, and commit only the files you changed (`git commit -- <paths>`).
- **The lab's pages may be open in JupyterLab** (`lab jupyter`), whose autosave can overwrite
  edits made underneath it. Before editing a page, ask whether it is open.
- **Commits:** the history is linear on `main`; never merge. Subjects are a sentence-case summary
  (`Split files: an object per version instead of appends`), with a prose body saying what
  changed and why. Commit only when the user asks.
