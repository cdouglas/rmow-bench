# rmow-bench

A benchmark of **region merges** for Parquet tables in Iceberg: writes that rewrite the row groups
they change, against Iceberg's **merge-on-read (MoR)**, which leaves them and marks their old rows
dead. The question is whether readers gain more than writers pay. Every design replays the same
seeded operations, and the benchmark records the requests, bytes and CPU of each write and read;
latency on S3 Express and Azure Blob is simulated from them, each request priced at its size.

## The designs

A *row group* is the part of a Parquet file a reader reads or skips as a whole; the file's *footer*,
at its end, lists its row groups. A *deletion vector* (DV) is Iceberg's list of a data file's dead
rows, in a file of its own.

| Design | What a write does with a row group whose rows it changes | What hides the old copy |
|---|---|---|
| **MoR** | Leaves it; the changed rows' new versions go to a new file, with the write's new rows | a DV over the old rows |
| **MoR (compacted)** | As MoR; a compaction rewrites the small files and those with many dead rows, as Iceberg's bin-pack plans it after every write | the compaction's files replace them |
| **RMoR** | Copies it, with the changes, into the write's new file, cut into row groups where the write changes it | a DV over the whole old row group |
| **RMoW-external** | As RMoR | a new footer appended to the old file, which no longer lists it |
| **RMoW-append** | Appends the rewritten row group to its own file | the new footer appended after it |
| **RMoR-density** | As RMoR once its dead and changed rows reach a share θ of it (6%); until then as MoR, the new versions waiting in small *delta* row groups, one per base row group, for that copy | a DV |

RMoR, RMoW-external, RMoW-append and RMoR-density are the *region designs*: a write's unit is the
row group. In their names, R or W says when a row group's old and new rows are reconciled: on read
(RMoR: readers apply the DV, which covers the old row group whole, so a reader skips it) or on write
(RMoW: the old copy is gone from the footer readers use).

The two RMoW designs append to an existing Parquet object instead of writing a new file: a region
(one or more row groups) and then a new footer, which lists the old row groups still live, at their
original offsets, and the new ones. Every version of the file is the object's prefix up to that
version's footer, and stock readers read every version, since every Parquet offset is absolute
([docs/HARNESS.md, Compatibility](docs/HARNESS.md#compatibility)). Appends need a store that takes
them (S3 Express, Azure Append Blobs) and the atomicio fork of Iceberg
([docs/STORES.md](docs/STORES.md)). RMoR and RMoR-density write Iceberg's format as it is.

Each design is an arm of the harness with its settings ([docs/CONFIG.md, Arms](docs/CONFIG.md#arms)),
which also lists the code's other arms.

## The workloads

Two workloads, run apart, on a table of 205,480,000 rows of 11 columns (about 6.2 GB) in Iceberg's
default 128 MiB row groups and 512 MiB files, over 240 writes (RMoW-append, which copies whole row
groups, 30):
- **Updates only:** each write changes 150,000 rows (5/6 updates, 1/6 deletes), clustered: each
  statement changes 3% of a region of 513,700 rows, and regions are picked by a heat that follows
  Zipf's law. An update reads the rows it rewrites.
- **Inserts only:** each write inserts 350,000 rows by event time, some late (median 0.05 writes,
  p99 5). The region designs put each into the row group its event time falls in (the event-time
  merge); RMoR, RMoW-external and RMoW-append would rewrite 2.3 GB a write there, so they are
  projected, not run.

After every write the table is read: the newest 1% and five 1% windows (by key on the updates, by
event time on the inserts); every 10 writes also a full scan and a read sorted by event time.
[docs/EXPERIMENTS.md](docs/EXPERIMENTS.md#the-two-workloads) has the details and the earlier,
smaller workloads.

## Build and run

Java 17 and Maven, and the atomicio fork's Iceberg 1.12.0 (`1.12.0-atomicio-SNAPSHOT`) in the local
Maven repository (built from the fork's `atomicio` branch with
`./gradlew publishToMavenLocal -x test -x integrationTest`).

```bash
mvn -q package -DskipTests                       # target/rmow-bench-*.jar + target/lib
mvn test                                         # unit, compatibility and end-to-end smoke tests
bin/run.sh configs/smoke.json                    # one configuration; prints the run directory
bin/matrix.sh --build configs/w128k-upd-*.json   # several, from a snapshot of the build
bin/run.sh configs/smoke.json --verify           # a verification run: checks every row
```

A run writes `results/<label>-<config hash>/<UTC start>/`: its manifest, the op log it replayed,
and one record per arm, step and phase (`steps.jsonl`). The lab (`lab/`, labbook) reads them:
`lab ingest`, then `lab sql` and the result pages (`lab build`). Campaigns too large for a laptop
run in containers on havoc ([docker/README.md](docker/README.md)), and cloud runs on a VM beside
the store ([deploy/](deploy/README.md)). [docs/HARNESS.md](docs/HARNESS.md) has every flag, the
outputs and the fuzzer.

## What a run measures

- **Requests and bytes,** by kind, for every phase of every write and read. They depend only on
  the op log, the configuration and the code, so two runs of a configuration must match exactly.
- **Work,** as the user-mode instructions of each phase, where the kernel allows CPU counters; a
  phase's time is that work at its arm's pace.
- **What reads and writes hold** at once, the files and row groups a table holds, and its storage.

Latency is simulated, never taken from a local run's wall clock. Measured runs check no rows: a
**verification run** of the same configuration checks every row a read returns against a reference
model, issues the same requests and bytes, and vouches for the measured run. The I/O parity rules
keep the designs' reads and writes comparable to stock Iceberg's. All of it is in
[docs/HARNESS.md](docs/HARNESS.md).

## Documentation

| Document | What it is for |
|---|---|
| [docs/EXPERIMENTS.md](docs/EXPERIMENTS.md) | The map: the runs that stand, what each experiment varied and found, and the runs retired. Start here. |
| [docs/HARNESS.md](docs/HARNESS.md) | Running, measuring, checking: flags and outputs, the fuzzer, the phases, correctness, what is held, the I/O parity rules, the code's layout, compatibility. |
| [docs/CONFIG.md](docs/CONFIG.md) | Every arm and every knob of a configuration. |
| [docs/STORES.md](docs/STORES.md) | The S3 and Azure stores, and appends through the atomicio fork. |
| [docs/HYPOTHESES.md](docs/HYPOTHESES.md) | The question, and every hypothesis, registered before the runs that decide it. |
| [docs/LESSONS.md](docs/LESSONS.md) | What the retired experiments taught: the measurement method, the stores, the designs dropped. |
| [docs/ROADMAP.md](docs/ROADMAP.md) | The outstanding work, and the decisions that shaped it. |
| [docs/DESIGN.md](docs/DESIGN.md) | Format-level findings: the writer's rules, footer order, split files, compaction. |
| [docs/drafts/](docs/drafts/) | Plans in progress, among them the density plan. |
| `lab/` | The results database and pages: `lab serve` shows them as a site. |

## Status

- **Results at 128 MiB** ([lab/pages/workloads-128.md](lab/pages/workloads-128.md)): every design
  on both workloads, simulated on S3 Express and Azure Blob from local runs. On the updates RMoR,
  RMoW-external and RMoR-density write in about half MoR's time and read a 1% range 3 to 6 times as
  fast; RMoW-append, copying whole row groups, writes the slowest. docs/EXPERIMENTS.md has the
  numbers.
- **The simulation checked in the cloud** at 16 MiB row groups (2026-11-11): priced from each VM's
  own latency test, within 4.0% of measured on S3 Express and 12.3% on Azure Blob
  ([lab/pages/simulation-cloud.md](lab/pages/simulation-cloud.md)).
- **Next:** the density plan
  ([docs/drafts/density-plan-2026-11-16.md](docs/drafts/density-plan-2026-11-16.md)): a compaction
  that merges RMoR-density's pieces back (step 3), the sparse workload where θ chooses which row
  groups to copy (step 2), then the cloud run at 128 MiB ([docs/ROADMAP.md](docs/ROADMAP.md)).
