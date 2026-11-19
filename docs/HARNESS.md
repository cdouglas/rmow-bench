# The harness: running, measuring, checking

How to run the benchmark, what a run records, how its correctness is checked, and the rules that
keep the designs' I/O comparable. Moved from the README on 2026-11-18; a citation "README, X" of
one of these sections means the section of that name here. The designs and the workloads are in
the [README](../README.md); every configuration knob is in [CONFIG.md](CONFIG.md); the stores and
the fork's appends in [STORES.md](STORES.md).

## Build and run

Requires JDK 17 and Maven, and the atomicio fork of Iceberg 1.12.0 (`1.12.0-atomicio-SNAPSHOT`)
in the local Maven repository: the region designs append through its `SupportsAtomicOperations` on
every store ([STORES.md, Appends](STORES.md#appends-the-forks-atomic-operations)). Build it from the
fork's `atomicio` branch with `./gradlew publishToMavenLocal -x test -x integrationTest`;
`-Diceberg.version` picks another of its builds. parquet-java (1.17.1) and everything else come
from Maven Central.

```bash
mvn -q package -DskipTests        # the jar plus target/lib
bin/run.sh configs/smoke.json     # prints the run directory
.venv/bin/python analysis/summary.py results/<label>-<hash>/<start time>
mvn test                          # unit tests, the compatibility spike, end-to-end smoke tests
```

For experiments, `bin/matrix.sh` runs configurations one after another from a snapshot of the
build (`results/builds/`), so `mvn package` can run while they do:

```bash
bin/matrix.sh --build configs/w128k-upd-*.json
```

The snapshot holds the jar, hard links to its released dependencies, the configurations, and for a
dirty tree the diff and the untracked files; each run's manifest names it. The script refuses a
jar older than `src/main` or `pom.xml` (`--build` rebuilds it first). Runs keep their objects under
`store/` and delete them when they end (`--keep-store` keeps them). Both scripts give the JVM
`-Xmx8g` unless `JAVA_OPTS` sets its options. Arrow needs
`--add-opens=java.base/java.nio=ALL-UNNAMED` on Java 17, which `bin/run.sh`, `bin/matrix.sh`, the
container's runner and the tests pass; a JVM started any other way needs it.

Campaigns too large for a laptop run in a container on another machine (havoc), each configuration
on whole idle cores of one NUMA node: `docker/run.sh build`, on the fork's artifacts carried as a
tar (`docker/fork-m2.sh`), then `docker/run.sh start <campaign>`
([docker/README.md](../docker/README.md)).

Cloud runs go on a VM beside the store, deployed by [deploy/](../deploy/README.md). Its `bench`
step runs the configurations it is given at one commit; one with a local store runs on the cloud's
store, its label suffixed `-s3x` or `-azure`, so it is the twin of a run made on the laptop or
havoc.

**Flags** (on `bin/run.sh` and `bin/matrix.sh`):
- `--verify` makes a verification run ([Correctness](#correctness)).
- `--log-requests` writes every request each phase issued to `<run>/requests.jsonl`, with its
  split task and each task's CPU, for the latency replay (the lab's `figures.replay`, which times a
  logged run on any store, with parallel reads' critical paths; docs/LESSONS.md, Pricing latency).
  `RequestLogTest` checks the log.
- `--log-held` writes every change of what each phase's readers and writers hold to
  `<run>/held.jsonl` ([What is held](#what-is-held)).
- `--dump-versions` keeps every version of every appendable object in `<run>/versions/`, checked
  against the reference model at every step, for `analysis/interop_check.py <run>/versions` to
  read with pyarrow and DuckDB ([Compatibility](#compatibility)).
- `--keep-store` keeps the run's objects.

Python tools live in a project venv:

```bash
python3 -m venv .venv
.venv/bin/pip install pyarrow duckdb pandas matplotlib
```

A run writes `results/<label>-<config hash>/<UTC start>/`:

- `manifest.json`: the full configuration with defaults, the git commit, the Iceberg, Parquet and
  JVM versions, the host, and the op log's SHA-256. A run that fails records its error as `failed`.
- `oplog.bin`: the exact operations that were replayed.
- `steps.jsonl`: one record per arm, step and phase.
- `requests.jsonl` with `--log-requests`, `held.jsonl` with `--log-held`, and `versions/` with
  `--dump-versions`.

Runs that share a configuration share the `<label>-<hash>` directory. Requests and bytes are a
function of the op log, the configuration and the code, so `analysis/compare_runs.py` (the lab's
`steps_diff`) checks that two runs match exactly. Every run names its files alike (`bench://run/…`,
mapped to the run's own prefix in the store), since paths end up inside Puffin footers.

## Fuzzing

`bin/fuzz.sh` runs random small configurations through the runner (`fuzz.Fuzz`). A seed draws the
whole configuration: the workload's mix, key distribution, regions and lateness, every layout knob,
a subset of the arms, and readers of random flags, on the in-memory or the local store. Every read
of every arm under every reader is checked against the reference model, payloads included: the
fuzzer verifies every run. Beyond that, each arm's objects and bytes on a local store must be what
its `state` records say, and a share of seeds run twice, once on each store, and must match record
for record in requests, bytes, rows, memory held (readers on one thread) and state. Another share
dump every version of the appendable objects and check each against the model, and another log
every request and every change of what the phases hold, as the cloud runs do.

```bash
bin/fuzz.sh --parallel 8 --out results/fuzz/<name> --minutes 60   # 8 workers, seeds 1, 2, ...
bin/fuzz.sh --seed 1234                                            # one seed again
bin/fuzz.sh --print 1234                                           # the configuration it draws
bin/fuzz.sh --parallel 8 --out results/fuzz/<name> --profile heavy  # nearly dead row groups
bin/fuzz-mutants.py --seeds 60 --workers 8                         # calibration: planted bugs
bin/fuzz-mutants.py --seeds 60 --workers 8 --profile heavy         # and on the heavy draws
bin/fuzz.sh --parallel 8 --out results/fuzz/<name> --profile event-time  # the event-time merge
bin/fuzz-mutants.py --seeds 60 --workers 8 --profile event-time    # and its planted bugs
docker/run.sh fuzz <name> --minutes 180                            # on havoc (docker/README.md)
```

The `heavy` profile makes every seed change rows in heavy statements (most of a region's rows),
with regions of about a row group and a reader that skips dead row groups, so row groups go dead
but for a few rows, where skipping them is easiest to get wrong; the default profile draws such
seeds now and then. The `event-time` profile makes every seed insert only, onto an initial load in
event-time order, with the region arms placing the inserts by the event-time merge
(`layout.inserts`); the default profile draws it on about a third of its inserts-only seeds.

A failing seed writes `fail-<seed>/`: its configuration (runnable with `bin/run.sh`), the error, and
the run's records. `bin/fuzz-mutants.py` plants each of a set of known bugs in a copy of the
source and fuzzes it with a fixed budget of seeds: each must be caught by the check it names, under
the profiles that reach it, and the unmutated code must stay clean. The bugs too rare for that
budget are measured, not required; the script's header says what budget catches them, and
docs/ROADMAP.md (Harness, the fuzzer) what the rounds on havoc found. The fuzzer covers the arms
and readers on the in-memory and local stores, not the cloud stores' clients. Rerun it, and the
calibration, after changing an arm, a reader or the I/O layer.

## What is measured

Every arm replays the same seeded op log in lockstep, in an order shuffled at every step. They all
start from the **same initial-load bytes**: a stock Parquet file is already the first version of an
appendable one.

**Parquet is read and written as Spark reads and writes an Iceberg table**, the same way in every
arm. Every read (the reads, writers' scans to locate rows, merge reads) goes through Iceberg's
vectorized reader (iceberg-arrow, `ParquetBatches`): batches of 5,000 rows of Arrow vectors, the
file's position deletes applied per batch by position, and the residual compiled to comparisons
on the batch's columns (`RowBatch`), with Arrow's per-value bounds and null checks off, as
Iceberg's Spark reader sets them. Writes go through Iceberg's generic writer over parquet-java, as
Spark's and Flink's do, with strings as UTF-8 bytes (Avro's `Utf8`), as Spark's rows hold them.

### Phases

The phases recorded in `steps.jsonl` are:

| Phase | What it covers |
|---|---|
| `batch` | One whole batch (a write). It contains `locate`, `update-read`, `merge-read`, `write`, `delete-write` and `clean`, so don't add them to it. |
| `locate` | With the `locate` knob at `scan` or `keys`: finding the rows the batch changes, as a MERGE would. |
| `update-read` | With `updateRead` at `row` (or `scan`, for the arms that rewrite regions): reading whole each row group that holds a row the batch updates and that it does not rewrite, since a new version is its old row with some columns set. |
| `merge-read` | Reading back what the batch rewrites or merges: the row groups the region designs rewrite, and the other arms' files and tail regions (CoW's files, TailCoW's tail files). |
| `write` | Writing data files, or appending regions and footers. |
| `delete-write` | Deletes: each file's DV merged with its previous one into the batch's Puffin file (MoR, RMoR, RMoR-density; position-delete files for the v2 arms); for RMoW-external, the footers appended to hide the old row groups. |
| `compact` | A compaction, after the batch, every `compactEvery` batches; not part of `batch`. |
| `read_full` | Reading the whole current table. |
| `read_recent` | Reading rows in the newest 1% of event time. Files are pruned by column metrics, row groups by stats. |
| `read_old` | Reading the table as it was `travelBack` batches ago. |
| `read_window` | With `reads.windows`: reading the rows in one window of a column's span (`window_by`, `id` or `ts`, centred at `window_at` of it). Pruned as `read_recent` is. |
| `read_sorted` | Reading the whole current table in event-time order, under a reader with `sortedRead`: a merge of the table's row groups, each a sorted run, opened as the merge reaches its smallest `ts`. |
| `state` | The footprint: objects, stored, live and reclaimable bytes, row groups, files, footer bytes, and RMoW-append's row groups listed out of id order. |

### Requests, bytes and work

Each phase records wall time, thread CPU time, and the requests a tracing FileIO saw: GETs (ranged
or streamed), HEADs, PUTs, APPENDs, aborts, bytes read and bytes written. Requests are counted the
way Iceberg's object-store streams issue them. An append counts as one APPEND and a new appendable
file as one PUT on every store. Where a store caps an append (Azure's Append Block, 100 MiB), the
fork splits a larger one into several, and `append_splits` counts the extra ones, for a store
profile with a cap to price.

Besides thread CPU time (`cpu_ns`), a phase records the thread's user-mode time (`user_ns`), the
process's CPU time (`process_cpu_ns`), and, where the kernel allows the CPU's counters
(`ThreadCounters`, through `perf_event_open`, with `kernel.perf_event_paranoid` at most 2), the
thread's user-mode `instructions` and `cycles`. Thread CPU time moves with the clock speed and
instructions do not, so instructions are a phase's work, and its time is that work at its arm's
pace ([LESSONS.md, Measuring work](LESSONS.md#measuring-work)). Local runs report requests, bytes
and work, never latency: WSL2 and the page cache make local wall time meaningless. Latency is
simulated, each request priced at its size from a store's latency test
([LESSONS.md, Pricing latency](LESSONS.md#pricing-latency)).

### Correctness

**Correctness is checked apart from what is measured** (since 2026-11-17). A run measures: its
reads only count the rows they return, a batch at a time, and each count must equal the reference
model's. A **verification run** (`--verify` on `bin/run.sh` and `bin/matrix.sh`, or a campaign's
`flags`) checks every row instead: each read hashes the (id, version) of every row it returns
(order-independent) and must equal the model's, and with `verifyRows` (the default) every decoded
row's payload is checked as well. A mismatch stops the run. The checks read nothing, so a
verification run issues the same requests and bytes as a measured run of its config, record for
record (`MeasuredRunTest`), and vouches for it (`steps_diff`); it measures nothing itself, so the
lab excludes it (manifest `verify`, lab/annotations.toml). Never put a check of correctness, or any
other per-row work of the harness's, inside a measured phase. Runs before 2026-11-17 measured with
the (id, version) hash in their reads: 0.6% to 2.9% of a read's instructions as it usually
compiled, and up to 7.7% of MoR's full scans as the JIT compiled it in one run
(lab/pages/suite-128.md, the diagnostic under K1–K3; campaign `oracle-out`).

### Where the time went

Each phase also splits its thread's time by kind of work (`Spans`), as wall and CPU time
(`<kind>_ns`, `<kind>_cpu_ns`), each exclusive of the others:

| Kind | What it covers |
|---|---|
| `io` | Every call into the store, through the tracing FileIO: the client's work (checksums, HTTP, TLS) and the wait. A PUT through an output stream is its close, which uploads: its writes copy into the client's buffer, as part of the encoding. |
| `encode` | Parquet encoding of rows: data files and RMoW-append's regions. |
| `decode` | Parquet decoding in a scan: footers, pages into column batches, the deletes and the residual applied to each batch, and the scan's consumer (a count of rows, a verification run's hash, or rows kept to merge). |
| `footer` | RMoW's footers: Thrift decoding, encoding and merging. |
| `deletes` | Delete files: DVs and position deletes built, encoded, loaded and decoded. |
| `rows` | Rows the harness makes for a writer from their id, event time and version, a chunk at a time as the encoder takes them. The lab counts it in other CPU. |

The rest of the thread's CPU is planning and bookkeeping, and the rest of its wall time is off the
CPU outside requests (garbage collection, the JIT, scheduling). A read pool's tasks record the same
(`pool_<kind>_ns`, `pool_<kind>_cpu_ns`, with `pool_wall_ns`). The lab's `phase_breakdown` view
turns them into components that add up to a phase's wall time, for stacked bars. The store
client's threads of its own (Azure's event loop) are in `process_cpu_ns`, not in `io`. In the
request log, each request has its start in its phase (`start_ns`), wall time (`dur_ns`) and thread
CPU (`cpu_ns`). Each span reads the thread's CPU clock twice, about 2 µs here, and a batch or read
opens tens to hundreds of them; on a 7-arm run they moved user-mode instructions by -0.5% to +2%.

CPU and wall time must run on one clock for the breakdown to add up. On this laptop (WSL2 on
Hyper-V) the thread CPU clock runs 11% fast against the wall clock, so a busy phase's `cpu_ns`
exceeds its `wall_ns`. Each run spins for 200 ms before it starts and records the ratio in its
manifest (`host.thread_cpu_per_wall`, 1 where the clocks agree), and `phase_breakdown` divides CPU
times by it.

### What is held

Each phase also records the most its readers and writers held at once (`Held`): `held_rows`, the
rows kept to sort or merge, and `held_bytes`, the encoded bytes, with the peak of each kind of byte
(`held_<kind>`):

| Kind | What is held |
|---|---|
| `rows` | Rows a writer reads back to rewrite or merge, and a batch's rows to sort, until its files are written; rows a sorted read decodes to sort a file it fetched whole. In rows, since their size in memory is the engine's choice. |
| `chunks` | The column chunks a reader fetched for the row group it is decoding: every data read its stream serves from storage, by row group (the file's split offsets), held until it fetches from the next. A file Iceberg fetches whole counts whole. |
| `writer` | A Parquet writer's buffered row group, as large as the largest it wrote. |
| `pending` | Encoded data waiting for its request: RMoW-append's regions until the file's append, a data file the store can extend until its create. |
| `deletes` | DVs and position deletes loaded (a DV's blob, a delete file's length), by a read's delete cache until the read ends, or by a scan until it ends; and the DVs a batch builds. |

The levels are counted where the code holds data, so two runs record the same peaks, as they do
requests and bytes. They are not the JVM's heap, which holds the benchmark's own state too and
moves with the collector. Every hold must be released by the end of the outermost phase that made
it, or the run fails. Writers stream: the region designs read, rewrite and encode one row group at
a time, as parts of `merge-read` and `write`, each recorded once. Every arm keeps each row it reads
back as its id and version, and makes the rows again as the encoder takes them (`MadeRows`). A read
whose splits run on a pool holds as many row groups as its threads do at once, so its peaks depend
on how they interleave.

With `--log-held`, `held.jsonl` has every change, in order (`seq`), with the levels after it, the
innermost phase it came in (`in`) and what it was (`what`: a row group fetched, a region encoded,
an append sent), for drawing a phase's memory against the stages of its work rather than time.

## I/O parity rules

Each rule guards against a bias found in Iceberg's code. A change to any arm's read or write path
must keep them, and the tests that check them must pass.

1. **Every stream is `RangeReadable`.** Iceberg's Parquet reader uses vectored reads, which here
   means one bounded ranged GET per run of adjacent column chunks, only through `RangeReadable`.
   Without it, `S3InputStream` issues open-ended GETs and reads through gaps of up to 1 MiB. That
   would charge only the appending designs for their dead bytes. Iceberg's local FileIOs are not
   `RangeReadable`, so `PreadFileIO` and `MemFileIO` stand in for them.
2. **No arm pays a HEAD that stock Iceberg would not.** Delete files are opened with their length
   (`LengthAwareDeleteFilter`). The stock filter does not pass the length, which would cost a HEAD
   per delete file. Files are created as Iceberg's writer factories create them, without an
   existence check (`overwrite`). One HEAD is stock Iceberg's and is counted: `PuffinWriter`
   creates each DV file with `create()`, which on S3 and ADLS checks that it does not exist, so
   every arm that writes DVs pays one HEAD per Puffin file.
3. **Footers are read the same way in every arm.**
4. **Local and in-memory runs report requests, bytes and CPU, not latency** (above).
5. **Every file is planned as Iceberg plans it**: one split task per row group, on the split
   offsets of the version read. A footer-cache reader reads a file of at most 1 MiB in one task,
   since Iceberg fetches such a file whole and each split would fetch it again. A split file's
   objects are planned the same way, each on its own footer's offsets.
6. **Writers hold a footer for one transaction, in every arm.** A batch or a compaction is a
   transaction, and an Iceberg writer starts each from storage: no footer carries over to the
   next. Within one, a writer reads a file version's footer once, on its first use, and reuses it
   (`writerFooters`), whether it is RMoW-append finding and rewriting row groups or a MoR-family
   arm scanning and merging files. A file of at most 1 MiB is fetched whole on every open.

## Layout of the code

| Package | Contents |
|---|---|
| `rmow` | The append layer: `OffsetOutputFile` (the stock writer, with positions starting at the object's length), `RegionWriter`, `FooterCodec` (merging Thrift footers), `VersionedInputFile` (one version's view, with the optional footer prefetch), `LogicalFile` with its two layouts `RmowFile` (appendable) and `SplitFile` (an object per version), and `ParquetBatches` (the one read path every arm uses: Iceberg's vectorized reader). |
| `arms` | `MorArm` (with the subclasses `MorV2Arm`, `MorV3Arm`, `CowArm` and `TailCowArm`), `RmowArm`, `TailPolicy`, `DataFileWriter`, `MadeRows` (a writer's rows, made as the encoder takes them) and `InitialLoad`; `Reader` (the reader models), `Scans` (the one file-scan path every arm uses, and the sorted read), `DeletedRanges` (whether a file's deletes cover a row group whole, a range check on the deletion vector's bitmap), `RowBatch` (a batch's live rows: its deletes and compiled residual applied), `LengthAwareDeleteFilter`, `DeleteCache`, `ChunksHeld` (a scan's column chunks, by row group) and `WriterFooters`. |
| `workload`, `oracle` | The seeded op log and the reference model. |
| `io` | `MemFileIO` and `PreadFileIO` (with the fork's atomic operations, `LocalCas`), `TracingFileIO`, `IoTrace`, `RelocatedFileIO` (a run's logical paths on its own prefix), `Spans` (where the time went), `Held` (what is held), `StoreLatency` (injected latency), the SDK recorders (`SdkRecorder`: `S3Recorder`, `AzureRecorder`), and for Azure `AdlsClients`. |
| `append` | `RegionAppender`, `AtomicAppender` (every store's, through the fork's `SupportsAtomicOperations`) and `EmulatorAppenders` (the emulators' stand-ins). |
| `bench` | `Runner`, `Phases`, `BenchConfig`, `ThreadCounters` (instructions and cycles), `Store` (mem, local, S3 or Azure) and `Main`; `arms.VersionDump` and `rmow.VersionDigest` dump every version for the interop check. |
| `fuzz` | `Fuzz`: random configurations through the runner, with the checks beyond a run's own (`bin/fuzz.sh`, [Fuzzing](#fuzzing)). |
| `micro` | JMH micro-benchmarks (`FooterBench`), and the cloud spikes (`S3ExpressSpike`, `AzureSpike`). |
| `analysis/` | `compare_runs.py` (exact repeatability), `interop_check.py` (pyarrow and DuckDB), `summary.py`, `tune.py` (the work formula and the stores' latency tests the simulated pages price with), the models (`hybrid_model.py`, `layout_model.py`, `event_time_model.py`, `recut_model.py`), and the scripts behind retired write-ups (`consolidate.py`, `plots.py`, `sweep.py`, `layouts.py`, `compaction.py`). Results are read through the lab (`lab/`, labbook). |

## Compatibility

`AppendableParquetTest` builds a file with three versions:
1. a first row group;
2. an appended region;
3. a merged region that replaces the first row group, listed last (offset order) or in its place
   (logical order, out of offset order).

It then reads each version at its own length. Every check passes with stock Iceberg 1.12.0 and
parquet-java 1.17.1:
- all three versions return exactly the rows written;
- `_pos` follows footer order;
- a split selects exactly one region, and planned splits read every row once in either order;
- row-group statistics skip the other regions.

`VersionDumpTest` runs the appending arms (the split arms included) with compaction, under both
footer orders, and dumps every version they write. `analysis/interop_check.py` then reads every
version with pyarrow 26.0.0 and DuckDB 1.5.6. For each, both readers must list the same row groups,
decode the same rows (with every payload column checked), number them as Iceberg's `_pos` does, and
filter by id range without losing rows. A split file's footer-only versions must be reassembled by
pyarrow's dataset API and refused by its plain reader and by DuckDB, which read no references.
`SplitFileTest` checks that stock Iceberg refuses them too.
