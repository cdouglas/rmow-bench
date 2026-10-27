# rmow-bench

A proof of concept and benchmark of **region-merge-on-write (RMoW)** for Parquet, against Iceberg
**merge-on-read (MoR)**.

RMoW appends to an existing Parquet object instead of writing new files. Each batch appends a
region (one or more row groups, with their page indexes) and then a new footer. That footer lists
the old row groups that are still live, at their original offsets, and the new ones. A row group
whose rows were merged into the new region is simply left out, and the row group that replaces it
takes its place in the list: `<R1> … <RN> <F1>`, updated at `RK`, becomes `<R1> … <RN> <F1> <RK'>
<F2>`, with F2 listing `R1 … RK' … RN`. Every version of the file is
the object's prefix up to that version's footer, so old versions stay readable. Stock readers
already read every version (see [Compatibility](#compatibility)): they find the footer from the
length they are given, and every Parquet offset is absolute.

RMoW is a partial merge-on-write, applied only to the regions a batch affects. The writer pays
more. In return, a reader reads one version of each file, summarized in exactly one footer, and
applies no deletes and merges nothing.

MoR writes a new data file per batch for inserts and the new versions of updated rows. It marks
the replaced and deleted rows with position-delete files (Iceberg v2) or deletion vectors in Puffin
files (v3).

The question this benchmark answers is whether merging (and sorting) the trailing regions is
cheaper than MoR. It looks at both sides: the cost of adding data, and the cost of reading it back
as updates accumulate. Ingest in rough timestamp order, where updates mostly hit recent rows, is
the case of most interest. The measurement is of I/O; this does not patch Iceberg to accept the
layout.

Four documents go with this one:
- [docs/EXPERIMENTS.md](docs/EXPERIMENTS.md): a map of what has been run, what each experiment
  varied and found, and where each hypothesis stands. Start here.
- [docs/PLAN.md](docs/PLAN.md): the plan, with its milestones.
- [docs/DESIGN.md](docs/DESIGN.md): the format-level findings and the writer's rules.
- [docs/HYPOTHESES.md](docs/HYPOTHESES.md): the hypotheses, fixed before any cloud run.

## Build and run

Requires JDK 17 and Maven, and the atomicio fork of Iceberg 1.12.0 (`1.12.0-atomicio-SNAPSHOT`)
in the local Maven repository: RMoW appends through its `SupportsAtomicOperations` on every store
([Appends: the fork's atomic operations](#appends-the-forks-atomic-operations)). Build it from the
fork's `atomicio` branch with `./gradlew publishToMavenLocal -x test -x integrationTest`;
`-Diceberg.version` picks another of its builds. parquet-java (1.17.1) and everything else come
from Maven Central.

```bash
mvn -q package -DskipTests        # the jar plus target/lib
bin/run.sh configs/smoke.json     # prints the run directory
.venv/bin/python analysis/summary.py results/<label>-<hash>/<start time>
mvn test                          # unit tests, the S1 compatibility spike, end-to-end smoke tests
```

For experiments, `bin/matrix.sh` runs configurations one after another from a snapshot of the
build (`results/builds/`), so `mvn package` can run while they do:

```bash
bin/matrix.sh --build configs/local-m1-*.json
```

The snapshot holds the jar, hard links to its released dependencies, the configurations, and for a
dirty tree the diff and the untracked files; each run's manifest names it. Runs keep their objects
under `store/` and delete them when they end (`--keep-store` keeps them).

Campaigns too large for a laptop run in a container on another machine, each configuration on
whole idle cores of one NUMA node: `docker/run.sh build`, then `docker/run.sh start <campaign>`
([docker/README.md](docker/README.md)).

`--dump-versions` (on `bin/run.sh` or `bin/matrix.sh`) keeps every version of every appendable
object in `<run>/versions/`, checked against the reference model at every step, for
`analysis/interop_check.py <run>/versions` to read with pyarrow and DuckDB (see
[Compatibility](#compatibility)).

Python tools live in a project venv:

```bash
python3 -m venv .venv
.venv/bin/pip install pyarrow duckdb pandas matplotlib
```

A run writes `results/<label>-<config hash>/<UTC start>/`:

- `manifest.json`: the full configuration with defaults, the git commit, the Iceberg, Parquet and
  JVM versions, the host, and the op log's SHA-256.
- `oplog.bin`: the exact operations that were replayed.
- `steps.jsonl`: one record per arm, step and phase.

Runs that share a configuration share the `<label>-<hash>` directory. Requests and bytes are a
function of the op log, the configuration and the code, so `analysis/compare_runs.py` checks that
two runs match exactly. Every run names its files alike (`bench://run/…`, mapped to the run's own
prefix in the store), since paths end up inside Puffin footers.

## What is measured

All arms replay the same seeded op log in lockstep, in an order shuffled at every step. They all
start from the **same initial-load bytes**: a stock Parquet file is already the first version of an
appendable one.

The phases recorded in `steps.jsonl` are:

| Phase | What it covers |
|---|---|
| `batch` | One whole batch. It contains the phases below, so don't add them to it. |
| `merge-read` | RMoW only: reading back the row groups the batch touches. |
| `write` | Writing data files, or appending regions and footers. |
| `delete-write` | MoR only: loading the previous DV and writing the new Puffin file. |
| `read_full` | Reading the whole current table. |
| `read_recent` | Reading rows in the newest 1% of event time. Files are pruned by column metrics, row groups by stats. |
| `read_old` | Reading the table as it was `travelBack` batches ago. |
| `state` | The footprint: objects, stored, live and reclaimable bytes, row groups, files, footer bytes, and RMoW's row groups listed out of id order. |

Each phase records wall time, thread CPU time, and the requests a tracing FileIO saw: GETs (ranged
or streamed), HEADs, PUTs, APPENDs, aborts, bytes read and bytes written. Requests are counted the
way Iceberg's object-store streams issue them. Every read is checked against a reference model,
using an order-independent hash of (id, version), and a mismatch stops the run. With `verifyRows`
(the default), every decoded row's payload is checked as well. That check is the harness's, not an
engine's, and it costs about 70% of a read's CPU, so runs made for latency simulation turn it off.

**Where the time went** (`Spans`, since 2026-10-26). Each phase also splits its thread's time by
kind of work, as wall and CPU time (`<kind>_ns`, `<kind>_cpu_ns`), each exclusive of the others:

| Kind | What it covers |
|---|---|
| `io` | Every call into the store, through the tracing FileIO: the client's work (checksums, HTTP, TLS) and the wait. A PUT through an output stream is its close, which uploads: its writes copy into the client's buffer, as part of the encoding. |
| `encode` | Parquet encoding of rows: data files and RMoW's regions. |
| `decode` | Parquet decoding in a scan: footers, pages and rows, with deletes applied, and the scan's consumer (a hash, or rows kept to merge). |
| `footer` | RMoW's footers: Thrift decoding, encoding and merging. |
| `deletes` | Delete files: DVs and position deletes built, encoded, loaded and decoded. |

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

### I/O parity rules

Each rule guards against a bias found in Iceberg's code:

1. **Every stream is `RangeReadable`.** Iceberg's Parquet reader uses vectored reads, which here
   means one bounded ranged GET per run of adjacent column chunks, only through `RangeReadable`.
   Without it, `S3InputStream` issues open-ended GETs and reads through gaps of up to 1 MiB. That
   would charge only RMoW for its dead bytes. Iceberg's local FileIOs are not `RangeReadable`, so
   `PreadFileIO` and `MemFileIO` stand in for them.
2. **No arm pays a HEAD that stock Iceberg would not.** Delete files are opened with their length
   (`LengthAwareDeleteFilter`). The stock filter does not pass the length, which would cost a HEAD
   per delete file. Files are created as Iceberg's writer factories create them, without an
   existence check (`overwrite`). One HEAD is stock Iceberg's and is counted: `PuffinWriter`
   creates each DV file with `create()`, which on S3 and ADLS checks that it does not exist, so
   every arm that writes DVs pays one HEAD per Puffin file.
3. **Footers are read the same way in every arm.**
4. **Local and in-memory runs report requests, bytes and CPU, not latency.** WSL2 and the page
   cache make local wall times meaningless. Latency is simulated from requests, bytes and CPU
   ([lab/pages/store-latency.md](lab/pages/store-latency.md)).
5. **Every file is planned as Iceberg plans it**: one split task per row group, on the split
   offsets of the version read. A footer-cache reader reads a file of at most 1 MiB in one task,
   since Iceberg fetches such a file whole and each split would fetch it again. A split file's
   objects are planned the same way, each on its own footer's offsets.
6. **Writers hold a footer for one transaction, in every arm.** A batch or a compaction is a
   transaction, and an Iceberg writer starts each from storage: no footer carries over to the
   next. Within one, a writer reads a file version's footer once, on its first use, and reuses it
   (`writerFooters`), whether it is RMoW finding and rewriting row groups or a MoR-family arm
   scanning and merging files. A file of at most 1 MiB is fetched whole on every open. Runs
   before 2026-10-24 let RMoW's writer keep its footers across batches (`writerCachesFooters`),
   which no MoR-family writer did; at 4 MiB that left out 3.5 of RMoW-C's 9.5 requests per batch.

## The S3 store

`store.type` `s3` runs every arm on Iceberg's `S3FileIO`, over one AWS SDK client that this
project builds (`Store`), with credentials from the default chain, so none are in a configuration
or its manifest. Only directory buckets (S3 Express One Zone, `<name>--<az id>--x-s3`) append, so
the bucket decides:

- **a directory bucket:** RMoW appends through the fork (`AtomicAppender`): one PutObject at the
  object's length (`writeOffsetBytes`), conditioned on its ETag, with no HEAD;
- **a standard bucket:** the runner refuses RMoW's appendable arms; split files run;
- **an emulator** (`store.endpoint`): appends read the object and CAS it back longer, on an
  unmeasured client (`EmulatorAppenders.rewriting`). Creates are the fork's.

An interceptor on the client (`S3Recorder`) counts the requests the SDK sends. Each phase then
records them beside the trace's model, as `sdk_*` fields: requests by operation, retries, and
bytes. `S3StoreTest` runs every arm family against versitygw (in Docker; skipped without it), and
checks that in every phase the SDK's GETs, PUTs, HEADs and DELETEs equal the model's, with no
retries, and that the run deletes its objects. So local runs count the requests S3 receives.

What the emulator cannot check is the S3 Express append (versitygw ignores `writeOffsetBytes`),
S3 Express's session requests (`CreateSession`, counted as `sdk_sessions`: they count in cost,
not in the model), and latency. Two things check them, given a directory bucket:

- **`S3ExpressTest`** runs when `RMOW_S3X_ROOT` names a prefix in a directory bucket
  (`s3://<bucket>--<az id>--x-s3/<prefix>`; region from `RMOW_S3X_REGION` or `AWS_REGION`). It
  checks that:
  - an append at the object's length lands;
  - one anywhere else fails and changes nothing, and so does a create where an object is;
  - the recorder sees a create as one PutObject and an append as one PutObject with a write
    offset, neither with a HEAD;
  - a run of every arm family matches the model, appends and bytes written included.
- **`org.rmow.bench.micro.S3ExpressSpike <s3://bucket/prefix>`** measures latencies, interleaved:
  the store's own appends (a write offset alone) against RMoW's (the fork's, conditioned on the
  ETag too), PUTs against RMoW's creates (conditioned on absence), from 64 KiB to 32 MiB; and GETs
  (whole, last 64 KiB, middle 64 KiB) of objects of 1 to 1000 appended parts. It writes
  `results/spike/s3x-spike-<UTC>/spike.jsonl`.

`configs/s3x-calibration.json` is the first cloud run, to make once the bucket exists: a small
workload and every default arm, from an EC2 instance in the bucket's zone. Its twin,
`configs/s3x-calibration-local.json`, differs only in the store. Its requests and bytes are what
the S3 Express run's model and SDK counts must match exactly, apart from `CreateSession`.

## The Azure store

`store.type` `azure` runs every arm on Iceberg's `ADLSFileIO`, on a hierarchical-namespace account
(`store.root` `abfss://<container>@<account>.dfs.core.windows.net/<prefix>`). Credentials come from
the default chain (a VM's managed identity, the environment, the Azure CLI), so none are in a
configuration or its manifest. For an emulator, `store.endpoint` names it, and the account key is
in `AZURE_STORAGE_KEY`.

The fork's ADLS FileIO sends what the model counts. An output stream is one Put Blob up to
256 MiB, and `create()` adds its existence check, a HEAD. A stream opens nothing until it reads,
and a ranged read is one GET of exactly its range. Iceberg 1.12.0 as shipped reads more: every
stream opens with a GET of the file's first 4 MiB, and a ranged read at an offset fetches up to a
4 MiB chunk, since it asks for `position + length` bytes ([docs/AZURE_RUNBOOK.md](docs/AZURE_RUNBOOK.md)).
The fork's sequential reads still download the SDK's 4 MiB blocks whole, so they read more bytes
than the model counts, but no more requests.

`AppendBlock` extends only an Append Blob, and Iceberg writes block blobs. So RMoW's files are
Append Blobs, created through the fork (`AtomicAppender`) as a temporary Append Blob, one Append
Block and a rename with `If-None-Match: *`: three requests, where the model counts one PUT. Each
append is one `AppendBlock`, conditioned on the blob's length and ETag.

The recorder (`AzureRecorder`) counts what the SDK sends through every client in the process,
Iceberg's included: azure-core adds its policies to every storage pipeline
(`META-INF/services`). Phases record the counts as `sdk_*` fields, and `sdk_latency` in the lab
prices them.

`AzureStoreTest` runs the store's contract (`AzureStoreContract`) against Azurite, in Docker
(skipped without it). Azurite has no Data Lake endpoint, so the store deletes an emulator's runs
blob by blob, and it cannot rename, so RMoW creates its files there through a stand-in
(`EmulatorAppenders.appendBlobs`): an empty Append Blob made in place, a HEAD, and an Append
Block. The contract checks that:
- Iceberg's write is a HEAD and one Put Blob;
- its reads open nothing and ask for exactly their range;
- Append Blobs append at their length only, a create fails where a blob is, and a file Iceberg
  wrote cannot be appended to (`InvalidBlobType`);
- every phase of every arm family sends what the model counts, request for request (a create as
  above), with nothing else and no retries.

`AzureTest` runs the same contract against Azure when `RMOW_AZURE_ROOT` names a prefix in a
container. `org.rmow.bench.micro.AzureSpike <abfss://...>` measures latencies, interleaved: Put
Blobs, the store's own Append Blocks (conditioned on the length alone) and RMoW's (on the ETag
too), from 64 KiB to 32 MiB; creates (an empty Append Blob, the store's own appendable file, and
RMoW's atomic one) and HEADs; GETs of Append Blobs of 1 to 1000 blocks; and Iceberg's ranged read
of a 32 MiB file against the SDK's. `configs/azure-calibration.json` is the calibration run. Its
Azurite twin (`-azurite.json`) gives the requests it must send, but for its creates: each costs a
HEAD there where Azure's costs a rename.

## Appends: the fork's atomic operations

The atomicio fork's `SupportsAtomicOperations` is the conditional write its catalog commits
through, and APPEND is RMoW's primitive. Every store's appender is `AtomicAppender` over it:
S3FileIO and ADLSFileIO implement it on the fork, and the local stores (`MemFileIO`,
`PreadFileIO`) here, so local runs take the same path. Each write returns the next snapshot,
holding a length and an ETag, and the appender keeps it, so appends chain:
- **An append** is one request, at the snapshot's length and conditioned on its ETag: a PutObject
  on S3 Express, an Append Block on ADLS. Of several writers appending to one version, exactly one
  lands. Where the store caps an append (Azure's Append Block, 100 MiB), it is several, in order.
- **A new appendable file** is a CAS from a snapshot that has seen nothing, which assumes the
  object is absent and sends no lookup (every RMoW file has a name of its own):
  - on S3, one PutObject with `If-None-Match: *`;
  - on ADLS, a temporary Append Blob (created, then one Append Block) renamed onto the file with
    `If-None-Match: *`. An Append Blob is created empty, so only the rename makes the create all or
    nothing: one request more than the store's own way, an empty Append Blob and its first block.
  - on the local filesystem, a temporary file renamed onto the file.

  The model counts it as one PUT.

`prepare()` reads the content once for its checksum (CRC32C on S3 and locally, MD5 on ADLS), so a
write's CPU includes it on every store. Emulators lack part of the API, and run RMoW's appendable
arms on stand-ins (`EmulatorAppenders`, above); a store on a cloud endpoint never uses them.

The fork's own contract (`SupportsAtomicOperationsContractTest`, from its test jar) runs against
both local stores (`LocalAtomicContractTest`): creates, replaces and appends pinned to the
version a snapshot saw, and serial and concurrent races with one winner. `AtomicAppenderTest`
checks the appender, and that it sends no lookup. Against the services, `S3ExpressTest` and
`AzureTest` check it; the spikes time it against the stores' own requests.

`atomicio` has the fixes RMoW needs, beyond the API:
- ADLS reads open nothing until they read, and ask for exactly their range;
- S3 and ADLS appends to a snapshot send no lookup, and a create from a snapshot that has seen
  nothing sends none either;
- S3's `prepare()` no longer needs commons-io, which Iceberg does not ship.

The two read fixes are upstream performance bugs: the reads fetch more than they need, but return
the right bytes. So they are on `atomicio`, not `core-fixes`, which holds correctness fixes.

Against the release, a run on the fork differs in one byte count: Puffin files name the build
that wrote them (`created-by`), and `1.12.0-atomicio-SNAPSHOT` is 18 bytes longer than `1.12.0`.
So MoR-v3's and TailCoW-C's DV files are 18 bytes larger than in runs before 2026-10-26; every
request, and every other byte, is the same (checked on 7 arms, 2026-10-26).

For latency, a configuration can set more things, and a run can log its requests:
- **`warmup`** runs a small op log on the in-memory store before the measured run, so the JIT has
  compiled the paths first. It changes no request or byte (`RunnerSmokeTest`).
- **`reads.readers`** can list a reader model with `parallelism` above 1, to read on several
  threads as an engine does. With `deleteCache` (the `cached` model), it loads each data file's
  deletes once per read rather than once per split, as an engine with a delete cache would. With
  `skipDeadRowGroups` (the `cached-skip` model; it needs `footerCache`), it plans no split for a
  row group the file's deletes cover whole, nor any for a file whose DV covers every row, checking
  the deletes, loaded once, against the footer it reads. Iceberg's reader fetches such row groups
  and drops their rows one by one, so RMoR-C's reads are reported under both. A file of at most
  1 MiB, fetched whole, is skipped only whole.
- **`store.latency`** injects a simulated store's latency into every measured request: median
  latencies by kind (GET and HEAD, PUT and DELETE, append) with their lognormal sigmas, an extra
  GET latency per appended part, and transfer times per MiB. Each request sleeps its expected
  latency, so a phase's wall time is what it would take on that store (`StoreLatency`).
- **`--log-requests`** (on `bin/run.sh` and `bin/matrix.sh`) writes `requests.jsonl`: every
  request each phase issued, with its split task and each task's CPU. The lab's
  `figures.replay` times a logged run on any store, with parallel reads' critical paths
  (lab/pages/store-latency.md); `RequestLogTest` checks the log and the injection.

## Arms

| Arm | Inserts | Updates and deletes |
|---|---|---|
| `mor-v2-file` | a new data file per batch | Parquet position-delete files, one per touched data file per batch, never merged |
| `mor-v2-partition` | the same | one position-delete file per batch, matched to every older data file by sequence number (as Iceberg 1.12 plans them) |
| `mor-v3` | the same | deletion vectors in a Puffin file per batch; a data file's DV is replaced, merged with its predecessor |
| `cow` | a new data file per batch | every touched data file is rewritten whole |
| `tailcow-b`, `tailcow-c` | merged into open tail *data files* by policy B or C | DVs, except in tail files being merged |
| `rmor-c` | as `tailcow-c` | regional merge-on-read: a batch rewrites each region it changes when it changes at least θ (`rmorRewriteShare`) of the region's live rows. A sealed file's row group goes, with the batch's changes, into the batch's new data file, and a DV covers the old row group; an open tail file is rewritten whole and takes its place in the tail. Below θ, DVs. A file with no live row left leaves the table. θ = 0 rewrites every region a batch changes; above 1 it is `tailcow-c`. The format is Iceberg's, and no store needs to append |
| `rmow-a`, `rmow-b`, `rmow-c` | appended to the tail object as a row group, merged with open tail *row groups* by policy A, B or C | every touched row group is rewritten as a region of its own object (row-group copy-on-write) |
| `rmow-a-split`, `rmow-b-split`, `rmow-c-split` | as `rmow-*`, but each version's regions go to a new object, and its footer to a footer-only object naming the objects its row groups live in (`file_path`) | the same row-group copy-on-write; no append, so standard S3 and GCS can run them, and whole objects become deletable as versions expire ([docs/DESIGN.md](docs/DESIGN.md#regions-in-separate-objects-split-files)) |

The tail policies (`TailPolicy`) are:
- **A:** never merge.
- **B:** re-merge the one open tail until it reaches `tailSealBytes`.
- **C:** tiered: `tierFanout` open units of one level merge into one unit a level up.

RMoW against TailCoW therefore isolates the layout, a single appendable object against separate
data files. TailCoW against MoR-v3 isolates the tail-merge policy. RMoR-C against TailCoW-C
isolates update handling (regions rewritten against DVs on rows), and RMoR-C against RMoW-C where
a rewritten region goes (a new data file with a DV on the old, against appended to its object). RMoW against RMoW-split
isolates append: the same policy, footer and reads, with history kept inside one object or spread
over objects that can be deleted.

Hybrid `-dv` arms (RMoW's layout with DVs on sealed row groups) ran in one ablation and were
removed; [docs/DESIGN.md](docs/DESIGN.md#deletion-vectors-on-an-appendable-file-an-ablation-removed)
has what they showed.

### Knobs (`layout` in the config)

| Knob | Meaning |
|---|---|
| `codec` | The Parquet codec. The default is zstd. |
| `rowGroupBytes` | The row-group target of data files and the initial load, as Parquet counts it (buffered bytes), so row groups are smaller on disk: about a third of a 1 MiB target for this schema, three quarters of a 4 MiB one. RMoW compaction packs row groups up to the size the writer's come out at on disk. |
| `targetFileBytes` | The size at which data files, and an appendable file's live bytes, roll over. |
| `sortWrites` | Sort every write by `ts`. |
| `tailSealBytes` | The compressed size at which an open tail unit is sealed (policies B and C). An RMoW unit is a row group of inserts; a TailCoW unit is a batch's data file, updated versions included. |
| `tierFanout` | How many units of one level merge into one (policy C). |
| `maxRowGroupsPerFile` | The RMoW tail rolls over at this many row groups. Footers cost about 1.2 KB and 30 µs per row group. |
| `footerOrder` | Where RMoW lists a row group that replaces another. `logical` (the default) puts it in the replaced one's place, so footer order, which is row order, stays id order and a sorted file stays sorted. `offset` lists it last, keeping the footer in file-offset order. The Parquet spec requires neither, and the I/O is the same ([docs/DESIGN.md](docs/DESIGN.md#footer-order)). |
| `writerFooters` | What every arm's writer holds of the footers it reads (I/O parity rule 6). `transaction` (the default): a file version's footer is read once per batch or compaction and reused within it. `open`: every open reads it again, as parquet-java does. Nothing carries over from one transaction to the next. It replaces `writerCachesFooters`, under which RMoW's writer kept its footers across batches. |
| `footerPrefetchBytes` | Every reader prefetches this tail on its first footer probe, so a footer costs 1 request instead of 2. 0 means stock parquet-java. |
| `locate` | `oracle` (the default) takes row positions from an in-memory index, at no cost. `scan` finds them the way a MERGE would, in a timed `locate` phase checked against the index. MoR scans `id` and `_pos` of every file whose `id` bounds may hold a target; RMoW looks the ids up in its footers' row-group statistics. |
| `reads.readers` | The reader models every probe runs under, on the same table state. The first is primary. `one-footer` (the default) opens each file at one version and reads that version's one footer once, in one request (a 64 KiB prefetch), then only the row groups it needs. `stock` is stock Iceberg today: one split per row group (`FileScanTask.split`), each split re-reading the footer with two requests. Each model also sets `splitTargetBytes` and `parallelism`, the threads a read's split tasks run on. |
| `compactEvery` | Check the compaction triggers every this many batches (0 means never). MoR-family arms bin-pack files smaller than `compactSmallFileFraction` of the target and files whose delete ratio is at least `compactDeleteRatio`, as Iceberg's `RewriteDataFiles` does. RMoW triggers on space and on reads separately: it rewrites an appendable file, or moves the live row groups out of a split file's objects, whose dead bytes are at least `compactGarbageRatio` of their length; and it packs small sealed row groups once a file holds `compactSmallRowGroups` of them ([docs/DESIGN.md](docs/DESIGN.md#compaction)). When to compact is an outcome: see `analysis/compaction.py`. |
| `rmorRewriteShare` | `rmor-c`'s θ: a batch rewrites a region it changes (a sealed file's row group, or an open tail file) when it updates or deletes at least this share of the region's live rows, counted per batch, and otherwise puts DVs on the rows. 0 (the default) rewrites every region a batch changes; above 1 none, which is `tailcow-c`. |
| `writerSkipsDeadRowGroups` | The MoR family's writers (their scans to locate rows, merge reads and compaction) read no row group the file's deletes cover whole, nor any file whose DV covers every row, as the `skipDeadRowGroups` reader does. Off (the default) is Iceberg's MERGE scan, which fetches such row groups and drops their rows. In practice only RMoR-C has them: on two configurations, it changed no other arm's request or byte. Configurations that report RMoR set it, with the `cached-skip` reader beside a stock one. |

### The workload (`workload` in the config)

Every arm replays one seeded op log (`OpLogGenerator`): an initial load, then batches of inserts,
updates and deletes on disjoint rows. Ids grow with insertion, and writes keep rows in roughly id
order, so a range of ids stands for a region of the table. The manifest records the log's SHA-256.

| Knob | Meaning |
|---|---|
| `seed`, `initialRows`, `batches`, `opsPerBatch` | The log's seed and size. |
| `insertFrac`, `updateFrac`, `deleteFrac` | Each batch's operations, by share. The rows updated and deleted per batch are fixed. |
| `keyDist` | How updates and deletes pick rows. `RECENCY_EXP` (the ingest window): a row's age in rows, newest first, is exponential with mean `recencyMeanBatches` batches of inserts. `UNIFORM`: every live row alike. `REGIONS`: by region, each with a heat drawn once from the seed, `U^-regionSkew` for a uniform U, so regions ranked by heat follow Zipf's law with exponent `regionSkew`. A region is picked with weight heat times live rows (as of the batch's start), then a row in it. Hot regions can be anywhere and stay hot; a skew of 0 is uniform over live rows. |
| `regionRows` | Consecutive ids per region (default 100,000). A region stands for a row group: a 4 MiB one holds about 98,000 rows of this schema, a 1 MiB one 10,521, and the initial load's row groups vary by a few percent, so a region need not line up with one. |
| `regionSkew` | `REGIONS`' Zipf exponent (default 1). On 1M rows and 60 batches of 10,000 operations in regions of 10,521 rows (135 by the end), the hottest tenth of the regions took 12% of the updates at 0, 54% at 1 and 89% at 2. |
| `updateShare` | The share p of a region's live rows one update statement updates. 0 (the default) makes each update a row of its own. Above 0, a statement takes the region of a row the key distribution picks, under any `keyDist`, and updates p of its live rows (at least one), picked uniformly among those the batch has not touched. The batch's last statement takes what is left of its updates, so p counts only up to updates per batch over `regionRows`. Deletes stay one row each. |
| `lateMedianBatches`, `lateP99Batches` | Insert lateness in event time (`ts`), lognormal, in batches. |

Logs without regions, the ingest window or uniform keys at an update share of 0, are what they
were before regions existed (`OpLogGeneratorTest` pins three runs' hashes).

## Layout of the code

| Package | Contents |
|---|---|
| `rmow` | The append layer: `OffsetOutputFile` (the stock writer, with positions starting at the object's length), `RegionWriter`, `FooterCodec` (merging Thrift footers), `VersionedInputFile` (one version's view, with the optional footer prefetch), `LogicalFile` with its two layouts `RmowFile` (appendable) and `SplitFile` (an object per version), and `ParquetReads` (the one read path every arm uses). |
| `arms` | `MorArm` (with the subclasses `MorV2Arm`, `MorV3Arm`, `CowArm` and `TailCowArm`), `RmowArm`, `TailPolicy`, `DataFileWriter` and `InitialLoad`. |
| `workload`, `oracle` | The seeded op log and the reference model. |
| `io` | `MemFileIO` and `PreadFileIO` (with the fork's atomic operations), `TracingFileIO`, `IoTrace`, `RelocatedFileIO` (a run's logical paths on its own prefix), the SDK recorders (`SdkRecorder`: `S3Recorder`, `AzureRecorder`), and for Azure `AdlsClients`. |
| `append` | `RegionAppender`, `AtomicAppender` (every store's, through the fork's `SupportsAtomicOperations`) and `EmulatorAppenders` (the emulators' stand-ins). |
| `bench` | `Runner`, `Phases`, `Store` (mem, local, S3 or Azure) and `Main`; `arms.VersionDump` and `rmow.VersionDigest` dump every version for the interop check. |
| `micro` | JMH micro-benchmarks (`FooterBench`), and the cloud spikes (`S3ExpressSpike`, `AzureSpike`). |
| `analysis/` | `interop_check.py` (pyarrow and DuckDB), `summary.py`, `plots.py` (figures and a table twin of each), `sweep.py` (runs that differ in row-group size), `layouts.py` (appendable against split files), `compaction.py` (runs that differ in compaction policy: each arm's cheapest policy per read rate and price model), and `compare_runs.py` (exact repeatability). |

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

`VersionDumpTest` runs the RMoW arms (the split arms included) with compaction, under
both footer orders, and dumps every version they write. `analysis/interop_check.py` then reads every version with
pyarrow 26.0.0 and DuckDB 1.5.6. For each, both readers must list the same row groups, decode the
same rows (with every payload column checked), number them as Iceberg's `_pos` does, and filter
by id range without losing rows. A split file's footer-only versions must be reassembled by
pyarrow's dataset API and refused by its plain reader and by DuckDB, which read no references.
`SplitFileTest` checks that stock Iceberg refuses them too.

## Status

- **M0 is done.**
- **M1 is done:**
  - 12 arms, three tail policies, and the RMoW-DV hybrid (removed after its ablation);
  - compaction, locate-by-scan, footer prefetch, and parallel split reads.

  Every arm passes the oracle on the in-memory and local stores: full reads, recent-window
  reads, and time travel across compactions.
- **Done locally:** the matrix of recency and uniform keys, with compaction off and every 10
  batches (`configs/local-m1-*`).
- **Done locally:** a row-group size sweep under recency-skewed updates
  ([lab/pages/sweep.md](lab/pages/sweep.md)), and the S3 store against an emulator.
- **Done locally:** RMoW on split files, an object per version instead of appends, against
  appendable files ([lab/pages/layouts.md](lab/pages/layouts.md)).
- **Done locally:** store latency, simulated from the runs' requests, bytes and CPU, with endive's
  store profiles, and validated against injected latency
  ([lab/pages/store-latency.md](lab/pages/store-latency.md)).
- **Done locally:** when to compact, as an outcome. Eleven compaction policies were run, and each
  arm's cheapest one was found per store, read rate and storage weight
  ([lab/pages/compaction.md](lab/pages/compaction.md)).
- **Done locally:**
  - layout against tail policy, H3
    ([lab/pages/layout-vs-policy.md](lab/pages/layout-vs-policy.md));
  - time travel and dead bytes, H4
    ([lab/pages/time-travel.md](lab/pages/time-travel.md)), on the uncompacted configurations
    rerun once time travel planned an old version with its own split offsets, as Iceberg plans
    an old snapshot;
  - layout against update handling, the `-dv` ablation, whose runs and arms were deleted once its
    conclusions were recorded
    ([docs/DESIGN.md](docs/DESIGN.md#deletion-vectors-on-an-appendable-file-an-ablation-removed)).
- **Rerun on 2026-10-25:** every core experiment above, on the current build: writer footers
  held per transaction, the `cached` reader models, the payload check off, and CPU in
  instructions. Their write-ups read the reruns, and
  [lab/pages/summary.md](lab/pages/summary.md) collects the numbers a write-up quotes.
- **Done on havoc:** the scale campaign, the 4 MiB recency setting scaled to Iceberg's defaults
  (128 MiB row groups, 512 MiB files), in a container (`docker/campaigns/scale.json`,
  [docker/README.md](docker/README.md); [lab/pages/scale.md](lab/pages/scale.md)). Requests,
  bytes and storage were decided there. Thread CPU time could not decide anything: one read's
  CPU varied by up to 9% between identical twins. CPU was then measured in instructions on this
  laptop ([lab/pages/cpu-measure.md](lab/pages/cpu-measure.md)), which decided P3–P5.
- **Next:** the cloud runs, only to check the simulation. Both are prepared, not run:
  - S3 Express (M3), near RMoW's break-even ([docs/S3X_RUNBOOK.md](docs/S3X_RUNBOOK.md));
  - Azure (M4), far above it, through Iceberg's ADLS FileIO with its reads as shipped and exact
    ([docs/AZURE_RUNBOOK.md](docs/AZURE_RUNBOOK.md)).

  Split files can also run on standard S3.
