# rmow-bench

A proof of concept and benchmark of **region-merge-on-write (RMoW)** for Parquet, against Iceberg
**merge-on-read (MoR)**.

RMoW appends to an existing Parquet object instead of writing new files. Each batch appends a
region (one or more row groups, with their page indexes) and then a new footer. That footer lists
the old row groups that are still live, at their original offsets, followed by the new ones. A row
group whose rows were merged into the new region is simply left out. Every version of the file is
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

Three documents go with this one:
- [docs/PLAN.md](docs/PLAN.md): the plan, with its milestones.
- [docs/DESIGN.md](docs/DESIGN.md): the format-level findings and the writer's rules.
- [docs/HYPOTHESES.md](docs/HYPOTHESES.md): the hypotheses, fixed before any cloud run.

## Build and run

Requires JDK 17 and Maven. Iceberg (1.12.0) and parquet-java (1.17.1) are the released versions
from Maven Central. The fork is not needed.

```bash
mvn -q package -DskipTests        # the jar plus target/lib
bin/run.sh configs/smoke.json     # prints the run directory
.venv/bin/python analysis/summary.py results/<label>-<hash>/<start time>
mvn test                          # unit tests, the S1 compatibility spike, end-to-end smoke tests
```

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

Runs that share a configuration share the `<label>-<hash>` directory.

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
| `state` | The footprint: objects, stored, live and reclaimable bytes, row groups, files, footer bytes. |

Each phase records wall time, thread CPU time, and the requests a tracing FileIO saw: GETs (ranged
or streamed), HEADs, PUTs, APPENDs, aborts, bytes read and bytes written. Requests are counted the
way Iceberg's object-store streams issue them. Every read is checked against a reference model,
using an order-independent hash of (id, version), and a mismatch stops the run.

### I/O parity rules

Each rule guards against a bias found in Iceberg's code:

1. **Every stream is `RangeReadable`.** Iceberg's Parquet reader uses vectored reads, which here
   means one bounded ranged GET per run of adjacent column chunks, only through `RangeReadable`.
   Without it, `S3InputStream` issues open-ended GETs and reads through gaps of up to 1 MiB. That
   would charge only RMoW for its dead bytes. Iceberg's local FileIOs are not `RangeReadable`, so
   `PreadFileIO` and `MemFileIO` stand in for them.
2. **No arm pays an extra HEAD.** Delete files are opened with their length
   (`LengthAwareDeleteFilter`). The stock filter does not pass the length, which would cost a HEAD
   per delete file.
3. **Footers are read the same way in every arm.**
4. **Local and in-memory runs report requests, bytes and CPU, not latency.** WSL2 and the page
   cache make local wall times meaningless. Latency comes from cloud runs.

## Arms

| Arm | Inserts | Updates and deletes |
|---|---|---|
| `mor-v2-file` | a new data file per batch | Parquet position-delete files, one per touched data file per batch, never merged |
| `mor-v2-partition` | the same | one position-delete file per batch, matched to every older data file by sequence number (as Iceberg 1.12 plans them) |
| `mor-v3` | the same | deletion vectors in a Puffin file per batch; a data file's DV is replaced, merged with its predecessor |
| `cow` | a new data file per batch | every touched data file is rewritten whole |
| `tailcow-b`, `tailcow-c` | merged into open tail *data files* by policy B or C | DVs, except in tail files being merged |
| `rmow-a`, `rmow-b`, `rmow-c` | appended to the tail object as a row group, merged with open tail *row groups* by policy A, B or C | every touched row group is rewritten as a region of its own object (row-group copy-on-write) |
| `rmow-a-dv`, `rmow-b-dv`, `rmow-c-dv` (an ablation, not in the default matrix) | the same | DVs on each object's stable prefix (the row groups no tail merge will touch), with updated versions in the tail; open tail row groups are rewritten as above |

The tail policies (`TailPolicy`) are:
- **A:** never merge.
- **B:** re-merge the one open tail until it reaches `tailSealBytes`.
- **C:** tiered: `tierFanout` open units of one level merge into one unit a level up.

RMoW against TailCoW therefore isolates the layout, a single appendable object against separate
data files. TailCoW against MoR-v3 isolates the tail-merge policy.

The `-dv` arms are outside the goal, because their readers apply deletion vectors. They are kept
only as an ablation: they measure what RMoW's region rewrites cost compared with deleting by DV.
`rmow-a-dv` is MoR-v3's update handling inside one appendable object. See
[docs/DESIGN.md](docs/DESIGN.md) for why a DV stays valid on the stable prefix.

### Knobs (`layout` in the config)

| Knob | Meaning |
|---|---|
| `codec` | The Parquet codec. The default is zstd. |
| `rowGroupBytes` | The row-group size of data files. |
| `targetFileBytes` | The size at which data files, and an appendable file's live bytes, roll over. |
| `sortWrites` | Sort every write by `ts`. |
| `tailSealBytes` | The size at which an open tail unit is sealed (policies B and C). |
| `tierFanout` | How many units of one level merge into one (policy C). |
| `maxRowGroupsPerFile` | The RMoW tail rolls over at this many row groups. Footers cost about 1.2 KB and 30 µs per row group. |
| `writerCachesFooters` | Writers keep the footers they wrote. This affects RMoW's reads of touched row groups and its footer statistics. |
| `footerPrefetchBytes` | Every reader prefetches this tail on its first footer probe, so a footer costs 1 request instead of 2. 0 means stock parquet-java. |
| `locate` | `oracle` (the default) takes row positions from an in-memory index, at no cost. `scan` finds them the way a MERGE would, in a timed `locate` phase checked against the index. MoR scans `id` and `_pos` of every file whose `id` bounds may hold a target; RMoW looks the ids up in its footers' row-group statistics. |
| `reads.readers` | The reader models every probe runs under, on the same table state. The first is primary. `one-footer` (the default) opens each file at one version and reads that version's one footer once, in one request (a 64 KiB prefetch), then only the row groups it needs. `stock` is stock Iceberg today: one split per row group (`FileScanTask.split`), each split re-reading the footer with two requests. Each model also sets `splitTargetBytes` and `parallelism`, the threads a read's split tasks run on. |
| `compactEvery` | Compact every this many batches (0 means never). MoR-family arms bin-pack small files and files whose delete ratio is at least `compactDeleteRatio`. RMoW rewrites objects whose dead-byte ratio is at least `compactGarbageRatio`, or that hold `compactSmallRowGroups` small row groups. |

## Layout of the code

| Package | Contents |
|---|---|
| `rmow` | The append layer: `OffsetOutputFile` (the stock writer, with positions starting at the object's length), `RegionWriter`, `FooterCodec` (merging Thrift footers), `VersionedInputFile` (one version's view, with the optional footer prefetch), `RmowFile`, and `ParquetReads` (the one read path every arm uses). |
| `arms` | `MorArm` (with the subclasses `MorV2Arm`, `MorV3Arm`, `CowArm` and `TailCowArm`), `RmowArm`, `TailPolicy`, `DataFileWriter` and `InitialLoad`. |
| `workload`, `oracle` | The seeded op log and the reference model. |
| `io` | `MemFileIO`, `PreadFileIO`, `TracingFileIO` and `IoTrace`. |
| `bench` | `Runner`, `Phases` and `Main`. |
| `micro` | JMH micro-benchmarks (`FooterBench`). |
| `analysis/` | `interop_check.py` (pyarrow and DuckDB), `summary.py`, and `plots.py` (figures and a table twin of each). |

## Compatibility

`AppendableParquetTest` builds a file with three versions:
1. a first row group;
2. an appended region;
3. a merged region that replaces the first row group.

It then reads each version at its own length. Every check passes with stock Iceberg 1.12.0 and
parquet-java 1.17.1:
- all three versions return exactly the rows written;
- `_pos` follows footer order;
- a split selects exactly one region;
- row-group statistics skip the other regions.

`analysis/interop_check.py target/interop` reads each version's prefix copy with pyarrow 26.0.0
and DuckDB 1.5.6. Both return the same rows, and both see only the live row groups.

## Status

- **M0 is done.**
- **M1 is done:**
  - 12 arms, three tail policies, and the RMoW-DV hybrid;
  - compaction, locate-by-scan, footer prefetch, and parallel split reads.

  Every arm passes the oracle on the in-memory and local stores: full reads, recent-window
  reads, and time travel across compactions.
- **Running:** the local matrix of recency and uniform keys, with compaction off and every 10
  batches (`configs/local-m1-*`).
- **Next:** repetitions and analysis (M2), then S3 Express (M3), which needs credentials and a
  directory bucket.
