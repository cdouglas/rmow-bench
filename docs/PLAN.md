# Plan (as approved 2026-10-20)

Copied from the planning session. Deviations since: see README.md, Status, and docs/DESIGN.md (the hybrid RMoW-DV arms were moved forward from "deferred" after the first local results).


## Context

We want an honest, reproducible measurement of two ways to absorb inserts, updates and deletes into an Iceberg table:

- **Merge-on-read (MoR).** New Parquet data files, plus v2 position-delete files or v3 deletion vectors (DVs) in Puffin.
- **Region-merge-on-write (RMoW).** Append a new row-group "region" and a new footer to the *same* Parquet object. The new footer references the still-live old row groups at their original offsets and omits the row groups whose rows were merged into the new region. Old versions are read by locating the footer at an older length.

The question is whether merging and sorting the trailing region(s) beats MoR, on both update latency and read-back latency, as updates accumulate. It matters most for rough timestamp-ordered ingest, where updates hit recent rows. We measure I/O, not Iceberg acceptance of the layout.

Decisions taken:
- **Code home:** a new sibling Maven project, `/home/chris/work/catalog/rmow-bench`.
- **Stores:** RMoW runs only on stores with native append: local, S3 Express, and ADLS Append Blob. Standard S3 runs MoR arms only.
- **Scope:** data-plane I/O first (level L1); real Iceberg commits are optional (level L2, deferred).
- **First cloud target:** S3 Express.

## Why it is feasible (verified in code)

- **Offsets are absolute.** Every live offset in parquet.thrift is from the start of the file (data, dictionary and index pages, column and offset indexes, bloom filters). Bytes between row groups are legal (BloomFilter.md:338-342). `README.md:237-243` already suggests writing cumulative footers.
- **The footer is found from the length alone.** parquet-java 1.17.1 (Iceberg 1.12's version) locates it only from `InputFile.getLength()` (seek to len−8) and never checks the leading magic. It also has `ParquetFileReader.open(InputFile, ParquetMetadata, ParquetReadOptions, SeekableInputStream)` for a footer read in advance.
- **Iceberg already passes the length through.** It hands the manifest's `file_size_in_bytes` to `FileIO.newInputFile(path, len)` (`iceberg/api/.../io/FileIO.java:50-55`), and S3, GCS, ADLS and Hadoop FileIO keep it. A snapshot that records an older length therefore reads the older footer.
- **The writer needs no Iceberg changes.** Iceberg's `ParquetWriter` (`iceberg/parquet/.../ParquetWriter.java:113-137`) takes every offset from `PositionOutputStream.getPos()`. A stream whose reported position starts at the object's current length produces correct absolute offsets with the stock writer.
- **Row positions follow footer order.** `_pos` is a running sum of `num_rows` in footer-list order (`ParquetValueReaders.PositionReader`). Dropping a row group shifts the positions of every later row. Pure RMoW never uses positions, so this only matters for a future hybrid with DVs and for v3 row lineage.
- **Append support per store:**

  | Store | Append mechanism | Limit |
  |---|---|---|
  | S3 Express | `PutObject` with `writeOffsetBytes` | 10k parts per object |
  | ADLS | `AppendBlock` on an Append Blob | 50k blocks; each block ≤ the maximum append size |
  | GCS, standard S3 | none | none |

  The fork's S3 append also does a HEAD first (`iceberg/aws/.../S3OutputFile.java:210`). A known-length append needs only the PUT.

## Project layout: `/home/chris/work/catalog/rmow-bench`

**Build and dependencies:**
- Maven, Java 17. Depends on the **released Apache Iceberg 1.12.0** (core, data, parquet, aws, azure) and parquet 1.17.1. hadoop-common is needed at runtime because `ParquetWriter` uses `Configuration`.
- The cloud appenders call the AWS and Azure SDKs directly, so others can reproduce the results without building the fork.
- A `-Pfork` profile swaps in `1.12.0-atomicio-SNAPSHOT` and adds `ForkAtomicAppender`, which uses `AtomicOutputFile` APPEND, as a cross-check.
- Distributed as a tarball of `lib/*.jar` (maven-dependency-plugin plus assembly), not a shaded jar. Shading breaks the merged `META-INF/services` entries.

Packages (`org.rmow.bench.*`):

| Package | Classes | Role / reuse |
|---|---|---|
| `workload` | `WorkloadSpec`, `OpLogGenerator`, `OpLog`, `KeyChooser` | Seeded op log, persisted with a SHA-256. Each row is a pure function of (key, version, ts), so payloads are never stored. |
| `oracle` | `ReferenceModel`, `TableHash` | Expected state for each version, plus an order-independent 64-bit hash with a row count. |
| `io` | `PreadFileIO` (local), `MemFileIO`, `TracingFileIO`, `S3Recorder`, `AzureRecorder` | Every stream implements `RangeReadable` (see the I/O parity rules). The tracing layer logs each operation's type, byte range, bytes and ns. The SDK-level recorders (AWS `ExecutionInterceptor` + `MetricPublisher`, Azure `HttpPipelinePolicy`) count real requests and aborted connections. |
| `rmow` | `OffsetOutputFile`, `RegionWriter`, `FooterCodec`, `VersionedInputFile`, `RmowFile`, `RmowTable`, `RgRewriter` | The append layer, described below. |
| `append` | `RegionAppender` with implementations `Mem`, `Local` (FileChannel), `S3Express` (known offset, no HEAD), `AzureAppendBlob`, and `ForkAtomic` (`-Pfork`) | One append per region together with its footer. Azure splits regions into blocks, writing the footer last. |
| `arms` | `MorV2Arm` (FILE, PARTITION), `MorV3Arm`, `TailCowArm`, `CowFullArm`, `RmowArm`; `MorCompactor` | The arms compared in the benchmark. |
| `policy` | `AppendCow` (A), `TrailingMerge` (B), `Tiered` (C) | RMoW merge policies. |
| `bench` | `Runner`, `StepRecord`, `RunManifest`, `Main` | Runs arms in lockstep and writes one JSONL record per step. |
| `micro` (JMH) | `FooterBench`, `DvMergeBench` | CPU micro-benchmarks. |
| `analysis/` | Python: `consolidate.py`, `plots.py`, `interop_check.py` | Follows the plotting style of `YCSB-data/ycsb-analysis`. |

### RMoW file layer

- **`OffsetOutputFile`** is an Iceberg `OutputFile` that buffers in memory. Its stream drops the first 4 bytes (the `PAR1` that `ParquetFileWriter.start()` writes) and reports `getPos() = baseLen − 4 + written`, so every offset the stock writer records is absolute in the target object.
- **`RegionWriter`** writes the region's rows through the same Iceberg path and writer properties the MoR arms use (`FormatModelRegistry.dataWriteBuilder` / `GenericFileWriterFactory`: same codec, page and dictionary settings), so the encoding is identical across arms.
  - After `close()` it parses the writer's own footer from the buffer tail (the last 8 bytes, then `org.apache.parquet.format.Util.readFileMetaData`) and cuts it off. The page indexes and bloom filters stay.
- **`FooterCodec`** builds the merged thrift `FileMetaData`:
  - `row_groups` = old live row groups, minus the replaced ones, plus the new ones.
  - New row groups go at the end of the list, so list order is ascending offset order. That keeps the PARQUET-2078 fallback path safe.
  - `num_rows` is the sum; `ordinal` is the list position; `created_by` and `iceberg.schema` are kept.
  - It adds the key-value entries `rmow.prev-length` (a back-pointer to the previous version) and `rmow.version`.
  - It serializes with `Util.writeFileMetaData`, then the 4-byte length and `PAR1`.
  - It rejects schema drift, and more than 32767 row groups (`ordinal` is an i16).
- **`VersionedInputFile(delegate, L)`** is a truncated view:
  - `getLength()` returns L, and `newStream()` hits EOF at L. EOF at L is required because `EagerInputFile` checks that its ≤1 MiB stream ends exactly at the length.
  - It implements `RangeReadable`, with `readFully` bounded to L and `readTail` re-implemented relative to L.
  - Optionally it serves `[footerStart, L)` from a cache. The same "warm footer" knob applies to all arms.
  - It never wraps `HadoopInputFile`, because `ParquetIO.file` stats the file and ignores L.
- **Reads** use the stock `Parquet.read(VersionedInputFile)` with `GenericParquetReaders`. An old version is read at an older L, and `rmow.prev-length` walks the version chain.
- **`RgRewriter`** reads only the affected row groups with `.split(rgStart, rgLen)` over the cached-footer view. It applies updates and deletes, merge-sorts by `ts` when sort-on-merge is on, and hands the rows to `RegionWriter`.
- **`RmowFile` and `RmowTable`** hold the committed length, the live row groups and the version chain. They roll over to a new object when any of these is reached:
  - the footer exceeds F bytes;
  - parts or blocks near the store limit;
  - the ordinal nears its limit;
  - garbage exceeds ratio g. Compaction rewrites the live row groups to a fresh file, and that cost is counted.

### Arms

Every arm starts from the **same initial-load bytes**. It is written once and copied to each arm, because a stock Parquet file is already a valid RMoW base version.

- **MoR-v2-FILE** and **MoR-v2-PARTITION** use `SortingPositionOnlyDeleteWriter` (`iceberg/core/.../deletes/`).
- **MoR-v3-DV** uses `BaseDVFileWriter`, with `loadPreviousDeletes` backed by `BaseDeleteLoader`. The read of the previous DV is counted.
  - MoR state is kept in memory as `DataFile` and `DeleteFile` objects for each step, with history kept for time travel. Scans build `BaseFileScanTask`s, prune files with `InclusiveMetricsEvaluator` (as manifests would), and read with `LengthAwareDeleteFilter extends GenericDeleteFilter`.
  - `LengthAwareDeleteFilter` overrides `DeleteFilter.loadInputFile(DeleteFile)` (`iceberg/data/.../DeleteFilter.java:164`) to call `io.newInputFile(deleteFile)`. Without it, each delete file costs an extra HEAD, a bias against MoR.
  - Template for the write path: `iceberg/data/src/test/.../FileHelpers.java`.
- **TailCoW-v3** is plain Iceberg: one small open "tail" data file is rewritten every batch until it reaches the target size, and sealed files get DVs. This **isolates layout from policy**: RMoW-B/C vs TailCoW shows what the single appendable object buys, beyond the tail-merge policy.
- **CoW-full** rewrites the whole file each batch. It is the reference point, run at small sizes only.
- **RMoW policies.** Updates to a sealed row group always rewrite that row group with its survivors plus the new versions.
  - **A, append-CoW:** each batch's inserts become a new row group.
  - **B, trailing merge:** the open tail is re-merged with each batch until it reaches size S. Write amplification is about S/(2b) for batch size b, so B runs only when S/b ≤ 8.
  - **C, tiered (primary):** append a small row group per batch, then merge the trailing K small row groups when there are K of them.
- **Compaction:** the same trigger families on both sides (MoR by delete ratio or file count, RMoW by garbage ratio or footer size). Compaction cost is folded into the amortized results.

### Workload

- **Schema:** `id` (monotone with insertion), `ts`, 4 longs, 2 doubles, and 2 dictionary-friendly strings, about 40 B per row compressed. The payload width is configurable.
- **Updates** are full-row upserts that keep the row's `ts` (event time).
- **Key age for updates and deletes:** recency-exponential (mean m batches), uniform, zipf, or a hot set.
- **Insert lateness:** lognormal.
- **Locating rows:** `oracle` by default; the oracle index cost is excluded for every arm. `scan` mode reads `id`, `ts` and `_pos` of the candidate files or row groups, pruned by stats, and is reported separately.

## I/O parity rules (assert from traces; each one guards against a bias found in code)

1. **Every stream is `RangeReadable`.** Iceberg sets `withUseHadoopVectoredIo(true)` (`Parquet.java:1555`), but parquet-java uses vectored reads only through `ParquetRangeReadableInputStreamAdapter` (`ParquetIO.java:104,171`). On that path each run of adjacent column chunks is one bounded ranged GET. Otherwise `S3InputStream` issues open-ended GETs and reads through gaps of ≤1 MiB, which would charge only RMoW for its dead bytes.
2. **No arm pays an extra HEAD.** That means delete files opened with their length, and S3 Express appends at a known offset.
3. **Footer reads cost the same in every arm.** It is 2 requests per open unless the warm-footer knob is on, and the knob applies to all arms. Aborted connections are counted, because an old-version footer read aborts the stream when the reader seeks.
4. **Delete-loader caching is off/on for all arms together.** It is a knob that matters for PARTITION granularity.
5. **Parallel reads at p=1 and p=8 for both layouts.** MoR parallelizes across files; RMoW parallelizes across splits, and each split re-reads the footer.
6. **Local latency is not reported as a result.** WSL2 and the page cache make it meaningless. Local runs report bytes, requests and CPU time; latency comes from cloud runs.

## Measurements

Each `StepRecord` (one per arm per step) holds:
- arm, store, repetition, seed, step and phase;
- phases: `locate`, `write`, `delete-write`, `compact`, `read_full`, `read_recent` (a 1% `ts` window), `read_old` (100 steps back);
- wall-clock ns and thread CPU ns;
- bytes read and written;
- requests by type (GET, PUT, HEAD, APPEND, and aborts);
- object count, live and total bytes (space amplification), footer bytes, and the number of row groups, data files and delete files.

## Reproducibility

- Each experiment has its own directory named `<label>-<config-hash>`. `manifest.json` records:
  - the git SHAs of rmow-bench (and of iceberg under `-Pfork`);
  - the SHA-256 of every resolved jar;
  - the JVM version and flags;
  - host, instance type and AZ (from IMDS);
  - store, bucket class and region;
  - the op-log SHA-256 and the seeds.
- Per-step JSONL → `results.parquet` → `consolidated.parquet`, following the endive conventions (`/home/chris/work/catalog/endive/`). Plots and companion `.md` tables are regenerated from the plotting config.
- Arms run in lockstep, and their order is shuffled at every step so that drift in the cloud hits all arms equally.
- A JIT warmup run on `MemFileIO`, plus W discarded steps. Each run uses a fresh prefix and cleans it up afterwards.
- Hypotheses H1–H4 are written to `docs/HYPOTHESES.md` before the first cloud run:
  - **H1:** with recency-skewed updates, RMoW-C stays flat on reads and close to MoR-v3 on writes.
  - **H2:** with uniform updates, RMoW approaches CoW.
  - **H3:** the gap between RMoW-C and TailCoW is the single-object effect.
  - **H4:** with bounded ranged reads, old-version reads cost the same as current reads, and dead bytes cost nothing.
- Cloud runs reuse `/home/chris/work/catalog/YCSB/catalog-bench/bin/bench.sh` with a new `rmow` mode: Terraform VMs in the same AZ as the S3 Express bucket, the tarball copied by scp, results fetched back. Credentials come from environment variables, following `fileio-catalog/.../IntegTestEnv.java`.

## Milestones

- **M0, spikes plus a local end-to-end run.**
  - **S1:** build a 2-region file by hand. Read it at both lengths with Iceberg, parquet-java, pyarrow and DuckDB (via prefix copies), and check `_pos`, the split midpoint filter, and stats/dictionary filtering.
  - **S2:** assert parity from traces: ranged GETs, HEAD counts, the ≤1 MiB `EagerInputFile` path, and `readTail` on a truncated view.
  - **S3:** JMH `FooterBench`, measuring footer bytes and parse/serialize time for 10–10k row groups × 10 and 50 columns, to set the rollover threshold F.
  - **Code:** the `workload`, `oracle`, `io` (Mem, Pread, Tracing) and `rmow` packages, policy A, MoR-v3, and the JSONL runner.
  - **Exit:** the `smoke` config passes the oracle on local and mem.
- **M1, all arms and policies.** MoR-v2 FILE and PARTITION, TailCoW, CoW-full, policies B and C, rollover and compaction, locate-by-scan, warm and cold writer state, reads R1–R3 at p=1 and p=8, and `interop_check.py`.
- **M2, reproducibility and analysis.** The manifest, experiment directories, shuffled order and warmup, the `analysis/` plots, the full local matrix, and `DvMergeBench`.
- **M3, S3 Express.**
  - **S4:** check that an append at the wrong offset fails with `InvalidWriteOffset`, and measure append latency from 64 KB to 32 MB and GET latency against the number of parts.
  - **Code:** the `S3Express` appender, `S3Recorder` (with the S3 analytics accelerator pinned off), and the `bench.sh rmow` mode.
  - **Runs:** standard S3 runs the MoR arms only.
- **M4, ADLS.**
  - **S5:** check the maximum append-block size for the SDK version in use, that `ADLSFileIO` can read Append Blobs on a hierarchical-namespace (HNS) account, and how reads behave over many blocks.
  - **Code:** a block-splitting appender that writes the footer last.
  - **Tests:** a torn-append recovery test, since readers trust only the committed length.
- **Deferred:**
  - a hybrid policy D (DVs on the sealed prefix);
  - equality deletes;
  - L2 Iceberg commits (a same-path `RewriteFiles`; v3 row lineage and `validateNoDVsForAddedDataFiles` make this awkward);
  - GCS compose and S3 multipart-copy emulation;
  - encryption (the ordinal is part of the AAD);
  - partitioning;
  - a proposal write-up in `parquet-format/proposals/`.

## Default experiment matrix

| Dimension | Values |
|---|---|
| Initial load | 5M rows (about 200 MB), 128 MiB files, 16 MiB row groups |
| Batches | 200 × 10k rows. The local sweep adds 1k and 100k rows, row-group targets of 4, 16 and 64 MiB, and K of 2, 4 and 8. |
| Mixes | I100; I90/U10; I70/U25/D5 |
| Key age | recency-exponential (mean 2 batches) and uniform; zipf locally only; lateness lognormal with p99 = 5 batches |
| Arms | MoR-v2-FILE, MoR-v3-DV, TailCoW-v3, RMoW-A, RMoW-B (only when S/b ≤ 8), RMoW-C (MoR-v2-PARTITION locally only) |
| Reads | every 20 batches: R1 at p=1 and p=8, R2, R3 |
| Compaction | on, plus one run with compaction off to show degradation |
| Stores | local (all arms); S3 Express (all arms); standard S3 (MoR arms); ADLS (all arms) in M4 |
| Repetitions | seed s0 × 3 repetitions, plus seeds s1 and s2 × 1 repetition |

The cloud budget gets estimated after the S4 spike measures per-step time.

**Plots:**
- Per-step latency, bytes and requests against batch index (median with an IQR band).
- Amortized write bytes per row (compaction included) against R1 latency, as a Pareto plot.
- Space amplification over time.
- Requests by type, stacked, per arm.
- Footer size against the number of row groups.
- Paired-difference CDFs over the same op log, with bootstrap 95% confidence intervals.
- A request-plus-bytes $ model per store.

## Verification

- **Unit tests (JUnit 5):**
  - `FooterCodec` round trip and invariants;
  - `OffsetOutputFile` produces absolute offsets (compare against the same rows written as a standalone file);
  - `VersionedInputFile` EOF and `readTail` at L, including files of 1 MiB or less;
  - an N-version RMoW file where every version reads, through both Iceberg and parquet-java, to the oracle hash and continuous `_pos`.
- **jqwik property:** for random op logs, every arm's `TableHash` at every step equals `ReferenceModel`, and every earlier RMoW version still reads correctly.
- **Interop:** `python analysis/interop_check.py <dir>` reads each version prefix (`head -c L`) with pyarrow and DuckDB.
- **Trace assertions:** the parity rules 1–4 are checked in `smoke`, and the request counts from `TracingFileIO` and the SDK recorders must agree.
- **End to end:**
  1. `mvn -q package && bin/run.sh configs/smoke.json` produces an experiment directory, `consolidated.parquet` and plots.
  2. The same command on S3 Express from an EC2 instance in the same AZ, via `bench.sh rmow`.

## Existing code to reuse (paths under /home/chris/work/catalog/iceberg)

**Writing and reading Parquet:**
- `parquet/.../Parquet.java` (`ReadBuilder` and `split`), `ParquetIO.java`, `ParquetWriter.java`, `ParquetUtil.footerMetrics` and `getSplitOffsets`
- `core/.../io/EagerInputFile.java`, `RangeReadable.java`, `IOUtil.readFully`
- `data/.../GenericFileWriterFactory.java`, `GenericReader.java`

**Deletes:**
- `data/.../GenericDeleteFilter.java`, `DeleteFilter.java`, `BaseDeleteLoader.java`
- `core/.../deletes/BaseDVFileWriter.java`, `SortingPositionOnlyDeleteWriter.java`, `PositionDeleteIndex.java`
- `data/src/test/.../FileHelpers.java` (template for writing deletes)
- `data/src/jmh/.../DeleteFilterBenchmark.java` (template for benchmarks)

**Cloud append and measurement:**
- `aws/.../S3OutputFile.java:210`, `azure/.../ADLSOutputFile.java:176` (reference for the append calls)
- `/home/chris/work/catalog/fileio-catalog/src/test/java/org/apache/iceberg/io/IntegTestEnv.java` (environment-variable conventions)
- `/home/chris/work/catalog/YCSB/catalog-bench/` (VM orchestration)
- `/home/chris/work/catalog/endive/docs/analysis/distributions.json` (measured store latencies, used to sanity-check results)
