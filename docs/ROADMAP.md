# Roadmap: outstanding work

What is left to do before the post, and what was decided along the way. The experiments map
([EXPERIMENTS.md](EXPERIMENTS.md)) says what has been run and found; this page says what comes
next. Keep it current: mark items done (with the commit) rather than deleting them, until a
write-up covers them.

The lab pages it names that no longer exist were retired with their runs on 2026-11-12: what
they taught is in [LESSONS.md](LESSONS.md), and the pages are in git at bfbdf33.

## The question, narrowed (decided 2026-10-26)

**The idea:** treat a Parquet region (a row group) as the unit of a write. That adds write
latency, to cut read latency. In view-maintenance terms (Olteanu), it moves cost from enumerating
the table to integrating each update. The analysis asks three things:
1. **Where the merged region goes, and what hides its old copy:** appended to its object, a new
   footer hiding the old (RMoW-C); a new data file and a new footer on the old file (RMoR-C with
   footers, added 2026-10-27); or a new data file and a DV (RMoR-C).
2. **How it compares with the baseline,** Iceberg's merge-on-read (MoR-v3).
3. **How it compares with the hybrid** TailCoW-C (MoR, with its tail files merged).

| Arm | The merged region goes | The old region is hidden by |
|---|---|---|
| `mor-v3` | nowhere: updated rows go to a new file | DVs on rows |
| `tailcow-c` | nowhere, outside the tail | DVs on rows |
| `rmow-c` | appended to the same object | the new footer |
| `rmor-c` | a new data file | a DV covering the old row group |
| `rmor-c-footer` | a new data file | a footer appended to its file, without the old row group |

The other arms stay in the code, since the published write-ups use their runs, but new campaigns
leave them out. `rmow-c-split` (a region in a new object, the old hidden by a footer-only object
naming the objects its row groups live in) was dropped on 2026-10-27: such a footer points across
objects through `ColumnChunk.file_path`, which the Parquet spec disowns and no reader assembles
(DESIGN.md, Split files). RMoR-C writes a rewritten region to a new file in a form every reader
reads.

**How results are reported:**
- **Measured, not modelled.** Cloud runs report each phase's wall time; local runs report
  requests, bytes and simulated time.
- **Requests, as well as latency and bytes,** on both sides of the trade-off: a store bills them,
  and they bound throughput. The objects a write creates track its PUTs.
- **Attributed.** Each phase and sub-phase is split into CPU and waiting, shown as stacked bars, so
  readers can see where their own improvements over Iceberg's Java code would cut. Parquet
  encoding, for example, is the CPU of RMoW's write.
- **Compared per workload.** There is no blended verdict; each store and workload gets its own
  comparison.
- **CPU precision is not the point.** Instruction counts stand in for CPU only in the noisy local
  runs; inner loops get JMH if they need it.

## Fork (iceberg, `atomicio`)

- [x] **Creates without a lookup** (1f7ea07f1a, on `atomicio` and published locally, 2026-10-26;
  approved by fileio-catalog). No new API: a snapshot that has seen no version
  assumes the object does not exist, as `OutputFile.create()` does. A snapshot has seen a version
  if a write returned it, if a lookup through it (`getLength()`, `exists()`) found one, or, on ADLS,
  if a read did.
  - **A CAS from an unseen snapshot** creates the object only if none exists, with no lookup:
    - S3: one PutObject with `If-None-Match: *`;
    - GCS: one write at generation 0;
    - ADLS: the staged Append Blob and a rename with `If-None-Match: *`.
  - **To replace an object it has not read,** a caller looks it up first. FileIOCatalog's log
    already does, because `getLength()` pins the version it reads.
  - **An APPEND from an unseen snapshot** looks the object up when it writes.
  - **Tests:** the fork's aws, azure and gcp suites pass. New mock tests per store check that a
    create sends no lookup. The new contract cases need the real services.
- [x] **ADLS creates stay one request above the store's own appender** (decided, kept): 3 requests against 2.
  An Append Blob is created empty, so only the staged rename makes a create all or nothing.
  This is decided: SAO keeps the atomic create.
- [x] **Run the fork's contract suites on the real services** (done 2026-10-28, at af875ccfd1, the
  twins' build). No emulator runs the fork's paths: versitygw ignores write offsets, and Azurite
  has no rename. All pass:
  - `S3ExpressFileIOAtomicTest` on the cloud VM in usw2-az3: 14 passed, and the opt-in
    segment-limit case skipped;
  - `AdlsFileIOAtomicTest` on the cloud VM in westus: 19 passed, 1 skipped;
  - `GcsFileIOAtomicTest` (a standard bucket) and `GcsFileIOAtomicRapidTest` (a Rapid zonal
    one), from here on catalog-bench's buckets in us-west4: 10 and 12 passed, each with the
    APPEND cases and the segment limit skipped, since the fork has no APPEND on GCS.
- [ ] **APPEND on GCS** (found 2026-10-28). The fork's GCSFileIO has no APPEND on any bucket, so
  RMoW cannot run on GCS. Rapid (zonal) buckets have appendable objects, the counterpart of S3
  Express's appends; implementing APPEND on them would come first, then a GCS store and recorder
  here, a spike, a twin, a deploy layer (catalog-bench's `gcp/` has one) and hypotheses.
- [ ] **An APPEND at length 0 is an APPEND at any length** (found 2026-10-26, for
  fileio-catalog). The fork's snapshots, like Iceberg's, take a known length of 0 for an unknown
  one (`length > 0 ? length : null` in `S3InputFile`, `ADLSInputFile` and `GCSInputFile`). So an
  APPEND from `newInputFile(location, 0)` looks the object up and extends it at whatever length it
  has, where the contract says it lands only at the snapshot's length. On Azurite an append "at 0"
  to a 12-byte blob landed at 12. RMoW never appends at 0. The bench's stand-in for Azurite's
  creates relies on it, which is safe there. The local stores keep the length. `S3ExpressTest`
  probed it on its first run on S3 Express (2026-10-28) and now probes 1, 9 and 11, as the Azure
  contract does.

## Harness (rmow-bench)

- [x] **Parquet as applications read and write it** (decided and built 2026-10-31, 6f24575). The
  S3 Express base point ran at a third of its time on the store; a profile put about 85% of a
  read's CPU in Iceberg's generic row reader, which builds a Java object per row, and the user
  asked that the arms use the APIs applications use, so that costs are attributed as they would
  be in one. Every arm now reads through Iceberg's vectorized reader (iceberg-arrow, as Spark
  reads an Iceberg table): batches of 5,000 rows, deletes by position, and the residual compiled
  to comparisons on the batch's columns, as Spark compiles a filter. The writer stays Iceberg's
  generic writer over parquet-java, which Spark's and Flink's are too, with strings as UTF-8
  bytes as Spark's rows hold them. On the base
  point (laptop, V1–V3): no request, byte or held peak changed; a full read takes 0.26–0.28 of
  its instructions, and the run 40 minutes instead of 2 h 6 min. At S3 Express prices RMoW-C's
  batch went from 3.0 to 4.5 times MoR-v3's, whose batch was mostly decoding, and its full read
  stayed 0.87 of MoR-v3's, now about 70% the store's time. Engines with Parquet code of their own
  (Trino, DuckDB, Photon) may decode and encode faster still
  (lab/pages/vectorized-reader.md).
- [x] **Writers stream** (found and built 2026-10-29). Writers held every row a batch rewrote or
  merged as an Iceberg record (hundreds of bytes each) until the write, and built every file's rows
  as records before encoding: harmless at 1–4 MiB row groups, but 14 GB of live heap by batch 2 of
  the 16 MiB base point. Now RMoW reads, rewrites and encodes one row group at a time, as parts of
  `merge-read` and `write` (`Phases.part`, recorded once each), and the MoR family keeps each row
  it reads back as its id and version and makes the rows again as the encoder takes them, as it
  always made them (a `rows` span keeps that work out of `encode`). Requests, bytes, rows and table
  state are unchanged in every record of three configurations (1,939 records), and instructions
  moved by at most 3%. The base point runs in an 8 GB heap (7 batches, no full collection), so the
  sweep's jobs take 16 GB.
- [x] **What readers and writers hold** (`Held`, built 2026-10-29): the most each phase held at
  once, in rows kept to sort or merge and in encoded bytes (column chunks in flight, writers'
  buffers, encoded data waiting for its request, deletes loaded), counted where the code holds
  them, so they repeat from run to run as requests and bytes do (README, What is held). The JVM's
  heap was rejected as the measure (the user's question): it holds the benchmark's own state and
  moves with the collector, and allocation counters (`ThreadMXBean`, async-profiler) count churn,
  not what is held. `--log-held` writes every change, for memory against the stages of a phase.
  A JFR run is still to check that the heap follows the counted levels' shape.
- [x] **A sorted read** (`read_sorted`, built 2026-10-29, the user's): the table in event-time
  order, as a merge of its row groups opened as the merge reaches them, so it holds every row group
  whose event times overlap. It needs every run sorted, so sorted writes and an initial load in
  event-time order (`initialJitterRows` 0, set in the sweeps). On the calibration's workload with
  region statements, RMoW-C's sorted read held 0.33 MB of row groups at the start and 1.21 MB by
  batch 20 (its tail row groups take late inserts, so their event times reach back across older
  regions), while MoR-v3's small batch files were fetched whole and sorted in memory (36,328 rows
  held by batch 20).

- [x] **`AzureRecorder` counted the managed identity's token requests** (found and fixed
  2026-10-28). Its policies join every Azure SDK pipeline in the JVM, Azure Identity's too, so on a
  VM the token request to the instance metadata service (a GET with `resource=` in its query)
  counted as a LIST, with its 2.5 KB response as GET bytes. `AzureTest`'s first Iceberg write on
  Azure counted HEAD, PUT and that LIST, and failed. No calibration phase held one (every phase's
  `sdk_*` counts equal the Azurite twin's but for the creates). The recorder now counts only
  requests to the store's hosts: the root's account's Data Lake and Blob endpoints, or an
  emulator's (`AzureRecorderTest`; the Azurite contract still passes).

- [x] **Unify on SAO** (decided and done 2026-10-26). One appender,
  `AtomicAppender`, over the fork's `SupportsAtomicOperations`, for every store:
  - the fork's build is the only one: no `-Pfork` profile, no `src/fork`, no `target-fork/`;
  - `S3Appenders`, `AzureAppenders`, `ExactAdlsFileIO`, `store.append` and `store.adlsReads` are
    gone. S3 appends only in a directory bucket, decided by the bucket's name;
  - `MemFileIO` and `PreadFileIO` implement the API, and pass the fork's own contract
    (`LocalAtomicContractTest`), so local runs take the same path;
  - emulators run RMoW's appendable arms on stand-ins (`EmulatorAppenders`): versitygw ignores
    write offsets, so appends rewrite the object, unmeasured; Azurite cannot rename, so a create
    there is an empty Append Blob, a HEAD and an Append Block;
  - `azure-calibration-{exact,iceberg}` became `azure-calibration`, with one Azurite twin.

  The baseline now runs on Iceberg 1.12 plus the fork's fixes. A run of Iceberg's ADLS as shipped
  is optional, from an older build.
  - [x] **Request for request, locally:** the old build and this one sent the same requests and
    bytes on 7 arms. The one difference is 18 bytes per DV file, from Puffin's `created-by`
    (EXPERIMENTS.md, Builds). versitygw and Azurite match the model in every phase, with the
    stand-ins' counts.
  - [x] **Against the stores' own writes, in the cloud** (measured in the spikes, not judged): the spikes now time the store's own
    appends and creates beside RMoW's, interleaved, measured but not judged (HYPOTHESES.md).
    That leaves nothing native in the runs, only in the spikes.
- [x] **Where time goes, measured** (done 2026-10-26). Each phase splits its
  thread's time by kind of work (`Spans`), as wall and CPU time, each exclusive of the others:
  `io` (every call into the store), `encode` (Parquet encoding), `decode` (Parquet decoding in
  scans, deletes applied), `footer` (RMoW's footers), `deletes` (delete files written and loaded).
  A read pool's tasks are counted the same way, apart. In the request log, each request has its
  start, wall time and CPU. The lab's `phase_breakdown` view turns a phase into components that add
  up to its wall time: each kind's CPU, other CPU, the store client's CPU, waiting on the store,
  and other waiting.
  - Writes into an output stream count as encoding: Thrift writes a footer in many small writes,
    which would cost 2 µs each to time. The PUT is the stream's close.
  - The store client's own threads (Azure's event loop) are in `process_cpu_ns`, not `io`.
- [x] **Stacked bars of where time goes** (superseded 2026-11-16: workloads-128.md, sections 4
  and 9, splits each write and read into decode, encode, DVs and footers, other CPU and I/O on both
  stores, and section 4 the write's scan from what it copies; process CPU beside was dropped): a lab page with phase and sub-phase (locate,
  merge-read, write, delete-write; each read) split into CPU and waiting, with process CPU shown
  beside, for the five arms. The view is done (`phase_breakdown`). Since 2026-11-02 the results
  pages (base-point and cloud-sweep, sections 2 and 3) split each write and full read into
  decoding, encoding, the store and the rest, at every setting on both stores, and a write's steps
  likewise. Still to do: process CPU beside, and the reads other than the full read.
  - On the Azurite twin, process CPU exceeds thread CPU by 89–112 ms a phase: garbage
    collection, the JIT and the SDK's I/O threads. So "waiting" includes off-thread work.
  - Build it on the twins now, so the cloud results drop in.
- [x] **GETs by size in the spikes** (done 2026-10-27): `get_size`, the PUTs' single-part objects
  of 64 KiB to 32 MiB read whole, and `get_range`, the same sizes as ranges of a 32 MiB one, on
  both stores, interleaved over sizes; checked on versitygw and Azurite. The read rate per MiB is
  assumed, and it is the biggest I/O term in S3 Express full reads.
- [x] **Drop the instruction-counter advice from the cloud runbooks** (done 2026-10-27). The cloud
  runs measure wall time; they need the clocks to agree (`host.thread_cpu_per_wall`).
- [x] **The spikes reach 128 MiB** (done 2026-10-27), a row group at Iceberg's default: at
  128 MiB row groups RMoW-C's appends are 98 MiB at the median (137 MiB at most) and full reads'
  GETs reach 127 MiB, past the 32 MiB the spikes stopped at. On Azure a 128 MiB append is two
  Append Blocks, the store's own and RMoW's. GETs drain their bodies rather than copy them, and
  the S3 spike's own writes stream their bodies as the fork's do. Checked on versitygw and
  Azurite. The calibration runs keep the registered build; the runbooks build the spike from main
  beside it.
- [x] **Split appends counted** (done 2026-10-27). The fork splits an append, or an appendable
  create's staging, larger than Azure's 100 MiB Append Block into several; the model counted one.
  Phases record the extra ones (`append_splits`), and the `azure` profiles price each as an append
  (`max_append_mib`, in `phase_latency`, `figures.replay` and injected latency). Older runs take
  them from their request logs, appends only. Only the 128 MiB scale runs have any (0.65 per
  RMoW-C batch, 59 ms on a 13.7 s batch on Azure); no number on any page moved.
- [ ] **Iceberg's own writes above 256 MiB on Azure** (now live: MoR (compacted)'s compactions at
  128 MiB write files up to 512 MiB, priced as one PUT on workloads-128.md's Azure Blob): an output stream is one Put Blob up to
  256 MiB (README, The Azure store). A larger one is presumably several requests, the SDK's
  chunked upload, not yet counted; the model counts one PUT. **Counted on the Azure base point**
  (2026-11-01): RMoR-C's write files (418 MiB on average) went up as 85 Put Blocks a write on
  average, 4 MiB each, and 0.6 Put Block Lists; the write's time was priced right (0.98), but a
  16 MiB range from such a block blob takes 228 ms at the median against 140 ms from a blob written
  in one request, Append Blobs included, so RMoR-C's full reads took 1.44 times their price. The
  spikes should read a block blob too, and `adls.write.block-size-bytes` could be tried. No run so far writes one after its
  initial load, but compaction at 128 MiB row groups (512 MiB files) would. Count what the SDK
  sends for such a file on Azurite, then model it as the splits are.
- [x] **The cloud runs deploy from here** (done 2026-10-28; [deploy/](../deploy/README.md), after
  shield-rs's). Terraform makes the store, the VM's credentials and the VM on each cloud: a
  directory bucket and an EC2 instance in its zone, or an ADLS Gen2 account and a VM with a
  managed identity. `deploy.sh` ships rmow-bench and runs the runbooks' steps on the VM (builds,
  contract suites, spike, calibration), detached, and fetches the results. The fork's suites get
  the role's temporary keys, or a SAS signed here for 8 hours. The runbooks' fork build was wrong:
  the fork on GitHub has no release tag, without which the build does not version itself
  1.12.0-atomicio-SNAPSHOT. They now fetch the tag from Apache, as the deployment does. Checked
  offline and with a read-only plan of the AWS infra layer, and the VM's build step in an Ubuntu
  24.04 container: the local twin's configuration on that build sent the registered twin's
  requests and bytes (0 of 862 records differ). Applied on AWS (2026-10-28) in us-west-2, as
  catalog-bench's runs were, on catalog-bench's directory bucket and instance profile: `casuser`
  may neither create IAM roles nor open sessions on a bucket it makes. The fork's S3 Express
  contract passed there; rmow-bench's `S3ExpressTest` failed on two faults of its own, since fixed
  (an append at 0, which the fork takes for an unknown length, and a client its throwaway
  S3FileIOs closed when finalized).
- [x] **A fuzzer** (built 2026-11-08, before the cloud run of the tuner's picks; README,
  Fuzzing). `bin/fuzz.sh` draws a whole configuration from a seed (the workload, every layout
  knob, a subset of the 18 arms, readers of random flags, the store) and runs it through the
  runner, whose every read is checked against the reference model, payloads included. It checks
  besides that each arm's objects and bytes on a local store are what its `state` records say,
  that a share of seeds give the same records on the other store, and that every dumped version
  reads as the model. Tables are small (2,000 to 60,000 rows). Four rounds on havoc, 16 workers
  each:
  - round 1 (2673c7d): 725 seeds, 14 failed: RMoR's cleaner above a share of 1 put no DVs over
    the rows it carried (fixed in 1721299), and the fuzzer's own warmup outran its rows;
  - round 2 (1721299): 3,597 seeds, 2 failed (450 and 1592): a tail file whose every row died
    stayed among the tail policy's open units, so its next merge read it back whole and wrote
    its rows twice (fixed in f64508b);
  - round 3 (0572d48, draws that reach nearly dead row groups and packable tails): 2,700 seeds,
    none failed;
  - round 4 (c2e1e66, two hours): 6,721 seeds of the default draws and 3,657 of the `heavy`
    profile, none wrong; one (5727: 11 arms, 17 KB row groups, run on both stores) passed the
    20-minute limit, as the slowest passing seeds nearly do (19 minutes), and passes on the
    laptop in 7.7 minutes.

  Neither bug touched a run: no configuration ran the cleaner above a share of 1, and none held
  DVs on open tail files (RMoR above a share of 0, the deferred merges or the cleaner, on a
  workload that inserts). The four cloud picks reach neither. Each fix has a regression test.
  **Calibration** (`bin/fuzz-mutants.py`, 10 planted bugs, 60 seeds each on the laptop and on
  both profiles): the 8 it requires are caught by the check each names (a row group dead but for
  interior rows by the heavy profile only: 5 of 60 seeds, and 39 of 520 on havoc), and the code
  unmutated stays clean. Two are rarer: RMoW's packing taking row groups the tail policy holds
  open, 1 seed in 502 on havoc (no cloud pick compacts RMoW); and a row group dead but for its
  second row, in none of 461 heavy seeds on havoc. The cloud stores' clients are not fuzzed;
  their contract suites and the spikes cover them.

## Arms

- [x] **RMoR-C** (`rmor-c`, built 2026-10-27). It is `TailCowArm` with a rewrite share
  θ (`layout.rmorRewriteShare`, default 0, counted per batch, decided 2026-10-27). Inserts are
  TailCoW-C's. A batch rewrites a region it changes, outside the files it merges, when it updates
  or deletes at least θ of the region's live rows:
  - **a sealed file's row group:** its rows, with the batch's changes, go into the batch's new
    data file with its inserts, which the tail policy merges as it merges any, and a DV covers the
    old row group;
  - **an open tail file,** the tail policy's unit: rewritten whole as a new data file, which takes
    its place in the tail, as RMoW-C's open row groups do;
  - **a file with no live row left** leaves the table rather than take a DV covering it.

  Below θ the rows get DVs. θ = 0 rewrites every region a batch changes. Above 1 it is TailCoW-C,
  request for request (`RmorTest`), not MoR-v3 as proposed. The format is unchanged, and no store
  needs to append.
  - The writer reads the footer of each sealed file of several row groups it changes, once per
    batch. A file of at most 1 MiB is fetched whole to plan, and its row groups read from what was
    fetched.
  - A first version put each batch's rewritten row groups in a regions file of their own. Those
    small, hot, multi-row-group files were fetched whole up to three times a batch (50 of 66 MB
    read in 20 batches). The batch's data file holds them now.
  - **On the calibration's workload, in memory** (scratch run, 2026-10-27): a batch takes 16.2
    GETs, 1.4 PUTs, 1.6 MB read and 1.0 MB written. RMoW-C takes 6.3 GETs, 1 append, 0.8 MB and
    0.8 MB; TailCoW-C 12.2 GETs, 2.0 PUTs, 0.6 MB and 0.1 MB. A full read (`cached`) takes 33
    GETs and 9.0 MB, and 26 GETs and 7.0 MB with `cached-skip`: TailCoW-C's bytes, in 2 GETs
    fewer.
  - **A writer that skips dead row groups** (`layout.writerSkipsDeadRowGroups`, built
    2026-10-27, off by default). The MoR family's writers (locate scans, merge reads, compaction)
    then read no row group the file's deletes cover whole, as the `skipDeadRowGroups` reader
    does. With it, RMoR-C's `locate` takes 9.2 GETs a batch instead of 13.5 (TailCoW-C: 8.7), and
    a batch 11.9 GETs instead of 16.2 (TailCoW-C: 12.2), writing the same. It changed no other
    arm's request or byte, on the calibration's workload and on a 7-arm configuration. It stays
    off by default, Iceberg as shipped (decided 2026-10-26); configurations that report RMoR set it,
    with the `cached-skip` reader beside a stock one. None exists yet.
- [x] **RMoR-C with footers** (`rmor-c-footer`, built 2026-10-27). RMoR-C's rewrites at θ = 0,
  each old row group hidden by a footer appended to its file, listing its other row groups,
  instead of a DV. Against RMoR-C it changes only what hides the old copy; against RMoW-C only
  where the rewritten region goes. Its data files are created as objects the store can extend (on
  ADLS an Append Blob, three requests where Iceberg's Put Blob is one), and each append is a new
  version of the file, with its length, split offsets and metrics, as RMoW's are. It writes no
  delete file, so no reader has a dead row group to fetch or skip. Rows after an elided row group
  move up in their file, which would leave a DV on its other rows stale, so θ must be 0; a θ sweep
  would need its DVs rewritten as row groups leave. Compaction's delete-ratio trigger sees no dead
  rows in it (its dead bytes are old row groups, as an appendable RMoW file's are), so compaction
  never rewrites it for space. `RmorTest` checks it writes RMoR-C's data files, appends where
  RMoR-C writes DV files, and reads nothing a skipping reader would skip.
- [x] **A reader that skips row groups a DV covers whole** (`reads.readers[].skipDeadRowGroups`,
  the `cached-skip` model; built 2026-10-27). Before planning a file's splits, it
  checks the file's deletes, loaded once (through the read's delete cache, so its tasks reuse
  them), against the row groups of the footer it reads. It plans no split for a dead row group,
  and none for a file whose DV covers every row. It needs `footerCache`. A file of at most 1 MiB,
  fetched whole, is skipped only whole. Iceberg's reader (`ReadConf`) fetches dead row groups
  and drops every row, so RMoR reports under both. Which engines skip them is not checked.
  - [x] **The check by range** (done 2026-11-02). It looked up each position of a row group in
    the deletion vector, about 514,000 lookups a 16 MiB row group, which cost RMoR about 1.8 s of
    CPU per full read and 1.3 s per write (in its locate scan) in the cloud runs at 6f24575, the
    point of RMoR being a check per row group. Measured, the laptop's runs under both builds put it
    at 2.2 s a full read and 2.1 s a write at the base point, at the S3 Express
    VM's pace, and up to 2.3 s and 2.5 s across the sweeps' settings (docs/HYPOTHESES.md,
    2026-11-02). `DeletedRanges` now looks up a row group's two ends,
    then asks the vector's Roaring bitmaps (read back through the index's `serialize()`, Iceberg's
    index answering only per position) whether the whole range is deleted: 0.2 ms a file of 16
    row groups, half of them dead, against 54 ms. It decides the same row groups
    (`DeletedRangesTest`), so no request or byte changes.
  - [x] **RMoR re-measured with it** (done 2026-11-04)**:** every cloud run of RMoR (the base point and the sweeps, on
    both stores) charged it the per-position check; the results pages call RMoR's latencies an
    upper bound until it runs again. Registered 2026-11-02 (docs/HYPOTHESES.md, RMoR with the
    check by range, in the cloud; R1–R3): `configs/rmor-*.json`, MoR and RMoR alone at each of
    the sweeps' eleven settings, with the laptop's runs at 505ba07 as their reference (campaign
    `rmor-rerun`). Both stores done 2026-11-04 (lab/pages/rmor-rerun.md): R1 and R3 held on both.
    R2 is contradicted on S3 Express at p = 0.1 and a skew of 2, where RMoR's skipping read is
    0.91–0.92 of MoR's, not below 0.90 (0.88–0.89 at the other eight settings; the check cost less
    than predicted), and on Azure Blob at p = 0.1 (1.09, not above 1.1), where the time the read
    waits on the store fell 1.2 s from the earlier run. The results pages (base-point,
    cloud-sweep) take RMoR's times from these runs, each over the MoR of its own run; the model's
    pages keep the earlier runs they price.
- [x] **RMoR and RMoW-external with their row groups kept whole** (found 2026-11-04; built
  2026-11-04 as `rmor-c-kept` and `rmor-c-footer-kept`, which every standing run of RMoR and
  RMoW-external uses; docs/LESSONS.md). Their
  writer puts the rows of every row group a write rewrites in one list with the write's inserts,
  sorts it by event time and cuts it into new row groups. So late inserts land among old
  regions' rows, and a later update of a new region rewrites most of the recent files' row groups
  for a few rows each. At the base point that made 396–771 MB writes, against RMoW-append's
  161–292 MB. Arms `rmor-c-kept` and `rmor-c-footer-kept` will write each rewritten row group as
  one of its own, and the inserts apart, as RMoW-append does. Registered 2026-11-04
  (docs/HYPOTHESES.md, W1–W3): four laptop runs, `configs/kept-*.json`. Not built.
- [x] **What an update reads** (`layout.updateRead`, built 2026-11-05; found 2026-11-05). The
  MoR family's new versions were made from an id and a version (`MadeRows`), so MoR, TailCoW and
  RMoR below θ never read the old row's other columns: MoR-v3's locate read 36.7 MB a write at
  the base point (the `id` column, 5.9% of a row group), against RMoW-append's 224 MB merge read.
  A real update reads every column of the rows it rewrites. `row` makes every arm read whole each
  row group holding a row it updates, once (phase `update-read`, after the locate, for the row
  groups it does not rewrite); `scan` makes MoR read every column in its locate scan, as Spark's
  MERGE on a merge-on-read table does. Absent (the default), as before, so earlier
  configurations keep their hashes. Every campaign before it under-charges the MoR family's
  writes. The cloud runs are not run again (the user, 2026-11-06): MoR alone reruns on havoc at
  the eleven settings of RMoR's rerun, to price them (`configs/mor-row-*.json`, campaign
  `docker/campaigns/mor-row.json`). `DensityTest`.
- [x] **RMoR with deferred merges** (`rmor-density`, built 2026-11-05, `layout.defer`). RMoR with
  row groups kept, merging a region's row group once its dead rows and the write's changes reach
  θ of its rows, folding in the region's rows that wait in delta row groups; below θ, DVs, the new
  versions waiting in a delta row group per region (or per write). A region is numbered when first
  needed and keeps its number through merges, and a row waits for the region of the row group it
  left. A first version grouped the waiting rows by the row group they left; in the model that
  fragmented without bound (1,559 row groups after 30 writes at θ = 0.06), since rows updated
  again leave many small deltas. No cleaner; delta row groups never merge on their own.
  `DensityTest`. Registered with the split campaign (HYPOTHESES.md, 2026-11-06).

## Workload generator

Until 2026-10-27, all runs used recency keys with a mean of 2 batches, or uniform keys, and each
update and delete touched one row. The generator now has regions, a skew over them and an update
share (README, The workload). Logs without them are unchanged: `OpLogGeneratorTest` pins the
hashes of the calibration's log and of the 1M-row recency and uniform logs.
- [x] **Regions** (`workload.regionRows`, built 2026-10-27): consecutive ids, default 100,000,
  about the rows of a 4 MiB row group (98,000; a 1 MiB one holds 10,521). Rows are the steadier
  unit: a row group's bytes depend on the writer's buffered count and the codec. A region need not
  line up with a row group: the initial load's vary by a few percent.
- [x] **Skew over regions** (`keyDist: REGIONS`, `regionSkew`, built 2026-10-27). Each region's
  heat is `U^-s`, drawn once from the seed and the region, so regions ranked by heat follow
  Zipf's law with exponent s. A row is picked by region, with weight heat times live rows, then
  uniformly in it. Hot regions can be anywhere and stay hot; s = 0 is uniform over live rows. The
  ingest window stays `RECENCY_EXP`. Uniform itself is plain copy-on-write for the region arms and
  is not run; the sweep trends toward it until region merging is plainly not worth it, and finds
  out why.
- [x] **Update share** (`updateShare`, built 2026-10-27): above 0, updates come as statements,
  each taking the region of a row the key distribution picks (any of them, the ingest window
  included) and updating a share p of its live rows. Rows updated per batch stay fixed, so a
  statement is capped by what is left of the batch's updates, and p counts only up to updates per
  batch over `regionRows`. Deletes stay one row each. **Prediction (the user's):** where p is high,
  a region rewrite pays, and RMoW more than RMoR; at one row per region it does not; a crossover
  lies between. RMoR-C's θ (`rmorRewriteShare`, a share of a region's live rows per batch) is the
  knob to sweep against p.
  - [x] **Deletes join the statements** (decided and built 2026-10-29). Single-row deletes
    scattered over the table touched about 100 of 135 regions a batch at a skew of 1, whatever p
    was, so a batch's rewrites did not follow p. Now a statement changes p of a region's live rows,
    and the batch's deletes are drawn uniformly from the rows its statements changed, the rest
    updated, as a MERGE does. The rows updated and deleted per batch are unchanged, and p counts
    up to updates plus deletes over `regionRows`. Logs at an update share of 0 are unchanged (the
    pinned hashes hold); no run had used the update share.
  - [x] **θ against p, after the sweeps** (superseded: the sweeps were retired on 2026-11-12, and
    where θ can choose is step 2 of docs/drafts/density-plan-2026-11-16.md). RMoR-C's θ is a hybrid, choosing
    per region between DVs and a rewrite, and the story compares the alternatives before
    introducing one. The sweeps run θ = 0 (always rewrite), and TailCoW-C is the other end
    (never), so where RMoR-C's batch crosses TailCoW-C's along p, on each store's profile, is where
    θ belongs; the θ stage starts there.
- [x] **Arrival window** (done 2026-11-08: the windows campaign's lateness twins at p99 1, 5 and
  20; docs/LESSONS.md, The 16 MiB workloads): sweep insert lateness (`lateMedianBatches`, `lateP99Batches`). It
  exists but has never been swept; the sweeps take a p99 of 1 and 20 batches against the base's 5.

**The base point leaves the skew and the update share no room** (found 2026-10-27, from the
generator alone). At 1M rows, 60 batches of 10,000 operations and 4 MiB row groups, the table has
15 regions of 100,000 rows by the end, and 2,500 single-row updates a batch fall in 12–13 of them
at any skew from 0 to 2. A statement holds at most 2,500 rows, 2.5% of a region. With regions of
10,521 rows (1 MiB row groups) there are 135: single-row updates touch 115 a batch at a skew of 0
or 1 and 73 at 2, and p can reach 24%. To decide before the sweeps: more rows, smaller row
groups, or fewer updates a batch, so that a batch touches a minority of regions and p can
approach 1.

**The base point (decided 2026-10-29):** 16 MiB row groups, regions of a row group's rows
(513,700, measured), 50 regions in the initial load (25,685,000 rows) and 71 by the end, 30
batches of 500,000 operations (70/25/5), a region skew of 1, and statements at p = 0.03; files of
128 MiB (eight row groups, the calibration's ratio), the calibration's readers, no compaction.
- **16 MiB, not 1 MiB.** A first choice the same day took 1 MiB row groups, the only way to give
  p room at 1M rows. The user rejected it: 1 MiB is far from any real Iceberg table (whose
  default is 128 MiB), and 16 MiB is a defensible fine grain for RMo*, the fetch size Durner et
  al. find cost-optimal on object storage
  ([PVLDB 16, 2023](https://vldb.org/pvldb/vol16/p2769-durner.pdf)). The ratios that gave the
  axes room at 1 MiB were kept, scaled to 16 MiB's rows; the generator's limits are in rows, the
  experiment's size in row groups times their bytes. So the sweeps run on havoc.
- **A batch changes a minority of the regions** at every setting: where most regions change, the
  region arms are copy-on-write, which is known and not worth measuring (the user's call). By the
  generator (seed 1, batches 16–30): p of 0.03, 0.1 and 0.3 change 8.4, 3.6 and 1.4 regions a
  batch, of 71 (p = 0.01 would change 30, so it is out); skews of 0, 1 and 2 change 9.7, 8.4 and
  5.8; the ingest window 4.4, and 16.8 with a row a change. p's ceiling is 0.29 (150,000 changes
  over 513,700 rows); if the sweeps find no crossover below it, more runs follow.
- **The row-group axis** (16, 32 and 64 MiB) keeps the same log, so a batch changes 13%, 32% and
  47% of the row groups at the base's p; 128 MiB would change most of them.
- **The schema stays narrow** (11 columns, the user's call): a width knob would be more to explain,
  and other schemas extrapolate by bytes.

Neither more rows nor fewer updates was the answer to the room the old base point left: p's
ceiling is changes per batch over rows per region, which more rows leave alone and fewer updates
lower.

## Experiments

- [x] **Sweeps, one axis at a time,** from the base point (decided 2026-10-29, above: 16 MiB row
  groups, 50 regions growing to 71, 30 batches, a region skew of 1, statements at p = 0.03):
  update share, skew, arrival window and row-group size, for the five arms, with the ingest
  window as a reference. Requests and bytes are exact, so the simulation finds each crossover.
  Eleven configurations (`configs/sweep-*.json`), too large for the laptop: the campaign
  `docker/campaigns/sweep.json` runs them on havoc, as the user's runs, in 16 GB heaps. Each also
  reads the table sorted under the cached reader, and logs what every phase held (decided
  2026-10-29). A first start on havoc was stopped the same day, before the writers streamed and
  memory was measured; it needs the image rebuilt from the commit that has them.

  **Havoc's campaign finished** (a5e2ee2, 2026-10-30, campaign `sweep`), every job under foreign
  load: 27 of 1,650 batches and 1 of 330 full reads were clean, so its CPU decides nothing. Its
  requests, bytes and held peaks are exact (its `sweep-base` equals the laptop's in all 1,325
  records, but for the 8-thread reader's held peaks, which move with how its tasks interleave).
  **The sweep moves to the cloud** (decided 2026-10-30): the SKUs it will run on are where the
  experiments' claims hold, a VM's cores are its own, and wall time there is measured, not
  simulated. Havoc's runs stay its twins, for the counts, which the vectorized reader leaves
  unchanged (V1). The base point runs first, on one VM (Cloud campaign, below); the others follow
  as a fleet, one VM each, on the vectorized reader (held for its S3 Express rerun, decided
  2026-10-31: a sweep on the old reader would have to run again).

  **The sweeps ran in the cloud** (6f24575, 2026-11-01, campaign `cloud-sweep`; run by a session at
  the user's request). Each configuration ran first as a twin with a local store on its S3 Express
  VM (the deploy's `twin` step), from which the predictions were registered (HYPOTHESES.md, The
  sweeps in the cloud), then on S3 Express (nine VMs at once) and on Azure (one VM, in turn over
  20.5 h). Every twin sent what havoc's run counts, and every cloud run what its twin counts (S1).
  U1 and G1 held; U2 held as revised, its second half as registered contradicted. No region design's
  write catches MoR-v3's at any setting (RMoW-C's 2.1–8.2 times on S3 Express, 1.7–5.1 on Azure), so
  none crosses along p by 0.3; but at p = 0.3 RMoW-C repays its write after 1.2 full reads per write
  on S3 Express and 0.9 on Azure, against about six at the base point. RMoR-C's write crosses
  TailCoW-C's nowhere either (3.2–11.2 times MoR-v3's on S3 Express, 2.7–7.5 on Azure), so the θ
  stage (θ against p, below) has no crossover to start from: where it starts is open. On Azure,
  bigger row groups make RMoW-C's full read faster (0.71 of MoR-v3's at 64 MiB: fewer, larger GETs),
  and RMoR-C's block-uploaded write files read slowly
  (lab/pages/cloud-sweep.md; the model and the verdicts in
  lab/pages/cloud-sweep-model.md).
- [x] **The two workloads apart** (decided 2026-11-05; HYPOTHESES.md, 2026-11-06; ran 2026-11-06/07,
  campaigns `split` and `seal`, retired 2026-11-12, whose verdicts the `windows` runs carry). Updates
  only (the base point's 150,000 changes a write, clustered by region, no inserts) and inserts
  only (its 350,000 a write, sorted by event time, lateness p99 1, 5 and 20 writes), with
  `updateRead` `row`: `configs/upd-*.json` and `configs/ing-*.json`, campaign
  `docker/campaigns/split.json` on havoc (the user's runs). The ingest arms append alike at the
  base point (a write's inserts are sealed at once, so no tail merge happens); a writer that
  merges late rows into their row group is not built.
- [x] **Inserts merged by the writer** (hypothesis (b), open 2026-11-06; both built and run: the
  tiered seal judged by J1–J3, the event-time merge in the 128 MiB suite). Two ways:
  - **tiered, by arrival:** a tail seal above the row-group size (`tailSealBytes`, 16 MiB in
    every configuration so far, tied to the row-group size), so tail policy C merges consecutive
    writes' inserts into longer runs sorted by event time. A configuration, no code: registered
    2026-11-06 (HYPOTHESES.md, J1-J3), `configs/ing-seal160*.json`, campaign
    `docker/campaigns/seal.json` on havoc. RMoR's writer cuts a merged file into row groups;
    RMoW-append's merged unit is one region, so one row group, and its tail file rolls over at
    the target file size, sealing its units at about 38 MiB; the model says that spares no sorted
    read. Cutting RMoW's tail units into row groups is not built. Judged 2026-11-07: J3 held,
    J1 and J2 in part. Inserts take 33.7 bytes a row, so no 16-write unit formed under 160 MiB;
    RMoR's 4-write units hold 0.41 of the appending arms' share at p99 5 and 20, RMoW-append's
    all of it. A larger seal (or fan-out) would reach the top level;
  - **leveled, by event time:** a writer that merges each late row into the row group its event
    time falls in, so no two row groups overlap and a sorted read concatenates. Built 2026-11-12
    (`layout.inserts` `event-time`, `EventTimeRegions`) for RMoW-append, RMoR, RMoW-external and
    RMoR-density, and registered with the 128 MiB suite (below). The model
    (`analysis/event_time_model.py`, checked against the harness at a small scale) puts it at
    2.3 GB a write at 128 MiB when every late row merges at once (MoR: 10.5 MB), from lateness's
    long tail, and at 64 MB under RMoR-density's θ 0.06.
- [x] **Reads of a range** (registered 2026-11-07, HYPOTHESES.md Q1–Q4; ran 2026-11-08).
  `reads.windows` reads five 1% windows by key or event time at each probe; campaign
  `docker/campaigns/windows.json` on havoc and the same `configs/win-*` on the laptop, for
  instructions. Q1–Q4 held, and the laptop sent what havoc did in every record (EXPERIMENTS.md).
- [x] **Tuning write policy by simulation** (registered 2026-11-08, HYPOTHESES.md T1–T4; ran
  2026-11-08, lab/pages/tune.md). `analysis/tune.py` prices every policy
  of a workload (a configuration and an arm) per read mix and store: havoc's counts, a formula for
  the work fitted on the laptop's `windows` runs, and the requests priced by size
  (`sized_latency`, which now covers the windows, the sorted read and compactions). Its picks held
  on the laptop's measured work (T2); its formula misses small reads with row groups no training
  run had (T1 in part).
- [x] **The picks in the cloud** (registered 2026-11-08, HYPOTHESES.md K1–K3; ran 2026-11-08/09
  at be8d6f1, lab/pages/tune-cloud.md). The four configurations that
  hold every S3 Express pick (or one within 0.4% of it) and its runner-up: `tune-upd-rg4m`,
  `tune-upd-mor-c12`, `tune-ing-rg4m` and `tune-ing-rg4m-seal256`, on S3 Express (a fleet of four
  `c7a.4xlarge`, each with its twin and spike) and Azure Blob (one `D8as_v6`, in turn). K1–K3 held:
  the runs sent what havoc counted; every judged pick was the cheapest measured on both stores;
  the twin's work and the VM's spike priced S3 Express within 3.3%. The laptop-priced predictions
  ran 4–12% low there, the VM's pace against the laptop's (1.08–1.19 times its time for the same
  instructions), alike across policies, so the order held. The picks rest on a narrow table (11
  columns); see the next item.
- [ ] **Row-group size on a wide table**: the 4 MiB picks come from an 11-column table, whose
  4 MiB row groups still have column chunks of a few hundred KB. A wide table (100+ columns, read
  a few at a time) at 4, 16 and 128 MiB would show whether they carry over; on the laptop and
  havoc, no cloud run.
- [x] **The deferred merges at their own best settings** (superseded: the tuner and its cloud check
  were retired on 2026-11-12; RMoR-density runs at 128 MiB in `w128`): the cloud run timed them only at 4 MiB
  row groups, with RMoW-append's configuration, where the tuner prices them 1.09–1.47 times their
  best on S3 Express and 1.23–2.22 on Azure (lab/pages/tune-cloud.md, Limits). On Azure Blob under
  `scans` the tuner's pick on the whole grid is the deferred merges with 32 MiB row groups
  (`tune-upd-rg32m`, 27.1 s a write against compacted MoR's 27.8), which no cloud run has timed.
- [x] **The formula's misses** (superseded: the results pages price each phase with its twin's
  measured work; the formula serves only simulation-cloud.md's forecast): a read's work per row group (small reads with 4 and 8 MiB row
  groups came out 19–44% low). Train on a row-group sweep, or count row groups read per phase.
- [ ] **A writer that tunes itself**: the same prices could drive the writer, merging a row group
  when the reads expected before it next changes cost more than rewriting it.
- [x] **Every design on both workloads, at Iceberg's sizes** (registered 2026-11-12,
  HYPOTHESES.md W1–W5; ran 2026-11-13/14, lab/pages/suite-128.md). One definition of each
  design, on the updates and the inserts, at D1–D4's 128 MiB row groups: `configs/w128-*.json`,
  campaign `docker/campaigns/w128.json` on
  havoc (the user's runs). Built for it: the event-time merge (above), MoR compacted as Iceberg
  plans its bin-pack after every write (`compactMinInputFiles`), and range reads after every
  write (`reads.rangeEvery`). Before the runs, at 9ff9176 on havoc (2026-11-13): fuzz rounds
  `w128-mixed`, `w128-event-time` and `w128-heavy` (two hours each from seed 1001; 5,423 seeds),
  no wrong result. Their six failures were timeouts under a load average near 200, and each passed
  on the laptop, in 3 to 13 minutes. Every mutant's calibration (`results/fuzz/cal-9ff9176-*`,
  bin/fuzz-mutants.py's docstring): every required mutant caught under each profile, the five
  event-time ones as on the laptop. RMoW-append on the updates runs 30 writes, since it
  copies whole row groups. On the inserts RMoR, RMoW-external and RMoW-append are projected, not
  run (revised 2026-11-12, W4–W5; lab/pages/inserts-128.md): the event-time merge at θ 0 would
  write 2.3 GB a write. The runs of MoR, MoR (compacted) and RMoR-density check the projection.
  Judged 2026-11-14: W1 (RMoR-density's half) to W5 held. The inserts jobs first failed at start
  (the op log's hash held the 3 GB log in one array) and ran at 14727a8, which streams it. The
  model's check missed one part of nine: it writes 0.74 of RMoR-density's bytes
  (lab/pages/model-128.md), so the projection waits on its revision.
- [x] **Results pages from the 128 MiB suite** (2026-11-16,
  [lab/pages/workloads-128.md](../lab/pages/workloads-128.md), Results), by snapshot, to replace
  the 16 MiB workloads pages. Bytes and requests are havoc's runs'. Time needs each run's work,
  which havoc's container did not count (no instruction counters, and other users' load on every
  job's cores), so the user chose laptop twins: campaign `w128-twins`, `w128`'s nine
  configurations at 7590d4a, one at a time (a 12g heap; RMoW-external's updates ran out of it and
  ran with 18g), each equal to havoc's run in every record. On S3 Express, on the updates
  RMoW-external writes in 15.8 s on average and reads a 1% range in 1.0 s, RMoR in 18.5 s and 1.7
  s, MoR in 25.2 s and 6.5 s; MoR's writes and range reads slow as snapshots are added. On the
  inserts RMoR-density reads a range in 3.2 s against MoR's 10.1, for writes of 4.1 s against
  0.45.
- [x] **Retire the 16 MiB workloads pages** that the 128 MiB results page replaces
  (`workloads.md`, and its cloud check `workloads-cloud.md`): the user decides which pages and
  runs go; first record them in EXPERIMENTS.md (Retired runs) and carry what still matters into
  LESSONS.md. **Done 2026-11-16** (the user: drop them if unusable): both pages and the 36 runs only
  they read (campaigns `tune` and `workloads-curves`), recorded in EXPERIMENTS.md and LESSONS.md
  (The 16 MiB workloads). The runs the simulation's cloud check reads stay with
  simulation-cloud.md (`workloads-ref`, `workloads-cloud`, `windows`; 53 runs), until a cloud run
  at 128 MiB replaces it.
- [x] **Two mechanisms the 128 MiB results page states without explaining** (explained
  2026-11-16, workloads-128.md sections 2 and 4). RMoR-density's writes slow in the scan that finds
  their rows (2.2 to 13.3 s a write on S3 Express), which under `locate` `scan` reads the key column
  of every row group in the write's range of keys, as its cut copies multiply its row groups (58 to
  2,480). RMoW-external's write decodes less in that scan: it reads less of the key column (fewer
  files pass on their statistics) and decodes each MB faster (23 ms against RMoR's 31), the one design
  with no deletion vectors to apply; the designs with them slow per MB as they accumulate. Found on
  the way: the lab priced a writer's footer read, two streamed GETs, as one (fixed in
  `lab/views/65_sized.sql`; 0.27–0.60 s a write on S3 Express). As stated before: RMoR-density's
  update writes slow from 8.0 s over the first 10 to 20.9 s over the last 10 (S3 Express) as its
  requests a write grow from 8 to 1,299, while its bytes read rise to about 1.8 GB a write around
  writes 20 to 80 and fall back to about 550 MB; and RMoW-external's update writes decode 3.5 s
  where RMoR's decode 5.5 (its writes read 74 files at the 240th to RMoR's 137, and no deletion
  vectors to RMoR's 182, which the decode span's opens and deletes include, but not obviously 2 s
  of them).
- [x] **The 128 MiB updates with writers that prune by key** (registered 2026-11-16,
  HYPOTHESES.md K1–K3, commit 202bc66 on branch `keys-128`; ran 2026-11-17, judged on
  lab/pages/suite-128.md: K2 held, K1 and K3 contradicted in part): `w128`'s five update configurations
  with `locate` `keys` (`configs/w128k-upd-*.json`, campaign `w128-keys`), on the laptop alone, one
  at a time with requests logged, from build snapshot `20261117T005411Z-202bc66bc369` (the twins'
  jar and fork jars). Queued behind catalog-bench's w3pl on the laptop lock
  (`scratch-w128/keys.sh` in the session's project directory). Forecast: on S3 Express RMoR's,
  RMoW-external's and RMoR-density's writes 33–39% faster, MoR's about as before. **Decided (the
  user, 2026-11-16):** once K1–K3 are judged, the results page uses these runs, and Appendix A
  shows what pruning by key takes off against the `scan` runs. The work is about patterns a Parquet
  reader or writer *could* use, if they prove useful. **Done 2026-11-17:** on S3 Express RMoR's
  writes take 37% less, RMoW-external's 34%, RMoR-density's 33% and MoR's 12% (K3's bound for MoR
  was 5%: its locate reads half the bytes, skipping the initial load's row groups, and its work
  follows them, not its requests). RMoR-density's writes now slow from 6.8 s to 10.8 s (8.0 to 21.7
  before), in each file's footer and deletion vector. Every record but the locate's matched the
  `w128` runs, but MoR's full scans ran 6.4% more instructions than in its twin: a JIT compile of
  the benchmark's check of each row (diagnostic `diag-mor`, 2026-11-17, suite-128.md).
  The results page times the updates from these runs, writes, reads and compactions alike (the
  user, 2026-11-17); its Limits give the spread between the two runs' work.
- [x] **Appendix A in two parts** (the user, 2026-11-16; done 2026-11-17 for the designs built,
  with a table of which need appends; the re-cut by reference joins it when built, density plan
  step 5): *patterns*, information a reader or writer
  could use with Parquet as it is (a footer and deletion vectors once per file, skipping row groups
  a DV covers whole, pruning by key, skipping pages), and *format changes*, what a stock reader must
  understand or tolerate (a footer at an offset inside the object, row groups out of order, a
  re-cut by reference). For each design, say which it relies on and whether it needs appends:
  RMoR-density needs none, though its gains rely on skipping row groups; RMoW-external and
  RMoW-append do. The results pages show every design with the patterns, not without.
- [ ] **The inserts model, revised for RMoR-density's merges** (only if the projection of RMoR,
  RMoW-external and RMoW-append is to stand beside the runs): it rewrites 53 MB of stored rows a
  write where the run rewrites 73.
- [ ] **Reads at every snapshot, in a final cloud run** (decided 2026-11-12; deferred by the user
  2026-11-16 until the `keys` runs and the density plan's steps are settled): every run so far
  reads the table only after every 5th write (`reads.every` 5, one setting for every kind of read;
  `Runner.java`, the probe). Writes are measured at every snapshot. So the workloads pages'
  compaction charts sample a sawtooth every 5 snapshots. Compacted every 10 writes, the reads fall
  alternately right after a compaction and halfway through a cycle; every 3, 6 or 12 writes, at
  shifting points. If there is a final cloud run, make the most of its writes and read at every
  snapshot. The knob is built (`reads.rangeEvery`, 2026-11-12): the range reads and the newest 1%
  at every snapshot, while the full scan and the sorted read stay every `reads.every`. A range
  read takes about 0.5 s on S3 Express, against 10.5 of `wl-upd-base`'s 33 minutes there for
  every kind of read at 7 read points. Its control (the extra reads change no write record) is
  `SuiteKnobsTest`; the 128 MiB suite uses it on havoc first.
- [x] **The write-up of the two workloads** (retired 2026-11-16 for the 128 MiB results;
  docs/LESSONS.md) (`split`, `seal`, `windows`; 2026-11-09,
  [lab/pages/workloads.md](../lab/pages/workloads.md), Results). Simulated, at 16 MiB row groups:
  each design's write against its range read and full scan, the inserts as a choice of when to
  merge late rows, where the time goes, and row groups of 4 to 32 MiB. The updates' sorted read is
  left out: it ran only under `cached`, which reads dead row groups, so it charges RMoR and the
  deferred merges for the row groups they copied out (RMoR reads 3.4 times its live bytes).
- [x] **A sorted read that skips dead row groups** (built 2026-11-10, 6db916a): under
  `skipDeadRowGroups` the sorted read leaves out each row group the file's deletes cover whole, as
  the full read does. The two workloads' reference runs and cloud run read sorted under
  `cached-skip`.
- [x] **The two workloads in the cloud** (decided 2026-11-10; done 2026-11-11; its measured page
  retired 2026-11-16, its calibration page simulation-cloud.md stands): measure what the workloads page
  simulates, on S3 Express and Azure Blob, and make it the results page's data. Steps:
  - [x] the base point and the sweeps moved to Calibration (their MoR skipped the update read);
    the workloads page gains whiskers and latency as snapshots are added (ee7f046);
  - [x] the locate by each changed key (`locate` `keys`) and the skipping sorted read (6db916a),
    with tests and two mutants; five configurations `wl-*` and campaign `workloads-ref` (b49a57f);
  - [x] fuzz rounds on havoc: `keys-r1` and `keys-r1-heavy` at b49a57f (7,189 seeds), then
    `keys-r2` and `keys-r2-heavy` at 0942497 (9,043 seeds), whose `layout.eagerBytes` lets small
    tables reach the per-row-group paths. No wrong result; the timeouts were the cut's small strides
    decoding large footers. The two new mutants are caught at 0942497 (1 of 60 seeds each);
  - [x] every mutant's calibration at 0942497, since the fuzzer's draws changed
    (`results/fuzz/calibration-0942497` and `-heavy`, 2026-11-11): every required mutant caught
    in the default draws; in the heavy ones all but the locate by key (none of 60, where the
    default draws catch it in about one seed in 60), now measured there, not required;
  - [x] reference runs: `workloads-ref` on havoc at b49a57f (requests and bytes) and on the laptop
    at 0942497 (instructions), agreeing on every request and byte. The laptop's first runs ran
    beside another session's model checks and were rerun (lab/annotations.toml);
  - [x] hypotheses registered from the reference runs' simulation (L1-L4, 2026-11-10);
  - [x] the measured page (`lab/pages/workloads-cloud.md`, in the toc under Results since
    2026-11-11), sharing the workloads page's charts (`lab/figures.py` `wl_*`), checked on the
    tuner's cloud runs;
  - [x] the workloads page's main points from `workloads-ref`, and its update curves rerun with the
    same reader changes (`wlc-upd-*`, campaign `workloads-curves`): the locate by key takes 12-17%
    off the writes of MoR and the deferred merges, so the old curves (`win-upd-*`, `tune-upd-*`) do
    not meet the new main points. The insert curves and lateness twins stay on the earlier runs:
    those changes moved the inserts' main points under 3%, as the page computes. Havoc's ten ran
    2026-11-11 at 2e3152d; seven of the laptop's ten ran beside other sessions' jobs (3.70-4.44
    GHz) and were rerun on an idle laptop (4.52-4.64 GHz, every request and byte as havoc's);
  - [x] the cloud run (2026-11-11 at 0942497): on S3 Express a `c7a.4xlarge` per configuration
    (four at once, the fifth after), each after its twin and spike; on Azure Blob one `D8as_v6`, in
    turn, after one spike. L1-L4 held (`lab/pages/workloads-cloud.md`, docs/EXPERIMENTS.md).
- [ ] **The density hypothesis at Iceberg's row-group size** (raised by the user 2026-11-09; the
  work left is planned in [drafts/density-plan-2026-11-16.md](drafts/density-plan-2026-11-16.md),
  not yet agreed): use
  the deletion vector's share of a row group to copy at a finer grain than the file, copying whole
  row groups, parts of them (runs of pages, which the writer aligns across columns every 20,000
  rows), or merging small cold pieces back. As built, the deferred merges only choose when to copy
  a whole row group, and every two-workload run is at 4-32 MiB on an 11-column, 1 GB table, whose
  update regions (513,700 rows) are about one 16 MiB row group, so finer copies had little to win
  (the 2026-11-05 re-cut model ran at 16 MiB only). Needs 128 MiB row groups on a table with many
  of them (about 7 GB at this width), regions several to a row group, and later a wide table.
  Model first (`analysis/recut_model.py`, `analysis/hybrid_model.py`), then register, build, and
  run on havoc and the laptop. Open: the piece in the write's new file with a deletion vector over
  the old run (RMoR's way), or the old row group's footer re-cut (RMoW-append's way).
  - [x] **Modeled, registered, built and run** (2026-11-09/10; D1–D4, plan
    docs/drafts/region-cut-2026-11-09.md, `layout.cut` in a68eda3,
    [lab/pages/region-cut.md](../lab/pages/region-cut.md)): RMoR's way, a kept row group copied
    as several cut at runs of changed strides. D1, D2, D4 held, D3 contradicted by its writes
    (3.04 times the deferred merges', past 3). On S3 Express, simulated: a write 12.9 s against the
    deferred merges' 21.4, a 1% window 1.86 s against 2.90.
  - [ ] **The locate's requests:** every design's write reads the key column of each live row
    group between the write's smallest and largest changed key (`MorArm.locateByScan` prunes row
    groups to that one range, which spans most of the table when a write changes ten regions), a
    request each: about 1,070 a write for the cut, 850 for the deferred merges, 89 for compacted
    MoR. The model leaves it out, and it costs most of the cut's write on Azure Blob. Pruning row
    groups by each changed key, as files already are, would cut it: built as `locate` `keys`
    (2026-11-10, 6db916a); run at 128 MiB 2026-11-17 (K1–K3, above), and the results use it.
    What it leaves is a file's: its footer (two requests) and its deletion vector, loaded before
    its row groups are pruned (RMoR-density's last 10 writes: 317 and 157 requests against 35 for
    the key column).
  - [ ] **What `keys` leaves:** a writer that held footers across writes, or pruned a file's row
    groups by key before loading its deletion vector, would read less. Neither is built; both are
    patterns.
  - [ ] **The oracle's check in a read's work** (found 2026-11-17, diagnostic `diag-mor`): every read
    hashed each row it returns into the id-and-version oracle inside its decode, so the check counted
    in the read's work, and how the JIT compiled it moved MoR's full scans by 6.4% of their
    instructions between two runs of the same reads. **Decided (the user, 2026-11-17):** measured
    runs check no rows, and the benchmark checks correctness apart, in verification runs
    (`--verify`) that `steps_diff` ties to the measured ones; measurement in a run is off the
    critical path or negligible, particularly for the cloud runs. Built on branch `oracle-out`
    (README, Correctness); measured 2026-11-17 (campaign `oracle-out`): 0.6% to 2.9% of a read's
    instructions as usually compiled, 7.7% of MoR's full scans in its `w128-keys` run; writes none.
    Open: whether to rerun the 128 MiB laptop runs without it (the user's call), and the full
    fuzzer calibration on havoc on both profiles (a 20-seed spot check of three mutants passed).
    The instrumentation in the measured path is below what the runs resolve: `Held` takes its lock
    once a row-group fetch or scan step (about 1,160 times a full scan at 128 MiB, uncontended in
    one-thread reads), and the request log adds under 0.1% (the `w128-keys` runs logged requests,
    their twins did not); both logs are written after each phase.
  - [ ] **The windows' footers:** the region designs' windows read the footer of every file whose
    metrics overlap the window (276 requests for the cut at 128 MiB, the model's 14).
  - [ ] (planned: density plan, steps 2–5) **Cold pieces merged back,** a **sparse workload** (where density can choose differently
    from "copy what changed"), **cuts of a fixed size** (`copy:shrink:rg`, which first needs the
    model to free killed copies' rows), and the **page-skipping reader** (step 0 done).
- [x] **Hot-region isolation** (folded 2026-11-16 into the density plan: the cut isolates changed
  runs already, and step 3, merging cold pieces back, decides whether hot ones stay apart). Can RMoW
  isolate frequently updated regions as row groups of their own, and so cut its amplification?
  - RMoW sets row-group boundaries on every rewrite, so it can cut a hot range out as its own row
    group.
  - Bytes rewritten per update scale with the row group. Small row groups cost footer entries
    (about 1.2 KB and 30 µs each), read requests and compression.
  - Compaction's packing of small row groups (`compactSmallRowGroups`) would undo the isolation, so
    it must leave hot ones alone.
  - Measure bytes rewritten per updated row, footer size and read GETs under the region skew.
- [x] **Register the hypotheses** (a standing rule since, in CLAUDE.md; every campaign since has
  its dated section) before the runs that decide them. HYPOTHESES.md was refocused
  on 2026-10-27 on the write and read trade-off; the earlier ones are the file at commit 1145f89.
  - [x] the calibration's C1–C4 (registered 2026-10-27);
  - [x] the sweeps' U1–U3, K1 and G1 (registered 2026-10-29);
  - [x] the sweeps' memory and sorted read, M1–M4 (registered 2026-10-29, after a pilot);
  - [x] the base point in the cloud, C5–C8 (registered 2026-10-30);
  - [x] the vectorized reader, V1–V3 on the laptop and V4–V7 in the cloud (registered 2026-10-31);
  - [x] the vectorized base point on Azure, A1–A4 (registered 2026-10-31, 2e24c9b);
  - [x] the sweeps in the cloud: S1, and U1, U2 (revised) and G1's times restated for measured
    wall time (registered 2026-11-01, a10b3e7), predicted from each configuration's twin on its
    S3 Express VM.
- [ ] **A JFR run to check the held levels** against the heap (still open: W2 and W5 judge sorted
  reads' held peaks at 128 MiB; the base point it names was retired): a sorted read of MoR-v3 at the base
  point, whose counted level climbs by a row group per batch file, should move the heap after
  collection by about as much. A check of the measure, not a result.
- [x] **Cloud campaign,** on the fork build with the five arms, on S3 Express and Azure (done
  2026-10-28 to 11-04: the calibration, the base point and the sweeps; retired 2026-11-12,
  docs/LESSONS.md):
  - the base point;
  - settings on either side of each crossover the sweeps find;
  - each configuration's local twin rerun on the same build.

  The configurations planned so far are only the calibration (`s3x-calibration`,
  `azure-calibration`), with the five arms. Its twins were rerun on 2026-10-27 (campaign
  `twins`, after three earlier pairs), and the predictions recomputed from them
  (AZURE_RUNBOOK.md, HYPOTHESES.md): on Azure RMoW-C ÷ MoR-v3 is 0.36 for a batch and 0.59 for a
  full read, RMoR-C with footers' 0.54 and 0.58. The two twins' model requests and bytes match in
  all 862 records, every phase on Azurite sent what the model counts (RMoR-C with footers'
  creates aside), and the other arms match the earlier twins in every shared record. Across the
  runs the S3 Express ratios moved by up to 0.12, through the CPU's pace per run.

  **The deploy runs any configuration** (generalized 2026-10-29): its `bench` step, which replaced
  `calibrate`, runs `AWS_CONFIGS` or `AZURE_CONFIGS` at `BENCH_COMMIT`, with a heap and flags of
  their own; a configuration with a local store (a sweep's) runs on the cloud's store with its label
  suffixed `-s3x` or `-azure`, so its havoc run is its twin, and the calibration's run as before.
  Checked against a stub `run.sh` on both clouds' branches; not yet run on a VM. The sweeps' base
  point needs a 16 GB heap, so `c7a.4xlarge` on AWS.

  **The base point ran on S3 Express** (f51796a, 2026-10-30, 2 h 47 min; run by a session, at the
  user's request): C5–C8 held. The requests, bytes and held peaks equal the laptop twin's in all
  1,325 records; priced by request size with the VM's own spike, every judged mean came within
  1–11% of its wall time; every judged ratio fell on the side of 1 the twin put it (RMoW-C's
  batch 3.05 times MoR-v3's, its full read 0.85); the store took 13–28% of the batches and 32–36%
  of the full reads. Azure has not run it. **Rerun on the vectorized reader** (6f24575,
  2026-10-31, 1 h 36 min): V4–V7 held. The counts equal the laptop twin's; every judged mean came
  within 2–14% of the model; RMoW-C's batch took 4.18 times MoR-v3's (predicted 4.48) and its
  full read 0.87 (0.87); the store took 58–70% of the full reads and 18–36% of the batches
  (lab/pages/base-point-model.md; the results in
  lab/pages/base-point.md). **On Azure** (6f24575, 2026-11-01,
  2 h 18 min, from a `D8as_v6` in westus, run by a session at the user's request): A1, A3 and A4
  held, A2 is contradicted by RMoR-C's full reads alone. RMoW-C's write took 3.14 times MoR-v3's and
  its full read 0.89; on both stores it repays a write past about six full reads per write. RMoR-C's
  write files past 256 MiB are block blobs (below, Iceberg's own writes above 256 MiB on Azure), from
  which a 16 MiB range reads at 1.6 times the spike's price.

  **A fleet** (2026-10-30): `FLEET=true` brings up one VM per configuration, each running its own
  spike and run, all at once, and `VM=<name>` narrows any command to some of them (a VM fetched and
  taken down as it finishes). The VMs also count instructions where the hypervisor allows
  (`perf_event_paranoid` 2). Checked offline (a plan of two VMs on each cloud, and a stub of
  `terraform` and `ssh`). Open: Azure's westus quota is 10 vCPUs, one `D8as_v6`, so a fleet there
  needs an increase; AWS's quota is unreadable to `casuser`. **Used for the sweeps** (2026-11-01):
  nine VMs on AWS, each running its twin, spike and run, fetched and taken down as it finished; on
  Azure one VM (`FLEET=false`) ran the ten configurations in turn. AWS's limit turned out to be 64
  vCPUs (four `c7a.4xlarge`); nine got in only because they launched at once. Six of the nine first
  boots failed on the Ubuntu mirror's 503s; `cloud-init.sh` now retries (bb46e88).

  **Running it** is the user's: [deploy/](../deploy/README.md), each cloud step by step the first
  time (`infra-up`, `up`, `ship`, `wait`, `run test`, `wait`, then `run spike bench`), then
  `fetch`, `down` and `teardown`. **When the results are in `results/`:**
  1. `lab ingest`.
  2. In lab/pages/cloud.md, swap the pinned stand-ins for the cloud
     runs: `LATENCY` and `SENT` to the three calibration runs per provider, `SPIKES` to the
     cloud spikes, `PROFILES` to those spikes' run ids (`measured_profiles`), and set
     `STANDIN = False`. Build with `lab build --strict` and look at the charts.
  3. C1: `steps_diff` of every cloud run against its local twin
     (`s3x-calibration-local-8433ccfd4620/20261027T175847Z`) finds no difference in requests or
     bytes, and each Azure phase's `sdk_*` counts equal the Azurite twin's
     (`azure-calibration-azurite-a96fd56de2f0/20261027T175614Z`), but for the creates
     (AZURE_RUNBOOK.md, The twins).
  4. C2: the page's model check, priced with the measured profiles.
  5. C3 and C4, against the predictions in the runbooks and HYPOTHESES.md; report a contradicted
     hypothesis as contradicted.
  6. Ask the user before retiring the stand-ins (campaign `standins`), recorded first under
     EXPERIMENTS.md's Retired runs. Done 2026-10-28: deleted, with their build snapshots.

  **The calibration ran** on 2026-10-28 (campaign `cloud`): three runs on each store on the
  registered build, and the spikes from main, on S3 Express in usw2-az3 (`c7a.2xlarge`) and on
  Azure in westus (`D8as_v6`, Zen 4, since the subscription has no D8as_v5 quota there). Steps 1
  and 3 are done: C1 holds on both, with no difference in requests or bytes in any of the 862
  records, and each Azure phase's `sdk_*` counts equal the Azurite twin's but for RMoR-C with
  footers' creates (a rename for a HEAD). Steps 2, 4 and 5 are done in
  lab/pages/cloud.md: C2 is contradicted as registered, by the profile
  rather than the model (the spikes' profiles, latency at 64 KiB plus a slope, misprice the
  calibration's small GETs); C3 is contradicted for S3 Express's batches, where the region arms
  beat MoR-v3, and for RMoR-C with footers' batch on Azure; C4 holds on S3 Express and is
  contradicted on Azure. Step 6 is done. Both infra layers stay up, free while empty;
  the VMs are down.
- [x] **A profile priced by request size** (from the cloud runs, 2026-10-28; built 2026-10-29,
  [lab/views/65_sized.sql](../lab/views/65_sized.sql)). `measured_profiles`
  takes a request's latency at the spike's smallest size and a slope over 64 KiB–128 MiB, which
  overprices S3 Express's small GETs (latency is flat to 1 MiB) and underprices Azure's past
  128 KiB (a step there). `sized_requests` prices each logged request at its own size instead:
  the spike's lognormal mean per size, linear between sizes, flat below 64 KiB. Its rules came
  from the calibration's runs:
  - a smaller size is never priced above a larger one. Azure's 64 KiB `get_range` is each rep's
    first read of a freshly written 128 MiB blob, and took 33–118 ms, where the calibration's
    64 KiB GETs took 9 ms, first read of a file or not;
  - the log records an appendable create as a PUT, so a Parquet PUT by an arm that appends is
    priced as one: an append of its size plus the create's overhead (98 ms on Azure, 1 ms on S3
    Express). RMoR-C with footers' data files took 141 ms on Azure, priced as PUTs at 38;
  - appends past Azure's 100 MiB cap are several.

  On the calibration's requests, ranged GETs come within 7% of their measured time in every size
  band on S3 Express and within 0.70–1.23 on Azure; PUTs, appends and creates within 0.77–1.16,
  but for Azure's 24 footer appends of under 64 KiB (0.40–0.59). Streamed GETs of 64 KiB or less,
  which only the `stock` reader opens, are priced at half their time. Per one-thread phase, CPU
  plus prices against wall time, within 25%: each arm's batches in 83–100% on S3 Express and
  72–92% on Azure (the slope profile: 5–93% and 2–95%); reads apart from `stock` in 80% and 89%
  (1% and 59%); `stock`'s reads in 67% and 61% (32% and 94%). These are the runs its rules came
  from; its first test is the base point's cloud runs (Cloud campaign, above).
- [x] **`phase_work`'s CPU was 11% high on the laptop** (found 2026-10-27; divided by the ratio,
  decided and done 2026-10-27). The thread CPU clock runs 11.1% fast against the wall clock there
  (WSL2; `host.thread_cpu_per_wall` in manifests from 2026-10-26), and `phase_work` turned
  instructions into time with thread CPU time. The lab's `run_clock` gives each run its ratio
  (its own, else its host's: every counted laptop run ran on the boot of 2026-10-24, measured at
  1.1116), and `phase_work` and `phase_breakdown` divide by it. Modeled CPU fell 10.0%, simulated
  latencies 0.1–10%, and no verdict changed (EXPERIMENTS.md, Findings so far). Havoc's ratio is
  still unmeasured: its runs have no counters, so nothing there used it.
- [x] **The calibration configurations gain `rmow-c-split` and `rmor-c`**, and lose the arms new
  campaigns leave out, after the generator's region skew and update share (decided 2026-10-26,
  done 2026-10-27). With `rmor-c` they set `writerSkipsDeadRowGroups` and read with `cached-skip`
  beside the others, stock included. The twins were rerun and the Azure predictions recomputed
  (`twins-five`). The calibration's workload is the ingest window, unchanged: its op log is the
  same. On 2026-10-27 they dropped `rmow-c-split` again, with the arm, and took `rmor-c-footer`
  (above).

- [ ] **Later: a tool that recommends a layout** (row-group size, policy, heuristics) for a
  real table from these findings. Interesting once there are findings to inform it (the user,
  2026-10-29).

## Still open from before

- A second seed, and repetitions.
- A server core's pace on havoc, if an admin allows counters.
