# Roadmap: outstanding work

What is left to do before the post, and what was decided along the way. The experiments map
([EXPERIMENTS.md](EXPERIMENTS.md)) says what has been run and found; this page says what comes
next. Keep it current: mark items done (with the commit) rather than deleting them, until a
write-up covers them.

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
- [ ] **ADLS creates stay one request above the store's own appender:** 3 requests against 2.
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
  - [ ] **Against the stores' own writes, in the cloud:** the spikes now time the store's own
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
- [ ] **Stacked bars of where time goes:** a lab page with phase and sub-phase (locate,
  merge-read, write, delete-write; each read) split into CPU and waiting, with process CPU shown
  beside, for the five arms. The view is done (`phase_breakdown`); the page is not.
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
- [ ] **Iceberg's own writes above 256 MiB on Azure:** an output stream is one Put Blob up to
  256 MiB (README, The Azure store). A larger one is presumably several requests, the SDK's
  chunked upload, not yet counted; the model counts one PUT. No run so far writes one after its
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
  - [ ] **θ against p, after the sweeps** (decided 2026-10-29). RMoR-C's θ is a hybrid, choosing
    per region between DVs and a rewrite, and the story compares the alternatives before
    introducing one. The sweeps run θ = 0 (always rewrite), and TailCoW-C is the other end
    (never), so where RMoR-C's batch crosses TailCoW-C's along p, on each store's profile, is where
    θ belongs; the θ stage starts there.
- [ ] **Arrival window:** sweep insert lateness (`lateMedianBatches`, `lateP99Batches`). It
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

- [ ] **Sweeps, one axis at a time,** from the base point (decided 2026-10-29, above: 16 MiB row
  groups, 50 regions growing to 71, 30 batches, a region skew of 1, statements at p = 0.03):
  update share, skew, arrival window and row-group size, for the five arms, with the ingest
  window as a reference. Requests and bytes are exact, so the simulation finds each crossover.
  Eleven configurations (`configs/sweep-*.json`), too large for the laptop: the campaign
  `docker/campaigns/sweep.json` runs them on havoc, as the user's runs, in 16 GB heaps. Each also
  reads the table sorted under the cached reader, and logs what every phase held (decided
  2026-10-29). A first start on havoc was stopped the same day, before the writers streamed and
  memory was measured; it needs the image rebuilt from the commit that has them.
- [ ] **Hot-region isolation** (a micro-benchmark question, not a Parquet benchmark). Can RMoW
  isolate frequently updated regions as row groups of their own, and so cut its amplification?
  - RMoW sets row-group boundaries on every rewrite, so it can cut a hot range out as its own row
    group.
  - Bytes rewritten per update scale with the row group. Small row groups cost footer entries
    (about 1.2 KB and 30 µs each), read requests and compression.
  - Compaction's packing of small row groups (`compactSmallRowGroups`) would undo the isolation, so
    it must leave hot ones alone.
  - Measure bytes rewritten per updated row, footer size and read GETs under the region skew.
- [ ] **Register the hypotheses** before the runs that decide them. HYPOTHESES.md was refocused
  on 2026-10-27 on the write and read trade-off; the earlier ones are the file at commit 1145f89.
  - [x] the calibration's C1–C4 (registered 2026-10-27);
  - [x] the sweeps' U1–U3, K1 and G1 (registered 2026-10-29);
  - [x] the sweeps' memory and sorted read, M1–M4 (registered 2026-10-29, after a pilot).
- [ ] **A JFR run to check the held levels** against the heap: a sorted read of MoR-v3 at the base
  point, whose counted level climbs by a row group per batch file, should move the heap after
  collection by about as much. A check of the measure, not a result.
- [ ] **Cloud campaign,** on the fork build with the five arms, on S3 Express and Azure:
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

  **Running it** is the user's: [deploy/](../deploy/README.md), each cloud step by step the first
  time (`infra-up`, `up`, `ship`, `wait`, `run test`, `wait`, then `run spike bench`), then
  `fetch`, `down` and `teardown`. **When the results are in `results/`:**
  1. `lab ingest`.
  2. In [lab/pages/cloud.md](../lab/pages/cloud.md), swap the pinned stand-ins for the cloud
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
  [lab/pages/cloud.md](../lab/pages/cloud.md): C2 is contradicted as registered, by the profile
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
