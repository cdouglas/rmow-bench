# Roadmap: outstanding work

What is left to do before the post, and what was decided along the way. The experiments map
([EXPERIMENTS.md](EXPERIMENTS.md)) says what has been run and found; this page says what comes
next. Keep it current: mark items done (with the commit) rather than deleting them, until a
write-up covers them.

## The question, narrowed (decided 2026-10-26)

**The idea:** treat a Parquet region (a row group) as the unit of a write. That adds write
latency, to cut read latency. In view-maintenance terms (Olteanu), it moves cost from enumerating
the table to integrating each update. The analysis asks three things:
1. **Where the merged region goes:** appended to its object (RMoW-C), or a new object (RMoW-C-split).
2. **How it compares with the baseline,** Iceberg's merge-on-read (MoR-v3).
3. **How it compares with the hybrids:** TailCoW-C (MoR, with its tail files merged) and RMoR-C (a region
   merged into a new data file, its old copy hidden by a DV).

| Arm | The merged region goes | The old region is hidden by |
|---|---|---|
| `mor-v3` | nowhere: updated rows go to a new file | DVs on rows |
| `tailcow-c` | nowhere, outside the tail | DVs on rows |
| `rmow-c` | appended to the same object | the new footer |
| `rmow-c-split` | a new object | the new footer, which points across objects |
| `rmor-c` (to build) | a new data file | a DV covering the old row group |

The other arms stay in the code, since the published write-ups use their runs, but new campaigns
leave them out.

**How results are reported:**
- **Measured, not modelled.** Cloud runs report each phase's wall time; local runs report
  requests, bytes and simulated time.
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
- [ ] **Run the fork's contract suites on the real services** (`S3ExpressFileIOAtomicTest`,
  `AdlsFileIOAtomicTest`, `GcsFileIOAtomicRapidTest`), first on each cloud VM. No emulator runs
  the fork's paths: versitygw ignores write offsets, and Azurite has no rename.
  The runbooks now list them as step 1. They need keys in the environment, not the VM's
  identity: AWS keys for S3 Express, a SAS token for ADLS.
- [ ] **An APPEND at length 0 is an APPEND at any length** (found 2026-10-26, for
  fileio-catalog). The fork's snapshots, like Iceberg's, take a known length of 0 for an unknown
  one (`length > 0 ? length : null` in `S3InputFile`, `ADLSInputFile` and `GCSInputFile`). So an
  APPEND from `newInputFile(location, 0)` looks the object up and extends it at whatever length it
  has, where the contract says it lands only at the snapshot's length. On Azurite an append "at 0"
  to a 12-byte blob landed at 12. RMoW never appends at 0. The bench's stand-in for Azurite's
  creates relies on it, which is safe there. The local stores keep the length.

## Harness (rmow-bench)

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
    appends and creates beside RMoW's, interleaved (draft hypotheses X5 and Z7). That leaves
    nothing native in the runs, only in the spikes.
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
  assumed, and it is the biggest I/O term in S3 Express full reads (draft X6, Z8).
- [x] **Drop the instruction-counter advice from the cloud runbooks** (done 2026-10-27). The cloud
  runs measure wall time; they need the clocks to agree (`host.thread_cpu_per_wall`).

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
- [ ] **Arrival window:** sweep insert lateness (`lateMedianBatches`, `lateP99Batches`). It
  exists but has never been swept.

**The base point leaves the skew and the update share no room** (found 2026-10-27, from the
generator alone). At 1M rows, 60 batches of 10,000 operations and 4 MiB row groups, the table has
15 regions of 100,000 rows by the end, and 2,500 single-row updates a batch fall in 12–13 of them
at any skew from 0 to 2. A statement holds at most 2,500 rows, 2.5% of a region. With regions of
10,521 rows (1 MiB row groups) there are 135: single-row updates touch 115 a batch at a skew of 0
or 1 and 73 at 2, and p can reach 24%. To decide before the sweeps: more rows, smaller row
groups, or fewer updates a batch, so that a batch touches a minority of regions and p can
approach 1.

## Experiments

- [ ] **Local sweeps, one axis at a time,** from a base point (4 MiB row groups, 1M rows, 60
  batches, the ingest window): skew, update share, arrival window and row-group size, for the five
  arms. The simulation finds each crossover cheaply, since requests and bytes are exact locally.
- [ ] **Hot-region isolation** (a micro-benchmark question, not a Parquet benchmark). Can RMoW
  isolate frequently updated regions as row groups of their own, and so cut its amplification?
  - RMoW sets row-group boundaries on every rewrite, so it can cut a hot range out as its own row
    group.
  - Bytes rewritten per update scale with the row group. Small row groups cost footer entries
    (about 1.2 KB and 30 µs each), read requests and compression.
  - Compaction's packing of small row groups (`compactSmallRowGroups`) would undo the isolation, so
    it must leave hot ones alone.
  - Measure bytes rewritten per updated row, footer size and read GETs under the region skew.
- [ ] **Register the hypotheses** before the cloud runs:
  - the model checks: X1–X6 and Z1–Z8, drafted for registration in HYPOTHESES.md (Added
    2026-10-27, before the calibration runs), awaiting review; the held H1 draft is a later
    latency campaign, and stays held;
  - the sweeps' predictions.

  Comparisons are reported, not judged.
- [ ] **Cloud campaign,** on the fork build with the five arms, on S3 Express and Azure:
  - the base point;
  - settings on either side of each crossover the local sweeps find;
  - each configuration's local twin rerun on the same build.

  The configurations planned so far are only the calibration (`s3x-calibration`,
  `azure-calibration`), with the five arms. Its twins were rerun on 2026-10-27 (campaign
  `twins-five`, replacing `twins-fork`), and the Azure predictions recomputed from them
  (AZURE_RUNBOOK.md): RMoW-C ÷ MoR-v3 is 0.36 for a batch and 0.59 for a full read. The two
  twins' model requests and bytes match in all 842 records, every phase on Azurite sent what the
  model counts, and the arms both twin pairs ran match in every record they share.
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
  same.

## Still open from before

- A second seed, and repetitions.
- H4's abort on S3, from the GET bytes the SDK declares.
- docker/README.md says the repository has no remote; it has one (`self`).
- A server core's pace on havoc, if an admin allows counters.
