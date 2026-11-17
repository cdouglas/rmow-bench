# Lessons and decisions from the retired experiments

On 2026-11-12 every run before the two workloads' current settings was deleted, with the lab
pages that read them, and on 2026-11-16 the 16 MiB workloads pages with the runs only they read
(EXPERIMENTS.md, Retired runs). This page keeps what they taught: why they
no longer stand, how the benchmark measures, what the stores did that no model said, which
designs were dropped and why, and which results should carry over. Numbers are the retired runs';
treat them as directions, not as results to quote.

The full record is in git at bfbdf33: `git show bfbdf33:docs/EXPERIMENTS.md` (every experiment's
row and findings, and the verdicts of H1–H6, C1–C8, V1–V7, A1–A4, R1–R3, W1–W3, T1–T4 and K1–K3),
and the pages under `lab/pages/` there. Every hypothesis stands in [HYPOTHESES.md](HYPOTHESES.md)
as registered (H1–H6 in its version at 1145f89).

## Why the earlier runs no longer stand

Each of these changed what a design's write or read costs, so runs from before it do not sit
beside runs after it:

| Since | Change | What it moved |
|---|---|---|
| 2026-10-25 | Writers read their footers once per write, not once per run (I/O parity rule 6); readers that load a file's footer and deletion vectors once per read (`cached`); the payload check off; work in instructions | RMoW's writes took 3–4 requests more; every reader with DVs read less |
| 2026-10-26 | The atomicio fork's Iceberg the only build: every append through `SupportsAtomicOperations` | config hashes; DV files 18 bytes larger |
| 2026-10-27 | Five designs instead of twelve (below) | which arms run |
| 2026-10-31 | Every arm reads Parquet through Iceberg's vectorized reader, strings as UTF-8 bytes | a full read took about a quarter of its instructions; MoR's write, mostly decoding, fell most, so RMoW-append's write over MoR's rose from 3.05 to 4.18 at the base point: runs on the row reader understate the region designs' write gap by about a quarter |
| 2026-11-02 | RMoR's check of dead row groups by range, not position by position | RMoR's skipping read and its write lost about 2.2 s and 2.1 s at the base point on S3 Express |
| 2026-11-04 | RMoR and RMoW-external keep each rewritten row group whole (`-kept` arms) | as built they sorted rewritten rows in with the inserts and wrote 396–771 MB a write against RMoW-append's 161–292; kept, they write RMoW-append's bytes within 0.03% (W1) |
| 2026-11-06 | **MoR's updates read the rows they rewrite** (`updateRead` `row`); updates and inserts as two workloads | until then MoR's updates were blind, under-charging its writes in every campaign by 62–644 MB a write at the base point's settings |
| 2026-11-10 | The locate prunes row groups by each changed key; the sorted read skips dead row groups | MoR's write 12–16% less, the deferred merges' 12–17%; RMoR's sorted read 0.31 of before; the inserts moved under 3% |

## Measuring work

- **Thread CPU time does not measure work on the laptop.** The same read repeated back to back
  took up to twice the time, and twin reads (two arms reading exactly the same bytes) differed by
  up to 19%. User-mode instructions do: twin reads within 0.17%, the same phase in two runs within
  1.3% from the 10th write on (JIT warm-up before). So a phase's work is its instructions, and its
  time is that work at its arm's own pace (median cycles per instruction ÷ the run's clock;
  `phase_work`, lab/views/35_work.sql).
- **Pace is the noisy part, and differs by arm.** Cycles per instruction vary 3–7% across probes,
  and RMoW's writes ran at 1.12–1.13 times MoR's. Modeled time can decide a 25% margin, not 2%.
  Kernel time (1–6% of thread CPU) is left out, which slightly favours phases heavy in system
  calls, such as MoR's writes.
- **The laptop's thread clock runs 11.1% fast against its wall clock** (WSL2 on Hyper-V; manifest
  `host.thread_cpu_per_wall`). The lab divides by it (`run_clock`, since 2026-10-27). Cloud VMs'
  clocks agree with wall time.
- **The payload check was about 70% of a read's CPU**, so simulation runs turned `verifyRows` off
  and kept the id-and-version oracle. That oracle, a hash of each row's (id, version) inside the
  read's decode, was still the harness's work in a measured phase: how the JIT compiled it moved
  MoR's full scans by 6.4% of their instructions between two runs of the same reads (2026-11-17,
  campaign `diag-mor`). Since then measured runs check only row counts, and verification runs
  (`--verify`) check every row apart (README, Correctness). Measured without it (campaign
  `oracle-out`), the oracle had been 0.6% to 2.9% of a read's instructions, 7.7% of MoR's full scans
  in the run the JIT compiled it badly. Keep the harness's work per row group or per request,
  never per row, in a measured phase.

## Machines

- **havoc counts requests, bytes and held memory, never CPU.** Leaving a job's SMT siblings idle
  does not keep other users' threads off its cores: the scale campaign's longer jobs saw 1.6–2.9
  CPUs of foreign load. The runner now samples foreign load every second, and only a phase under
  0.25 CPUs of it throughout could decide anything about CPU (`phase_load`); even then, twins
  differed by 0.94–1.09. havoc allows no counters (`perf_event_paranoid` 4).
- **The laptop counts instructions, but only when idle.** Runs beside a lab ingest or build,
  another session's model checks or Maven tests ran at 2.5–4.4 GHz against 4.5–4.6, at a pace
  that is not the laptop's. Such runs were excluded and rerun (four times between 2026-11-08 and
  11-11); check each laptop run's clock against its campaign's before using its work. Nothing
  checks it automatically yet.
- **A cloud VM does the laptop's instructions 1.08–1.19 times slower** (`c7a.4xlarge`, 1.12
  overall). Laptop-priced times ran 4–12% low on S3 Express, evenly across policies, so rankings
  held; a twin of the configuration on the VM's own disk, with counters, priced every policy
  within 0.967–1.009 of measured. Azure's VMs expose no counters, so Azure is priced with S3
  Express twins' work (0.88–1.11 of measured), and the Azure SDK does its I/O on its own threads,
  so a phase's thread CPU there is 0.76–0.85 of S3 Express's for the same work.

## Pricing latency

- **The model.** A phase's time is its work plus its requests' latencies plus its bytes at a
  rate; split tasks run on the reader's threads, and a parallel read costs its critical path
  (`figures.replay`, lab/views/40_latency.sql). Replaying a request log against the same latency
  injected matched 280 of 281 one-thread phases within 10%: the model is applied consistently,
  which says nothing about whether its parameters are the store's. Writes are modeled serially,
  with no throttling, and only means.
- **endive's store profiles were wrong.** Azure's GETs took 9.5 ms (profile 38), the fork's
  appends 13.8 ms (88), its reads 5.7 ms/MiB (25); S3 Express's GETs 6.2 ms (2.5) and appends
  8.7 ms (13). The profiles also assumed a read rate equal to the write rate, and priced a HEAD as
  a GET and a DELETE as a PUT. So every cloud run starts with a spike (a latency test) from its own
  VM.
- **Price each request at its size** (`sized_latency`, lab/views/65_sized.sql). A profile of a
  latency at 64 KiB plus a slope mispriced the calibration's small GETs by +31% to +45% on S3
  Express and −43% for Azure's larger ones (C2, contradicted as registered), though CPU and store
  waits explained all but 0.6% of the phases' wall time. Priced by size, the base point came
  within 1–11% (row reader) and 2–14% (vectorized) of measured, and the two workloads within 4.0%
  and 12.3% (7.7% on S3 Express before the fix below).
- **A footer read is two GETs.** A writer reads a Parquet footer as parquet-java does, in two
  streamed GETs of one object (its last 8 bytes, then the footer). Until 2026-11-16 the lab's
  `request_ops` merged the two into one GET, so every simulated write was priced one request short
  for each file whose footer it read: at 128 MiB on S3 Express 0.27–0.60 s a write for RMoW-external,
  RMoR, RMoR-density and MoR (0.60–1.35 s on Azure Blob), reads not at all. Fixed, the two
  workloads' cloud runs priced from their own twins came within 4.0% on S3 Express, from 7.7%.
- **What the model misses:** a multipart PUT over 48 MiB overlaps the writing, but the model
  charges it after (RMoR's write priced at 1.14 of measured); a removed loop is not priced at its
  phase's pace (RMoR's position-by-position check ran at 16–18 billion instructions a second
  against its phase's 13–15, so removing it saved 72–89% of what was forecast); the locate's
  requests (the cut's 1,070 a write, region-cut.md); per-row-group work, so the work formula
  missed small reads with 4 or 8 MiB row groups by 19–44%.
- **The break-even framing** still serves: the request latency above which a design that sends
  fewer requests wins is its extra CPU and extra bytes over the requests it saves. It is why the
  store decides the trade-off and CPU (encoding) decides the region designs' writes.

## The stores

- **Each store receives exactly what the model counts** (C1, every calibration and cloud run
  since): every cloud run and twin sent what havoc's run of its configuration did, in every
  record. The exceptions are known: Azure's appendable create (a staged Append Blob and a rename,
  three requests, 112 ms) and S3 Express's `CreateSession`.
- **Azure block blobs read slower.** Iceberg uploads a file over 256 MiB in 4 MiB blocks, and a
  16 MiB range from one took 228 ms at the median against about 140 ms from a blob written in one
  request; the spike reads no block blob. It was A2's only failure (RMoR's write files), and will
  recur for any design with files that large (512 MiB compacted files).
- **Azure's store waits drift between runs** by −5.2 to +0.9 s a read (S3 Express's by at most
  0.04 s). Compare designs within one run; never put one run's design over another run's MoR.
- **An old footer's open-ended GET declares the rest of an appendable object** under the stock
  reader (379 MB on S3 Express, 191 MB on Azure, against 7 MB read). S3 Express aborts it at no
  visible cost; on Azure RMoW's time-travel reads took 3.7 s against at most 1.1 s. Bounded
  readers avoid it (README, I/O parity).
- **Azure reads whole 4 MiB blocks** before the fork's range fix (reader-changes.md), more bytes
  than the model counts.
- **Shared resources confound time:** nine S3 Express VMs on one bucket (AWS's 64-vCPU limit),
  Azure's runs in turn over a day. The two workloads' cloud run kept to four at once.

## The tuner

`analysis/tune.py` ranked write policies per read mix and store from havoc's counts and a formula
for their work fitted on the laptop's `windows` runs (T1–T4, 2026-11-08), then the picks ran in
the cloud (K1–K3, 2026-11-09). The workloads page still prices with it.
- **It picked as measured work would** (T2): in all 16 workloads, stores and mixes the pick cost
  at most 2.3% more than the cheapest finalist. In the cloud each of the 11 judged picks was the
  cheapest of the seven policies on measured time (K2).
- **The formula's median error was 3.7%**, every write and full read within 13.5%, but it fails
  outside its training range (above).
- **What it picked, before the locate by key:** on S3 Express, MoR compacted (updates) or MoR
  with 4 MiB row groups (inserts) below about 1–3 windows and one full read a write, and
  RMoW-append with 4 MiB row groups (updates) or RMoR sealed at 256 MiB (inserts) above. On Azure
  the policies cost within about 1.4% of one another at the median mix. The deferred merges never
  won on S3 Express: their windows read 27–36 MB but in 57–75 requests. On Azure MoR's update
  scan beat RMoW-append's 1.51 times: 4 MiB row groups take 238 GETs a scan against 84, at 62 ms
  a GET against 21 on S3 Express.
- **Limits:** one read at a time on one thread (parallel readers favour policies that send many
  requests), storage not in the cost, an 11-column table.
- Judged on 2026-11-09: the tuner and its cloud check measured the model's accuracy, not the
  designs' trade-off, which is the question. Proposals stay on the trade-off.

## The format and its layouts

These come from the mixed workload's local runs (1M rows, 4 MiB row groups) and hold whatever the
workload, since they are properties of the files. DESIGN.md has the format-level detail.
- **Old versions read like current ones** (H4): under every reader, an old version cost exactly
  what the current read of the same state cost, in all 1,200 pairs, given that each version is
  planned on its own split offsets, as Iceberg plans an old snapshot. History was recorded before
  compaction, so time travel across a compaction was never compared.
- **Dead bytes cost reads nothing** (H4): appendable files read 1.000–1.002 times their live
  bytes, because every GET is a bounded range.
- **Uncompacted, an appendable file keeps 7.8–8.3 times its live bytes** by the 60th write, and
  growing; RMoW compacts only for space, which pays only when storage weighs enough (about a day
  between writes at that table size).
- **Split files** (each version's regions in a new object, its footer in a footer-only object)
  read the same and wrote the same bytes as appendable ones, with one more PUT a changed file and
  1.73 fewer GETs a write, kept 1.4–1.5 times their live bytes, and stored 156 objects against 2.
  The GET saving was only parquet-java's two-request footer read, which a footer prefetch erases;
  `file_path` made footers about 20% larger. **Dropped 2026-10-27:** their footers point across
  objects through `ColumnChunk.file_path`, which the Parquet spec disowns and no reader assembles
  (readers given one return wrong rows silently). RMoR writes a rewritten region as a new file
  every reader reads.
- **Uniform updates make the region designs copy-on-write** (H2): 0.996–0.997 of CoW's bytes.
- **Row-group size:** the bytes rewritten follow where updates fall, not the row-group size. From
  4 to 1 MiB, RMoW wrote 23% less but its full read rose from 17 to 112 GETs. At 128 MiB RMoW
  wrote 22 times MoR's bytes a write (15 at 4 MiB), the same live bytes sitting in fewer, larger
  row groups. Iceberg's 1 MiB fetch-whole threshold is absolute, so small settings flattered
  MoR's request counts (43 a write at 4 MiB, 94 at 128).
- **Compaction:** the store decides whether MoR should compact (21–41% cheaper on S3 Standard at
  up to one full read a write, at most 8% on S3 Express). MoR's only threshold that ever fired was
  the small-file fraction, which the sweep did not vary. RMoW's reads depend on packing (19 to
  14.6 GETs), since tier rollovers leave small row groups sealed. Kept bytes assume old versions
  expire at once.
- **Layout against tail policy** (H3): RMoW against TailCoW (one appendable object against
  separate files) is the smaller difference in requests, and mostly in reads; in bytes the larger,
  from rewriting the row groups updates touch. The dv ablation put 70% to all of RMoW's extra
  bytes on that update handling, and most of the read savings on the single object.
- **TailCoW is MoR** when its seal is below the fanout times a write's file (every unit sealed on
  arrival), and about MoR at 16 MiB: a control only.

## The designs dropped

- **2026-10-23:** `rmow-*-dv` (RMoW with deletion vectors on each object's stable prefix), after
  its ablation (DESIGN.md).
- **2026-10-26/27:** new campaigns run five arms: MoR-v3, TailCoW-C, RMoR-C, RMoR-C with footers
  (added 2026-10-27), RMoW-C. MoR-v2 (file- and partition-scoped position deletes), CoW, TailCoW-B
  and RMoW-A and -B stay in the code but in no standing run. Tail policy C (tiered merging, fanout
  4) was kept as the one tail policy.
- **2026-10-27:** split files (above).
- **2026-11-02:** results name the designs MoR, RMoR, RMoW-external (`rmor-c-footer`) and
  RMoW-append (`rmow-c`), without the tail policy's letter. TailCoW and RMoR without the skipping
  reader went to an appendix; the reader changes are one sentence up front and Appendix A.
- **2026-11-05:** the deferred merges (`rmor-density`), after the sweeps showed RMoR's write never
  crossing TailCoW's along the update share, so a threshold had no starting point.
- **2026-11-04:** the region designs keep their rewritten row groups whole (`-kept`), as
  RMoW-append does.
- **2026-11-10:** the cut (`layout.cut`), at 128 MiB (region-cut.md).

## Results on the old workloads that should carry over

From the base point (16 MiB row groups, updates by region statement with inserts) and its sweeps,
measured on S3 Express and Azure Blob in 2026-10-30 to 11-04. MoR's updates were blind and the
locate unpruned, so the size of every gap is off; the directions match what the two workloads'
cloud run measured.
- **No region design's write caught MoR's at any setting:** RMoW-append's 2.1–8.2 times MoR's on
  S3 Express, 1.7–5.1 on Azure; its full read about 0.87–0.89 of MoR's. It repaid a write after
  about six full reads at the base point, after one at an update share of 0.3.
- **Where the time goes:** the region designs' writes are mostly CPU, chiefly parquet-java's
  encoding (61% of RMoW-append's); full reads mostly wait on the store (58–81%).
- **Changing more of a region helps the region designs** (U1): RMoW-append's write over MoR's fell
  from 4.18 to 2.13 on S3 Express as the update share rose from 0.03 to 0.3. RMoW-append's lead
  over RMoR narrows as the share rises, since both then write mostly the inserts (U2).
- **Bigger row groups widen the write gap** (G1): 4.18, 5.32 and 8.16 times MoR's at 16, 32 and
  64 MiB on S3 Express. That is the density hypothesis's starting point (region-cut.md).
- **With the check by range, RMoR's skipping full read** was 0.88–0.92 of MoR's on S3 Express and
  0.96–1.31 on Azure Blob, and its write cost more than RMoW-append's by ratio over MoR at every
  setting (R3).
- **Eight threads hide MoR's extra GETs:** every design's full read within 0.91–1.01 of MoR's on
  S3 Express on eight threads.
- **Not carried over:** the early simulated claim that RMoW wins outright on Azure Blob (0.50 of
  MoR's write) came from endive's Azure profile; measured, the region designs' writes took 1.5–3.1
  times MoR's there (the two workloads, and the base point).

## The 16 MiB workloads (retired 2026-11-16)

The two workloads on the 1 GB table at 16 MiB row groups, 30 writes each, simulated
(`lab/pages/workloads.md`) and their main points measured in the cloud on 2026-11-11
(`workloads-cloud.md`), on campaigns `workloads-ref`, `workloads-curves`, `tune` and `windows`. The
128 MiB results replaced them: an update region was about one 16 MiB row group, so copying a whole
row group cost about what copying the region would, no design's choice of what to copy mattered,
and no insert was merged into the row group its event time falls in. The pages are in git before
the commit that deleted them (`git log --diff-filter=D -- lab/pages/workloads.md`). Their numbers,
on S3 Express unless said, are directions for 128 MiB, not results:
- **Updates:** the region designs took 2.2–2.4 times MoR's simulated write (1.6–1.7 on Azure Blob)
  and read a 1% range up to 3.9 times as fast (6.7); measured, 2.3 times (1.5–1.7) and 2.8–3.9
  times as fast (3.8–6.4: L2). They wrote 39 times MoR's bytes, 6.6 GB over 30 writes against 0.17.
  MoR's range read grew 3.1 times from the 5th write to the 30th (measured 3.11; 3.93 on Azure
  Blob); the region designs' stayed level.
- **Compaction:** MoR compacted every 10 writes read a range in 0.39 s after 30 writes against
  1.40 never compacted, for no slower write with its compactions spread over the writes (4.01 s
  against 4.15: a compacted table is cheaper to update) and 0.18 GB more written; every 3 writes,
  0.38–0.47 s for 0.68 GB.
- **RMoR-density's θ** (1 to 0.06) moved its write little (4.8–5.4 s). At θ 1, never merging, it
  read a range faster than MoR (0.51 s against 0.92): the gain came from the new versions waiting in
  a delta row group per region, not from merging.
- **Row groups of 4 to 32 MiB:** whole copies got dearer as row groups grew (RMoW-append's write
  6.5 to 12.1 s, its range read 0.12 to 0.41 s), and on Azure Blob full scans faster (19.0 to 7.6 s,
  fewer and larger requests; 6.8 and 6.9 on S3 Express).
- **Inserts:** what mattered was whether late rows were merged. RMoR, merging its four newest
  writes' files every 4th write into a 47 MB file cut into 16 MiB row groups, read a range 2.1 times
  as fast as MoR for 0.36 s more a write (measured 2.1 times, 0.41 s: L3); RMoW-append, appending
  the merge as one row group, gained little (1.88 s against 1.92). With 99% of rows at most a write
  late MoR read a range in 0.69 s, near RMoR's 0.92 at five; merging four at a time up to 256 MiB
  read in 0.73 s for 1.02 s a write (0.91 and 0.85 up to 160 MiB). MoR compacted every 10 writes
  matched RMoR's reads for 0.47 s a write.
- **The locate by key** took 12–17% off MoR's and RMoR-density's writes there (the
  `workloads-curves` runs against `windows`').
- **Measured against simulated:** the simulation of the main points came within 4.0% (S3 Express)
  and 12.3% (Azure Blob) of the cloud runs, priced from each VM's own twin and latency test
  ([simulation-cloud.md](../lab/pages/simulation-cloud.md), which stands with its runs until a
  cloud run at 128 MiB replaces it).

## Harness bugs the runs found

Each fixed, most with a test that pins it:
- MoR-v2-file's deletes were not file-scoped: 807 delete files for 61 data files
  (`RunnerSmokeTest` asserts one live delete file per data file).
- Time travel planned old RMoW versions without their split offsets, so stock-reader time travel
  took 62% fewer GETs (150cc93; `RunnerSmokeTest.timeTravelReadsLikeTheCurrentRead`).
- RMoW's writer kept footers across writes, leaving out 3.5 of RMoW-C's 9.5 requests a write
  (README, I/O parity rule 6); it flipped H5's verdict on requests.
- Compaction packed row groups to twice the writer's size, faking a read advantage; split files
  never reached the old per-file trigger, one object 63% dead (rework 0564114).
- Split objects were planned as one task, then objects under 1 MiB fetched again for each split
  (`RequestLogTest`).
- Puffin's `create()` HEAD went uncounted in early builds (README, I/O parity rule 2).
- `bin/matrix.sh --store-root` replaced Azure roots with local paths; `docker/campaign.py`
  matched labels by prefix and gave one job's load samples to another (a14e010).
- The fuzzer (2026-11-08) found two RMoR-family bugs in settings no run used.
- RMoW-external's footprint (found 2026-11-12): a row group hidden by a footer appended to its
  file was counted reclaimable, though the file stays in use, so the state records of every run
  of `rmor-c-footer` and `rmor-c-footer-kept` before the fix overstate what expiring old snapshots
  frees (0.75 GB kept after 30 update writes, where RMoR, with the same data files, keeps 3.2).
  Requests, bytes and every other field were right (`RmorTest.footersHideTheOldRowGroups`).

## How results are reported

- Results pages show what a paper would: measured on the cloud stores, or simulated where the page
  says so; the model's accuracy and the controls (forecast against measured, the cloud runs' work
  against havoc's) go to calibration pages.
- The measured phase is "a write"; "batch" names only the workload's unit. Axes read "write
  latency (s)" and "read latency (s)"; panels "snapshot write latency", "1% range read latency",
  "full scan latency"; the x axis of anything over time "snapshot".
- One name per strategy, the same on every workload: MoR, MoR (compacted), RMoR, RMoW-external,
  RMoW-append, RMoR-density (2026-11-12; earlier records say "the deferred merges", "appending",
  "RMoR sealed").
- Read latency against write latency, one point per design with nothing compacted, with whiskers
  (the spread over writes, which is the table aging, not noise); latency and data written as
  snapshots are added; where the time goes by part (decode, encode, DVs and footers, other CPU,
  I/O); compaction and each knob in sections of their own. Data written, not stored over live
  bytes: no run expires a snapshot, so the store holds the initial load plus everything written.
- Enhancements over stock Iceberg go to every design, MoR included (the locate by key mostly
  helps MoR): the question is the designs' trade-off, not beating Iceberg's code.
