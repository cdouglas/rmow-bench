# Running on Azure (prepared, not yet run)

Every latency in the write-ups is simulated: requests, bytes and CPU from local runs, priced with
endive's store profiles (lab/pages/store-latency.md). S3 Express and Azure Blob sit on either
side of RMoW's break-even request latency (lab/pages/tradeoffs.md):
- **S3 Express** is near it, and its run ([S3X_RUNBOOK.md](S3X_RUNBOOK.md)) checks the model
  where CPU dominates.
- **Azure Blob** is far above it, so the simulation predicts that RMoW wins outright there. This
  run checks that, where requests dominate.

It answers five questions:
- **Requests:** does Azure receive what the model counts, through the fork's ADLS FileIO?
- **Request latencies:** does Azure charge what the `azure` profile says? Its GET latency comes
  from a 2023 benchmark in West Europe, and its PUT latency is an estimate.
- **The fork's writes:** do RMoW's appends and creates, through the atomicio fork's
  `SupportsAtomicOperations`, cost what the store's own do?
- **The model:** does a phase take its CPU plus its requests' latencies?
- **The prediction:** are RMoW-C's batches and reads faster than MoR-v3's?

## The fork's ADLS FileIO

Every arm runs on the atomicio fork's `ADLSFileIO`, on the Azure SDK Iceberg 1.12.0 is built with.
It was read and then checked on Azurite (`AzureStoreContract`).
- **Writes are what the model counts.** An output stream is the SDK's block-blob stream: one Put
  Blob up to 256 MiB, else 4 MiB blocks and a block list. `create()` adds the existence check, a
  HEAD. Files in these runs are far below 256 MiB.
- **Reads send the model's requests.** A stream opens nothing until it reads, and a ranged read is
  one GET of exactly its range. A sequential read downloads the SDK's 4 MiB blocks whole, so it
  reads more bytes than the model counts: about 15% more for RMoW in the test runs. Iceberg 1.12.0
  as shipped does worse, and the fork fixes it:
  - **every stream opens with a GET:** its constructor opens the SDK's input stream, which
    downloads the file's first 4 MiB chunk before any read, so a footer read costs two GETs and up
    to 4 MiB more;
  - **ranged reads at an offset over-read:** `readFully(position, ...)` builds `new
    FileRange(position, position + length)`, whose second argument is a byte count, not an end
    offset, so a read of 64 KiB at 6 MiB fetches 4 MiB.
- **RMoW needs Append Blobs.** `AppendBlock` extends only an Append Blob (409 `InvalidBlobType`
  otherwise), and Iceberg's output stream writes block blobs. So RMoW creates its files through
  the fork's atomic operations (`AtomicAppender`): a temporary Append Blob, one Append Block, and
  a rename onto the file with `If-None-Match: *`. The store's own way, an empty Append Blob and its
  first block, is one request fewer, but an Append Blob is created empty, so only the rename makes
  the create all or nothing. Each append is one `AppendBlock`, conditioned on the blob's length
  (`x-ms-blob-condition-appendpos`) and its ETag.

What Iceberg's reads as shipped cost on Azure was a second calibration run (`adlsReads:
iceberg`) on builds before 2026-10-26. It needs such a build now, and is optional.

## What it needs

- **A storage account:** StorageV2, Standard performance, hot tier, with **hierarchical
  namespace** enabled (ADLS Gen2), and a container in it. Any region.
- **A VM in the account's region,** so the run measures the store, not the network. Something
  like 8 vCPUs and 16 GiB (D8s v5), with Java 17, Maven and git, and the atomicio fork's build in
  its local Maven repository (built there, or `~/.m2/repository/org/apache/iceberg` copied from
  here).
  - **Counters:** instruction counters need a PMU, which many cloud VM sizes do not expose, and
    `kernel.perf_event_paranoid` at most 2. Without them each manifest says why
    (`host.thread_counters`), and CPU falls back to thread time. That is enough here: this run
    checks requests and latencies, and each phase is compared with its own CPU.
- **Credentials** from the VM's managed identity, with **Storage Blob Data Contributor** on the
  account. The SDK's default chain finds them; no configuration holds them.
- **Cost.** The workload is small: about 10,000 requests per calibration run, and a few thousand
  for the spike, with a few GiB stored for minutes. The VM-hours dominate.

## Steps, on the VM

```sh
# the fork (its atomicio branch, at the commit the run is registered against), unless its build
# was copied over
git clone -b atomicio git@github.com:cdouglas/iceberg.git
(cd iceberg && git checkout <fork commit> && ./gradlew publishToMavenLocal -x test -x integrationTest)
git clone git@github.com:cdouglas/rmow-bench.git && cd rmow-bench
git checkout <the commit the run is registered against>
mvn -q package -DskipTests
cat /proc/sys/kernel/perf_event_paranoid          # counters if <= 2 and the VM has a PMU

ROOT=abfss://<container>@<account>.dfs.core.windows.net/rmow-bench

# 1. The fork's atomic operations on the real service, then the store's semantics: its contract
#    (creates, replaces and appends pinned to a version; races with one winner); Iceberg's writes
#    are a HEAD and a Put Blob, its reads ask for exactly their ranges, Append Blobs append at
#    their length only, and a run of every arm family matches the model. The fork's suite takes a
#    SAS token, not the managed identity: AZURE_STORAGE_ACCOUNT, AZURE_TEST_CONTAINER and
#    AZURE_STORAGE_SAS_TOKEN (it skips without them)
(cd ../iceberg && ./gradlew :iceberg-azure:test --tests org.apache.iceberg.azure.adlsv2.AdlsFileIOAtomicTest)
RMOW_AZURE_ROOT=$ROOT/test mvn test -Dtest=AzureTest

# 2. The spike: Put Blobs, the store's own Append Blocks and RMoW's (the fork's), from 64 KiB to
#    32 MiB, interleaved; creates (an empty Append Blob, the store's own appendable file, RMoW's
#    atomic one) and HEADs; GETs of Append Blobs of 1 to 1000 blocks; and Iceberg's ranged read
#    of a 32 MiB file against the SDK's. Writes results/spike/azure-spike-<UTC>/spike.jsonl,
#    deletes its objects
java -cp "target/rmow-bench-0.1.0-SNAPSHOT.jar:target/lib/*" \
  org.rmow.bench.micro.AzureSpike $ROOT/spike --reps 20

# 3. The calibration run: the root goes into a copy of the configuration (its root is a
#    placeholder); requests logged for the replay
sed "s#abfss://REPLACE-container@REPLACE-account.dfs.core.windows.net/rmow-bench#$ROOT/calibration#" \
  configs/azure-calibration.json > /tmp/azure-calibration.json
bin/run.sh /tmp/azure-calibration.json --log-requests
```

The configuration's label stays `azure-calibration`. The manifest records the root, which is not
a secret.

Each append is one Append Block. Each new appendable file is a staged Append Blob, an Append Block
and a rename, with no lookup.

## Bringing it back

```sh
# here
rsync -av '<vm>:rmow-bench/results/azure-calibration-*' '<vm>:rmow-bench/results/spike' results/
lab ingest
```

## The twins

Two kinds of twin, both rerun on the build the cloud runs use:
- **The local twin** is `configs/s3x-calibration-local.json`, the S3 Express run's twin. It has the
  same workload and arms. Its requests and bytes are the model's, which both cloud runs record
  too (`gets`, `puts`, ...).
- **The Azurite twin,** `configs/azure-calibration-azurite.json`, runs the same configuration
  through the real SDK and the fork's code, against Azurite on this machine. Its `sdk_*` counts are
  what the cloud run must send, but for creates: Azurite cannot rename, so each create there is an
  empty Append Blob, a HEAD and an Append Block (`EmulatorAppenders.appendBlobs`), where Azure's is
  a staged Append Blob, an Append Block and a rename. So the cloud run's HEADs are the twin's less
  its Append Blob creates, and its other requests (renames) equal them.

```sh
docker run -d --rm --name rmow-azurite -p 10000:10000 \
  mcr.microsoft.com/azure-storage/azurite:3.37.0 \
  azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --loose
# Azurite's well-known development key, not a secret
export AZURE_STORAGE_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
bin/matrix.sh --build configs/azure-calibration-azurite.json configs/s3x-calibration-local.json --log-requests
docker stop rmow-azurite
```

## What the analysis will do

- **Twin match.** `steps_diff` between the cloud run and the local twin finds no difference in
  the model's requests or bytes. Each phase's `sdk_*` counts equal its Azurite twin's, retries
  and the creates' HEADs and renames aside (above).
- **A measured profile.** The spike gives:
  - medians and lognormal sigmas per operation and size, as `store_profiles` holds them;
  - what the fork's writes cost: `append_sao` against `append`, `create_sao` against `create_own`;
  - a GET's extra latency per appended block, from the 1- to 1000-block blobs;
  - what Iceberg's ranged read costs against the SDK's alone (`iceberg_mid`, `get_mid_32m`).
- **Where the time went.** `phase_breakdown` splits each phase into encoding, decoding,
  footers, deletes, other CPU, the store client's CPU, waiting on the store, and other waiting.
  It needs the VM's two clocks to agree: its manifest's `host.thread_cpu_per_wall` should be 1.
- **The model.** Replay the run with the measured profile (`sdk_latency` prices what was sent;
  `figures.replay` the logged requests), and compare each one-thread phase's simulated latency
  with its wall time (`wall_ns`).
- **The prediction.** RMoW-C against MoR-v3, a batch and a full read (`cached`, one thread), by
  wall time.

## Hypotheses (DRAFT, to register before the run)

They overlap the held H1 draft's L1–L4 and the S3 Express draft's X1–X5 in docs/HYPOTHESES.md,
and should be registered with them, in one dated section, once reviewed. The predicted numbers
come from the Azurite twins priced with the `azure` profile (`sdk_latency`; below).

**The predictions below come from twins on the release build** (2026-10-26, before the fork
became the only build). Their exact reads (`ExactAdlsFileIO`) sent the requests the fork's reads
send now, though fewer bytes for sequential reads. They are to be recomputed from
`azure-calibration-azurite` rerun on the build the cloud run uses, and the Iceberg-as-shipped
columns dropped unless that run is made too.

### The twins' predictions

The Azurite twins of 2026-10-26 (campaign `azure-twins`, build snapshot
`20261026T055316Z-9713a7c52d0c-dirty`) are:
- `azure-calibration-exact-azurite-fa06fdb7634c/20261026T055317Z`;
- `azure-calibration-iceberg-azurite-bbc35d39598f/20261026T055554Z`.

The query is `sdk_latency`, profile `azure`. Means are over batches 1–20 and the full-read probes
after the initial load, under `cached`. CPU is each twin's modeled time on this laptop.

**What each read path sends:**

| Arm, phase | GETs: model → exact → Iceberg's | MB read: model → exact → Iceberg's |
|---|---|---|
| MoR-v3, a batch | 29.4 → 29.4 → 47.1 | 0.64 → 0.64 → 7.4 |
| RMoW-C, a batch | 6.3 → 6.3 → 11.6 | 0.80 → 0.80 → 24.1 |
| MoR-v3, a full read | 46.0 → 46.0 → 79.5 | 7.2 → 7.2 → 122.9 |
| RMoW-C, a full read | 24.5 → 24.5 → 49.0 | 6.9 → 6.9 → 157.9 |

With exact reads every phase sends what the model counts; writes are the same under both.

**Simulated time on the `azure` profile (ms):**

| Arm | A batch, exact | A full read, exact | A batch, Iceberg's | A full read, Iceberg's |
|---|---|---|---|---|
| MoR-v3 | 1,556 | 2,585 | 2,556 | 6,935 |
| TailCoW-C | 786 | 1,704 | 1,250 | 5,627 |
| RMoW-B | 454 | 1,408 | 964 | 5,769 |
| RMoW-C | 573 | 1,514 | 1,370 | 6,282 |
| **RMoW-C ÷ MoR-v3** | **0.37** | **0.59** | **0.54** | **0.91** |

### Draft hypotheses

- **Z1: the model counts what Azure receives.** Every phase's `sdk_*` counts match its Azurite
  twin's, retries and the creates' HEADs and renames aside. They equal the model's: a PUT is one
  Put Blob, or, for a new RMoW file, a staged Append Blob, one Append Block and a rename; an
  append is one Append Block.
- **Z2: the profile holds.** Median GET, Put Blob and Append Block latencies (64 KiB) are within
  50% of the `azure` profile's 38, 45 and 87.6 ms.
- **Z3: appended blocks cost reads little.** A GET of the last 64 KiB of a 1,000-block Append Blob
  takes at most 100 ms more than one of a 1-block blob.
- **Z4: latency is CPU plus requests.** With the measured profile, each one-thread phase of the
  run is within 25% of its wall time. This is the H1 draft's L4, on Azure.
- **Z5: RMoW-C beats MoR-v3 where requests dominate.** In the calibration run, RMoW-C's mean
  batch and mean full read (`cached`, one thread) both take less wall time than MoR-v3's.
  Predicted: 0.37× and 0.59×.
- **Z6: Iceberg's shipped reads cost RMoW more than MoR** (only with a run on a build before
  2026-10-26). In the Iceberg run, RMoW-C's full read is a larger fraction of MoR-v3's than in the
  exact run, predicted 0.91× against 0.59×. Its ranged reads at offsets over-read most: 23× the
  bytes it needs, against MoR-v3's 17×.
- **Z7: the fork's conditions cost nothing measurable.** At every size, the median `append_sao`
  is within 10% of the median `append`. A `create_sao` costs about one request more than a
  `create_own`: its median is within 10% of `create_own`'s plus the median of a rename.
