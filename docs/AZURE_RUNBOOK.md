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
  - **Any VM size:** the run measures wall time, so it needs no instruction counters. It needs the
    thread CPU clock to agree with the wall clock, which the manifest records
    (`host.thread_cpu_per_wall`, 1 where they agree).
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
#    32 MiB, interleaved; GETs of the Put Blobs' blobs by size, whole and as ranges of a 32 MiB
#    one; creates (an empty Append Blob, the store's own appendable file, RMoW's atomic one) and
#    HEADs; GETs of Append Blobs of 1 to 1000 blocks; and Iceberg's ranged read of a 32 MiB file
#    against the SDK's. Writes results/spike/azure-spike-<UTC>/spike.jsonl,
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
  - the read rate per MiB, the slope of `get_size` and `get_range` over 64 KiB to 32 MiB: the
    profile assumes it equal to the write rate;
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

## Hypotheses

C1–C4 in docs/HYPOTHESES.md (before the calibration runs, drafted 2026-10-27): the model counts
what Azure receives (C1), a phase takes its CPU plus its requests' latencies (C2), the trade-off
falls where the twin puts it (C3: the region arms' batches and full reads faster than MoR-v3's),
and CPU is a third or less of every phase here (C4). The spike's numbers are measured, not
judged: request latencies by operation and size, the read rate per MiB, the extra latency per
appended block, `append_sao` and `create_sao` against the store's own, and Iceberg's ranged read
against the SDK's.

### The twins' predictions

The predictions come from the Azurite twin on the build the cloud run uses, 2026-10-27 (build
snapshot `20261027T175613Z-1145f89a36fa-dirty`, which the commit "RMoR-C with footers, a variant
that hides old row groups without DVs" holds; the atomicio fork at af875ccfd1). The cloud run
checks out that commit, or a later one that changes no code. It and the local twin are campaign
`twins`:
- `azure-calibration-azurite-a96fd56de2f0/20261027T175614Z`;
- `s3x-calibration-local-8433ccfd4620/20261027T175847Z`.

The query is `sdk_latency`, profile `azure`. Means are over batches 1–20 and the full-read probes
after the initial load, under `cached` (RMoR-C also under `cached-skip`). CPU is each twin's
modeled time on this laptop (`phase_work`), on the wall clock (`run_clock`).

**What it sends.** Every phase sent what the model counts, 10,222 requests in all, with no
retries, but for RMoR-C with footers' creates. Its data files are appendable, so each batch's
is created through the appender: on Azurite an empty Append Blob, a HEAD and an Append Block,
where the model counts one PUT (40 requests more over 20 batches); on Azure a staged Append Blob,
an Append Block and a rename. The twin's model requests and bytes equal the local twin's in all
862 records, and the other arms' equal the earlier twins' in every record they share.

| Arm | A batch: requests | of them PUTs and appends | MB read | MB written | A full read: requests | MB read |
|---|---|---|---|---|---|---|
| MoR-v3 | 31.4 | 2.0 | 0.64 | 0.08 | 46.0 | 7.2 |
| TailCoW-C | 15.1 | 2.0 | 0.62 | 0.13 | 28.0 | 7.0 |
| RMoR-C | 13.7 | 1.4 | 1.56 | 1.02 | 33.0 (`cached-skip`: 26.0) | 9.0 (7.0) |
| RMoR-C with footers | 10.2 | 1.4 | 1.54 | 1.02 | 24.0 | 7.0 |
| RMoW-C | 7.3 | 1.0 | 0.80 | 0.84 | 24.5 | 6.9 |

**Simulated time on the `azure` profile (ms):**

| Arm | A batch | A full read |
|---|---|---|
| MoR-v3 | 1,561 | 2,565 |
| TailCoW-C | 785 | 1,681 |
| RMoR-C | 864 | 2,012 (`cached-skip`: 1,587) |
| RMoR-C with footers | 843 | 1,478 |
| RMoW-C | 563 | 1,508 |
| **RMoW-C ÷ MoR-v3** | **0.36** | **0.59** |

Earlier twins, with the same requests and bytes for the arms they share, gave 0.36–0.37 and 0.59
(EXPERIMENTS.md, Retired runs); what moves between runs is the modeled CPU.

**Iceberg's reads as shipped,** for reference (measuring them needs a build before 2026-10-26),
from the release build's twin
(`azure-calibration-iceberg-azurite-bbc35d39598f/20261026T055554Z`, retired 2026-10-27; its
numbers are kept here and in EXPERIMENTS.md, Retired runs): 47.1 and 79.5 GETs and 7.4
and 122.9 MB for MoR-v3's batch and full read, 11.6 and 49.0 GETs and 24.1 and 157.9 MB for
RMoW-C's. Simulated, RMoW-C ÷ MoR-v3 was 0.54 for a batch and 0.91 for a full read.
