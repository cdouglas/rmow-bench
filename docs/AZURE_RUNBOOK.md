# Running on Azure (prepared, not yet run)

Every latency in the write-ups is simulated: requests, bytes and CPU from local runs, priced with
endive's store profiles (lab/pages/store-latency.md). S3 Express and Azure Blob sit on either
side of RMoW's break-even request latency (lab/pages/tradeoffs.md):
- **S3 Express** is near it, and its run ([S3X_RUNBOOK.md](S3X_RUNBOOK.md)) checks the model
  where CPU dominates.
- **Azure Blob** is far above it, so the simulation predicts that RMoW wins outright there. This
  run checks that, where requests dominate.

It answers five questions:
- **Requests:** does Azure receive what the model counts, through Iceberg's own ADLS FileIO?
- **Request latencies:** does Azure charge what the `azure` profile says? Its GET latency comes
  from a 2023 benchmark in West Europe, and its PUT latency is an estimate.
- **The model:** does a phase take its CPU plus its requests' latencies?
- **The prediction:** with exact reads, are RMoW-C's batches and reads faster than MoR-v3's?
- **Iceberg as shipped:** what do Iceberg's own ADLS reads cost each arm (below)?

## Iceberg's ADLS FileIO, as shipped

Iceberg 1.12.0's `ADLSFileIO`, on the Azure SDK it is built with, was read and then checked on
Azurite (`AzureStoreContract`).
- **Writes are what the model counts.** An output stream is the SDK's block-blob stream: one Put
  Blob up to 256 MiB, else 4 MiB blocks and a block list. `create()` adds the existence check, a
  HEAD. Files in these runs are far below 256 MiB.
- **Reads are not.** Iceberg's `ADLSInputStream` has two problems:
  - **Every stream opens with a GET.** Its constructor opens the SDK's input stream, which
    downloads the file's first 4 MiB chunk before any read. A footer read therefore costs two
    GETs and up to 4 MiB more.
  - **Ranged reads at an offset over-read.** `readFully(position, ...)` builds `new
    FileRange(position, position + length)`, whose second argument is a byte count, not an end
    offset. A read of 64 KiB at 6 MiB fetches 4 MiB.
- **RMoW needs Append Blobs.** `AppendBlock` extends only an Append Blob (409 `InvalidBlobType`
  otherwise), and Iceberg's output stream writes block blobs. So the appender creates RMoW's
  files: an empty Append Blob, then the first version appended (`AzureAppenders`). Its appends
  are one `AppendBlock` each, conditioned on the blob's length
  (`x-ms-blob-condition-appendpos`).

So `store.adlsReads` picks the reads, and the calibration runs once with each:
- **`exact`** (`ExactAdlsFileIO`) sends what the model counts, as S3FileIO does. A stream opens
  nothing until it reads, each ranged read is one GET of its range, and a sequential read is one
  streamed GET. It validates the simulation.
- **`iceberg`** is ADLSFileIO as shipped. It measures what Iceberg on Azure costs today.

Writes are ADLSFileIO's in both.

## What it needs

- **A storage account:** StorageV2, Standard performance, hot tier, with **hierarchical
  namespace** enabled (ADLS Gen2), and a container in it. Any region.
- **A VM in the account's region,** so the run measures the store, not the network. Something
  like 8 vCPUs and 16 GiB (D8s v5), with Java 17, Maven and git.
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
git clone git@github.com:cdouglas/rmow-bench.git && cd rmow-bench
git checkout <the commit the run is registered against>
mvn -q package -DskipTests
cat /proc/sys/kernel/perf_event_paranoid          # counters if <= 2 and the VM has a PMU

ROOT=abfss://<container>@<account>.dfs.core.windows.net/rmow-bench

# 1. The store's semantics on the real service: Iceberg's writes are a HEAD and a Put Blob, its
#    reads behave as described above, Append Blobs append at their length only, and a run of
#    every arm family matches the model (exact reads) or exceeds it (Iceberg's)
RMOW_AZURE_ROOT=$ROOT/test mvn test -Dtest=AzureTest

# 2. The spike: Put Blobs against Append Blocks from 64 KiB to 32 MiB, creates and HEADs, GETs of
#    Append Blobs of 1 to 1000 blocks, and Iceberg's reads of a 32 MiB file against exact ones.
#    Writes results/spike/azure-spike-<UTC>/spike.jsonl, deletes its objects
java -cp "target/rmow-bench-0.1.0-SNAPSHOT.jar:target/lib/*" \
  org.rmow.bench.micro.AzureSpike $ROOT/spike --reps 20

# 3. The calibration runs, one per read path: the root goes into a copy of each configuration
#    (theirs is a placeholder); requests logged for the replay
for reads in exact iceberg; do
  sed "s#abfss://REPLACE-container@REPLACE-account.dfs.core.windows.net/rmow-bench#$ROOT/calibration#" \
    configs/azure-calibration-$reads.json > /tmp/azure-calibration-$reads.json
  bin/run.sh /tmp/azure-calibration-$reads.json --log-requests
done
```

The configurations' labels stay `azure-calibration-exact` and `azure-calibration-iceberg`. The
manifests record the root, which is not a secret.

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
- **The Azurite twins,** `configs/azure-calibration-{exact,iceberg}-azurite.json`, run the same
  configurations through the real SDK and Iceberg code, against Azurite on this machine. Their
  `sdk_*` counts are what each cloud run must send.

```sh
docker run -d --rm --name rmow-azurite -p 10000:10000 \
  mcr.microsoft.com/azure-storage/azurite:3.37.0 \
  azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --loose
# Azurite's well-known development key, not a secret
export AZURE_STORAGE_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
bin/matrix.sh --build configs/azure-calibration-*-azurite.json configs/s3x-calibration-local.json --log-requests
docker stop rmow-azurite
```

## What the analysis will do

- **Twin match.** `steps_diff` between each cloud run and the local twin finds no difference in
  the model's requests or bytes. Each phase's `sdk_*` counts equal its Azurite twin's, retries
  aside.
- **A measured profile.** The spike gives:
  - medians and lognormal sigmas per operation and size, as `store_profiles` holds them;
  - a GET's extra latency per appended block, from the 1- to 1000-block blobs;
  - what Iceberg's shipped reads cost against exact ones, on the real service.
- **The model.** Replay the exact run with the measured profile (`sdk_latency` prices what was
  sent; `figures.replay` the logged requests), and compare each one-thread phase's simulated
  latency with its wall time (`wall_ns`).
- **The prediction.** RMoW-C against MoR-v3, a batch and a full read (`cached`, one thread), by
  wall time, in the exact run; and what Iceberg's reads change, in the other.

## Hypotheses (DRAFT, to register before the run)

They overlap the held H1 draft's L1–L4 and the S3 Express draft's X1–X4 in docs/HYPOTHESES.md,
and should be registered with them, in one dated section, once reviewed. The predicted numbers
come from the Azurite twins priced with the `azure` profile (`sdk_latency`; below).

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

- **Z1: the model counts what Azure receives, given the read path.** Every phase's `sdk_*`
  counts match its Azurite twin's exactly, retries aside. Under exact reads they equal the model's:
  a PUT is one Put Blob, or, for a new RMoW file, an Append Blob create and one Append Block; an
  append is one Append Block.
- **Z2: the profile holds.** Median GET, Put Blob and Append Block latencies (64 KiB) are within
  50% of the `azure` profile's 38, 45 and 87.6 ms.
- **Z3: appended blocks cost reads little.** A GET of the last 64 KiB of a 1,000-block Append Blob
  takes at most 100 ms more than one of a 1-block blob.
- **Z4: latency is CPU plus requests.** With the measured profile, each one-thread phase of the
  exact run is within 25% of its wall time. This is the H1 draft's L4, on Azure.
- **Z5: with exact reads, RMoW-C beats MoR-v3 where requests dominate.** In the exact run,
  RMoW-C's mean batch and mean full read (`cached`, one thread) both take less wall time than
  MoR-v3's. Predicted: 0.37× and 0.59×.
- **Z6: Iceberg's shipped reads cost RMoW more than MoR.** In the Iceberg run, RMoW-C's full read
  is a larger fraction of MoR-v3's than in the exact run, predicted 0.91× against 0.59×. Its
  ranged reads at offsets over-read most: 23× the bytes it needs, against MoR-v3's 17×.
