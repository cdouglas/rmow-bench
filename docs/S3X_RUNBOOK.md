# Running on S3 Express (prepared, not yet run)

Every latency in the write-ups is simulated: requests, bytes and CPU from local runs, priced with
endive's store profiles (lab/pages/store-latency.md). The model has been checked only against
itself, by injecting its own latencies (`sim-validate-s3x`). This run anchors it to the real
service. It answers four questions:
- **Request latencies:** does S3 Express charge what the profile says for GETs, PUTs and appends?
- **The fork's writes:** do RMoW's appends and creates, through the atomicio fork's
  `SupportsAtomicOperations` (conditioned on an ETag, or on absence), cost what the store's own
  do?
- **Appended parts:** what does a GET pay for an object made of many appends? The sweep assumed up
  to 0.1 ms per part, and nothing has measured it.
- **The model:** does a phase take its CPU plus its requests' latencies?

## What it needs

[deploy/](../deploy/README.md) provisions all of it with Terraform (`CLOUD=aws`) and runs the
steps below on the instance; what follows is what it does, by hand.

- **A directory bucket** in one Availability Zone: `<name>--<az id>--x-s3`, for example
  `rmow-bench--use1-az4--x-s3` in `us-east-1`.
- **An EC2 instance in the same zone**, so the run measures the store, not the network. Something
  like 8 vCPUs and 16 GiB, with Java 17, Maven and git, and the atomicio fork's build in its local
  Maven repository (built there, or `~/.m2/repository/org/apache/iceberg` copied from here).
  - **Any instance type:** the run measures wall time, so it needs no instruction counters. It
    needs the thread CPU clock to agree with the wall clock, which the manifest records
    (`host.thread_cpu_per_wall`, 1 where they agree).
- **Credentials** from the instance's role: `s3express:CreateSession`, and object read, write and
  delete on the bucket. The SDK's default chain finds them; no configuration holds them.
- **Cost.** The workload is small: about 10,000 requests for the calibration run and a few
  thousand for the spike. The spike's writes reach 128 MiB, so it uploads about 20 GiB and keeps
  about 20 GiB stored while it runs (tens of minutes at most; S3 Express charges $0.0032 per GB
  uploaded past a request's first 512 KB, so about $0.07). The instance-hours dominate.

## Steps, on the instance

```sh
# the fork (its atomicio branch, at af875ccfd1, the twins' build), unless its build was copied
# over. Its version comes from the release tag, which the fork on GitHub does not carry: without
# the tag it would not build 1.12.0-atomicio-SNAPSHOT
git clone -b atomicio https://github.com/cdouglas/iceberg.git
(cd iceberg && git checkout -B atomicio af875ccfd1 \
  && git fetch --no-tags https://github.com/apache/iceberg.git tag apache-iceberg-1.12.0 \
  && ./gradlew publishToMavenLocal -x test -x integrationTest)
# the calibration run's build: 9a69028, where C1-C4 were registered, whose code is the twins'
# (f0ae9ae's); not main, whose code has changed since
git clone git@github.com:cdouglas/rmow-bench.git && cd rmow-bench
git checkout 9a69028
mvn -q package -DskipTests
# the spike's build: main, whose spike reaches 128 MiB (since 2026-10-27); the calibration run
# keeps the registered build
git worktree add ../rmow-spike main && (cd ../rmow-spike && mvn -q package -DskipTests)

ROOT=s3://<bucket>--<az id>--x-s3/rmow-bench
export AWS_REGION=<region>

# 1. The fork's atomic operations on the real service, then RMoW's appends through them: its
#    contract (creates, replaces and appends pinned to a version; races with one winner), an
#    append at the object's length lands, one elsewhere fails, a create or an append sends no HEAD,
#    and a run of every arm family matches the model
#    The fork's suite skips without keys in the environment (AWS_ACCESS_KEY_ID and
#    AWS_SECRET_ACCESS_KEY; the instance role is not enough), and takes its bucket from
#    S3_EXPRESS_TEST_BUCKET
(cd ../iceberg && S3_EXPRESS_TEST_BUCKET=<bucket>--<az id>--x-s3 \
  ./gradlew :iceberg-aws:test --tests org.apache.iceberg.aws.s3.S3ExpressFileIOAtomicTest)
RMOW_S3X_ROOT=$ROOT/test mvn test -Dtest=S3ExpressTest

# 2. The spike: the store's own appends against RMoW's (the fork's), PUTs against RMoW's creates,
#    from 64 KiB to 128 MiB, interleaved; GETs of the PUTs' objects by size, whole and as ranges of
#    a 128 MiB one; and GETs of objects of 1 to 1000 appended parts. Writes
#    results/spike/s3x-spike-<UTC>/spike.jsonl, deletes its objects
java -cp "../rmow-spike/target/rmow-bench-0.1.0-SNAPSHOT.jar:../rmow-spike/target/lib/*" \
  org.rmow.bench.micro.S3ExpressSpike $ROOT/spike --reps 20

# 3. The calibration run: the bucket goes into a copy of the configuration (its root is a
#    placeholder); requests logged for the replay
sed "s#s3://REPLACE-bucket--use1-az4--x-s3/rmow-bench#$ROOT/calibration#" \
  configs/s3x-calibration.json > /tmp/s3x-calibration.json
bin/run.sh /tmp/s3x-calibration.json --log-requests
```

The configuration's label stays `s3x-calibration`. The manifest records the bucket, which is not
a secret.

Each append is one PutObject at the snapshot's length, conditioned on its ETag. Each new
appendable file is one PutObject conditioned on its absence (`If-None-Match: *`), with no lookup:
the fork sends what the model counts.

## Bringing it back

```sh
# here
rsync -av '<instance>:rmow-bench/results/s3x-calibration-*' '<instance>:rmow-bench/results/spike' results/
lab ingest
```

The local twin, `configs/s3x-calibration-local.json`, differs only in the store. It is rerun on
the build the cloud run uses, so its requests and bytes are what the cloud run must match. The
current one is `s3x-calibration-local-8433ccfd4620/20261027T175847Z` (campaign `twins`; build
snapshot `20261027T175613Z-1145f89a36fa-dirty`, which the commit "RMoR-C with footers, a variant
that hides old row groups without DVs" holds), rerun if the code changes before the run. On the
`s3x` profile it simulates (ms; means over batches 1–20 and the full reads after the initial load,
`cached`, CPU on the wall clock):

| Arm | A batch | A full read |
|---|---|---|
| MoR-v3 | 133 | 398 |
| TailCoW-C | 92 | 356 |
| RMoR-C | 205 | 409 (`cached-skip`: 315) |
| RMoR-C with footers | 190 | 302 |
| RMoW-C | 182 | 317 |

CPU is 66–74% of the region arms' batches (MoR-v3's 20%, TailCoW-C's 36%) and 49–58% of every
full read, so this run checks the model where CPU decides. Across three twin runs with the same
requests and bytes, these times moved by up to 12% against MoR-v3's: the CPU's pace (cycles per
instruction) varies from run to run.

## What the analysis will do

- **Twin match.** `steps_diff` between the cloud run and its local twin should find no difference
  in requests or bytes. `CreateSession` is counted apart (`sdk_sessions`); it costs, but the model
  does not hold it.
- **A measured profile.** The spike gives:
  - medians and lognormal sigmas per operation and size, as `store_profiles` holds them;
  - what the fork's conditions cost: `append_sao` against `append`, `create_sao` against `put`;
  - the read rate per MiB, the slope of `get_size` and `get_range` over 64 KiB to 128 MiB: the
    profile assumes it equal to the write rate, and it is the largest I/O term of a full read;
  - the write rate per MiB, and whether it holds at a row group of Iceberg's default size: at
    128 MiB row groups an RMoW-C append is about 100 MiB, and a full read's GETs reach 127 MiB
    (the scale campaign, lab/pages/scale.md);
  - a GET's extra latency per appended part, from the 1-, 10-, 100- and 1000-part objects.
- **Where the time went.** `phase_breakdown` splits each phase into encoding, decoding,
  footers, deletes, other CPU, the store client's CPU, waiting on the store, and other waiting.
  It needs the VM's two clocks to agree: its manifest's `host.thread_cpu_per_wall` should be 1.
- **The model.** Replay the calibration run with the measured profile, and compare each one-thread
  phase's simulated latency with its measured wall time (`wall_ns`).

## Hypotheses

C1–C4 in docs/HYPOTHESES.md (before the calibration runs, registered 2026-10-27): the model counts
what S3 Express receives (C1), a phase takes its CPU plus its requests' latencies (C2), the
trade-off falls where the twin puts it (C3: the region arms' batches slower than MoR-v3's, their
full reads faster), and CPU is the bottleneck of the region arms' batches here (C4). The spike's
numbers are measured, not judged: request latencies by operation and size, the read rate per MiB,
the extra latency per appended part, and `append_sao` and `create_sao` against `append` and `put`.
