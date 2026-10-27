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

- **A directory bucket** in one Availability Zone: `<name>--<az id>--x-s3`, for example
  `rmow-bench--use1-az4--x-s3` in `us-east-1`.
- **An EC2 instance in the same zone**, so the run measures the store, not the network. Something
  like 8 vCPUs and 16 GiB, with Java 17, Maven and git, and the atomicio fork's build in its local
  Maven repository (built there, or `~/.m2/repository/org/apache/iceberg` copied from here).
  - **Instance type and counters:** instruction counters need a PMU, which only some EC2 types
    expose (full-socket and metal sizes) and `kernel.perf_event_paranoid` at most 2. Without them
    each manifest says why (`host.thread_counters`), and CPU falls back to thread time. That is
    enough here: this run checks request latencies, not CPU.
- **Credentials** from the instance's role: `s3express:CreateSession`, and object read, write and
  delete on the bucket. The SDK's default chain finds them; no configuration holds them.
- **Cost.** The workload is small: about 10,000 requests for the calibration run and a few
  thousand for the spike, with a few GiB stored for minutes. The instance-hours dominate.

## Steps, on the instance

```sh
# the fork (its atomicio branch, at the commit the run is registered against), unless its build
# was copied over
git clone -b atomicio git@github.com:cdouglas/iceberg.git
(cd iceberg && git checkout <fork commit> && ./gradlew publishToMavenLocal -x test -x integrationTest)
git clone git@github.com:cdouglas/rmow-bench.git && cd rmow-bench
git checkout <the commit the run is registered against>
mvn -q package -DskipTests
cat /proc/sys/kernel/perf_event_paranoid          # counters if <= 2 and the instance has a PMU

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
#    from 64 KiB to 32 MiB, interleaved; and GETs of objects of 1 to 1000 appended parts. Writes
#    results/spike/s3x-spike-<UTC>/spike.jsonl, deletes its objects
java -cp "target/rmow-bench-0.1.0-SNAPSHOT.jar:target/lib/*" \
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
the build the cloud run uses, so its requests and bytes are what the cloud run must match.

## What the analysis will do

- **Twin match.** `steps_diff` between the cloud run and its local twin should find no difference
  in requests or bytes. `CreateSession` is counted apart (`sdk_sessions`); it costs, but the model
  does not hold it.
- **A measured profile.** The spike gives:
  - medians and lognormal sigmas per operation and size, as `store_profiles` holds them;
  - what the fork's conditions cost: `append_sao` against `append`, `create_sao` against `put`;
  - a GET's extra latency per appended part, from the 1-, 10-, 100- and 1000-part objects.
- **Where the time went.** `phase_breakdown` splits each phase into encoding, decoding,
  footers, deletes, other CPU, the store client's CPU, waiting on the store, and other waiting.
  It needs the VM's two clocks to agree: its manifest's `host.thread_cpu_per_wall` should be 1.
- **The model.** Replay the calibration run with the measured profile, and compare each one-thread
  phase's simulated latency with its measured wall time (`wall_ns`).

## Hypotheses (DRAFT, to register before the run)

They overlap the held H1 draft's L1–L4 in docs/HYPOTHESES.md, and should be registered with it,
in one dated section, once reviewed.
- **X1: the model counts what S3 Express receives.** Every phase's requests and bytes match the
  local twin's exactly. `CreateSession` is the only request the model does not hold.
- **X2: the profile holds.** Median GET, PUT and append latencies (64 KiB) are within 50% of the
  `s3x` profile's 2.5, 6.5 and 13 ms.
- **X3: appended parts cost reads little.** A GET of the last 64 KiB of a 1,000-part object
  takes at most 100 ms more than one of a 1-part object, which is the sweep's 0.1 ms per part.
- **X4: latency is CPU plus requests.** With the measured profile, each one-thread phase's
  simulated latency is within 25% of its wall time. This is the H1 draft's L4, on one run.
- **X5: the fork's conditions cost nothing measurable.** At every size, the median `append_sao`
  is within 10% of the median `append`, and the median `create_sao` within 10% of the median
  `put`. The requests are the same; only an ETag or absence condition, and the checksum
  `prepare()` computes, differ.
