#!/usr/bin/env bash
#
# The runbooks' steps on the VM (docs/S3X_RUNBOOK.md, docs/AZURE_RUNBOOK.md). deploy.sh ships this
# with ~/rmow.env, which names the store and the builds, and starts the steps detached, so a dropped
# SSH session does not stop them:
#   remote.sh start <step>...   run the steps in order in the background, stopping at a failure
#   remote.sh state             running, done, failed or none
#   remote.sh status [lines]    the state and the log's tail
# The steps:
#   build      the fork at FORK_COMMIT into ~/.m2; rmow-bench at CALIBRATION_COMMIT (~/rmow-bench)
#              and at SPIKE_COMMIT (~/rmow-spike, a worktree of it)
#   test       the fork's atomic-operations contract on the store, then rmow-bench's (step 1)
#   spike      the spike, SPIKE_REPS reps (step 2)
#   calibrate  the calibration run, its requests logged, CALIBRATION_REPS times (step 3)
# Its log is ~/run.log; results land in ~/rmow-bench/results, as on the laptop.
set -euo pipefail
cd "$HOME"

STEPS=(build test spike calibrate)
# Gradle without the engines (Spark, Flink, Kafka Connect), which rmow-bench does not use
GRADLE=(./gradlew --no-daemon -q -DsparkVersions= -DflinkVersions= -DkafkaVersions=)

log() { printf '\n==> %s  %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { echo "error: $*" >&2; exit 1; }

# A JUnit report's counts; fails when a test failed or none ran (a suite without credentials
# skips every test rather than failing)
ran() {
  python3 - "$1" <<'EOF'
import sys, xml.etree.ElementTree as ET
s = ET.parse(sys.argv[1]).getroot()
n, skipped = int(s.get("tests")), int(s.get("skipped", 0))
failed = int(s.get("failures", 0)) + int(s.get("errors", 0))
print(f"  {s.get('name')}: {n} tests, {skipped} skipped, {failed} failed")
sys.exit(1 if failed or n == skipped else 0)
EOF
}

# The fork commit the local Maven repository's iceberg-core was built from
fork_built() {
  local jar=.m2/repository/org/apache/iceberg/iceberg-core/1.12.0-atomicio-SNAPSHOT/iceberg-core-1.12.0-atomicio-SNAPSHOT.jar
  [[ -f "$jar" ]] && unzip -p "$jar" iceberg-build.properties | sed -n 's/^git\.commit\.id=//p'
}

step_build() {
  log "waiting for cloud-init's packages"
  for _ in $(seq 1 180); do
    [[ -f /var/lib/cloud/rmow-bench-ready ]] && break
    sleep 5
  done
  [[ -f /var/lib/cloud/rmow-bench-ready ]] \
    || die "cloud-init has not finished (/var/log/cloud-init-output.log)"

  log "the fork at $FORK_COMMIT"
  [[ -d iceberg/.git ]] || git clone -q --filter=blob:none --no-checkout "$FORK_REPO" iceberg
  git -C iceberg cat-file -e "$FORK_COMMIT^{commit}" 2>/dev/null \
    || git -C iceberg fetch -q origin "$FORK_COMMIT"
  # its version is its release tag's (build.gradle, getProjectVersion), which the fork on GitHub
  # does not carry
  git -C iceberg fetch -q --no-tags https://github.com/apache/iceberg.git tag "$FORK_BASE_TAG"
  # on its branch, so the jars' build properties are the twins' (git.branch=atomicio)
  git -C iceberg checkout -q -B atomicio "$FORK_COMMIT"
  (cd iceberg && "${GRADLE[@]}" publishToMavenLocal -x test -x integrationTest)
  [[ "$(fork_built)" == "$FORK_COMMIT" ]] \
    || die "the local Maven repository holds the fork at '$(fork_built)', not $FORK_COMMIT"

  log "rmow-bench at $CALIBRATION_COMMIT (the calibration) and $SPIKE_COMMIT (the spike)"
  if [[ -d rmow-bench/.git ]]; then
    git -C rmow-bench fetch -q "$HOME/rmow-bench.bundle" main
  else
    git clone -q --no-checkout rmow-bench.bundle rmow-bench
  fi
  git -C rmow-bench checkout -q --detach "$CALIBRATION_COMMIT"
  if [[ -e rmow-spike/.git ]]; then
    git -C rmow-spike checkout -q --detach "$SPIKE_COMMIT"
  else
    git -C rmow-bench worktree add -q --detach ../rmow-spike "$SPIKE_COMMIT"
  fi
  (cd rmow-bench && mvn -q -B clean package -DskipTests)
  (cd rmow-spike && mvn -q -B clean package -DskipTests)
}

step_test() {
  case "$STORE" in
    aws)
      log "the fork's contract on $S3_EXPRESS_BUCKET"
      # The suite skips without keys in the environment: the instance role's, from its metadata
      local token role creds
      token="$(curl -fsS -X PUT http://169.254.169.254/latest/api/token \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600')"
      role="$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
        http://169.254.169.254/latest/meta-data/iam/security-credentials/)"
      creds="$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/$role")"
      (
        AWS_ACCESS_KEY_ID="$(jq -r .AccessKeyId <<<"$creds")"
        AWS_SECRET_ACCESS_KEY="$(jq -r .SecretAccessKey <<<"$creds")"
        AWS_SESSION_TOKEN="$(jq -r .Token <<<"$creds")"
        export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
        export S3_EXPRESS_TEST_BUCKET="$S3_EXPRESS_BUCKET"
        cd iceberg && "${GRADLE[@]}" :iceberg-aws:cleanTest :iceberg-aws:test \
          --tests org.apache.iceberg.aws.s3.S3ExpressFileIOAtomicTest
      )
      ran iceberg/aws/build/test-results/test/TEST-org.apache.iceberg.aws.s3.S3ExpressFileIOAtomicTest.xml

      log "rmow-bench's store contract under $ROOT/test"
      (cd rmow-bench && RMOW_S3X_ROOT="$ROOT/test" mvn -q -B test -Dtest=S3ExpressTest)
      ran rmow-bench/target/surefire-reports/TEST-org.rmow.bench.bench.S3ExpressTest.xml
      ;;
    azure)
      log "the fork's contract on $AZURE_ACCOUNT/$AZURE_CONTAINER"
      # The suite takes a SAS, not the managed identity: deploy.sh sends a short-lived one, which
      # lives on the VM only for this step
      [[ -s .rmow-sas ]] || die "no SAS in ~/.rmow-sas (deploy.sh run sends one with the test step)"
      local sas
      sas="$(cat .rmow-sas)"
      rm -f .rmow-sas
      (
        export AZURE_STORAGE_ACCOUNT="$AZURE_ACCOUNT" AZURE_TEST_CONTAINER="$AZURE_CONTAINER"
        export AZURE_STORAGE_SAS_TOKEN="$sas"
        cd iceberg && "${GRADLE[@]}" :iceberg-azure:cleanTest :iceberg-azure:test \
          --tests org.apache.iceberg.azure.adlsv2.AdlsFileIOAtomicTest
      )
      ran iceberg/azure/build/test-results/test/TEST-org.apache.iceberg.azure.adlsv2.AdlsFileIOAtomicTest.xml

      log "rmow-bench's store contract under $ROOT/test"
      (cd rmow-bench && RMOW_AZURE_ROOT="$ROOT/test" mvn -q -B test -Dtest=AzureTest)
      ran rmow-bench/target/surefire-reports/TEST-org.rmow.bench.bench.AzureTest.xml
      ;;
  esac
}

step_spike() {
  local cls jar
  case "$STORE" in
    aws) cls=S3ExpressSpike ;;
    azure) cls=AzureSpike ;;
  esac
  for jar in "$HOME"/rmow-spike/target/rmow-bench-*.jar; do
    [[ "$jar" == *-tests.jar ]] || break
  done
  log "the spike ($cls, the build at $SPIKE_COMMIT), $SPIKE_REPS reps, under $ROOT/spike"
  # from rmow-bench, so its results land beside the calibration's (results/spike)
  (cd rmow-bench && java -cp "$jar:$HOME/rmow-spike/target/lib/*" "org.rmow.bench.micro.$cls" \
    "$ROOT/spike" --reps "$SPIKE_REPS")
}

step_calibrate() {
  local label cfg
  case "$STORE" in
    aws) label=s3x-calibration ;;
    azure) label=azure-calibration ;;
  esac
  # The configuration's root is a placeholder: the store's goes into a copy of it, outside the
  # checkout so that the run's manifest records a clean tree. Its label stays the same
  cfg="/tmp/$label.json"
  jq --arg root "$ROOT/calibration" '.store.root = $root' "rmow-bench/configs/$label.json" > "$cfg"
  if [[ "$STORE" == aws ]]; then
    jq --arg region "$AWS_REGION" '.store.region = $region' "$cfg" > "$cfg.tmp" && mv "$cfg.tmp" "$cfg"
  fi
  ! grep -q REPLACE "$cfg" || die "a placeholder is left in $cfg"
  # Repeated, for the spread of measured latency (the requests and bytes of every run are the
  # same, C1); each run deletes its objects when it ends
  local i out
  for i in $(seq 1 "$CALIBRATION_REPS"); do
    log "calibration run $i of $CALIBRATION_REPS ($label, the build at $CALIBRATION_COMMIT)," \
      "under $ROOT/calibration"
    out="$(cd rmow-bench && bin/run.sh "$cfg" --log-requests | tee /dev/stderr | tail -1)"
    # The model's checks need the VM's two clocks to agree (the runbooks, Where the time went)
    jq -r '"  host.thread_cpu_per_wall: \(.host.thread_cpu_per_wall) (should be about 1)"' \
      "rmow-bench/$out/manifest.json" 2>/dev/null || echo "  (no manifest at rmow-bench/$out)"
  done
}

running() { [[ -f run.pid ]] && kill -0 "$(cat run.pid)" 2>/dev/null; }

state() {
  if running; then
    echo running
  elif [[ -f run.result ]]; then
    cut -d' ' -f1 run.result
  else
    echo none
  fi
}

case "${1:-}" in
  start)
    shift
    (($#)) || die "no steps (${STEPS[*]})"
    for s in "$@"; do
      [[ " ${STEPS[*]} " == *" $s "* ]] || die "no step '$s' (${STEPS[*]})"
    done
    ! running || die "steps still running: $(cat run.steps)"
    rm -f run.result
    echo "$*" > run.steps
    setsid nohup bash "$0" _run "$@" >> run.log 2>&1 < /dev/null &
    echo $! > run.pid
    echo "started on $(hostname): $* (log: ~/run.log)"
    ;;
  _run)
    shift
    echo $$ > run.pid
    # however the runner ends, it leaves a result
    trap '[[ -f run.result ]] || echo "failed runner" > run.result' EXIT
    # shellcheck source=/dev/null
    source "$HOME/rmow.env"
    log "steps: $* (store $STORE, root $ROOT)"
    for s in "$@"; do
      # each step in a shell of its own, where set -e holds (it would not inside `||`)
      if ! bash "$0" _step "$s"; then
        echo "failed $s" > run.result
        log "$s failed"
        exit 1
      fi
    done
    echo "done $*" > run.result
    log "done: $*"
    ;;
  _step)
    # shellcheck source=/dev/null
    source "$HOME/rmow.env"
    # the SDKs' region, and the identity Azure's default chain asks the VM's metadata for
    [[ -z "${AWS_REGION:-}" ]] || export AWS_REGION
    [[ -z "${AZURE_CLIENT_ID:-}" ]] || export AZURE_CLIENT_ID
    "step_$2"
    ;;
  state)
    state
    ;;
  status)
    case "$(state)" in
      running) echo "running: $(cat run.steps)" ;;
      none) echo "no steps run" ;;
      *) echo "ended: $(cat run.result)" ;;
    esac
    [[ ! -f run.log ]] || tail -n "${2:-30}" run.log
    ;;
  *)
    die "usage: $0 {start <step>...|state|status [lines]}; steps: ${STEPS[*]}"
    ;;
esac
