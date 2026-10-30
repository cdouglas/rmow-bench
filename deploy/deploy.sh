#!/usr/bin/env bash
#
# Runs on a cloud store from one VM beside it (docs/S3X_RUNBOOK.md, docs/AZURE_RUNBOOK.md): the
# calibration, or the cloud twins of runs made on the laptop or havoc. Terraform makes the store and
# the VM, and deploy/remote.sh runs the runbook's steps there.
#
# Two Terraform layers per cloud, as in shield-rs:
#   infra/  the store, the VM's credentials and the network: free while the store is empty, kept
#   bench/  the VM and its disk: billable, made and destroyed per campaign
#
#   ./deploy/deploy.sh infra-up      # the store and the network, once
#   ./deploy/deploy.sh up            # the VM
#   ./deploy/deploy.sh ship          # rmow-bench to the VM, and the builds there (detached)
#   ./deploy/deploy.sh run [step]... # test, spike, bench (by default all three, in order)
#   ./deploy/deploy.sh status        # whether the VM's steps still run, and the log's tail
#   ./deploy/deploy.sh wait          # until they end
#   ./deploy/deploy.sh fetch         # the results into results/, the VM's log into deploy/logs/
#   ./deploy/deploy.sh ssh           # a shell on the VM
#   ./deploy/deploy.sh down          # destroy the VM; the store stays
#   ./deploy/deploy.sh teardown      # destroy the store, every object in it, and the network
#   ./deploy/deploy.sh all           # infra-up, up, ship, wait, run, wait, fetch
#
# The cloud is CLOUD (aws | azure) in deploy/deploy.env, overridable per command:
#   CLOUD=azure ./deploy/deploy.sh up
# SSH ingress is restricted to this machine's public IP/32 unless INGRESS_CIDR is set.
# Prerequisites: terraform, jq, curl, rsync, git, and the cloud's CLI logged in (aws, or az). See
# deploy/README.md.
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$DEPLOY_DIR/.." && pwd)"
ENV_FILE="${DEPLOY_ENV:-$DEPLOY_DIR/deploy.env}"

[[ -f "$ENV_FILE" ]] || { echo "error: missing $ENV_FILE (copy deploy.env.example)" >&2; exit 1; }
CLOUD_OVERRIDE="${CLOUD:-}"
# shellcheck disable=SC1090
source "$ENV_FILE"
CLOUD="${CLOUD_OVERRIDE:-${CLOUD:-aws}}"
# The runs (the bench step); a deploy.env from before them names the calibration's
BENCH_COMMIT="${BENCH_COMMIT:-${CALIBRATION_COMMIT:-}}"
BENCH_REPS="${BENCH_REPS:-${CALIBRATION_REPS:-1}}"
BENCH_HEAP="${BENCH_HEAP:-8g}"
BENCH_FLAGS="${BENCH_FLAGS:---log-requests}"
case "$CLOUD" in
  aws) CONFIGS="${AWS_CONFIGS:-s3x-calibration}" SUFFIX=s3x ;;
  azure) CONFIGS="${AZURE_CONFIGS:-azure-calibration}" SUFFIX=azure ;;
  *) echo "error: CLOUD must be aws|azure (got '$CLOUD')" >&2; exit 1 ;;
esac

TF_INFRA=(terraform -chdir="$DEPLOY_DIR/$CLOUD/infra")
TF_BENCH=(terraform -chdir="$DEPLOY_DIR/$CLOUD/bench")
# The VM's host key is new with every `up`: it is trusted on first use, in a file of its own
KNOWN_HOSTS="$DEPLOY_DIR/.known_hosts.$CLOUD"
ssh_opts=(-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$KNOWN_HOSTS"
  -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -i "$SSH_PRIVATE_KEY")

die() { echo "error: $*" >&2; exit 1; }
say() { echo "==> $*" >&2; }

# Operator ingress CIDR: auto-detect this machine's public IP/32 unless INGRESS_CIDR is set.
resolve_ingress() {
  if [[ -n "${INGRESS_CIDR:-}" ]]; then
    echo "$INGRESS_CIDR"
    return
  fi
  local ip
  ip="$(curl -fsS https://checkip.amazonaws.com 2>/dev/null || curl -fsS https://api.ipify.org 2>/dev/null || true)"
  ip="${ip//[$'\r\n\t ']/}"
  [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || die "could not auto-detect your public IP; set INGRESS_CIDR in deploy.env"
  echo "$ip/32"
}

# infra vars: the store's place and the network's SSH rule.
build_infra_vars() {
  local cidr
  cidr="$(resolve_ingress)"
  say "restricting SSH ingress to $cidr"
  case "$CLOUD" in
    aws) INFRA_VARS=(-var "region=$AWS_REGION" -var "az_id=$AWS_AZ_ID" -var "ssh_ingress_cidr=$cidr"
      -var "ssh_pub_key=$SSH_PUBLIC_KEY" -var "bucket=${AWS_BUCKET:-}"
      -var "instance_profile=${AWS_INSTANCE_PROFILE:-}"
      -var "create_endpoint=${AWS_CREATE_ENDPOINT:-true}") ;;
    azure) INFRA_VARS=(-var "location=$AZURE_LOCATION" -var "resource_group_name=$AZURE_RESOURCE_GROUP"
      -var "source_address_prefix=$cidr") ;;
  esac
}

# bench vars: the VM's size, disk and login.
build_bench_vars() {
  case "$CLOUD" in
    aws) BENCH_VARS=(-var "region=$AWS_REGION" -var "instance_type=$AWS_INSTANCE_TYPE"
      -var "ssh_user=$SSH_USER" -var "disk_gb=$DISK_GB") ;;
    azure) BENCH_VARS=(-var "vm_size=$AZURE_VM_SIZE" -var "admin_username=$SSH_USER"
      -var "ssh_pub_key=$SSH_PUBLIC_KEY" -var "disk_gb=$DISK_GB") ;;
  esac
}

infra_out() { "${TF_INFRA[@]}" output -raw "$1"; }
bench_out() { "${TF_BENCH[@]}" output -raw "$1"; }
host() { echo "$(bench_out ssh_user)@$(bench_out public_ip)"; }
remote() { ssh -n "${ssh_opts[@]}" "$(host)" "$@"; }

wait_ssh() {
  local h
  h="$(host)"
  say "waiting for sshd on $h"
  for _ in $(seq 1 60); do
    ssh -n "${ssh_opts[@]}" "$h" true 2>/dev/null && return
    sleep 5
  done
  die "no SSH to $h after 5 minutes"
}

# The labels the bench step's runs take: a configuration with a local store runs on the cloud's
# as <label>-<suffix> (remote.sh, step_bench); one with a cloud store under its own label.
bench_labels() {
  local name src
  for name in $CONFIGS; do
    src="$(git -C "$REPO_DIR" show "$BENCH_COMMIT:configs/$name.json" 2>/dev/null)" \
      || die "no configs/$name.json at $BENCH_COMMIT"
    if [[ "$(jq -r .store.type <<<"$src")" == local ]]; then
      echo "$(jq -r .label <<<"$src")-$SUFFIX"
    else
      jq -r .label <<<"$src"
    fi
  done
}

# ~/rmow.env on the VM: the store, the builds and the knobs remote.sh reads. No secrets: the
# credentials are the VM's (its role or identity), but for the Azure suite's SAS (cmd_run).
write_env() {
  local bench spike
  [[ -n "$BENCH_COMMIT" ]] || die "no BENCH_COMMIT in $ENV_FILE"
  bench="$(git -C "$REPO_DIR" rev-parse --verify "$BENCH_COMMIT^{commit}")"
  spike="$(git -C "$REPO_DIR" rev-parse --verify "$SPIKE_COMMIT^{commit}")"
  bench_labels >/dev/null # every configuration exists at the build
  {
    printf '%s=%q\n' STORE "$CLOUD" ROOT "$(infra_out root)" \
      FORK_REPO "$FORK_REPO" FORK_COMMIT "$FORK_COMMIT" FORK_BASE_TAG "$FORK_BASE_TAG" \
      BENCH_COMMIT "$bench" SPIKE_COMMIT "$spike" SPIKE_REPS "$SPIKE_REPS" \
      CONFIGS "$CONFIGS" BENCH_REPS "$BENCH_REPS" BENCH_HEAP "$BENCH_HEAP" \
      BENCH_FLAGS "$BENCH_FLAGS"
    case "$CLOUD" in
      aws) printf '%s=%q\n' AWS_REGION "$(infra_out region)" S3_EXPRESS_BUCKET "$(infra_out bucket)" ;;
      azure) printf '%s=%q\n' AZURE_ACCOUNT "$(infra_out account)" \
        AZURE_CONTAINER "$(infra_out container)" AZURE_CLIENT_ID "$(infra_out client_id)" ;;
    esac
  } | ssh "${ssh_opts[@]}" "$(host)" 'cat > ~/rmow.env'
}

cmd_infra_up() {
  "${TF_INFRA[@]}" init -input=false
  build_infra_vars
  "${TF_INFRA[@]}" apply -auto-approve -input=false "${INFRA_VARS[@]}"
}

cmd_up() {
  [[ -f "$DEPLOY_DIR/$CLOUD/infra/terraform.tfstate" ]] \
    || die "no infra state: run '$0 infra-up' first"
  "${TF_BENCH[@]}" init -input=false
  build_bench_vars
  rm -f "$KNOWN_HOSTS"
  "${TF_BENCH[@]}" apply -auto-approve -input=false "${BENCH_VARS[@]}"
}

cmd_down() {
  build_bench_vars
  "${TF_BENCH[@]}" destroy -auto-approve -input=false "${BENCH_VARS[@]}"
  rm -f "$KNOWN_HOSTS"
}

cmd_teardown() {
  if [[ -f "$DEPLOY_DIR/$CLOUD/bench/terraform.tfstate" ]] \
    && [[ -n "$("${TF_BENCH[@]}" state list 2>/dev/null)" ]]; then
    die "the VM is still up: '$0 down' first (and '$0 fetch' before that)"
  fi
  build_infra_vars
  # An existing bucket stays: only the runs' prefix in it goes
  if [[ "$CLOUD" == aws && -n "${AWS_BUCKET:-}" ]]; then
    local root
    root="$(infra_out root 2>/dev/null || true)"
    if [[ -n "$root" ]]; then
      [[ "$root" == "s3://$AWS_BUCKET/rmow-bench" ]] || die "unexpected root '$root'"
      say "deleting every object under $root/; the bucket stays"
      aws s3 rm --recursive --only-show-errors --region "$AWS_REGION" "$root/"
    fi
    say "destroying the key pair and the security group"
  else
    say "destroying the store ($(infra_out root 2>/dev/null || echo '?')) with every object in it"
  fi
  "${TF_INFRA[@]}" destroy -auto-approve -input=false "${INFRA_VARS[@]}"
}

# rmow-bench as a bundle of main (the repository is private; the fork is public, and the VM clones
# it), then the builds on the VM.
cmd_ship() {
  local c
  for c in "$BENCH_COMMIT" "$SPIKE_COMMIT"; do
    git -C "$REPO_DIR" merge-base --is-ancestor "$c" main \
      || die "$c is not on main, which ship bundles"
  done
  wait_ssh
  local tmp
  tmp="$(mktemp -d)"
  git -C "$REPO_DIR" bundle create -q "$tmp/rmow-bench.bundle" main
  say "shipping rmow-bench's main and remote.sh to $(host)"
  scp -q "${ssh_opts[@]}" "$tmp/rmow-bench.bundle" "$DEPLOY_DIR/remote.sh" "$(host):"
  rm -rf "$tmp"
  write_env
  remote bash remote.sh start build
}

cmd_run() {
  local steps=("$@")
  ((${#steps[@]})) || steps=(test spike bench)
  write_env
  if [[ "$CLOUD" == azure && " ${steps[*]} " == *" test "* ]]; then
    # The fork's ADLS suite authenticates with a SAS only: an account SAS for the blob service,
    # signed here with the account's key (which stays here), valid for SAS_HOURS, and kept on the
    # VM only until the test step reads it
    local rg account key expiry sas
    rg="$(infra_out resource_group_name)"
    account="$(infra_out account)"
    key="$(az storage account keys list -g "$rg" -n "$account" --query '[0].value' -o tsv)"
    expiry="$(date -u -d "+${SAS_HOURS} hours" +%Y-%m-%dT%H:%MZ)"
    sas="$(az storage account generate-sas --account-name "$account" --account-key "$key" \
      --services b --resource-types sco --permissions acdlrw --https-only --expiry "$expiry" -o tsv)"
    ssh "${ssh_opts[@]}" "$(host)" 'umask 077 && cat > ~/.rmow-sas' <<<"$sas"
  fi
  remote bash remote.sh start "${steps[@]}"
}

cmd_status() { remote bash remote.sh status "${1:-30}"; }

cmd_wait() {
  local state
  while state="$(remote bash remote.sh state)" && [[ "$state" == running ]]; do
    sleep 60
  done
  remote bash remote.sh status 15
  [[ "$state" == "done" ]] || die "the VM's steps ended: $state"
}

cmd_fetch() {
  local h stamp label
  h="$(host)"
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$REPO_DIR/results" "$DEPLOY_DIR/logs"
  local labels=() includes=() listed
  listed="$(bench_labels)" # fails here if a configuration is missing
  mapfile -t labels <<<"$listed"
  for label in "${labels[@]}"; do
    includes+=(--include="/$label-*/***")
  done
  say "fetching ${labels[*]} and spike/ from $h into results/"
  # results are only ever added: rsync never deletes here, and each run has a directory of its own
  rsync -a --itemize-changes -e "ssh ${ssh_opts[*]}" \
    "${includes[@]}" --include='/spike/***' --exclude='*' \
    "$h:rmow-bench/results/" "$REPO_DIR/results/"
  scp -q "${ssh_opts[@]}" "$h:run.log" "$DEPLOY_DIR/logs/$CLOUD-$stamp.log" || true
  # The model's checks need the VM's two clocks to agree: thread_cpu_per_wall about 1
  local m run
  for label in "${labels[@]}"; do
    for m in "$REPO_DIR"/results/"$label"-*/*/manifest.json; do
      [[ -f "$m" ]] || continue
      run="${m#"$REPO_DIR"/results/}"
      run="${run%/manifest.json}"
      [[ "${run%%/*}" =~ ^$label-[0-9a-f]{12}$ ]] || continue # another label's
      echo "  $run: thread_cpu_per_wall $(jq -r '.host.thread_cpu_per_wall' "$m")"
    done
  done
  say "VM log in deploy/logs/$CLOUD-$stamp.log; next: lab ingest"
}

say "cloud: $CLOUD"
case "${1:-}" in
  infra-up) cmd_infra_up ;;
  up) cmd_up ;;
  ship) cmd_ship ;;
  run) shift; cmd_run "$@" ;;
  status) cmd_status "${2:-}" ;;
  wait) cmd_wait ;;
  fetch) cmd_fetch ;;
  ssh) exec ssh "${ssh_opts[@]}" "$(host)" ;;
  down) cmd_down ;;
  teardown | infra-down) cmd_teardown ;;
  all)
    cmd_infra_up
    cmd_up
    cmd_ship
    cmd_wait
    cmd_run
    cmd_wait
    cmd_fetch
    ;;
  *) die "usage: [CLOUD=aws|azure] $0 {infra-up|up|ship|run [step]...|status|wait|fetch|ssh|down|teardown|all}" ;;
esac
