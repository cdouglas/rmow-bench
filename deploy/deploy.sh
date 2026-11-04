#!/usr/bin/env bash
#
# Runs on a cloud store from VMs beside it (docs/S3X_RUNBOOK.md, docs/AZURE_RUNBOOK.md): the
# calibration, or the cloud twins of runs made on the laptop or havoc. Terraform makes the store and
# the VMs, and deploy/remote.sh runs the runbook's steps there.
#
# Two Terraform layers per cloud, as in shield-rs:
#   infra/  the store, the VM's credentials and the network: free while the store is empty, kept
#   bench/  the VM and its disk: billable, made and destroyed per campaign
#
#   ./deploy/deploy.sh infra-up      # the store and the network, once
#   ./deploy/deploy.sh up            # the VM (or VMs)
#   ./deploy/deploy.sh ship          # rmow-bench to the VM, and the builds there (detached)
#   ./deploy/deploy.sh run [step]... # test, spike, bench (by default all three, in order)
#   ./deploy/deploy.sh status        # whether the VM's steps still run, and the log's tail
#   ./deploy/deploy.sh wait          # until they end
#   ./deploy/deploy.sh fetch         # the results into results/, the VM's log into deploy/logs/
#   ./deploy/deploy.sh ssh           # a shell on the VM
#   ./deploy/deploy.sh down          # destroy the VMs; the store stays
#   ./deploy/deploy.sh teardown      # destroy the store, every object in it, and the network
#   ./deploy/deploy.sh all           # infra-up, up, ship, wait, run, wait, fetch
#
# The cloud is CLOUD (aws | azure) in deploy/deploy.env, overridable per command:
#   CLOUD=azure ./deploy/deploy.sh up
# The VMs (FLEET in deploy.env): false, one VM, named after the store (s3x, azure), runs every
# configuration in turn; true, one VM per configuration, named after it, runs only that one, all at
# once. Every command acts on all of them, or on the ones VM names (space-separated):
#   VM=sweep-p10 ./deploy/deploy.sh status 40
#   VM="sweep-p10 sweep-p30" ./deploy/deploy.sh down   # the others keep running
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
FLEET="${FLEET:-false}"
# How long `wait` waits on a VM it cannot reach before it gives up on it (minutes)
WAIT_UNREACHABLE_MIN="${WAIT_UNREACHABLE_MIN:-60}"
case "$CLOUD" in
  aws) CONFIGS="${AWS_CONFIGS:-s3x-calibration}" SUFFIX=s3x ;;
  azure) CONFIGS="${AZURE_CONFIGS:-azure-calibration}" SUFFIX=azure ;;
  *) echo "error: CLOUD must be aws|azure (got '$CLOUD')" >&2; exit 1 ;;
esac

TF_INFRA=(terraform -chdir="$DEPLOY_DIR/$CLOUD/infra")
TF_BENCH=(terraform -chdir="$DEPLOY_DIR/$CLOUD/bench")

die() { echo "error: $*" >&2; exit 1; }
say() { echo "==> $*" >&2; }

# The campaign's VMs: one named after the store, or one per configuration, named after it
case "$FLEET" in
  true) read -ra VMS <<<"$CONFIGS" ;;
  false) VMS=("$SUFFIX") ;;
  *) die "FLEET must be true|false (got '$FLEET')" ;;
esac
for vm in "${VMS[@]}"; do
  [[ "$vm" =~ ^[a-z0-9]([a-z0-9-]{0,48}[a-z0-9])?$ ]] \
    || die "'$vm' cannot name a VM: lowercase letters, digits and hyphens, at most 50"
done

# The VMs a command acts on, one per line: those VM names, or the campaign's
targets() {
  if [[ -n "${VM:-}" ]]; then
    # shellcheck disable=SC2086 # the names are words
    printf '%s\n' $VM
  else
    printf '%s\n' "${VMS[@]}"
  fi
}

# Names, one per line, as a JSON list (sorted, without blanks): the bench layer's `vms`
json_list() { sort -u | jq -R 'select(length > 0)' | jq -sc .; }

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

# bench vars: the VMs' size, disk and login.
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

# The VMs in the bench layer's state, one per line: the names any of their resources carry. It
# fails when the state cannot be read, so that `down` never takes a misread for an empty list.
vms_up() {
  [[ -f "$DEPLOY_DIR/$CLOUD/bench/terraform.tfstate" ]] || return 0
  local list
  list="$("${TF_BENCH[@]}" state list)" || die "cannot read the bench layer's state"
  # A VM made before the fleet (2026-10-30) has no name: this deploy would take it for none
  ! grep -v -e '^data\.' -e '^$' <<<"$list" | grep -qv '\["' \
    || die "the bench layer holds a VM from before the fleet: take it down with the deploy that made it"
  sed -n 's/^[^[]*\["\(.*\)"\]$/\1/p' <<<"$list" | sort -u
}

# One `up` or `down` at a time per cloud: each reads the VMs in the state, then applies the new list
lock() {
  exec 9>"$DEPLOY_DIR/.lock.$CLOUD"
  flock 9
}

# A VM's host key is new with every `up`: it is trusted on first use, in a file of its own
known_hosts() { echo "$DEPLOY_DIR/.known_hosts.$CLOUD.$1"; }

# The VM the functions below act on (use_vm): its name, its address and the options to reach it
CUR="" HOST="" IPS=""
ssh_opts=()
use_vm() {
  CUR="$1"
  [[ -n "$IPS" ]] || IPS="$("${TF_BENCH[@]}" output -json public_ips 2>/dev/null || echo '{}')"
  local ip
  ip="$(jq -r --arg vm "$CUR" '.[$vm] // empty' <<<"$IPS")"
  [[ -n "$ip" ]] || die "no VM '$CUR' is up on $CLOUD (up: $(jq -r 'keys | join(" ")' <<<"$IPS"))"
  HOST="$(bench_out ssh_user)@$ip"
  ssh_opts=(-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$(known_hosts "$CUR")"
    -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -i "$SSH_PRIVATE_KEY")
}

remote() { ssh -n "${ssh_opts[@]}" "$HOST" "$@"; }

# The VM's state (remote.sh state), tried three times, so a dropped connection is not an ending
remote_state() {
  local _
  for _ in 1 2 3; do
    remote bash remote.sh state 2>/dev/null && return
    sleep 10
  done
  echo unreachable
}

wait_ssh() {
  say "waiting for sshd on $CUR ($HOST)"
  for _ in $(seq 1 60); do
    ssh -n "${ssh_opts[@]}" "$HOST" true 2>/dev/null && return
    sleep 5
  done
  die "no SSH to $CUR ($HOST) after 5 minutes"
}

# The configurations the current VM runs: its own, or all of them
vm_configs() {
  if [[ "$FLEET" == true ]]; then echo "$CUR"; else echo "$CONFIGS"; fi
}

# The labels the bench step's runs take, for the configurations given: a configuration with a local
# store runs on the cloud's as <label>-<suffix> (remote.sh, step_bench); one with a cloud store
# under its own label.
bench_labels() {
  local name src
  for name in $1; do
    src="$(git -C "$REPO_DIR" show "$BENCH_COMMIT:configs/$name.json" 2>/dev/null)" \
      || die "no configs/$name.json at $BENCH_COMMIT"
    if [[ "$(jq -r .store.type <<<"$src")" == local ]]; then
      echo "$(jq -r .label <<<"$src")-$SUFFIX"
    else
      jq -r .label <<<"$src"
    fi
  done
}

# The labels of the twin step's runs (remote.sh, step_twin): each configuration with a local store,
# under its own label
twin_labels() {
  local name src
  for name in $1; do
    src="$(git -C "$REPO_DIR" show "$BENCH_COMMIT:configs/$name.json" 2>/dev/null)" \
      || die "no configs/$name.json at $BENCH_COMMIT"
    [[ "$(jq -r .store.type <<<"$src")" != local ]] || jq -r .label <<<"$src"
  done
}

# ~/rmow.env on the current VM: the store, the builds and the knobs remote.sh reads, and in a fleet
# the VM's name, which keeps its test and spike objects and its spike's results apart from the
# others'. No secrets: the credentials are the VM's (its role or identity), but for the Azure
# suite's SAS (cmd_run).
write_env() {
  local bench spike configs name=""
  [[ -n "$BENCH_COMMIT" ]] || die "no BENCH_COMMIT in $ENV_FILE"
  bench="$(git -C "$REPO_DIR" rev-parse --verify "$BENCH_COMMIT^{commit}")"
  spike="$(git -C "$REPO_DIR" rev-parse --verify "$SPIKE_COMMIT^{commit}")"
  configs="$(vm_configs)"
  bench_labels "$configs" >/dev/null # every configuration exists at the build
  [[ "$FLEET" != true ]] || name="$CUR"
  {
    printf '%s=%q\n' STORE "$CLOUD" ROOT "$(infra_out root)" NAME "$name" \
      FORK_REPO "$FORK_REPO" FORK_COMMIT "$FORK_COMMIT" FORK_BASE_TAG "$FORK_BASE_TAG" \
      BENCH_COMMIT "$bench" SPIKE_COMMIT "$spike" SPIKE_REPS "$SPIKE_REPS" \
      CONFIGS "$configs" BENCH_REPS "$BENCH_REPS" BENCH_HEAP "$BENCH_HEAP" \
      BENCH_FLAGS "$BENCH_FLAGS"
    case "$CLOUD" in
      aws) printf '%s=%q\n' AWS_REGION "$(infra_out region)" S3_EXPRESS_BUCKET "$(infra_out bucket)" ;;
      azure) printf '%s=%q\n' AZURE_ACCOUNT "$(infra_out account)" \
        AZURE_CONTAINER "$(infra_out container)" AZURE_CLIENT_ID "$(infra_out client_id)" ;;
    esac
  } | ssh "${ssh_opts[@]}" "$HOST" 'cat > ~/rmow.env'
}

cmd_infra_up() {
  "${TF_INFRA[@]}" init -input=false
  build_infra_vars
  "${TF_INFRA[@]}" apply -auto-approve -input=false "${INFRA_VARS[@]}"
}

# Brings up the target VMs beside those already up, which it leaves as they are
cmd_up() {
  [[ -f "$DEPLOY_DIR/$CLOUD/infra/terraform.tfstate" ]] \
    || die "no infra state: run '$0 infra-up' first"
  "${TF_BENCH[@]}" init -input=false
  build_bench_vars
  lock
  local up new vm
  up="$(vms_up)"
  new="$(targets)"
  for vm in $(comm -13 <(sort -u <<<"$up") <(sort -u <<<"$new")); do
    rm -f "$(known_hosts "$vm")"
  done
  "${TF_BENCH[@]}" apply -auto-approve -input=false -lock-timeout=10m "${BENCH_VARS[@]}" \
    -var "vms=$(printf '%s\n%s\n' "$up" "$new" | json_list)"
}

# Destroys every VM; with VM, only those, and the others keep running
cmd_down() {
  build_bench_vars
  lock
  if [[ -z "${VM:-}" ]]; then
    "${TF_BENCH[@]}" destroy -auto-approve -input=false -lock-timeout=10m "${BENCH_VARS[@]}"
    rm -f "$DEPLOY_DIR/.known_hosts.$CLOUD".*
    return
  fi
  local up gone keep vm
  up="$(vms_up)"
  gone="$(targets)"
  keep="$(comm -23 <(sort -u <<<"$up") <(sort -u <<<"$gone") | json_list)"
  say "destroying $(tr '\n' ' ' <<<"$gone")and keeping $keep"
  for vm in $gone; do
    rm -f "$(known_hosts "$vm")"
  done
  "${TF_BENCH[@]}" apply -auto-approve -input=false -lock-timeout=10m "${BENCH_VARS[@]}" \
    -var "vms=$keep"
}

cmd_teardown() {
  local up
  up="$(vms_up)" # stops here if the state cannot be read
  [[ -z "$up" ]] \
    || die "VMs are still up ($(tr '\n' ' ' <<<"$up")): '$0 down' first (and '$0 fetch' before that)"
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

# rmow-bench as a bundle of main (the repository is private; the fork is public, and each VM clones
# it), then the builds on each VM.
cmd_ship() {
  local c vm
  for c in "$BENCH_COMMIT" "$SPIKE_COMMIT"; do
    git -C "$REPO_DIR" merge-base --is-ancestor "$c" main \
      || die "$c is not on main, which ship bundles"
  done
  BUNDLE_DIR="$(mktemp -d)"
  trap 'rm -rf "$BUNDLE_DIR"' EXIT
  git -C "$REPO_DIR" bundle create -q "$BUNDLE_DIR/rmow-bench.bundle" main
  for vm in $(targets); do
    use_vm "$vm"
    wait_ssh
    say "shipping rmow-bench's main and remote.sh to $CUR ($HOST)"
    scp -q "${ssh_opts[@]}" "$BUNDLE_DIR/rmow-bench.bundle" "$DEPLOY_DIR/remote.sh" "$HOST:"
    write_env
    remote bash remote.sh start build
  done
}

cmd_run() {
  local steps=("$@") sas="" vm
  ((${#steps[@]})) || steps=(test spike bench)
  if [[ "$CLOUD" == azure && " ${steps[*]} " == *" test "* ]]; then
    # The fork's ADLS suite authenticates with a SAS only: an account SAS for the blob service,
    # signed here with the account's key (which stays here), valid for SAS_HOURS, and kept on the
    # VM only until the test step reads it
    local rg account key expiry
    rg="$(infra_out resource_group_name)"
    account="$(infra_out account)"
    key="$(az storage account keys list -g "$rg" -n "$account" --query '[0].value' -o tsv)"
    expiry="$(date -u -d "+${SAS_HOURS} hours" +%Y-%m-%dT%H:%MZ)"
    sas="$(az storage account generate-sas --account-name "$account" --account-key "$key" \
      --services b --resource-types sco --permissions acdlrw --https-only --expiry "$expiry" -o tsv)"
  fi
  for vm in $(targets); do
    use_vm "$vm"
    write_env
    [[ -z "$sas" ]] || ssh "${ssh_opts[@]}" "$HOST" 'umask 077 && cat > ~/.rmow-sas' <<<"$sas"
    remote bash remote.sh start "${steps[@]}"
  done
}

# Each VM's state and the log's tail: 30 lines of one VM's, 3 of each in a fleet
cmd_status() {
  local vm n
  n="$(targets | wc -l)"
  for vm in $(targets); do
    use_vm "$vm"
    if ((n > 1)); then
      echo "== $CUR"
      remote bash remote.sh status "${1:-3}" || echo "(unreachable)"
    else
      remote bash remote.sh status "${1:-30}"
    fi
  done
}

# Until no VM's steps run; fails if any ended without finishing them. A VM it cannot reach is
# waited on, not taken for ended: its steps run detached, and the likeliest cause is this machine's
# address leaving the SSH rule (rerun infra-up). After WAIT_UNREACHABLE_MIN minutes unreachable a VM
# is no longer waited on, and the wait fails naming it. The report gives the states the last poll
# saw, rather than polling again, so a VM that drops out once its steps end is reported as done.
cmd_wait() {
  local vm state pending failed=() limit=$((WAIT_UNREACHABLE_MIN * 60))
  local -A last=() since=()
  while :; do
    pending=0
    for vm in $(targets); do
      use_vm "$vm"
      state="$(remote_state)"
      last[$vm]="$state"
      if [[ "$state" == unreachable ]]; then
        if [[ -z "${since[$vm]:-}" ]]; then
          since[$vm]=$SECONDS
          say "$vm is unreachable; waiting on it (if this machine's address changed, run infra-up)"
        fi
        ((SECONDS - since[$vm] >= limit)) || pending=$((pending + 1))
      else
        if [[ -n "${since[$vm]:-}" ]]; then
          unset 'since[$vm]'
          say "$vm is reachable again ($state)"
        fi
        [[ "$state" != running ]] || pending=$((pending + 1))
      fi
    done
    ((pending)) || break
    sleep 60
  done
  for vm in $(targets); do
    use_vm "$vm"
    echo "== $CUR: ${last[$vm]}"
    [[ "${last[$vm]}" == unreachable ]] || remote bash remote.sh status 15 || true
    [[ "${last[$vm]}" == done ]] || failed+=("$CUR (${last[$vm]})")
  done
  ((${#failed[@]} == 0)) || die "steps ended without finishing on: ${failed[*]}$(
    [[ " ${last[*]} " != *" unreachable "* ]] \
      || echo "; unreachable for ${WAIT_UNREACHABLE_MIN} minutes: if this machine's address changed, run infra-up, then wait again")"
}

cmd_fetch() {
  local vm
  mkdir -p "$REPO_DIR/results" "$DEPLOY_DIR/logs"
  for vm in $(targets); do
    use_vm "$vm"
    fetch_vm
  done
  say "next: lab ingest"
}

# The current VM's runs and spikes into results/, and its log into deploy/logs/
fetch_vm() {
  local stamp label
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  local labels=() includes=() listed
  listed="$(bench_labels "$(vm_configs)")" # fails here if a configuration is missing
  mapfile -t labels <<<"$listed"
  # and the local twins the twin step ran, under the configurations' own labels
  listed="$(twin_labels "$(vm_configs)")"
  [[ -z "$listed" ]] || mapfile -t -O "${#labels[@]}" labels <<<"$listed"
  for label in "${labels[@]}"; do
    includes+=(--include="/$label-*/***")
  done
  say "fetching ${labels[*]} and spike/ from $CUR ($HOST) into results/"
  # results are only ever added: rsync never deletes here, and each run has a directory of its own
  rsync -a --itemize-changes -e "ssh ${ssh_opts[*]}" \
    "${includes[@]}" --include='/spike/***' --exclude='*' \
    "$HOST:rmow-bench/results/" "$REPO_DIR/results/"
  scp -q "${ssh_opts[@]}" "$HOST:run.log" "$DEPLOY_DIR/logs/$CLOUD-$CUR-$stamp.log" || true
  # The model's checks need the VM's two clocks to agree (thread_cpu_per_wall about 1); the CPU
  # measure, instruction counts (thread_counters available)
  local m run
  for label in "${labels[@]}"; do
    for m in "$REPO_DIR"/results/"$label"-*/*/manifest.json; do
      [[ -f "$m" ]] || continue
      run="${m#"$REPO_DIR"/results/}"
      run="${run%/manifest.json}"
      [[ "${run%%/*}" =~ ^$label-[0-9a-f]{12}$ ]] || continue # another label's
      echo "  $run: thread_cpu_per_wall $(jq -r '.host.thread_cpu_per_wall' "$m"), thread_counters $(jq -r '.host.thread_counters' "$m")"
    done
  done
  say "$CUR's log in deploy/logs/$CLOUD-$CUR-$stamp.log"
}

# A shell on the one VM named (VM), or on the only one
cmd_ssh() {
  local names
  names="$(targets)"
  [[ "$(wc -l <<<"$names")" == 1 ]] || die "name one VM: VM=<name> (of: $(tr '\n' ' ' <<<"$names"))"
  use_vm "$names"
  exec ssh "${ssh_opts[@]}" "$HOST"
}

say "cloud: $CLOUD, VMs: $(targets | tr '\n' ' ')"
case "${1:-}" in
  infra-up) cmd_infra_up ;;
  up) cmd_up ;;
  ship) cmd_ship ;;
  run) shift; cmd_run "$@" ;;
  status) cmd_status "${2:-}" ;;
  wait) cmd_wait ;;
  fetch) cmd_fetch ;;
  ssh) cmd_ssh ;;
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
