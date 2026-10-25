#!/usr/bin/env bash
# Builds rmow-bench's campaign image and runs campaigns in it, on this machine (docker/README.md).
#
#   docker/run.sh build                # the image, from `git archive HEAD` (DIRTY_OK=1: the tree)
#   docker/run.sh cores [THRESHOLD]    # topology, what is busy, and a cpuset to use
#   docker/run.sh start  CAMPAIGN      # run docker/campaigns/CAMPAIGN.json detached
#   docker/run.sh status [CAMPAIGN]    # how far along; safe any time (no name: every campaign)
#   docker/run.sh logs   CAMPAIGN      # follow the campaign's output
#   docker/run.sh stop   CAMPAIGN      # stop it; finished jobs are kept, and `start` resumes
#   docker/run.sh run    CAMPAIGN      # the same as start, in the foreground
#   docker/run.sh shell                # a shell in the image
#
# Env (all optional):
#   RESULTS=<dir>        where runs go (default: <repo>/results); bind-mounted at /results
#   STORE=<dir>          the local store's objects (default: <repo>/store); bind-mounted at /store.
#                        Tens of GiB for the scale campaign: check `df -h` first
#   CPUSET=36-71,108-143 the CPUs the campaign may use (`cores` proposes whole idle cores)
#   JOBS=N               at most N jobs at once (default: as many as fit)
#   CORES_PER_JOB=N      physical cores per job (default: the campaign's cores_per_job)
#   THREADS_PER_CORE=N   threads a job runs on per core (default 1: SMT siblings stay idle)
#   JAVA_EXTRA="..."     extra JVM options for every run
#
# A container fixes the toolchain, not the machine: CPU counts toward every simulated latency, so
# run on idle cores (`cores`), and read each job's foreign load (`status`) before using its numbers.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
IMAGE=rmow-bench
VERB="${1:-}"
CAMPAIGN="${2:-}"
CONTAINER="rmow-bench-${CAMPAIGN}"

usage() { sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2; }

need_campaign() {
  [ -n "$CAMPAIGN" ] || usage
  [ -f "$HERE/campaigns/$CAMPAIGN.json" ] || {
    echo "no campaign docker/campaigns/$CAMPAIGN.json" >&2; exit 2; }
}

dirs() {
  RESULTS="$(realpath -m "${RESULTS:-$REPO/results}")"
  STORE="$(realpath -m "${STORE:-$REPO/store}")"
  mkdir -p "$RESULTS" "$STORE"
}

GIT_SHA="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"

# The image was built from a commit; a checkout that has moved on would run old code while its
# records named the new one.
check_image() {
  local built
  built="$(docker image inspect -f '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
    "$IMAGE" 2>/dev/null || true)"
  if [ -z "$built" ]; then
    echo "no $IMAGE image: docker/run.sh build" >&2; exit 2
  elif [ "$built" != "$GIT_SHA" ]; then
    echo "WARNING: the image is $built, this checkout $GIT_SHA. Its runs record $built." >&2
  fi
}

# Rootless Docker maps container root to this user, so files written through the mounts are
# already ours. A rootful daemon's root is the host's: run as this user instead.
user_args() {
  if docker info --format '{{.SecurityOptions}}' 2>/dev/null | grep -q rootless; then
    USER_ARGS=()
  else
    USER_ARGS=(--user "$(id -u):$(id -g)" -e HOME=/tmp)
  fi
}

# Under rootless Docker the daemon is a systemd user service: without lingering, logging out
# stops it and the campaign with it (exit 143).
logout_hazard() {
  docker info --format '{{.SecurityOptions}}' 2>/dev/null | grep -q rootless || return 0
  [ "$(loginctl show-user "$(id -un)" -p Linger 2>/dev/null)" = "Linger=yes" ] && return 0
  echo "rootless Docker without lingering: logging out stops this campaign. Once: loginctl enable-linger"
}

run_args() {
  dirs
  user_args
  local host_os
  host_os="$(. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-}")"
  # CAP_PERFMON lets Docker's seccomp profile pass perf_event_open, for each thread's instruction
  # counts (ThreadCounters); the host must also allow it (kernel.perf_event_paranoid <= 2)
  RUN_ARGS=(--network none --cap-add PERFMON "${USER_ARGS[@]}"
    -v "$RESULTS:/results" -v "$STORE:/store"
    -e RMOW_IMAGE="$(docker image inspect -f '{{.Id}}' "$IMAGE")"
    -e RMOW_HOST_NAME="$(hostname)" -e RMOW_HOST_OS="$host_os"
    -e CPUSET="${CPUSET:-}" -e JOBS="${JOBS:-}" -e CORES_PER_JOB="${CORES_PER_JOB:-}"
    -e THREADS_PER_CORE="${THREADS_PER_CORE:-}" -e JAVA_EXTRA="${JAVA_EXTRA:-}")
  if [ -n "${CPUSET:-}" ]; then
    RUN_ARGS+=(--cpuset-cpus "$CPUSET")
  fi
}

case "$VERB" in
  build)
    dirty=false
    if [ -n "$(git -C "$REPO" status --porcelain)" ]; then
      dirty=true
    fi
    if [ "${DIRTY_OK:-}" = 1 ]; then
      echo "building from the working tree (dirty=$dirty): its runs say so" >&2
      context=(-f "$HERE/Dockerfile" "$REPO")
      DOCKER_BUILDKIT=1 docker build --build-arg GIT_SHA="$GIT_SHA" --build-arg GIT_DIRTY="$dirty" \
        -t "$IMAGE" -t "$IMAGE:${GIT_SHA:0:12}" "${context[@]}"
    else
      [ "$dirty" = true ] && echo "note: the tree is dirty; building HEAD ($GIT_SHA) only" >&2
      git -C "$REPO" archive --format=tar HEAD |
        DOCKER_BUILDKIT=1 docker build --build-arg GIT_SHA="$GIT_SHA" --build-arg GIT_DIRTY=false \
          -t "$IMAGE" -t "$IMAGE:${GIT_SHA:0:12}" -f docker/Dockerfile -
    fi
    ;;
  cores)
    shift || true
    python3 "$HERE/cores.py" "$@"
    ;;
  start | run)
    need_campaign
    check_image
    if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
      echo "already running: $CONTAINER (docker/run.sh status $CAMPAIGN)" >&2; exit 1
    fi
    run_args
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if [ "$VERB" = run ]; then
      exec docker run --rm --name "$CONTAINER" "${RUN_ARGS[@]}" "$IMAGE" \
        run "docker/campaigns/$CAMPAIGN.json"
    fi
    # Not --rm: the container stays after it exits, so `logs` still works after a reconnect.
    docker run -d --name "$CONTAINER" --stop-timeout 120 "${RUN_ARGS[@]}" "$IMAGE" \
      run "docker/campaigns/$CAMPAIGN.json" >/dev/null
    echo "started $CONTAINER"
    echo "  progress: docker/run.sh status $CAMPAIGN"
    echo "  output:   docker/run.sh logs $CAMPAIGN"
    echo "  runs:     $RESULTS/<label>-<hash>/<UTC>/, records in $RESULTS/campaigns/$CAMPAIGN/"
    hazard="$(logout_hazard)"
    if [ -n "$hazard" ]; then
      echo "NOT safe to disconnect: $hazard"
    else
      echo "Safe to disconnect. Running start again later resumes."
    fi
    ;;
  status)
    dirs
    names=("$CAMPAIGN")
    if [ -z "$CAMPAIGN" ]; then
      mapfile -t names < <(ls "$RESULTS/campaigns" 2>/dev/null)
      [ ${#names[@]} -gt 0 ] || { echo "no campaigns in $RESULTS/campaigns"; exit 0; }
    fi
    for name in "${names[@]}"; do
      c="rmow-bench-$name"
      if docker ps --format '{{.Names}}' | grep -qx "$c"; then
        echo "== $name: container running"
      elif docker ps -a --format '{{.Names}}' | grep -qx "$c"; then
        echo "== $name: container exited $(docker inspect -f '{{.State.ExitCode}}' "$c")"
      else
        echo "== $name: no container"
      fi
      python3 "$HERE/campaign.py" status "$RESULTS/campaigns/$name" || true
    done
    ;;
  logs)
    need_campaign
    docker logs ${LOGS_ARGS:--f} "$CONTAINER"
    ;;
  stop)
    need_campaign
    docker stop "$CONTAINER" >/dev/null
    echo "stopped $CONTAINER: finished jobs are kept, and start resumes"
    ;;
  shell)
    run_args
    exec docker run --rm -it "${RUN_ARGS[@]}" --entrypoint /bin/bash "$IMAGE"
    ;;
  *) usage ;;
esac
