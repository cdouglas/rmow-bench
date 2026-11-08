#!/usr/bin/env bash
# Fuzzes the runner with random small configurations (org.rmow.bench.fuzz.Fuzz; docs in its
# javadoc). Build first: mvn -q package -DskipTests
#
#   bin/fuzz.sh --seed S [--out DIR]          one seed
#   bin/fuzz.sh --print S                     the configuration seed S draws
#   bin/fuzz.sh --out DIR [--from S] [--count N] [--minutes M] [--worker K --workers W]
#                                             one worker: seeds S+K, S+K+W, ...
#   bin/fuzz.sh --parallel W --out DIR [--from S] [--count N] [--minutes M]
#                                             W workers at once, each restarted with --resume
#                                             after a timeout (exit 3) or an Error (exit 4)
#
# Each worker's heap: FUZZ_HEAP (default 3g). CPUSET, if set, confines this script and its workers
# (docker/run.sh passes it: rootless Docker drops --cpuset-cpus silently).
set -euo pipefail
if [ -n "${CPUSET:-}" ]; then
  taskset -cp "$CPUSET" $$ >/dev/null
fi
here="$(cd "$(dirname "$0")/.." && pwd)"
jar="$(ls "$here"/target/rmow-bench-*.jar | grep -v -- '-tests' | head -1)"
java_run() {
  java -Xmx"${FUZZ_HEAP:-3g}" --add-opens=java.base/java.nio=ALL-UNNAMED \
    -cp "$jar:$here/target/lib/*" org.rmow.bench.fuzz.Fuzz "$@"
}
if [ "${1:-}" != --parallel ]; then
  java_run "$@"
  exit $?
fi
shift
workers="$1"
shift
# --minutes counts from now, across restarts
args=()
while [ $# -gt 0 ]; do
  if [ "$1" = --minutes ]; then
    args+=(--until "$(awk -v m="$2" -v now="$(date +%s)" 'BEGIN { printf "%d", now + m * 60 }')")
    shift 2
  else
    args+=("$1")
    shift
  fi
done
for ((k = 0; k < workers; k++)); do
  (
    resume=()
    while true; do
      set +e
      java_run "${args[@]}" --worker "$k" --workers "$workers" "${resume[@]}"
      code=$?
      set -e
      if [ "$code" = 3 ] || [ "$code" = 4 ]; then # a timeout or an Error, recorded: go on past it
        resume=(--resume)
        continue
      fi
      echo "worker $k exited $code"
      break
    done
  ) &
done
wait
