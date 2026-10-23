#!/usr/bin/env bash
# Runs benchmark configurations one after another from a snapshot of the build, so the working
# tree can be edited and rebuilt while they run:
#
#   bin/matrix.sh [--build] [--out <dir>] [--store-root <dir>] [--keep-store] [--dump-versions]
#                 [--log-requests] <config.json>...
#
#   --build          run `mvn package -DskipTests` first; without it the jar must be newer than
#                    the sources
#   --out            results root (default: results)
#   --store-root     where local stores put their objects (default: store/); each run deletes
#                    its own objects when it ends, unless --keep-store
#   --dump-versions  dump every version of the appendable objects (see analysis/interop_check.py)
#   --log-requests   log every request each phase issued (requests.jsonl), for latency models
#
# The snapshot, results/builds/<UTC>-<git sha>[-dirty]/, holds a copy of the project jar, hard
# links to its released dependencies (a released jar's name changes with its version, so a link
# keeps the bytes the run loaded) and copies of SNAPSHOT ones (-Pfork), the configurations as
# they were at the start, and, for a dirty tree, the uncommitted diff and untracked files. Each
# run's manifest names the snapshot and its git state, and its console output goes to the
# snapshot's logs/. One failed configuration does not stop the others; the exit status says
# whether any failed.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
out="$here/results"
store="$here/store"
build=0
pass=()
configs=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) build=1 ;;
    --out) out="$(realpath -m "$2")"; shift ;;
    --store-root) store="$(realpath -m "$2")"; shift ;;
    --keep-store | --dump-versions | --log-requests) pass+=("$1") ;;
    -*) echo "unknown option: $1" >&2; exit 2 ;;
    *) configs+=("$(realpath "$1")") ;;
  esac
  shift
done
if [[ ${#configs[@]} -eq 0 ]]; then
  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
names="$(for c in "${configs[@]}"; do basename "$c"; done | sort | uniq -d)"
if [[ -n "$names" ]]; then
  echo "configurations share a file name: $names" >&2
  exit 2
fi

if [[ $build == 1 ]]; then
  (cd "$here" && mvn -q -B package -DskipTests)
fi
jar="$(ls "$here"/target/rmow-bench-*.jar 2>/dev/null | grep -v -- '-tests' | head -1 || true)"
if [[ -z "$jar" ]]; then
  echo "no jar in target/: build with --build" >&2
  exit 2
fi
stale="$(cd "$here" && find src pom.xml -newer "$jar" -type f | head -3)"
if [[ -n "$stale" ]]; then
  echo "the jar is older than the sources ($(echo $stale)): rebuild with --build" >&2
  exit 2
fi

sha="$(git -C "$here" rev-parse HEAD)"
dirty=false
if [[ -n "$(git -C "$here" status --porcelain)" ]]; then
  dirty=true
fi
snap="$out/builds/$(date -u +%Y%m%dT%H%M%SZ)-${sha:0:12}$([[ $dirty == true ]] && echo -dirty || true)"
mkdir -p "$snap/lib" "$snap/configs" "$snap/logs"
cp -p "$jar" "$snap/"
for dep in "$here"/target/lib/*.jar; do
  # a released jar's name changes with its version; a SNAPSHOT's does not, so it is copied
  if [[ "$dep" == *-SNAPSHOT* ]] || ! ln "$dep" "$snap/lib/" 2>/dev/null; then
    cp -p "$dep" "$snap/lib/"
  fi
done
cp "${configs[@]}" "$snap/configs/"
if [[ $dirty == true ]]; then
  git -C "$here" diff HEAD > "$snap/dirty.patch"
  (cd "$here" && git ls-files --others --exclude-standard -z | tar --null -T - -czf "$snap/untracked.tgz")
fi
echo "build snapshot: $snap"

classpath="$snap/$(basename "$jar"):$snap/lib/*"
failed=()
for c in "${configs[@]}"; do
  name="$(basename "$c")"
  echo "== $name"
  if ! java ${JAVA_OPTS:--Xmx8g} \
      -Drmow.build.git="$sha" -Drmow.build.dirty="$dirty" -Drmow.build.snapshot="$snap" \
      -cp "$classpath" org.rmow.bench.bench.Main "$snap/configs/$name" \
      --out "$out" --store-root "$store" ${pass[@]+"${pass[@]}"} 2>&1 | tee "$snap/logs/${name%.json}.log"; then
    failed+=("$name")
  fi
done
if [[ ${#failed[@]} -gt 0 ]]; then
  echo "failed: ${failed[*]}" >&2
  exit 1
fi
