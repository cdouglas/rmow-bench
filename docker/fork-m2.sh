#!/usr/bin/env bash
# Packs the atomicio fork's artifacts this project builds against, from this machine's Maven
# repository, for `docker/run.sh build` on a machine that has none (docker/README.md).
#
#   docker/fork-m2.sh [OUT]            # default /tmp/rmow-fork-m2.tar
#
# The fork (1.12.0-atomicio-SNAPSHOT) is published only to a local repository, by
# `./gradlew publishToMavenLocal` in the iceberg checkout. The tar holds the artifacts Maven
# resolves for this pom (each one's jar, classified jars such as the tests jar, pom and local
# metadata), laid out as a Maven repository. Every run's manifest names the fork's commit
# (versions.iceberg), so a run built from the tar says which build it ran.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
OUT="$(realpath -m "${1:-/tmp/rmow-fork-m2.tar}")"
M2="${M2_REPO:-$HOME/.m2/repository}"

list="$(mktemp)"
trap 'rm -f "$list"' EXIT
(cd "$REPO" && mvn -q -B dependency:list -DincludeGroupIds=org.apache.iceberg -DoutputFile="$list")

# groupId:artifactId:type[:classifier]:version:scope, with a module note after the scope
files=()
while read -r coords _; do
  IFS=: read -r -a f <<<"$coords"
  case "${#f[@]}" in
    5) artifact="${f[1]}" classifier="" version="${f[3]}" ;;
    6) artifact="${f[1]}" classifier="-${f[3]}" version="${f[4]}" ;;
    *) continue ;;
  esac
  dir="org/apache/iceberg/$artifact/$version"
  for name in "$artifact-$version$classifier.jar" "$artifact-$version.pom" maven-metadata-local.xml; do
    [ -f "$M2/$dir/$name" ] || { echo "missing $M2/$dir/$name: publishToMavenLocal the fork" >&2; exit 1; }
    files+=("$dir/$name")
  done
done < <(grep -E '^ +org\.apache\.iceberg:' "$list")

[ "${#files[@]}" -gt 0 ] || { echo "no org.apache.iceberg artifacts resolved" >&2; exit 1; }
printf '%s\n' "${files[@]}" | sort -u | tar -C "$M2" -cf "$OUT" -T -

core="$(printf '%s\n' "${files[@]}" | grep -m1 '/iceberg-core-[^/]*-SNAPSHOT\.jar$' || true)"
commit="$( [ -n "$core" ] && unzip -p "$M2/$core" iceberg-build.properties 2>/dev/null |
  sed -n 's/^git.commit.id.abbrev=//p' )"
echo "$OUT: $(tar -tf "$OUT" | grep -c '\.jar$') jars, the fork at ${commit:-an unknown commit}"
