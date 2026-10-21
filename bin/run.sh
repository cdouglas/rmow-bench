#!/usr/bin/env bash
# Runs one benchmark configuration: bin/run.sh <config.json> [--out <dir>]
# Build first with: mvn -q package -DskipTests
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
jar="$(ls "$here"/target/rmow-bench-*.jar | grep -v -- '-tests' | head -1)"
exec java ${JAVA_OPTS:--Xmx8g} -cp "$jar:$here/target/lib/*" org.rmow.bench.bench.Main "$@"
