#!/usr/bin/env bash
# Runs one benchmark configuration: bin/run.sh <config.json> [--out <dir>]
# Build first with: mvn -q package -DskipTests
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
jar="$(ls "$here"/target/rmow-bench-*.jar | grep -v -- '-tests' | head -1)"
# Arrow (the arms' vectorized Parquet reads) needs java.nio open on Java 17
exec java ${JAVA_OPTS:--Xmx8g} --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "$jar:$here/target/lib/*" org.rmow.bench.bench.Main "$@"
