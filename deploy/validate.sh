#!/usr/bin/env bash
#
# Offline validation of the deploy scaffold: no cloud credentials, no billable resources.
#   - terraform fmt -check        (every *.tf is canonically formatted)
#   - terraform validate          (each of the 4 layers is internally consistent)
#   - bash -n / shellcheck        (the scripts parse and lint cleanly)
#
# `terraform validate` needs the providers, so each layer is `init -backend=false` first (downloads
# the provider plugins from the registry; creates NO infrastructure and reads NO state/credentials).
# Run from anywhere: `./deploy/validate.sh`.
set -uo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAYERS=(aws/infra aws/bench azure/infra azure/bench)
SCRIPTS=(deploy.sh remote.sh cloud-init.sh validate.sh)
fail=0

note() { printf '\n== %s ==\n' "$1"; }

note "terraform fmt -check (recursive)"
if terraform -chdir="$DEPLOY_DIR" fmt -check -recursive; then
  echo "  ok: all *.tf canonically formatted"
else
  echo "  FAIL: run 'terraform -chdir=deploy fmt -recursive' to fix" >&2
  fail=1
fi

for layer in "${LAYERS[@]}"; do
  note "terraform validate: $layer"
  dir="$DEPLOY_DIR/$layer"
  if ! terraform -chdir="$dir" init -backend=false -input=false >/dev/null; then
    echo "  FAIL: init ($layer)" >&2
    fail=1
    continue
  fi
  if terraform -chdir="$dir" validate; then
    echo "  ok: $layer"
  else
    echo "  FAIL: validate ($layer)" >&2
    fail=1
  fi
done

note "bash -n (syntax) on shell scripts"
for s in "${SCRIPTS[@]}"; do
  if bash -n "$DEPLOY_DIR/$s"; then
    echo "  ok: $s"
  else
    echo "  FAIL: bash -n $s" >&2
    fail=1
  fi
done

note "shellcheck (if installed)"
if command -v shellcheck >/dev/null 2>&1; then
  if (cd "$DEPLOY_DIR" && shellcheck "${SCRIPTS[@]}"); then
    echo "  ok: shellcheck clean"
  else
    echo "  FAIL: shellcheck findings above" >&2
    fail=1
  fi
else
  echo "  skip: shellcheck not installed"
fi

note "result"
if [[ "$fail" -eq 0 ]]; then
  echo "  ALL CHECKS PASSED"
else
  echo "  SOME CHECKS FAILED" >&2
fi
exit "$fail"
