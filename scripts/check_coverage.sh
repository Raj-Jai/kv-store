#!/usr/bin/env bash
# Per-package coverage gate for CI (phase 3, B-side).
#
# Enforces >=COVERAGE_THRESHOLD% statement coverage on every library package.
# cmd/server is a command, not a library (its main.go glue is not unit-testable)
# and is excluded, matching the agreed strict-80%-on-libs policy.
set -euo pipefail

COVERAGE_THRESHOLD="${COVERAGE_THRESHOLD:-80}"
packages=(pkg/api pkg/raft pkg/storage pkg/chaos pkg/linearizability pkg/client pkg/util)

fail=0
for pkg in "${packages[@]}"; do
  cov=$(go test -cover "./${pkg}/" 2>/dev/null | grep -oP 'coverage: \K[0-9.]+(?=% of statements)' || true)
  if [[ -z "${cov}" ]]; then
    echo "FAIL: could not measure coverage for ${pkg}"
    fail=1
  elif awk -v c="${cov}" -v t="${COVERAGE_THRESHOLD}" 'BEGIN { exit !(c < t) }'; then
    echo "FAIL: ${pkg} coverage ${cov}% < ${COVERAGE_THRESHOLD}%"
    fail=1
  else
    echo "ok: ${pkg} coverage ${cov}%"
  fi
done

exit "${fail}"
