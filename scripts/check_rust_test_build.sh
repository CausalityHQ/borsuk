#!/usr/bin/env bash
# Build the complete workspace test surface with bounded compiler concurrency.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOBS="${BORSUK_TEST_BUILD_JOBS:-2}"

if [[ ! "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "BORSUK_TEST_BUILD_JOBS must be a positive integer, got '$JOBS'" >&2
  exit 2
fi

if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  case "$BORSUK_TEST_BUILD_COMMAND" in
    true) command=(true) ;;
    false) command=(false) ;;
    *)
      echo "BORSUK_TEST_BUILD_COMMAND is test-only and accepts true or false" >&2
      exit 2
      ;;
  esac
else
  command=(
    env
    "CARGO_BUILD_JOBS=$JOBS"
    cargo test
    --locked
    --workspace
    --all-targets
    --no-run
  )
fi

cd "$ROOT"
started_epoch="$(date +%s)"
started_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
"${command[@]}"
status=$?
set -e
finished_epoch="$(date +%s)"
finished_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
elapsed_seconds=$((finished_epoch - started_epoch))

printf '%s\n' \
  "rust-test-build status=$status elapsed_seconds=$elapsed_seconds jobs=$JOBS" \
  "started_at=$started_utc" \
  "finished_at=$finished_utc"
exit "$status"
