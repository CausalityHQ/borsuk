#!/usr/bin/env bash
# Exact-source diagnostic qualification; full workspace tests are compiled.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 RUST_TEST_THREADS=1
# Refuse pending config/test names before any native command.
python3 -c 'from scripts import launch_native_workspace_execution_spot as c
c.configure(constrained_split=True)
c.qualify()'

stage_record() {
  python3 -c 'import sys
from scripts.launch_native_workspace_execution_spot import record_constrained_split_stage
sys.exit(record_constrained_split_stage(sys.argv[1:]))' "$@"
}

stage_log="$(mktemp)"
trap 'rm -f "$stage_log"' EXIT
run_stage() {
  local stage="$1" started finished status
  local -a statuses
  shift
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" '' '' '' '' "$@"
  set +e
  "$@" 2>&1 | tee "$stage_log"
  statuses=("${PIPESTATUS[@]}")
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" "$finished" "${statuses[0]}" "$stage_log" "${statuses[1]}" "$@"
  status=$?
  set -e
  return "$status"
}

run_stage hierarchical-cell-tests cargo test --locked -p borsuk --lib hierarchical_semantic_cells::
run_stage split-balance-bin-tests cargo test --locked -p borsuk --bin check_hierarchical_split_balance
run_stage generation-integration cargo test --locked -p borsuk --test two_bit_generation
run_stage release cargo build --release --locked -p borsuk --bin check_hierarchical_split_balance --bin hierarchical_semantic_cells --example two_bit_http
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh
