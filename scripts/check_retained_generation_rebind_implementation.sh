#!/usr/bin/env bash
# Five serial exact-source retained-generation-rebind gates; root owns execution and source freeze.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= LC_ALL=C
unset BORSUK_FINE_TEST_ROOT BORSUK_FINE_TEST_REMOTE
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 RUST_TEST_THREADS=1 BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1
source_check() {
  python3 -c 'from scripts import launch_native_workspace_execution_spot as c
c.configure(retained_generation_rebind=True)
c.qualify()'
}
source_check

stage_record() {
  python3 -c 'import sys
from scripts import launch_native_workspace_execution_spot as c
c.configure(retained_generation_rebind=True)
sys.exit(c.record_constrained_split_stage(sys.argv[1:], cohere_cohort=True))' "$@"
}

stage_log="$(mktemp)"
stage_resources="$(mktemp)"
trap 'rm -f "$stage_log" "$stage_resources"' EXIT
run_stage() {
  local stage="$1" started finished status
  local -a statuses
  shift
  source_check
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" '' '' '' '' "$@" "$stage_resources"
  set +e
  /usr/bin/time -v -o "$stage_resources" "$@" 2>&1 | tee "$stage_log"
  statuses=("${PIPESTATUS[@]}")
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" "$finished" "${statuses[0]}" "$stage_log" "${statuses[1]}" "$@" "$stage_resources"
  status=$?
  if (( statuses[0] != 0 )); then status="${statuses[0]}"; fi
  set -e
  return "$status"
}

run_stage retained-rebind-integration-tests cargo test --locked -p borsuk --test two_bit_generation retained_semantic_ -- --test-threads=1
run_stage retained-rebind-publisher-tests cargo test --locked -p borsuk --bin publish_two_bit_generation -- --test-threads=1
run_stage release cargo build --release --locked -p borsuk --bin publish_two_bit_generation --bin check_cohere_native_baseline
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
