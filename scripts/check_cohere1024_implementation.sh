#!/usr/bin/env bash
# Six serial exact-source D1024 gates; root owns execution and source freeze.
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
c.configure(cohere1024=True)
c.qualify()'
}
source_check

stage_record() {
  python3 -c 'import sys
from scripts.launch_native_workspace_execution_spot import record_constrained_split_stage
sys.exit(record_constrained_split_stage(sys.argv[1:], cohere1024=True))' "$@"
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

run_stage generation-tests cargo test --locked -p borsuk --lib two_bit_generation:: -- --test-threads=1
run_stage generation-release-test cargo test --release --locked -p borsuk --lib two_bit_generation::source_walk_tests::native_100k_d1024_generation_serving_scalar_oracle -- --exact --test-threads=1
run_stage router-tests cargo test --locked -p borsuk --lib semantic_unit_router:: -- --test-threads=1
run_stage release cargo build --release --locked -p borsuk --example two_bit_http --bin build_two_bit_generation --bin build_semantic_unit_router --bin repackage_semantic_generation --bin check_semantic_router_scorer
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
