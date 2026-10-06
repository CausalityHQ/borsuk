#!/usr/bin/env bash
# Exact-source co-selection qualification; workspace tests are compiled.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER=
unset BORSUK_FINE_TEST_ROOT BORSUK_FINE_TEST_REMOTE
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 RUST_TEST_THREADS=1 BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1
# Refuse pending authority and test names before any native command.
source_check() {
  python3 -c 'from scripts import launch_native_workspace_execution_spot as c
c.configure(co_selection=True)
c.qualify()'
}
source_check

stage_record() {
  python3 -c 'import sys
from scripts.launch_native_workspace_execution_spot import record_constrained_split_stage
sys.exit(record_constrained_split_stage(sys.argv[1:], co_selection=True))' "$@"
}

stage_log="$(mktemp)"
trap 'rm -f "$stage_log"' EXIT
run_stage() {
  local stage="$1" started finished status
  local -a statuses
  shift
  source_check
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" '' '' '' '' "$@"
  set +e
  "$@" 2>&1 | tee "$stage_log"
  statuses=("${PIPESTATUS[@]}")
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" "$finished" "${statuses[0]}" "$stage_log" "${statuses[1]}" "$@"
  status=$?
  # A recorder/log failure must never replace a native command's exit status.
  if (( statuses[0] != 0 )); then status="${statuses[0]}"; fi
  set -e
  return "$status"
}

# Cheap synthetic native falsifiers precede the unchanged regression gates.
run_stage co-selection-tests cargo test --locked -p borsuk --lib co_selection_layout::tests -- --test-threads=1
run_stage co-selection-bin-tests cargo test --locked -p borsuk --bin hierarchical_semantic_cells co_selection_ -- --test-threads=1
run_stage pq-residual-codec-tests cargo test --locked -p borsuk --lib pq_residual_four_bit::tests -- --test-threads=1
run_stage pq-residual-source-tests cargo test --locked -p borsuk --lib fine_sq8_groups::pack_diagnostic::sq4_diagnostic::pq_residual_source::tests -- --test-threads=1
run_stage pq-residual-bin-tests cargo test --locked -p borsuk --bin hierarchical_semantic_cells pq_residual_source_strict_cli_dispatch_no_geometry_or_truth_override -- --test-threads=1
run_stage pq-residual-doc-tests cargo test --locked -p borsuk --doc pq_residual_four_bit -- --test-threads=1
run_stage corrected-four-bit-tests cargo test --locked -p borsuk --lib corrected_four_bit:: -- --test-threads=1
run_stage fine-sq8-bin-tests cargo test --locked -p borsuk --bin hierarchical_semantic_cells fine_ -- --test-threads=1
run_stage fine-sq8-tests cargo test --locked -p borsuk --lib fine_sq8_groups:: -- --test-threads=1 --skip fine_sq8_groups::pack_diagnostic::sq4_diagnostic::pq_residual_source::tests::
run_stage pq-codes-graph-tests cargo test --locked -p borsuk --lib resident_vector_graph::bounded_pq_tests -- --test-threads=1
run_stage source-pq-tests cargo test --locked -p borsuk --lib pq64_nominee::source_codes_tests -- --test-threads=1
run_stage graph-regressions cargo test --locked -p borsuk --lib resident_vector_graph::tests -- --test-threads=1
run_stage pq-regressions cargo test --locked -p borsuk --lib pq64_nominee::tests -- --test-threads=1
run_stage page-cover-regressions cargo test --locked -p borsuk --lib budgeted_page_rank::tests -- --test-threads=1
run_stage returned-sq8-regressions cargo test --locked -p borsuk --lib returned_sq8::tests -- --test-threads=1
run_stage page-authority-regressions cargo test --locked -p borsuk --lib sq8_page_authority::tests -- --test-threads=1
run_stage s3-range-regressions cargo test --locked -p borsuk --lib sq8_s3_range::tests -- --test-threads=1
run_stage hierarchical-bin-regressions cargo test --locked -p borsuk --bin hierarchical_semantic_cells -- --test-threads=1 --skip fine_ --skip tests::pq_residual_source_strict_cli_dispatch_no_geometry_or_truth_override --skip tests::co_selection_
run_stage release cargo build --release --locked -p borsuk --bin hierarchical_semantic_cells
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
