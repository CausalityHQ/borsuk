#!/usr/bin/env bash
# Exact-source fixed48 gates; workspace tests are compiled, not all executed.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 RUST_TEST_THREADS=1

stage_record() {
  python3 -c 'import json, re, sys
required = {
    "semantic-unit-router-tests": (
        "semantic_unit_router::tests::fresh48_seed_completion_retains_all_3072_units_and_512_pages",
        "semantic_unit_router::tests::fresh_binary_root_selected_leaves_and_scattered_closure_without_training"),
    "source-walk-tests": (
        "two_bit_generation::source_walk_tests::semantic_object_store_parity",
        "two_bit_generation::source_walk_tests::fresh48_source_walk_completes_512_pages_once_and_rejects_excess_before_io"),
    "semantic-router-scorer-tests": (
        "tests::truth_free_v3_config_rejects_truth_unknown_fields_and_old_schema",
        "tests::frozen_marker_requires_complete64_and_binds_measurement_identity")}
stage, started, finished, status, log, log_status = sys.argv[1:7]
tests = None
passes = None
failed = 0
if finished:
    passes = dict.fromkeys(required.get(stage, ()), 0)
    if stage not in ("release", "clippy", "test-build"):
        tests = 0
        with open(log) as source:
            for line in source:
                test = re.fullmatch(r"test (\S+) \.\.\. ok\n?", line)
                if test and test[1] in passes:
                    passes[test[1]] += 1
                match = re.fullmatch(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; \d+ ignored; \d+ measured; \d+ filtered out;.*\n?", line)
                if match:
                    tests += int(match[1]) + int(match[2])
                    failed += int(match[2])
gate = (int(status) or int(log_status) or (96 if tests == 0 or failed or any(count != 1 for count in passes.values()) else 0)) if finished else None
print(json.dumps(dict(schema="borsuk-fixed48-implementation-stage-v1", stage=stage,
    started_at=started, finished_at=finished or None,
    exit_status=int(status) if finished else None, gate_status=gate,
    tests_run=tests, required_test_passes=passes, command=sys.argv[7:]), sort_keys=True), flush=True)
sys.exit(gate or 0)' "$@"
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

run_stage semantic-unit-router-tests cargo test --locked -p borsuk --lib semantic_unit_router::
run_stage source-walk-tests cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests::
run_stage semantic-router-scorer-tests cargo test --locked -p borsuk --bin check_semantic_router_scorer
run_stage generation-integration cargo test --locked -p borsuk --test two_bit_generation
run_stage release cargo build --locked -p borsuk --release --example two_bit_http --bin check_semantic_router_scorer --bin two_bit_plan_demo
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh
