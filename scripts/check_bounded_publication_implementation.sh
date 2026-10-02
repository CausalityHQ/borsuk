#!/usr/bin/env bash
# Exact-source bounded publication acceptance; no full workspace execution claim.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 RUST_TEST_THREADS=1

stage_record() {
  python3 -c 'import json, re, sys
stage, started, finished, status, log, log_status = sys.argv[1:7]
tests = None
cap_test = None
if finished and stage not in ("release", "clippy", "test-build"):
    tests = 0
    if stage == "two-bit-lib-tests":
        cap_test = False
    with open(log) as source:
        for line in source:
            if stage == "two-bit-lib-tests" and line.rstrip() == "test two_bit_store::root_seed_tests::publication_reserves_metadata_before_upload_buffers ... ok":
                cap_test = True
            match = re.fullmatch(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; \d+ ignored; \d+ measured; \d+ filtered out;.*\n?", line)
            if match:
                tests += int(match[1]) + int(match[2])
gate = (int(status) or int(log_status) or (96 if tests == 0 or cap_test is False else 0)) if finished else None
print(json.dumps(dict(schema="borsuk-bounded-publication-implementation-stage-v1",
    stage=stage, started_at=started, finished_at=finished or None,
    exit_status=int(status) if finished else None, gate_status=gate,
    tests_run=tests, publication_cap_test_passed=cap_test, command=sys.argv[7:]), sort_keys=True), flush=True)
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

run_stage semantic-object-store-parity cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests::semantic_object_store_parity -- --exact
run_stage two-bit-lib-tests cargo test --locked -p borsuk --lib two_bit_ -- --skip two_bit_generation::source_walk_tests::semantic_object_store_parity
run_stage sq8-s3-range cargo test --locked -p borsuk --lib sq8_s3_range::
run_stage generation-integration cargo test --locked -p borsuk --test two_bit_generation
run_stage release cargo build --locked -p borsuk --release --example two_bit_http
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh
