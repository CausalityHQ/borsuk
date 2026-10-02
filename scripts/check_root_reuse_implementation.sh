#!/usr/bin/env bash
# Exact-source authenticated root-reuse acceptance; no full workspace execution claim.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1

stage_record() {
  python3 -c 'import json, sys
print(json.dumps(dict(schema="borsuk-root-reuse-implementation-stage-v1",
    stage=sys.argv[1], started_at=sys.argv[2], finished_at=sys.argv[3] or None,
    exit_status=int(sys.argv[4]) if sys.argv[4] else None, command=sys.argv[5:]), sort_keys=True))' "$@"
}

run_stage() {
  local stage="$1" started finished status
  shift
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" '' '' "$@"
  set +e
  "$@"
  status=$?
  set -e
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" "$finished" "$status" "$@"
  return "$status"
}

run_stage object-native-generation-tests cargo test --locked -p borsuk --lib object_native_generation:: -- --test-threads=1
run_stage two-bit-store-tests cargo test --locked -p borsuk --lib two_bit_store:: -- --test-threads=1
run_stage two-bit-generation-tests cargo test --locked -p borsuk --lib two_bit_generation:: -- --test-threads=1
# Index/restart assertions live in the generation fixtures above; no index-local tests exist.
run_stage generation-integration cargo test --locked -p borsuk --test two_bit_generation -- --test-threads=1
run_stage gc-integration cargo test --locked -p borsuk --test two_bit_gc_delayed_delete -- --test-threads=1
run_stage application-ids-integration cargo test --locked -p borsuk --test two_bit_application_ids -- --test-threads=1
run_stage http-example-tests cargo test --locked -p borsuk --example two_bit_http -- --test-threads=1
run_stage release cargo build --release --locked -p borsuk --example two_bit_http
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh
