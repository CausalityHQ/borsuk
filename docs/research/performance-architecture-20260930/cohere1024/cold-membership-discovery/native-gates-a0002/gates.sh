#!/usr/bin/env bash
set -euo pipefail
test -f /mnt/borsuk-http/FROZEN_SOURCE_AND_ROSTER || exit 98
cd /mnt/borsuk-http/source
export CARGO_BUILD_JOBS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2
unset BORSUK_TEST_BUILD_COMMAND
readonly evidence=/mnt/borsuk-http/evidence
mkdir -p "$evidence"
cgroup_path=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
printf '%s\n' "$cgroup_path" > "$evidence/cgroup-path.txt"
for field in cpu.max memory.max memory.swap.max pids.max; do
  cat "/sys/fs/cgroup${cgroup_path}/${field}" > "$evidence/${field}.before"
done
test "$(cat "$evidence/cpu.max.before")" = '200000 100000'
test "$(cat "$evidence/memory.max.before")" = 8589934592
test "$(cat "$evidence/memory.swap.max.before")" = 0
test "$(cat "$evidence/pids.max.before")" = 512
sha256sum --check /mnt/borsuk-http/source-files.sha256 > "$evidence/source-before.log"
printf '%s\n' "$(rustc --version)" "$(cargo --version)" > "$evidence/toolchain.txt"
stage() {
  local name=$1
  shift
  printf '%s\n' "$*" > "$evidence/${name}.command"
  date -u +%FT%TZ > "$evidence/${name}.started"
  set +e
  /usr/bin/time -v -o "$evidence/${name}.time" "$@" 2>&1 | tee "$evidence/${name}.log"
  local statuses=("${PIPESTATUS[@]}")
  set -e
  printf '%s\n' "${statuses[0]}" > "$evidence/${name}.native-exit"
  printf '%s\n' "${statuses[1]}" > "$evidence/${name}.tee-exit"
  date -u +%FT%TZ > "$evidence/${name}.finished"
  (( statuses[0] == 0 && statuses[1] == 0 ))
}
set +e
(
  set -e
  stage affected-compile cargo test --locked -p borsuk --lib --bin check_cohere_native_baseline --bin check_semantic_router_scorer --example compare_native_replay --no-run
  stage router-regressions cargo test --locked -p borsuk --lib semantic_unit_router:: -- --test-threads=1
  stage generation-regressions cargo test --locked -p borsuk --lib two_bit_generation:: -- --test-threads=1
  stage source-regressions cargo test --locked -p borsuk --lib two_bit_source:: -- --test-threads=1
  stage store-regressions cargo test --locked -p borsuk --lib two_bit_store:: -- --test-threads=1
  stage range-regressions cargo test --locked -p borsuk --lib sq8_s3_range:: -- --test-threads=1
  stage baseline-regressions cargo test --locked -p borsuk --bin check_cohere_native_baseline -- --test-threads=1
  stage scorer-regressions cargo test --locked -p borsuk --bin check_semantic_router_scorer -- --test-threads=1
  stage example-regressions cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1
  stage retained-integration cargo test --locked -p borsuk --test two_bit_generation retained_semantic_ -- --test-threads=1
  stage release cargo build --locked --release -p borsuk --bin check_cohere_native_baseline --example compare_native_replay --example two_bit_http
  stage workspace-clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
  stage real-test-build env -u BORSUK_TEST_BUILD_COMMAND CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
)
status=$?
set -e
printf '%s\n' "$status" > "$evidence/gates-exit"
sha256sum --check /mnt/borsuk-http/source-files.sha256 > "$evidence/source-after.log" || status=96
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
  if [[ -f "/sys/fs/cgroup${cgroup_path}/${field}" ]]; then
    cat "/sys/fs/cgroup${cgroup_path}/${field}" > "$evidence/${field}.after"
  fi
done
printf '%s\n' "$status" > "$evidence/final-exit"
exit "$status"
