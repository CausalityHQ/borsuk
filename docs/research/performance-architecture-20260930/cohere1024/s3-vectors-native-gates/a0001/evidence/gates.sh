#!/usr/bin/env bash
set -euo pipefail
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
  stage resolve-lock bash /mnt/borsuk-http/resolve-lock.sh
  cp Cargo.lock "$evidence/Cargo.lock.resolved"
  test "$(stat -c %s Cargo.lock)" -le 1048576
  sha256sum --check "$evidence/source-resolved.sha256" > "$evidence/resolved-source-before.log"
  stage sdk-tests env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --features s3-vectors-bench --bin check_s3_vectors_baseline -- --test-threads=1
  stage sdk-release env CARGO_BUILD_JOBS=1 cargo build --locked --release -p borsuk --features s3-vectors-bench --bin check_s3_vectors_baseline
  stage workspace-feature-clippy env CARGO_BUILD_JOBS=1 cargo clippy --locked --workspace --all-targets --features borsuk/s3-vectors-bench -- -D clippy::correctness -D clippy::suspicious
  stage workspace-default-test-build env -u BORSUK_TEST_BUILD_COMMAND CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
)
status=$?
set -e
printf '%s\n' "$status" > "$evidence/gates-exit"
if [[ -f "$evidence/source-resolved.sha256" ]]; then
  sha256sum --check "$evidence/source-resolved.sha256" > "$evidence/source-after.log" || status=96
else
  status=96
fi
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
  if [[ -f "/sys/fs/cgroup${cgroup_path}/${field}" ]]; then
    cat "/sys/fs/cgroup${cgroup_path}/${field}" > "$evidence/${field}.after"
  fi
done
printf '%s\n' "$status" > "$evidence/final-exit"
exit "$status"
