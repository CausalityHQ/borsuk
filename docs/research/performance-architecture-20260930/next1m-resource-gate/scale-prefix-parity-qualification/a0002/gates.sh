#!/usr/bin/env bash
set -euo pipefail
cd /mnt/borsuk-reducer/source
export CARGO_BUILD_JOBS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2
unset BORSUK_TEST_BUILD_COMMAND
readonly evidence=/mnt/borsuk-reducer/evidence
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
sha256sum --check /mnt/borsuk-reducer/source-files.sha256 > "$evidence/source-before.log"
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
  stage failed-dispatch cargo test --locked -p borsuk --example compare_native_replay tests::scale_prefix_parity_full_dispatch_seals_and_input_binding -- --exact --test-threads=1
  test "$(grep -Fxc 'test tests::scale_prefix_parity_full_dispatch_seals_and_input_binding ... ok' "$evidence/failed-dispatch.log")" = 1
  grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$evidence/failed-dispatch.log"
  stage failed-role-binding cargo test --locked -p borsuk --example compare_native_replay tests::scale_parity_role_aliases_and_native_config_reuse_are_refused -- --exact --test-threads=1
  test "$(grep -Fxc 'test tests::scale_parity_role_aliases_and_native_config_reuse_are_refused ... ok' "$evidence/failed-role-binding.log")" = 1
  grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$evidence/failed-role-binding.log"
  stage reducer-tests cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1
  test "$(grep -Fxc 'test scale_prefix_parity_tests::scale_prefix_authenticates_both_bodies_and_refuses_suffix_and_prefix_changes ... ok' "$evidence/reducer-tests.log")" = 1
  test "$(grep -Fxc 'test scale_prefix_parity_tests::scale_parity_refuses_order_score_recall_underfill_and_per_query_charge_changes ... ok' "$evidence/reducer-tests.log")" = 1
  test "$(grep -Fxc 'test scale_prefix_parity_tests::scale_parity_keeps_source_policy_and_unknown_fields_invariant ... ok' "$evidence/reducer-tests.log")" = 1
  test "$(grep -Fxc 'test scale_prefix_parity_tests::scale_parity_refuses_path_replacement_even_with_identical_bytes ... ok' "$evidence/reducer-tests.log")" = 1
  test "$(grep -Fxc 'test tests::scale_prefix_parity_full_dispatch_seals_and_input_binding ... ok' "$evidence/reducer-tests.log")" = 1
  test "$(grep -Fxc 'test tests::scale_parity_role_aliases_and_native_config_reuse_are_refused ... ok' "$evidence/reducer-tests.log")" = 1
  grep -Eq '^test result: ok\. 86 passed; 0 failed; 0 ignored;' "$evidence/reducer-tests.log"
  stage reducer-release cargo build --locked -p borsuk --release --example compare_native_replay
  stage workspace-clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
  stage workspace-test-build env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
)
status=$?
set -e
printf '%s\n' "$status" > "$evidence/gates-exit"
sha256sum --check /mnt/borsuk-reducer/source-files.sha256 > "$evidence/source-after.log" || status=96
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
  if [[ -f "/sys/fs/cgroup${cgroup_path}/${field}" ]]; then
    cat "/sys/fs/cgroup${cgroup_path}/${field}" > "$evidence/${field}.after"
  fi
done
if (( status == 0 )); then
  cp target/release/examples/compare_native_replay "$evidence/compare_native_replay"
  sha256sum "$evidence/compare_native_replay" > "$evidence/binary.sha256"
fi
printf '%s\n' "$status" > "$evidence/final-exit"
exit "$status"
