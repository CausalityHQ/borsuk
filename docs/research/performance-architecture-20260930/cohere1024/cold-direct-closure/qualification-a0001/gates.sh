#!/usr/bin/env bash
set -euo pipefail
# UNFROZEN draft; no launch authority. Root must bind final OWN2 source/inventory.
# Compiler/correctness only; no external corpus, requests, truth or science replay.
root=/mnt/borsuk-http
evidence=$root/evidence
test -f "$root/FROZEN_DIRECT_CLOSURE_AUTHORITY"
cd "$root/source"
export CARGO_BUILD_JOBS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1
unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
for f in cpu.max memory.max memory.swap.max pids.max; do
  cat "/sys/fs/cgroup${cg}/$f" > "$evidence/$f.before"
done
test "$(cat "$evidence/cpu.max.before")" = '200000 100000'
test "$(cat "$evidence/memory.max.before")" = 8589934592
test "$(cat "$evidence/memory.swap.max.before")" = 0
test "$(cat "$evidence/pids.max.before")" = 512
sha256sum --check "$root/source-files.sha256" > "$evidence/source-before.log"
rustc -vV > "$evidence/rustc.txt"
cargo --version > "$evidence/cargo.txt"
for d in /mnt/.cargo "$root/.cargo" "$CARGO_HOME" /root/.cargo "$root/source/.cargo"; do
  test ! -e "$d/config" && test ! -e "$d/config.toml"
done
stage() {
  local name=$1
  shift
  local remaining
  remaining=$(( qualification_deadline - SECONDS ))
  (( remaining > 0 )) || return 124
  printf '%s\n' "$*" > "$evidence/$name.command"
  printf '%s\n' "$remaining" > "$evidence/$name.timeout-seconds"
  date -u +%FT%TZ > "$evidence/$name.started"
  set +e
  /usr/bin/time -v -o "$evidence/$name.time" timeout --kill-after=30 "$remaining" "$@" 2>&1 | tee "$evidence/$name.log"
  local exits=("${PIPESTATUS[@]}")
  set -e
  printf '%s\n' "${exits[0]}" > "$evidence/$name.native-exit"
  printf '%s\n' "${exits[1]}" > "$evidence/$name.tee-exit"
  date -u +%FT%TZ > "$evidence/$name.finished"
  (( exits[0] == 0 && exits[1] == 0 ))
}
qualification_deadline=$(( SECONDS + 7200 ))
set +e
(
  set -e
  stage lib-inventory cargo test --locked -p borsuk --lib source_walk_tests -- --list
  stage bin-inventory cargo test --locked -p borsuk --bin check_cohere_native_baseline -- --list
  stage affected-lib cargo test --locked -p borsuk --lib source_walk_tests -- --test-threads=1
  stage affected-bin cargo test --locked -p borsuk --bin check_cohere_native_baseline -- --test-threads=1
  stage release cargo build --release --locked -p borsuk --lib --bin check_cohere_native_baseline
  stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
  stage test-build env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
)
qualification_exit=$?
set -e
printf '%s\n' "$qualification_exit" > "$evidence/qualification-exit"
sha256sum --check "$root/source-files.sha256" > "$evidence/source-after.log"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do
  if [[ -f "/sys/fs/cgroup${cg}/$f" ]]; then
    cat "/sys/fs/cgroup${cg}/$f" > "$evidence/$f.after"
  fi
done
(( qualification_exit == 0 )) || exit "$qualification_exit"
mkdir "$root/retained"
cp target/release/check_cohere_native_baseline "$root/retained/check_cohere_native_baseline"
sha256sum "$root/retained/check_cohere_native_baseline" > "$evidence/runner.binary.sha256"
stat -c %s "$root/retained/check_cohere_native_baseline" > "$evidence/runner.binary.bytes"
printf '%s\n' 0 > "$evidence/final-exit"
