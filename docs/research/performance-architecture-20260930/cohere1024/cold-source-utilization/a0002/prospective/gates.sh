#!/usr/bin/env bash
set -euo pipefail
# Draft: root must bind final reviewed source, inventory, and inputs before launch.
root=/mnt/borsuk-http
evidence=$root/evidence
test -f "$root/FROZEN_SOURCE_UTILIZATION_AUTHORITY"
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
  if [[ "$name" == replay ]]; then
    remaining=240
  else
    remaining=$(( qualification_deadline - SECONDS ))
    (( remaining > 0 )) || return 124
  fi
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
  stage inventory cargo test --locked -p borsuk --example compare_native_replay -- --list
  stage affected cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1
  stage release cargo build --release --locked -p borsuk --example compare_native_replay
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
cp target/release/examples/compare_native_replay "$root/retained/compare_native_replay"
sha256sum "$root/retained/compare_native_replay" > "$evidence/reducer.binary.sha256"
stat -c %s "$root/retained/compare_native_replay" > "$evidence/reducer.binary.bytes"
# Only the qualified evidence binary may run; no ANN, vectors, corpus, or truth.
sha256sum --check "$root/replay-inputs.sha256" > "$evidence/replay-inputs-before.log"
stage replay systemd-run --unit=borsuk-source-utilization-replay --slice=borsukauth.slice --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=120 -p RestrictAddressFamilies=AF_UNIX /bin/bash "$root/replay.sh"
sha256sum --check "$root/replay-inputs.sha256" > "$evidence/replay-inputs-after.log"
sha256sum --check "$evidence/reducer.binary.sha256" > "$evidence/reducer.binary-after.log"
printf '%s\n' 0 > "$evidence/final-exit"
