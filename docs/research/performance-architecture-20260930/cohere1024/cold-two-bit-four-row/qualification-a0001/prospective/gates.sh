#!/usr/bin/env bash
set -euo pipefail
test -f /mnt/borsuk-http/FROZEN_FOUR_ROW_SOURCE_AND_ROSTER || exit 98
cd /mnt/borsuk-http/source
export CARGO_BUILD_JOBS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2
unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
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

test "$(uname -m)" = x86_64
cat /proc/cpuinfo > "$evidence/cpuinfo.txt"
lscpu > "$evidence/lscpu.txt"
grep -m1 "^flags" /proc/cpuinfo | grep -qw avx2
printf "%s\n" "${RUSTFLAGS-}" "${CARGO_ENCODED_RUSTFLAGS-}" > "$evidence/compiler-env.txt"
test -z "${RUSTFLAGS-}"
test -z "${CARGO_ENCODED_RUSTFLAGS-}"

control=/mnt/borsuk-http/control-source
test ! -e "$control"
mkdir "$control"
tar -xzf /mnt/borsuk-http/source.tar.gz -C "$control"
cp /mnt/borsuk-http/control-rotated_two_bit.rs "$control/crates/borsuk/src/rotated_two_bit.rs"
cp /mnt/borsuk-http/control-two_bit_generation.rs "$control/crates/borsuk/src/two_bit_generation.rs"
(cd "$control" && sha256sum --check /mnt/borsuk-http/control-source-files.sha256) > "$evidence/control-source-before.log"
mkdir /mnt/borsuk-http/retained
rustc -vV > "$evidence/rustc-verbose.txt"
for dir in /mnt/borsuk-http/source /mnt/borsuk-http/control-source; do
  sha256sum "$dir/Cargo.toml" "$dir/Cargo.lock"
done > "$evidence/matched-profile-inputs.sha256"
# Both compiler invocations share the explicit empty Rust flags and toolchain.
test ! -e /mnt/.cargo/config && test ! -e /mnt/.cargo/config.toml
test ! -e /mnt/borsuk-http/.cargo/config && test ! -e /mnt/borsuk-http/.cargo/config.toml
test ! -e "$CARGO_HOME/config" && test ! -e "$CARGO_HOME/config.toml"
test ! -e /root/.cargo/config && test ! -e /root/.cargo/config.toml
for dir in /mnt/borsuk-http/source /mnt/borsuk-http/control-source; do
  test ! -e "$dir/.cargo/config" && test ! -e "$dir/.cargo/config.toml"
done
printf '%s\n' 'no ancestor/source/CARGO_HOME/root Cargo configuration overrides' > "$evidence/cargo-config-admission.txt"

set +e
(
 set -e
 stage runner-debug cargo test --locked -p borsuk --bin check_cohere_native_baseline -- --test-threads=1
 stage planner-debug cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests:: -- --test-threads=1
 stage codec-debug cargo test --locked -p borsuk --lib rotated_two_bit::tests:: -- --test-threads=1
 stage probe-compile cargo test --locked --release -p borsuk --lib --no-run --message-format=json
 python3 /mnt/borsuk-http/select-probe.py
 sha256sum "$(cat "$evidence/probe-executable.txt")" > "$evidence/probe-elf-before-tests.sha256"
 stage codec-release cargo test --locked --release -p borsuk --lib rotated_two_bit::tests:: -- --test-threads=1
 stage planner-release cargo test --locked --release -p borsuk --lib two_bit_generation::source_walk_tests:: -- --test-threads=1
 stage runner-release cargo test --locked --release -p borsuk --bin check_cohere_native_baseline -- --test-threads=1
 stage integration cargo test --locked -p borsuk --test rotated_two_bit --test two_bit_source --test two_bit_generation -- --test-threads=1
 stage release cargo build --locked --release -p borsuk --bin check_cohere_native_baseline --example two_bit_http
 cp /mnt/borsuk-http/source/target/release/check_cohere_native_baseline /mnt/borsuk-http/retained/candidate-check_cohere_native_baseline
 cp /mnt/borsuk-http/source/target/release/examples/two_bit_http /mnt/borsuk-http/retained/candidate-two_bit_http
 sha256sum /mnt/borsuk-http/retained/candidate-* > "$evidence/candidate-serving-before-control.sha256"
 stage control-release env CARGO_TARGET_DIR=/mnt/borsuk-http/source/target cargo build --locked --release --manifest-path /mnt/borsuk-http/control-source/Cargo.toml -p borsuk --bin check_cohere_native_baseline --example two_bit_http
 cp /mnt/borsuk-http/source/target/release/check_cohere_native_baseline /mnt/borsuk-http/retained/control-check_cohere_native_baseline
 cp /mnt/borsuk-http/source/target/release/examples/two_bit_http /mnt/borsuk-http/retained/control-two_bit_http
 (cd /mnt/borsuk-http/control-source && sha256sum --check /mnt/borsuk-http/control-source-files.sha256) > "$evidence/control-source-after.log"
 sha256sum --check "$evidence/candidate-serving-before-control.sha256" > "$evidence/candidate-serving-after-control.log"
 stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
 stage test-build env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
 stage scalar-control-release env CARGO_TARGET_DIR=/mnt/borsuk-http/target-scalar-control cargo test --locked --release --features scalar-control -p borsuk --lib --message-format=json rotated_two_bit::tests:: -- --test-threads=1
 python3 /mnt/borsuk-http/select-scalar-control.py
 sha256sum --check "$evidence/probe-elf-before-tests.sha256" > "$evidence/probe-elf-after-tests.log"
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
