#!/usr/bin/env bash
set -euo pipefail
test -f /mnt/borsuk-http/FROZEN_SOURCE_AND_ROSTER || exit 98
cd /mnt/borsuk-http/source
export CARGO_BUILD_JOBS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2
test -z "${OPENSSL_ia32cap+x}"
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

test -z "${OPENSSL_ia32cap+x}"
uname -m > "$evidence/architecture.txt"
test "$(cat "$evidence/architecture.txt")" = x86_64
cat /proc/cpuinfo > "$evidence/cpuinfo.txt"
lscpu > "$evidence/lscpu.txt"
grep -m1 '^flags' /proc/cpuinfo | grep -qw sha_ni
grep -m1 '^flags' /proc/cpuinfo | grep -qw avx512f
set +e
(
 set -e
 mkdir /mnt/borsuk-http/baseline-metadata
 tar -xzf /mnt/borsuk-http/source.tar.gz -C /mnt/borsuk-http/baseline-metadata
 cp /mnt/borsuk-http/baseline-Cargo.toml /mnt/borsuk-http/baseline-metadata/crates/borsuk/Cargo.toml
 cp /mnt/borsuk-http/baseline-Cargo.lock /mnt/borsuk-http/baseline-metadata/Cargo.lock
 stage baseline-metadata cargo metadata --locked --format-version 1 --manifest-path /mnt/borsuk-http/baseline-metadata/Cargo.toml
 stage candidate-metadata cargo metadata --locked --format-version 1
 stage baseline-features cargo tree --locked -e features -i aws-lc-rs@1.18.1 --manifest-path /mnt/borsuk-http/baseline-metadata/Cargo.toml
 stage candidate-features cargo tree --locked -e features -i aws-lc-rs@1.18.1
 stage baseline-sys-features cargo tree --locked -e features -i aws-lc-sys@0.45.0 --manifest-path /mnt/borsuk-http/baseline-metadata/Cargo.toml
 stage candidate-sys-features cargo tree --locked -e features -i aws-lc-sys@0.45.0
 python3 /mnt/borsuk-http/feature-check.py
  stage correctness-01 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib sq8_page_authority::tests:: -- --test-threads=1
  stage correctness-02 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib sq8_s3_range::tests:: -- --test-threads=1
  stage correctness-03 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib rotated_two_bit::tests:: -- --test-threads=1
  stage correctness-04 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib exact_sq8_nominee::tests:: -- --test-threads=1
  stage correctness-05 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib returned_sq8::tests:: -- --test-threads=1
  stage correctness-06 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests:: -- --test-threads=1
  stage correctness-07 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib two_bit_store::root_seed_tests:: -- --test-threads=1
  stage correctness-08 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib exact_sq8_mirror::range_tests:: -- --test-threads=1
  stage correctness-09 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --lib fine_sq8_groups::tests:: -- --test-threads=1
  stage correctness-10 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --test rotated_two_bit --test two_bit_source --test two_bit_generation --test exact_sq8_mirror_direct -- --test-threads=1
  stage correctness-11 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --bin publish_two_bit_generation -- --test-threads=1
  stage correctness-12 env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --features scalar-control --lib exact_sq8_nominee::tests:: -- --test-threads=1
 stage release-probe-compile env CARGO_BUILD_JOBS=1 cargo test --locked --release -p borsuk --lib --no-run --message-format=json
 python3 /mnt/borsuk-http/select-probe.py
 probe_binary=$(cat "$evidence/probe-executable.txt")
 systemd-run --unit=borsuk-auth-probe --slice=borsukauth.slice --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p LimitCPU=30 -p RuntimeMaxSec=120 -p WorkingDirectory=/mnt/borsuk-http/source /bin/bash /mnt/borsuk-http/probe.sh "$probe_binary" 2>&1 | tee "$evidence/probe.log"
 systemctl show borsuk-auth-probe.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result -p MemoryPeak -p MemorySwapPeak -p CPUUsageNSec -p RuntimeMaxUSec -p LimitCPU -p TasksMax -p MemoryMax -p MemorySwapMax -p AllowedCPUs > "$evidence/probe-systemd.txt"
 probe_cg=$(cat "$evidence/probe-cgroup-path.txt")
 for field in memory.events memory.peak memory.swap.peak cpu.stat pids.peak; do
   if [[ -f "/sys/fs/cgroup${probe_cg}/$field" ]]; then cat "/sys/fs/cgroup${probe_cg}/$field" > "$evidence/probe-$field.after"; fi
 done
 python3 /mnt/borsuk-http/probe-check.py
 stage qualification-01 env CARGO_BUILD_JOBS=1 cargo build --locked --release -p borsuk --bin check_cohere_native_baseline --bin check_semantic_router_scorer --bin publish_two_bit_generation --example two_bit_http
 stage qualification-02 env CARGO_BUILD_JOBS=1 cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
 stage qualification-03 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
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
