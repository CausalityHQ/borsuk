#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-http
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
printf '%s\n' "$cg" > "$evidence/probe-cgroup-path.txt"
for field in cpu.max memory.max memory.swap.max pids.max cpuset.cpus.effective; do
 cat "/sys/fs/cgroup${cg}/$field" > "$evidence/probe-$field.before"
done
test "$(cat "$evidence/probe-cpu.max.before")" = '100000 100000'
test "$(cat "$evidence/probe-memory.max.before")" = 268435456
test "$(cat "$evidence/probe-memory.swap.max.before")" = 0
test "$(cat "$evidence/probe-pids.max.before")" = 128
test "$(cat "$evidence/probe-cpuset.cpus.effective.before")" = 0
systemctl show borsuk-auth-probe.service -p LoadState -p MainPID -p ActiveState -p SubState -p RuntimeMaxUSec -p LimitCPU -p MemoryMax -p MemorySwapMax -p TasksMax -p AllowedCPUs -p ExecStart -p ControlGroup > "$evidence/probe-systemd-before.txt"
printf '%s\n' "$$" > "$evidence/probe-main-pid-before.txt"
grep -Fxq "MainPID=$$" "$evidence/probe-systemd-before.txt"
grep -Fxq "ControlGroup=$cg" "$evidence/probe-systemd-before.txt"
grep -qx 'LoadState=loaded' "$evidence/probe-systemd-before.txt"
grep -qx 'RuntimeMaxUSec=2min' "$evidence/probe-systemd-before.txt"
grep -qx 'LimitCPU=30' "$evidence/probe-systemd-before.txt"
grep -qx 'MemoryMax=268435456' "$evidence/probe-systemd-before.txt"
grep -qx 'MemorySwapMax=0' "$evidence/probe-systemd-before.txt"
grep -qx 'TasksMax=128' "$evidence/probe-systemd-before.txt"
ulimit -t > "$evidence/probe-cpu-limit.txt"
test "$(cat "$evidence/probe-cpu-limit.txt")" = 30
test -z "${OPENSSL_ia32cap+x}"
export BORSUK_TWO_BIT_FOUR_ROW_SOURCE_COMMIT=fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b
export BORSUK_TWO_BIT_FOUR_ROW_ROTATED_SHA256=2f97ac15a2ffdcc69b78c67a74469703163520e075f85eaf01fba25a0c8727bd
export BORSUK_TWO_BIT_FOUR_ROW_GENERATION_SHA256=5d98b86bc8ec32a2e3ec3c96eb162f14298a1ad80534077f442e04d717c70282
export BORSUK_TWO_BIT_FOUR_ROW_ELF_SHA256=1c1a700d09588f6b423521597cf9dce8af5179557aa63b16a79a0622639355c9
export BORSUK_TWO_BIT_FOUR_ROW_CODEGEN_SHA256=4ffa662f787dff4bf23a5b94e4209b3cbb39ebbbacb27fdc2ce63cc1c535cbef
set +e
/usr/bin/time -v -o "$evidence/probe.time" "$1" --ignored --exact rotated_two_bit::tests::four_row_release_primitive_v1 --nocapture --test-threads=1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/probe-native-exit"
for field in memory.events memory.peak memory.swap.peak cpu.stat pids.peak; do
 cat "/sys/fs/cgroup${cg}/$field" > "$evidence/probe-$field.after"
done
exit "$status"
