#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-http
evidence=$root/evidence
test -z "${OPENSSL_ia32cap+x}"
test "$(uname -m)" = x86_64
cat /proc/cpuinfo > "$evidence/cpuinfo.txt"
lscpu > "$evidence/lscpu.txt"
grep -m1 '^flags' /proc/cpuinfo | grep -qw sha_ni
grep -m1 '^flags' /proc/cpuinfo | grep -qw avx512f
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
for field in cpu.max memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup${cg}/$field" > "$evidence/gates-$field.before"; done
test "$(cat "$evidence/gates-cpu.max.before")" = '200000 100000'
test "$(cat "$evidence/gates-memory.max.before")" = 8589934592
test "$(cat "$evidence/gates-memory.swap.max.before")" = 0
test "$(cat "$evidence/gates-pids.max.before")" = 512
set +e
systemd-run --unit=borsuk-auth-probe --slice=borsukauth.slice --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p LimitCPU=30 -p RuntimeMaxSec=120 -p WorkingDirectory="$root" /bin/bash "$root/probe.sh" "$root/probe-libtest" 2>&1 | tee "$evidence/probe.log"
statuses=("${PIPESTATUS[@]}")
set -e
printf '%s\n' "${statuses[0]}" > "$evidence/probe-unit-native-exit"
printf '%s\n' "${statuses[1]}" > "$evidence/probe-unit-tee-exit"
status=43
if (( statuses[0] == 0 && statuses[1] == 0 )); then
 set +e
 python3 "$root/probe-check.py"
 status=$?
 set -e
fi
sha256sum "$root/probe-libtest" > "$evidence/elf-after.sha256"
for field in memory.events memory.peak memory.swap.peak cpu.stat pids.peak; do cat "/sys/fs/cgroup${cg}/$field" > "$evidence/gates-$field.after"; done
printf '%s\n' "$status" > "$evidence/final-exit"
exit "$status"
