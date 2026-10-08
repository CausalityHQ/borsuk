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
export BORSUK_TWO_BIT_GATHER_SOURCE_COMMIT=751b5327571c7fec17af4bbdd7f1b1594f0aaf9d
set +e
/usr/bin/time -v -o "$evidence/probe.time" "$1" --ignored --exact rotated_two_bit::tests::two_bit_gather_release_primitive --nocapture --test-threads=1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/probe-native-exit"
for field in memory.events memory.peak memory.swap.peak cpu.stat pids.peak; do
 cat "/sys/fs/cgroup${cg}/$field" > "$evidence/probe-$field.after"
done
exit "$status"
