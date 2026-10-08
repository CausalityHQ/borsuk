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
ulimit -t > "$evidence/probe-cpu-limit.txt"
test "$(cat "$evidence/probe-cpu-limit.txt")" = 30
test -z "${OPENSSL_ia32cap+x}"
set +e
/usr/bin/time -v -o "$evidence/probe.time" "$1" --ignored --exact sq8_page_authority::tests::page_auth_backend_release_probe --nocapture --test-threads=1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/probe-native-exit"
for field in memory.events memory.peak memory.swap.peak cpu.stat pids.peak; do
 cat "/sys/fs/cgroup${cg}/$field" > "$evidence/probe-$field.after"
done
exit "$status"
