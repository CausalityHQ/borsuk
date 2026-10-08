#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-http
evidence=$root/evidence
test -z "${OPENSSL_ia32cap+x}"
test "$(uname -m)" = x86_64
cat /proc/cpuinfo > "$evidence/cpuinfo.txt"
lscpu > "$evidence/lscpu.txt"
grep -m1 '^flags' /proc/cpuinfo | grep -qw avx2
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
for field in cpu.max memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup${cg}/$field" > "$evidence/gates-$field.before"; done
test "$(cat "$evidence/gates-cpu.max.before")" = '200000 100000'
test "$(cat "$evidence/gates-memory.max.before")" = 8589934592
test "$(cat "$evidence/gates-memory.swap.max.before")" = 0
test "$(cat "$evidence/gates-pids.max.before")" = 512
systemd-run --unit=borsuk-four-row-staging-canary --slice=borsukauth.slice --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=30 -p WorkingDirectory="$root" "$root/probe-libtest" --list --exact rotated_two_bit::tests::four_row_release_primitive_v1 > "$evidence/staging-canary.log" 2>&1
printf '0\n' > "$evidence/staging-canary-native-exit"
grep -Fxq 'rotated_two_bit::tests::four_row_release_primitive_v1: test' "$evidence/staging-canary.log"
set +e
systemd-run --unit=borsuk-auth-probe --slice=borsukauth.slice --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p LimitCPU=30 -p RuntimeMaxSec=120 -p WorkingDirectory="$root" /bin/bash "$root/probe.sh" "$root/probe-libtest" 2>&1 | tee "$evidence/probe.log"
statuses=("${PIPESTATUS[@]}")
set -e
printf '%s\n' "${statuses[0]}" > "$evidence/probe-unit-native-exit"
printf '%s\n' "${statuses[1]}" > "$evidence/probe-unit-tee-exit"
status=43
# The single Rust test intentionally asserts ACCEPT after emitting its receipt.
# Exit 101 is eligible for classification only with the matching original exit,
# complete named harness failure and expected assertion, never from unit failure.
if (( statuses[1] == 0 )) && [[ -f "$evidence/probe-native-exit" ]]; then
 native=$(cat "$evidence/probe-native-exit")
 eligible=false
 if (( statuses[0] == 0 && native == 0 )); then eligible=true; fi
 if (( statuses[0] == 101 && native == 101 )) &&
   grep -Eq '^test rotated_two_bit::tests::four_row_release_primitive_v1 \.\.\. ' "$evidence/probe.log" &&
   grep -q 'primitive receipt must be independently classified' "$evidence/probe.log" &&
   grep -q 'test result: FAILED. 0 passed; 1 failed;' "$evidence/probe.log"; then eligible=true; fi
 if [[ "$eligible" == true ]]; then
  set +e
  python3 "$root/probe-check.py"
  status=$?
  set -e
  if (( status != 0 && status != 42 && status != 43 )); then status=43; fi
  if (( status == 0 && native != 0 )); then status=43; fi
  if (( status == 42 && native != 101 )); then status=43; fi
 fi
fi
sha256sum "$root/probe-libtest" > "$evidence/elf-after.sha256"
for field in memory.events memory.peak memory.swap.peak cpu.stat pids.peak; do cat "/sys/fs/cgroup${cg}/$field" > "$evidence/gates-$field.after"; done
printf '%s\n' "$status" > "$evidence/final-exit"
exit "$status"
