#!/bin/bash
set -euo pipefail
test -f /mnt/borsuk-retained-s3/FROZEN_MEMBERSHIP_ADMISSION || exit 98
root=/mnt/borsuk-retained-s3
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
    if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > "$evidence/$field.after"; fi
  done
  status=$original
  sha256sum -c "$root/pins.sha256" > "$evidence/pins-after.txt" || { if (( status == 0 )); then status=96; fi; }
  find "$root/membership-admission-a0001/query-scratch" -mindepth 1 -printf '%p\n' > "$evidence/scratch-files.txt" || { if (( status == 0 )); then status=96; fi; }
  if test -s "$evidence/scratch-files.txt" && (( status == 0 )); then status=96; fi
  printf '%s\n' "$original" > "$evidence/original-exit"
  printf '%s\n' "$status" > "$evidence/final-exit"
  exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM
for field in cpu.max memory.max memory.swap.max pids.max; do cat /sys/fs/cgroup${cg}/$field > $evidence/$field.before; done
test "$(cat $evidence/cpu.max.before)" = '100000 100000'
test "$(cat $evidence/memory.max.before)" = 536870912
test "$(cat $evidence/memory.swap.max.before)" = 0
test "$(cat $evidence/pids.max.before)" = 256
sha256sum -c $root/pins.sha256 > $evidence/pins-before.txt
export RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1 BORSUK_CPU_THREADS=1
set +e
/usr/bin/time -v -o $evidence/runner.time timeout --kill-after=10 2370 $root/runner $root/config.json e604a1ca02644329113e6c11a935d67115f784c40008859966f7e332fae53a3a $evidence/admission-result.jsonl > $evidence/runner.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/native-exit
exit "$status"
