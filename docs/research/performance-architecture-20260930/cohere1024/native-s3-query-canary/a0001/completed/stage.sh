#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-retained-s3
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
for field in cpu.max memory.max memory.swap.max pids.max; do cat /sys/fs/cgroup${cg}/$field > $evidence/$field.before; done
test "$(cat $evidence/cpu.max.before)" = '100000 100000'
test "$(cat $evidence/memory.max.before)" = 536870912
test "$(cat $evidence/memory.swap.max.before)" = 0
test "$(cat $evidence/pids.max.before)" = 256
sha256sum -c $root/pins.sha256 > $evidence/pins-before.txt
export RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1 BORSUK_CPU_THREADS=1
set +e
/usr/bin/time -v -o $evidence/runner.time timeout --kill-after=10 90 $root/runner $root/config.json 0000000000000000000000000000000000000000000000000000000000000000 $evidence/canary-result.jsonl > $evidence/runner.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/native-exit
if (( status != 2 )); then
  status=96
elif ! jq -se 'length == 2 and .[0].phase == "identity" and .[1].phase == "terminal" and .[1].summary.status == "INVALID" and .[1].summary.stage == "config" and .[1].summary.error == "artifact identity/SHA" and .[1].summary.completed_queries == 0 and .[1].summary.truth_opened == false and .[1].summary.all_queries_sealed == false and .[1].summary.sum.submitted_gets == 0 and .[1].summary.binding_charge.submitted_gets == 0' "$evidence/canary-result.jsonl" > "$evidence/expected-refusal.txt"; then
  status=96
else
  status=0
fi
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > $evidence/$field.after; fi; done
sha256sum -c $root/pins.sha256 > $evidence/pins-after.txt || status=96
find $root/query-scratch -mindepth 1 -printf '%p\n' > $evidence/scratch-files.txt
if test -s $evidence/scratch-files.txt; then status=96; fi
printf '%s\n' "$status" > $evidence/final-exit
exit "$status"
