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
set +e
/usr/bin/time -v -o $evidence/pair1.time timeout --kill-after=10 120 $root/reducer --paired-v2 $root/pair1.config.json 04c12e968b8dc574477fcd7963db42b4b157a9ad9114c895d188cfe536de4c7c $evidence/pair1.report.json > $evidence/pair1.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/pair1.native-exit
if (( status != 0 )); then printf '%s\n' "$status" > $evidence/native-exit; exit "$status"; fi
jq -e '.status == "MEASURED" and .complete == true and .semantic_parity == true and .trace_plan_parity == true and .logical_charge_parity == true' $evidence/pair1.report.json > $evidence/pair1.check.txt
set +e
/usr/bin/time -v -o $evidence/pair2.time timeout --kill-after=10 120 $root/reducer --paired-v2 $root/pair2.config.json 5c5fcdd4576d22c7dcb71a27e07d868d3859e29b329b20e5a5ab0eb5da7e73ef $evidence/pair2.report.json > $evidence/pair2.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/pair2.native-exit
if (( status != 0 )); then printf '%s\n' "$status" > $evidence/native-exit; exit "$status"; fi
jq -e '.status == "MEASURED" and .complete == true and .semantic_parity == true and .trace_plan_parity == true and .logical_charge_parity == true' $evidence/pair2.report.json > $evidence/pair2.check.txt
printf '0\n' > $evidence/native-exit
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > $evidence/$field.after; fi; done
sha256sum -c $root/pins.sha256 > $evidence/pins-after.txt
find $root/query-scratch -mindepth 1 -printf '%p\n' > $evidence/scratch-files.txt
test ! -s $evidence/scratch-files.txt
printf '0\n' > $evidence/final-exit
