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
/usr/bin/time -v -o $evidence/publisher.time timeout --kill-after=10 570 $root/publisher --retained $root/config.json bce8df643fc8bd996939ab1cd3c9c8362c279c76d215690aa6d6ae462cbbad9a $evidence/publication-receipt.json > $evidence/publisher.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/native-exit
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > $evidence/$field.after; fi; done
sha256sum -c $root/pins.sha256 > $evidence/pins-after.txt || status=96
find $root/scratch -mindepth 1 -printf '%p\n' > $evidence/scratch-files.txt
if test -s $evidence/scratch-files.txt; then status=96; fi
printf '%s\n' "$status" > $evidence/final-exit
exit "$status"
