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
status=0
sha256sum -c $root/pins.sha256 > $evidence/A1.pins-before.txt
set +e
/usr/bin/time -v -o $evidence/A1.time timeout --kill-after=10 1200 $root/runner $root/config-16.json d978441084d11a74deae7304e645a00a5abb528326430ac769dca05f3848decd $evidence/A1.jsonl > $evidence/A1.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/A1.native-exit
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/A1.pins-after.txt
sha256sum -c $root/pins.sha256 > $evidence/B1.pins-before.txt
set +e
/usr/bin/time -v -o $evidence/B1.time timeout --kill-after=10 1200 $root/runner $root/config-32.json a3b8b1f37feb4ff7a812b70ef8b2dff548c93d8eb7639175195338c60e2c0228 $evidence/B1.jsonl > $evidence/B1.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/B1.native-exit
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/B1.pins-after.txt
sha256sum -c $root/pins.sha256 > $evidence/B2.pins-before.txt
set +e
/usr/bin/time -v -o $evidence/B2.time timeout --kill-after=10 1200 $root/runner $root/config-32.json a3b8b1f37feb4ff7a812b70ef8b2dff548c93d8eb7639175195338c60e2c0228 $evidence/B2.jsonl > $evidence/B2.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/B2.native-exit
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/B2.pins-after.txt
sha256sum -c $root/pins.sha256 > $evidence/A2.pins-before.txt
set +e
/usr/bin/time -v -o $evidence/A2.time timeout --kill-after=10 1200 $root/runner $root/config-16.json d978441084d11a74deae7304e645a00a5abb528326430ac769dca05f3848decd $evidence/A2.jsonl > $evidence/A2.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/A2.native-exit
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/A2.pins-after.txt
printf "%s\n" "$status" > $evidence/native-exit
a_bytes=$(stat -c %s $evidence/A1.jsonl)
a_sha=$(sha256sum $evidence/A1.jsonl | cut -d' ' -f1)
b_bytes=$(stat -c %s $evidence/B1.jsonl)
b_sha=$(sha256sum $evidence/B1.jsonl | cut -d' ' -f1)
jq --arg ap "$evidence/A1.jsonl" --arg bp "$evidence/B1.jsonl" --argjson ab "$a_bytes" --argjson bb "$b_bytes" --arg ah "$a_sha" --arg bh "$b_sha" '.arms[0].input = {path:$ap,bytes:$ab,sha256:$ah} | .arms[1].input = {path:$bp,bytes:$bb,sha256:$bh}' $root/paired-reduction-template.json > $evidence/pair1.config.json
config_sha=$(sha256sum $evidence/pair1.config.json | cut -d' ' -f1)
set +e
/usr/bin/time -v -o $evidence/pair1.time timeout --kill-after=10 120 $root/reducer --paired-v2 $evidence/pair1.config.json "$config_sha" $evidence/pair1.report.json > $evidence/pair1.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/pair1.native-exit
if (( status != 0 )); then exit "$status"; fi
jq -e '.status == "MEASURED" and .complete == true and .semantic_parity == true and .trace_plan_parity == true and .logical_charge_parity == true' $evidence/pair1.report.json > $evidence/pair1.check.txt
a_bytes=$(stat -c %s $evidence/A2.jsonl)
a_sha=$(sha256sum $evidence/A2.jsonl | cut -d' ' -f1)
b_bytes=$(stat -c %s $evidence/B2.jsonl)
b_sha=$(sha256sum $evidence/B2.jsonl | cut -d' ' -f1)
jq --arg ap "$evidence/A2.jsonl" --arg bp "$evidence/B2.jsonl" --argjson ab "$a_bytes" --argjson bb "$b_bytes" --arg ah "$a_sha" --arg bh "$b_sha" '.arms[0].input = {path:$ap,bytes:$ab,sha256:$ah} | .arms[1].input = {path:$bp,bytes:$bb,sha256:$bh}' $root/paired-reduction-template.json > $evidence/pair2.config.json
config_sha=$(sha256sum $evidence/pair2.config.json | cut -d' ' -f1)
set +e
/usr/bin/time -v -o $evidence/pair2.time timeout --kill-after=10 120 $root/reducer --paired-v2 $evidence/pair2.config.json "$config_sha" $evidence/pair2.report.json > $evidence/pair2.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/pair2.native-exit
if (( status != 0 )); then exit "$status"; fi
jq -e '.status == "MEASURED" and .complete == true and .semantic_parity == true and .trace_plan_parity == true and .logical_charge_parity == true' $evidence/pair2.report.json > $evidence/pair2.check.txt
for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > $evidence/$field.after; fi; done
sha256sum -c $root/pins.sha256 > $evidence/pins-after.txt || status=96
find $root/query-scratch -mindepth 1 -printf '%p\n' > $evidence/scratch-files.txt
if test -s $evidence/scratch-files.txt; then status=96; fi
printf '%s\n' "$status" > $evidence/final-exit
exit "$status"
