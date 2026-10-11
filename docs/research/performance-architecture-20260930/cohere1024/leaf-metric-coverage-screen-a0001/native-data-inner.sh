#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-leaf-metric-data
stage=$1
[[ "$stage" == experiment ]]
ev="$root/evidence/$stage"
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = '100000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 536870912
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
printf '%s  %s\n' e6dede4dbb786a5d90bf7cecdf0959cba32e7ae30cf9812381603494da0f33d4 "$root/bin/compare_native_replay" | sha256sum -c -
config_sha=fca117c22f6368d425026cbd3b9950223dd78ac44f32b708cd503746610496c9
printf '%s  %s\n' "$config_sha" "$root/config.json" | sha256sum -c -
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 15 300 "$root/bin/compare_native_replay" --leaf-metric-coverage "$root/config.json" "$config_sha" "$ev/selection.json" "$ev/report.json" > "$ev/native.log" 2>&1
native=$?
set -e
printf '%s\n' "$native" > "$ev/native-exit.txt"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
(( native == 0 )) || exit "$native"
jq -e '.status=="SEALED" and .truth_opened==false and .leaf_count==48 and .control_trace_parity==true' "$ev/selection.json"
jq -e '.status=="MEASURED" and .complete==true and .l2_coverage==314 and .original_returned_hits10==313 and .truth_slots==320 and .planner_only==true and .source_payload_read==false and .native_ann_called==false and .cold_s3_claim==false and .vendor_or_scientific_win_claim==false' "$ev/report.json"
jq 'del(.queries)' "$ev/report.json" > "$ev/human-summary.json"
actual=$(sha256sum "$ev/selection.json"); actual=${actual%% *}
jq -e --arg sha "$actual" '.selection_seal_sha256==$sha' "$ev/report.json"
