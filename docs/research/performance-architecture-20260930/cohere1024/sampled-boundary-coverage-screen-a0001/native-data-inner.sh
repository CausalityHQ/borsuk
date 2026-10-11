#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-boundary-data
stage=$1
[[ "$stage" == coverage ]]
ev=$root/evidence/$stage
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = '100000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 536870912
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
printf '%s  %s\n' 299ee9c2c7b73fe37f37f91f772ea527d1451b27067e5a4859ea773d7168fd07 "$root/bin/compare_native_replay" | sha256sum -c -
config_sha=91855a93e057a815ca3d3ebb849abb0bd8d388ff0ffe5ecbb0475884a489088f
printf '%s  %s\n' "$config_sha" "$root/config.json" | sha256sum -c -
test "$(du -sb "$root" | cut -f1)" -lt 2147483648
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 15 900 "$root/bin/compare_native_replay" --leaf-boundary-coverage "$root/config.json" "$config_sha" "$ev/adjacency.json" "$ev/selection.json" "$ev/report.json" > "$ev/native.log" 2>&1
native=$?
set -e
printf '%s\n' "$native" > "$ev/native-exit.txt"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
(( native == 0 )) || exit "$native"
test "$(cat "$ev/memory.swap.peak.after")" = 0
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/memory.events.after"
jq -e '.schema=="borsuk-leaf-boundary-selection-seal-v1" and .status=="SEALED" and .truth_opened==false and .leaf_count==48 and .control_trace_parity==true and .candidate=="sampled_boundary_v1" and .adjacency_sealed_before_requests==true' "$ev/selection.json" >/dev/null
jq -e '.status=="MEASURED" and .complete==true and .l2_coverage==314 and .original_returned_hits10==313 and .truth_slots==320 and .planner_only==true and .source_payload_read==false and .native_ann_called==false and .cold_s3_claim==false and .vendor_or_scientific_win_claim==false and .adjacency_sealed_before_requests==true' "$ev/report.json" >/dev/null
selection=$(sha256sum "$ev/selection.json"); selection=${selection%% *}
adjacency=$(sha256sum "$ev/adjacency.json"); adjacency=${adjacency%% *}
jq -e --arg s "$selection" --arg a "$adjacency" '.selection_seal_sha256==$s and .adjacency_sha256==$a' "$ev/report.json" >/dev/null
jq -e --arg a "$adjacency" '.adjacency_sha256==$a' "$ev/selection.json" >/dev/null
test "$(du -sb "$root" | cut -f1)" -lt 2147483648
printf 'REAL_INPUT_AND_DURABLE_SEALS_AUTHENTICATED\n'
jq -e '.schema=="borsuk-leaf-boundary-coverage-v1" and .candidate=="sampled_boundary_v1" and (.sampled_boundary_v1_coverage|type)=="number" and (.gained_slots|type)=="number" and (.lost_slots|type)=="number" and .sampled_boundary_v1_coverage+.lost_slots==.l2_coverage+.gained_slots and .coverage_gate_passed==(.sampled_boundary_v1_coverage>=315 and .gained_slots>.lost_slots)' "$ev/report.json" >/dev/null
jq '{schema:"borsuk-sampled-boundary-screen-summary-v1",outcome:(if .coverage_gate_passed then "MEASURED_SURVIVE" else "MEASURED_KILL" end),status,complete,candidate,control_source_coverage:.l2_coverage,candidate_source_coverage:.sampled_boundary_v1_coverage,truth_slots,gained_slots,lost_slots,coverage_gate_passed,original_returned_hits10,config_sha256,selection_seal_sha256,adjacency_sha256,adjacency_sealed_before_requests,truth_opened_after_durable_paired_selection,modeled_payload_bytes,planner_only,source_payload_read,native_ann_called,cold_s3_claim,vendor_or_scientific_win_claim}' "$ev/report.json" > "$ev/human-summary.json"
sync -f "$ev/human-summary.json"
sync -f "$ev"
