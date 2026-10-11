#!/bin/bash
set -euo pipefail
d=/data/target/borsuk-source-neighborhood-data-gates-a0002.pending
cd "$d"
jq -e '.Status=="Success" and .ResponseCode==0 and .CommandId=="e7908e37-d784-4bc0-a0c1-4fdace7a399e" and .InstanceId=="i-0bc04948ec797f4a0"' data-gates.ssm-terminal.json > /dev/null
jq -e '[.Reservations[].Instances[]]|length==1 and .[0].InstanceId=="i-0bc04948ec797f4a0" and .[0].State.Name=="terminated"' instance.terminated.json > /dev/null
test "$(cat volume-after.exit)" = 254
grep -Fq InvalidVolume.NotFound volume-after.stderr
grep -Fq vol-0a11957cb4ec25cb0 volume-after.stderr
test "$(cat external-timer-closed.txt)" = inactive
test "$(cat external-stop.exit)" = 0
jq -e '.original_driver_exit==0 and .batch_exit==0 and .completed_stages==2 and .cleanup_exit==0 and .last_phase=="complete" and .execution.instance_id=="i-0bc04948ec797f4a0"' batch-collected/terminal.json > /dev/null
for stage in admission smoke batch; do
 (cd "$stage-collected" && sha256sum -c SHA256SUMS) > "$stage-final-manifest-check.log"
done
for stage in admission smoke; do
 ev=$stage-collected
 cfg=$(cat "package/$stage.config.sha256")
 jq -e --arg stage "$stage" --arg cfg "$cfg" '.stage==$stage and .status=="GATE_ACCEPTED" and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .owning_slice_resources_ok==true and .config_sha256==$cfg and .source_commit=="fc60c18e780063804c625a72f722d3e697fb4b61" and .elf_sha256=="77a8e582e426f1d8500f26b66741ddb978ce7f0d1ef5250b5111c7a3e1da1010" and .scientific_measurement==false and .cold_s3_claim==false and .vendor_win==false and .execution.instance_id=="i-0bc04948ec797f4a0"' "$ev/terminal.json" > /dev/null
 for pair in anchor_seal_sha256:anchors.json report_sha256:report.json; do
  field=${pair%%:*}; file=${pair#*:}
  actual=$(sha256sum "$ev/$file"); actual=${actual%% *}
  test "$(jq -r --arg field "$field" '.[$field]' "$ev/terminal.json")" = "$actual"
 done
 test "$(cat "$ev/memory.max.before")" = 6442450944
 for f in slice.memory.high.before slice.memory.high.after owning-slice.memory.high; do test "$(cat "$ev/$f")" = max; done
 for f in memory.swap.peak.after slice.memory.swap.peak.after owning-slice.memory.swap.peak; do test "$(cat "$ev/$f")" = 0; done
 for f in memory.events.after slice.memory.events.after owning-slice.memory.events; do awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/$f"; done
 grep -qx RESOURCE_RECEIPTS_VALID "$ev/resource-evidence-valid.txt"
done
printf 'REAL_INPUT_GATES_CLOSED0; three opaque collections and lifecycle verified; no geometry parsed\n'
