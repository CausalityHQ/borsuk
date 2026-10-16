#!/bin/bash
# Completed metadata and opaque hashes only; never parse native result JSONL locally.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-diagnostic-a0001
c=$out/collected
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e '.Status=="Success" and .ResponseCode==0 and .CommandId=="cba9a540-9b2d-46a0-b827-229a32ce398a" and .InstanceId=="i-02d2118e1a18e2675"' "$out/run-terminal-observation.json" >/dev/null
jq -e '.original_exit==0 and .last_phase=="complete" and .execution_closed==true and .support_archive_sha256=="2eec6389c96198d1acfeef273777073a2217f73d9f344f79961eec987b044313"' "$out/terminal.json" >/dev/null
printf '%s  %s\n' "$(jq -r .archive_sha256 "$out/terminal.json")" "$out/evidence.tar.gz" | sha256sum --strict -c - >/dev/null
(cd "$c" && sha256sum --strict -c SHA256SUMS) > "$out/payload-sha-check.verified.log"
[[ ! -e $c/evidence/cleanup-unverified ]]
jq -e '.status=="CONTROL_CANARY_PASS" and .ann_executed==false and .scientific_bodies_opened==false' "$c/evidence/control-canary.json" >/dev/null
jq -e '.status=="FAULT_CANARY_PASS" and .native_actors_executed==false and .scientific_bodies_opened==false' "$c/evidence/cold-fault-canary.json" >/dev/null
jq -e '.status=="STAGE_PUBLICATION_CANARY_PASS" and .native_exit17_verified and .native_timeout124_verified and .client_timeout124_verified and .publication_failures_preserve_exits and .partial_receipts_unchanged_on_duplicate and .native_actors_executed==false and .scientific_bodies_opened==false' "$c/evidence/stage-publication-fault-canary.json" >/dev/null
for f in "$c/evidence/"prep-*.supervision.json; do
 jq -e '.manager_exit==0 and .forced_cleanup==false and .owned_cgroup_drained==true' "$f" >/dev/null
done
for f in "$out/pack/"*.json; do cmp "$f" "$c/config/${f##*/}"; done
phases=()
for kind in admission canary; do
 for arm in original candidate; do phases+=("$kind-$arm" "completed-$kind-$arm"); done
done
for prefix in canary-panel panel; do
 for label in a0 b0 a1 b1 b2 a2; do phases+=("$prefix-$label"); done
 phases+=("$prefix-attribution")
done
[[ ${#phases[@]} == 22 && $(wc -l < "$c/evidence/stages.tsv") == 44 ]]
awk -F '\t' 'NF!=4 || $3!~/^[0-9]+([.][0-9]+)?$/ || length($4)!=64 || $4!~/^[0-9a-f]+$/ {exit 1} NR%2==1 {if($2!="START")exit 1;n=$1;t=$3;h=$4} NR%2==0 {if($2!="END"||$1!=n||$4!=h||$3<t)exit 1}' "$c/evidence/stages.tsv"
awk -F '\t' 'NR%2==1{print $1}' "$c/evidence/stages.tsv" > "$out/observed-stage-roster.txt"
printf '%s\n' "${phases[@]}" > "$out/expected-stage-roster.txt"
cmp "$out/observed-stage-roster.txt" "$out/expected-stage-roster.txt"
peak=0
for phase in "${phases[@]}"; do
 ev=$c/evidence/$phase
 jq -e --arg p "$phase" '.phase==$p and .execution_closed==true and .manager_exit==0 and .native_exit==0 and .timeout_wrapper_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$ev/supervision.json" >/dev/null
 case $phase in
  admission-original|canary-original|canary-panel-a[012]|panel-a[012]) pin=59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083; extension=jsonl;;
  admission-candidate|canary-candidate|canary-panel-b[012]|panel-b[012]) pin=912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995; extension=jsonl;;
  *) pin=f456ea8a94f983a2266d3df661a7b4dfe120e607b79b13f968687d2af510880a; extension=json;;
 esac
 jq -e --arg pin "$pin" '.binary_sha256==$pin' "$ev/supervision.json" >/dev/null
 [[ $(cat "$ev/native-exit.txt") == 0 && $(cat "$ev/timeout-wrapper-exit.txt") == 0 && $(cat "$ev/manager-exit.txt") == 0 ]]
 [[ $(cat "$ev/cpu.max.before") == '100000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0 ]]
 [[ $(cat "$ev/memory.max.before") == 2147483648 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 128 ]]
 [[ $(cat "$ev/memory.swap.peak.after") == 0 ]]
 for event in oom oom_kill oom_group_kill; do grep -Fxq "$event 0" "$ev/memory.events.after"; done
 pids=$(cat "$ev/pids.peak.after"); [[ $pids =~ ^[0-9]{1,3}$ ]] && ((pids<128))
 ((pids<=peak)) || peak=$pids
 [[ $(stat -c %s "$c/results/$phase.$extension") == "$(cat "$ev/result.bytes")" ]]
 expected=$(cut -d ' ' -f 1 "$ev/result.sha256")
 printf '%s  %s\n' "$expected" "$c/results/$phase.$extension" | sha256sum --strict -c - >/dev/null
done
for prefix in canary-panel panel; do
 config=$c/config/$prefix.attribution.json
 report=$c/results/$prefix-attribution.json
 jq -e --slurpfile expected "$out/pack/attribution.EXPECTED.json" 'del(.runs[].completed.input)==($expected[0]|del(.runs[].completed.input))' "$config" >/dev/null
 config_sha=$(sha256sum "$config"); config_sha=${config_sha%% *}
 jq -e --arg sha "$config_sha" --slurpfile config "$config" '
  .schema=="borsuk-sq8-panel-attribution-v1" and .complete==true and .diagnostic_only==true and .population_percentiles_valid==false and .vendor_or_scientific_win_claim==false and .default_adoption==false and .within_actor_trace_off_parity==true and .selected_count==32 and .reduction_config_sha256==$sha and ([.runs[].label]==["A0","B0","A1","B1","B2","A2"]) and ([.runs[].input]==[$config[0].runs[].completed.input]) and ([.queries[].ordinal]==[range(0;32)|(. * 999 / 31|floor)]) and .reproduced_ordinals==([.queries[]|select(.reproduced_excess==true)]|length) and ((.status=="INCONCLUSIVE_FIXED_PANEL")==(.reproduced_ordinals==0)) and (.status=="MEASURED" or .status=="INCONCLUSIVE_FIXED_PANEL")
 ' "$report" >/dev/null
 jq -r '.runs[]|[.label,.completed.input.path,.completed.input.bytes,.completed.input.sha256]|@tsv' "$config" > "$out/$prefix.input-descriptors.tsv"
 while IFS=$'\t' read -r label path bytes sha; do
  [[ $path == /mnt/borsuk-v9-panel/results/"$prefix-${label,,}".jsonl ]]
  file=$c/results/$prefix-${label,,}.jsonl
  [[ $(stat -c %s "$file") == "$bytes" ]]
  printf '%s  %s\n' "$sha" "$file" | sha256sum --strict -c - >/dev/null
 done < "$out/$prefix.input-descriptors.tsv"
done
jq -n --argjson peak "$peak" --slurpfile report "$c/results/panel-attribution.json" '{schema:"borsuk-sq8-panel-diagnostic-root-verification-v1",verifier_exit:0,source_commit:"898b3fcf82f75085d8b01cd2e462652db34dcd41",stages:22,stage_rows:44,all_stage_exits_zero:true,all_owned_units_drained_unforced:true,pids_peak:$peak,oom_and_swap_zero:true,payload_hashes_verified:true,within_actor_trace_off_parity:$report[0].within_actor_trace_off_parity,native_report_status:$report[0].status,reproduced_ordinals:$report[0].reproduced_ordinals,predominantly_pre_barrier_falsified_ordinals:[$report[0].queries[]|select(.reproduced_excess and .predominantly_pre_barrier_falsified)|.ordinal],compute_closed:true,scope:"CLOSED_FIXED32_TRACED_DIAGNOSTIC_NOT_POPULATION_OR_VENDOR_PERFORMANCE",product_complete:false}' > "$out/independent-verification.json"
cat "$out/independent-verification.json"
