#!/usr/bin/env bash
set -euo pipefail
base=/data/target/borsuk-source-width128.pending
root=$base/cold-launch-a0002
out=$root/collected
[[ $(cat "$out/terminal.exit") == 0 ]]
instance=$(cat "$root/instance.id")
command=$(cat "$root/run-command.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ && $command =~ ^[0-9a-f-]{36}$ ]]
jq -e --arg i "$instance" --arg c "$command" '.Status=="Success" and .ResponseCode==0 and .CommandId==$c and .InstanceId==$i' "$root/native-terminal.json" > "$out/original-run-check.txt"
jq -e '.decision=="SOURCE_WIDTH128_PAIRED_MEASURED" and .original_campaign_exit==0 and .terminal_exit==0 and .cleanup_error==0 and .forced_cleanup==false' "$out/campaign-terminal.json" > "$out/campaign-check.txt"
[[ $(cat "$root/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
phases=(admission-original completed-admission-original admission-candidate completed-admission-candidate canary-original completed-canary-original canary-candidate completed-canary-candidate formal-original-a1 completed-formal-original-a1 formal-candidate-b1 completed-formal-candidate-b1 formal-candidate-b2 completed-formal-candidate-b2 formal-original-a2 completed-formal-original-a2 paired-1 paired-2)
[[ $(wc -l < "$out/stages.tsv") == 18 ]]
awk -F '\t' 'NR>1 {print "/mnt/borsuk-source-width64/"$5 ": OK"}' "$base/cold-frozen-a0001/transport.tsv" > "$out/expected-input-check.log"
printf '%s\n' '/mnt/borsuk-source-width64/inputs/prepared-parent_cohort_complete.json: OK' '/mnt/borsuk-source-width64/inputs/prepared-parent_derived_derivation.json: OK' >> "$out/expected-input-check.log"
for ordinal in "${!phases[@]}"; do
 phase=${phases[$ordinal]}
 row=$(sed -n "$((ordinal+1))p" "$out/stages.tsv")
 IFS=$'\t' read -r actual kind sha <<< "$row"
 [[ $actual == "$phase" && $sha =~ ^[0-9a-f]{64}$ ]]
 meta=$out/stage-metadata/$phase
 jq -e --arg phase "$phase" '.phase==$phase and .manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false and .execution_closed==true' "$meta.supervision.json" > /dev/null
 [[ $(cat "$meta.native-exit.txt") == 0 && $(cat "$meta.manager-exit.txt") == 0 ]]
 [[ $(cat "$meta.cpu.max.before") == '100000 100000' && $(cat "$meta.cpuset.cpus.effective.before") == 0 && $(cat "$meta.memory.max.before") == 1073741824 && $(cat "$meta.memory.swap.max.before") == 0 && $(cat "$meta.pids.max.before") == 128 && $(cat "$meta.memory.swap.peak.after") == 0 ]]
 awk '$1=="oom" || $1=="oom_kill" || $1=="oom_group_kill" {if($2!=0)exit 1;n++} END{if(n!=3)exit 1}' "$meta.memory.events.after"
 cmp "$meta.binary-before.log" "$meta.binary-after.log"
 cmp "$meta.config-before.log" "$meta.config-after.log"
 cmp "$meta.input-before.log" "$meta.input-after.log"
 cmp "$meta.input-before.log" "$out/expected-input-check.log"
 binary=compare_native_replay
 if [[ $kind == baseline ]]; then
  binary=check_cohere_native_baseline
 fi
 printf '/mnt/borsuk-source-width64/bin/%s: OK\n' "$binary" > "$out/expected-binary-check.log"
 cmp "$meta.binary-before.log" "$out/expected-binary-check.log"
done
for cell in original-a1 candidate-b1 candidate-b2 original-a2; do
 report=$out/$cell.native-completed-report.json
 width=64
 if [[ $cell == candidate-* ]]; then width=128; fi
 arm=${cell%%-*}
 approved=$base/cold-frozen-a0001/inputs/formal-$arm.completed.EXPECTED.json
 jq -e --slurpfile approved "$approved" '.identity==$approved[0].expected_identity and .inputs==$approved[0].expected_bound_inputs' "$report" > "$out/$cell.independent-header-check.txt"
 jq -e --argjson width "$width" '.status=="MEASURED" and .complete==true and .population_percentiles_valid==true and .diagnostic_only==false and .statistics.count==1000 and .identity.fetch_parallelism==$width and .inputs.fetch_parallelism==$width and .inputs.source_cache=="off" and .inputs.rows==1000000 and .inputs.generation_root_sha256=="80fb1e8fdea78d72363d28f1406924084227203dd6708d596be3519155b88638" and .inputs.requests_sha256=="8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e" and .inputs.truth_sha256=="0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f" and .native_terminal.total_hits10==9780 and .native_terminal.underfilled_queries==0' "$report" > "$out/$cell.check.txt"
done
for repetition in 1 2; do
 jq -e '.schema=="borsuk-scale-width128-native-reduction-v1" and .status=="MEASURED" and .complete==true and .semantic_parity==true and .trace_plan_parity==true and .logical_charge_parity==true and [.arms[].fetch_parallelism]==[64,128] and .vendor_or_scientific_win_claim==false and .performance_pass_claim==false' "$out/paired-$repetition.native-report.json" > "$out/paired-$repetition.check.txt"
done
jq -n --arg i "$instance" --arg c "$command" '{status:"CLOSED_NATIVE_SOURCE_WIDTH128_COMPARISON_VERIFIED",source:"5c4a82f46abb55ae9ce6afe678fae535f7c423f3",instance_id:$i,ssm_command_id:$c,stage_count:18,cell_count:4,queries_per_cell:1000,recall_at10:0.978,underfill:0,native_semantic_physical_plan_logical_charge_parity:true,outer_memory_bytes:1073741824,library_payload_budget_bytes:536870912,swap_oom:0,terminated:true,volume_deleted:true,watchdog_stopped:true,provider_cache_controlled:false,matched_vendor_win:false,production_ready:false}' > "$out/independent-verification.json"
sha256sum "$out/independent-verification.json" > "$out/independent-verification.sha256"
cat "$out/independent-verification.json"
