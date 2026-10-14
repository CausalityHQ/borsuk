#!/usr/bin/env bash
# Root-only metadata, sealed opaque hashes and native aggregate receipt audit.
set -euo pipefail
root=/data/target/borsuk-source-budget96.pending
base=$root/cold-launch-a0002
formal=$root/cold-draft-a0002
col=$base/collected
[[ $(cat "$base/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
[[ $(cat "$col/terminal.exit") == 0 && $(cat "$col/source-budget.status") == FOUR_FULL1000_CELLS_AND_NATIVE_SOURCE_BUDGET_ABBA_CLOSED ]]
[[ $(cat "$col/prerequisites.status") == ACTUAL_INPUT_ADMISSION_AND_CANARY_CLOSED ]]
jq -e '.schema=="borsuk-source-budget-campaign-terminal-v1" and .decision=="SOURCE_BUDGET_ABBA_MEASURED" and .original_campaign_exit==0 and .terminal_exit==0 and .cleanup_error==0 and .forced_cleanup==false' "$col/campaign-terminal.json" > /dev/null
printf '%s\n' admission-original completed-admission-original admission-candidate completed-admission-candidate canary-original completed-canary-original canary-candidate completed-canary-candidate formal-original-a1 completed-formal-original-a1 formal-candidate-b1 completed-formal-candidate-b1 formal-candidate-b2 completed-formal-candidate-b2 formal-original-a2 completed-formal-original-a2 source-budget-abba > "$base/expected-stage-roster.txt"
cut -f1 "$col/stages.tsv" > "$base/actual-stage-roster.txt"
cmp "$base/expected-stage-roster.txt" "$base/actual-stage-roster.txt"
while IFS=$'\t' read -r phase kind pin; do
 md=$col/stage-metadata/$phase
 [[ $(cat "$md.manager-exit.txt") == 0 && $(cat "$md.native-exit.txt") == 0 ]]
 jq -e --arg p "$phase" --arg k "$kind" '.phase==$p and .kind==$k and .manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false and .execution_closed==true' "$md.supervision.json" > /dev/null
 [[ $(cat "$md.cpu.max.before") == '100000 100000' && $(cat "$md.cpuset.cpus.effective.before") == 0 && $(cat "$md.memory.max.before") == 1073741824 && $(cat "$md.memory.swap.max.before") == 0 && $(cat "$md.pids.max.before") == 128 ]]
 [[ $(cat "$md.memory.swap.peak.after") == 0 && $(cat "$md.memory.peak.after") -le 1073741824 ]]
 awk '$1=="oom"||$1=="oom_kill"||$1=="oom_group_kill" {n++;if($2!=0)exit 1}END{if(n!=3)exit 1}' "$md.memory.events.after"
 for role in binary config input; do
  cmp "$md.$role-before.log" "$md.$role-after.log"
  [[ -s $md.$role-before.log ]]
  ! rg -n 'FAILED|WARNING' "$md.$role-before.log"
 done
 if [[ $kind == baseline ]]; then
  if [[ $phase == formal-* ]]; then
   cell=${phase#formal-}; arm=${cell%%-*}
   expected=$formal/inputs/formal-$arm.completed.EXPECTED.json
   report=$col/$cell.native-completed-report.json
   jq -e '.diagnostic_only==false and .population_percentiles_valid==true and .statistics.count==1000 and .native_terminal.executed_count==1000 and .native_terminal.total_hits10==9780 and .native_terminal.underfilled_queries==0 and .native_terminal.recall_denominator==10000' "$report" > /dev/null
  else
   expected=$formal/inputs/$phase.completed.EXPECTED.json
   report=$col/$phase.native-completed-report.json
   jq -e '.diagnostic_only==true and .diagnostic_query_count==1 and .population_percentiles_valid==false' "$report" > /dev/null
  fi
  jq -e --slurpfile e "$expected" '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .complete==true and .identity==$e[0].expected_identity and .inputs==$e[0].expected_bound_inputs and .performance_pass_claim==false' "$report" > /dev/null
  actual=$(tar -xOzf "$col/evidence.tar.gz" "results/$phase.jsonl" | sha256sum); actual=${actual%% *}
  [[ $actual == $(jq -r .result_sha256 "$report") && $pin == $(jq -r .identity.config_sha256 "$report") ]]
 fi
done < "$col/stages.tsv"
report=$col/source-budget-abba.native-report.json
jq -e '.schema=="borsuk-source-budget-abba-native-reduction-v1" and .status=="MEASURED" and .complete==true and .declared_run_order==["A1","B1","B2","A2"] and .quality_trace_sq8_parity==true and .recall_anchor_verified==true and .recall_anchor_hits10==9780 and .recall_denominator==10000 and .source_cache=="off" and .vendor_win_claim==false and .source_cover_replay_verified==true and .distinct_source_cover_verified==true' "$report" > /dev/null
for arm in original candidate; do
 body=$formal/inputs/formal-$arm.config.json
 h=$(sha256sum "$body"); h=${h%% *}
 n=$(stat -c %s "$body")
 index=0; [[ $arm != candidate ]] || index=1
 jq -e --argjson i "$index" --arg h "$h" --argjson n "$n" --arg p "/mnt/borsuk-source-width64/inputs/formal-$arm.config.json" '.runtime_configs[$i]=={path:$p,bytes:$n,sha256:$h}' "$report" > /dev/null
done
printf '%s\n' SEVENTEEN_NATIVE_STAGES_EXACT_FOUR_RUN_PARITY_RECALL_RESOURCES_AND_COMPUTE_CLOSURE_VERIFIED > "$base/independent-verification.txt"
