#!/usr/bin/env bash
set -euo pipefail
root=/data/target/borsuk-source-width64.pending/cpu-profile-a0004
ev=$root/launch/collected
[[ $(cat "$root/launch/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e '.CommandId=="4b5d885a-ea8f-47ae-98d4-07ad13673b3d" and .InstanceId=="i-0c4a2961e9a7537bd" and .Status=="Success" and .ResponseCode==0' "$root/launch/run-observation.json" > /dev/null
jq -e '.schema=="borsuk-source-width64-cpu-profile-terminal-v1" and .decision=="CPU_PROFILE_CLOSED" and .original_campaign_exit==0 and .terminal_exit==0 and .cleanup_error==0 and .forced_cleanup==false and .cold_performance_claim==false and .vendor_win_claim==false' "$ev/terminal.json" > /dev/null
phases=(admission-candidate completed-admission-candidate canary-candidate completed-canary-candidate formal-candidate-b1 completed-formal-candidate-b1)
mapfile -t observed < <(cut -f1 "$ev/stages.tsv")
[[ ${#observed[@]} == 6 ]]
for i in "${!phases[@]}"; do
 phase=${phases[$i]}; [[ ${observed[$i]} == "$phase" ]]
 jq -e --arg phase "$phase" '.phase==$phase and .manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false and .execution_closed==true' "$ev/$phase.supervision.json" > /dev/null
 [[ $(cat "$ev/$phase.cpu.max") == '100000 100000' && $(cat "$ev/$phase.cpuset") == 0 && $(cat "$ev/$phase.memory.swap.max") == 0 && $(cat "$ev/$phase.pids.max") == 128 ]]
 memory=1073741824; [[ $phase != formal-candidate-b1 ]] || memory=1342177280
 [[ $(cat "$ev/$phase.memory.max") == "$memory" && $(cat "$ev/$phase.memory.peak") -le $memory ]]
 awk '$1=="oom"||$1=="oom_kill"||$1=="oom_group_kill"{if($2!=0)bad=1;n++}END{exit(bad||n!=3)}' "$ev/$phase.memory.events"
 binary=check_cohere_native_baseline; [[ $phase != completed-* ]] || binary=compare_native_replay
 for when in before after; do
  [[ $(cat "$ev/$phase.binary-$when.log") == "/mnt/borsuk-source-width64/bin/$binary: OK" ]]
  [[ $(wc -l < "$ev/$phase.input-$when.log") == 19 ]]
  ! grep -v ': OK$' "$ev/$phase.input-$when.log"
  [[ $(wc -l < "$ev/$phase.config-$when.log") == 1 ]]
  grep -qx '/mnt/borsuk-source-width64/.*: OK' "$ev/$phase.config-$when.log"
 done
 if [[ $phase == completed-* ]]; then
  jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .complete==true and .diagnostic_only==true and .population_percentiles_valid==false and .native_terminal.complete==true and .native_terminal.underfilled_queries==0 and .performance_pass_claim==false and .vendor_or_scientific_win_claim==false' "$ev/$phase.native-report.json" > /dev/null
 fi
done
report=$ev/completed-formal-candidate-b1.native-report.json
jq -e --slurpfile expected "$root/inputs/formal-candidate.completed.EXPECTED.json" '.identity==$expected[0].expected_identity and .inputs==$expected[0].expected_bound_inputs and .diagnostic_query_count==128 and .native_terminal.executed_count==128 and .identity.execution.ordinals==[range(0;128)]' "$report" > /dev/null
[[ $(cat "$ev/perf.path") == /usr/lib/linux-tools-6.8.0-147/perf ]]
[[ $(cat "$ev/perf-package-version.txt") == $'linux-tools-6.8.0-147\t6.8.0-147.147\tamd64' ]]
grep -qx '# Total Lost Samples: 0' "$ev/perf-self-report.txt"
grep -q 'PreparedTwoBit5score' "$ev/perf-self-report.txt"
grep -q 'sha2563x8613digest_blocks' "$ev/perf-self-report.txt"
archive_sha=$(sha256sum "$ev/evidence.tar.gz"); archive_sha=${archive_sha%% *}
jq -n --arg archive "$archive_sha" --arg source eadadae549423531c91c3e58191f6d0b28145028 '{schema:"borsuk-native-cpu-profile-independent-verification-v1",status:"CLOSED_NATIVE_CPU_PROFILE_VERIFIED",instance_id:"i-0c4a2961e9a7537bd",command_id:"4b5d885a-ea8f-47ae-98d4-07ad13673b3d",frozen_commit:$source,archive_sha256:$archive,native_stages:6,all_native_and_manager_exits_zero:true,resource_and_input_checks:true,terminated:true,root_volume_absent:true,external_timer_inactive:true,lost_samples:0,query_count:128,population_or_latency_win_claim:false,product_ready:false}' > "$ev/independent-verification.json"
printf '%s\n' CLOSED_NATIVE_CPU_PROFILE_VERIFIED
