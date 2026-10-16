#!/usr/bin/env bash
# Closed metadata verification only; no scientific JSONL parsing or native replay.
set -euo pipefail
[[ $# == 1 ]] || exit 125
out=$1
collected=$out/collected
master=e75ebcf5149ff51b359137352b2eaad7874b5cf3fbc8878f18b27a05c415a51b
runtime=/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending
jq -se 'length==1 and (.[0]|.CommandId=="b2184862-5483-4914-934a-942c199b0fde" and .InstanceId=="i-05113071a227077e5" and .Status=="Success" and .ResponseCode==0)' "$out/native-original.json" >/dev/null
for line in prerequisite_exit=0 publication_exit=0 shutdown_request_exit=0 wrapper_exit=0; do
 jq -er .StandardOutputContent "$out/native-original.json" | grep -Fxq "$line"
done
jq -se --arg master "$master" 'length==1 and (.[0]|.original_exit==0 and .publication_complete==true and .master_sha256==$master and .performance_claim==false)' "$out/terminal.json" >/dev/null
jq -se --arg master "$master" 'length==1 and (.[0]|.status=="LOCAL_EXECUTION_COMPLETE" and .exit==0 and .original_exit==0 and .completed_unmeasured_phases==2 and .scientific_cells_executed==0 and .input_manifest_sha256==$master and .scientific_performance_claim==false)' "$collected/terminal.json" >/dev/null
printf '%s  %s\n' "$master" "$collected/input-manifest.json" | sha256sum --strict --check - >/dev/null
for stage in C1/C1 C1/reducer NULL_SMOKE/C0 NULL_SMOKE/reducer MOCKED/reducer PARITY/reducer; do
 ev=$collected/$stage
 jq -se 'length==1 and (.[0]|.valid==true and .owned==true and .drained==true and .forced_cleanup==false and .resources_verified==true and .native_exit==0 and .timeout_exit==0 and .time_exit==0 and .systemd_run_exit==0)' "$ev/execution.json" >/dev/null
 owner=$(cat "$ev/owner.txt")
 invocation=$(cat "$ev/invocation-id.txt")
 [[ $invocation =~ ^[0-9a-f]{32}$ && $owner == *":$master" ]]
 unit=${owner%:*}
 [[ $unit =~ ^borsuk-sq8-panel-[0-9a-f-]+-(C0|C1|reducer)\.service$ ]]
 for status in unit-start-status.txt unit-stop-status.txt; do
  grep -Fxq "Description=$owner" "$ev/$status"
  grep -Fxq "InvocationID=$invocation" "$ev/$status"
  grep -Fxq "ControlGroup=/system.slice/$unit" "$ev/$status"
 done
 for line in Result=success ExecMainCode=1 ExecMainStatus=0; do grep -Fxq "$line" "$ev/unit-stop-status.txt"; done
 [[ $(cat "$ev/service-result.txt") == success ]]
 for file in native-exit.txt timeout-exit.txt time-exit.txt systemd-run-exit.txt manager-envelope-exit.txt worker-exit.txt binary-after-exit.txt; do [[ $(cat "$ev/$file") == 0 ]]; done
 for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cmp "$ev/$file.before" "$ev/$file.after"; done
 [[ $(cat "$ev/cpu.max.before") == '100000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0 ]]
 [[ $(cat "$ev/memory.max.before") == 2147483648 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 128 ]]
 [[ $(cat "$ev/memory.swap.peak.after") == 0 ]]
 grep -Fxq 'oom 0' "$ev/memory.events.after"
 grep -Fxq 'oom_kill 0' "$ev/memory.events.after"
 if [[ $(cat "$ev/cgroup.events.drained") != absent ]]; then grep -Fxq 'populated 0' "$ev/cgroup.events.drained"; fi
done
for phase in C1 NULL_SMOKE; do
 jq -se --slurpfile h "$runtime/$phase.expected.json" 'length==1 and (.[0]|.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .complete==true and .diagnostic_only==true and .diagnostic_query_count==32 and .identity==$h[0].expected_identity and .inputs==$h[0].expected_bound_inputs)' "$collected/$phase/native-report.json" >/dev/null
done
jq -se 'length==1 and (.[0]|.schema=="borsuk-disabled-historical-parity-v1" and .status=="EXACT_CANONICAL_PARITY" and .complete==true and .native_validations==2 and .originals_unchanged==true and .query_count==32 and .qualified==false and .performance_claim==false)' "$collected/PARITY/results/parity.json" >/dev/null
jq -se 'length==1 and (.[0]|.historical_exit==0 and .null_exit==0)' "$collected/PARITY/results/native-exits.json" >/dev/null
cmp "$collected/PARITY/results/before.identities" "$collected/PARITY/results/after.identities"
[[ $(grep -c '^MOCKED .* exit=' "$collected/MOCKED/reducer/stdout.log") == 15 ]]
grep -Fxq 'MOCKED identical exit=0' "$collected/MOCKED/reducer/stdout.log"
grep -Fxq 'MOCKED validator-historical exit=73' "$collected/MOCKED/reducer/stdout.log"
grep -Fxq 'MOCKED validator-null exit=73' "$collected/MOCKED/reducer/stdout.log"
jq -se 'length==1 and (.[0].Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId=="i-05113071a227077e5" and .[0].State.Name=="terminated"))' "$out/instance-final.json" >/dev/null
[[ $(cat "$out/volume-final.exit") == 254 ]]
grep -Fq InvalidVolume.NotFound "$out/volume-final.stderr"
grep -Fq vol-0f3de034345b6fbeb "$out/volume-final.stderr"
grep -Fxq ActiveState=inactive "$out/external-timer-final.txt"
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
printf '%s\n' PREREQUISITES_VERIFIED_NO_PERFORMANCE_CLAIM
