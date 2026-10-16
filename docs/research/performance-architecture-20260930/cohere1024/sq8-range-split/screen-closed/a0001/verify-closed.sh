#!/usr/bin/env bash
# Closed metadata verification only; no scientific JSONL parsing or native replay.
set -euo pipefail
[[ $# == 1 ]] || exit 125
out=$1
collected=$out/collected
master=21a824b3ed20ef266d7834c4f3e200ccdbc05fe61f34ffbc83fa8c9d2b57e1a8
jq -se 'length==1 and (.[0]|.CommandId=="98e42169-f7ba-4b61-b10e-e613d7df6b46" and .InstanceId=="i-09055af56258dcb86" and .Status=="Success" and .ResponseCode==0)' "$out/native-original.json" >/dev/null
for line in original_exit=0 publication_exit=0 shutdown_request_exit=0 wrapper_exit=0; do
 jq -er .StandardOutputContent "$out/native-original.json" | grep -Fxq "$line"
done
jq -se --arg master "$master" 'length==1 and (.[0]|.original_exit==0 and .publication_complete==true and .master_sha256==$master and .performance_claim==false)' "$out/terminal.json" >/dev/null
jq -se --arg master "$master" 'length==1 and (.[0]|.status=="LOCAL_EXECUTION_COMPLETE" and .exit==0 and .original_exit==0 and .completed_cells==6 and .input_manifest_sha256==$master and .scientific_performance_claim==false)' "$collected/terminal.json" >/dev/null
printf '%s  %s\n' "$master" "$collected/input-manifest.json" | sha256sum --strict --check - >/dev/null
for stage in B0 C0 B1 C1 C2 B2 reducer; do
 ev=$collected/$stage
 jq -se 'length==1 and (.[0]|.valid==true and .owned==true and .drained==true and .forced_cleanup==false and .resources_verified==true and .native_exit==0 and .timeout_exit==0 and .time_exit==0 and .systemd_run_exit==0)' "$ev/execution.json" >/dev/null
 owner=$(cat "$ev/owner.txt")
 invocation=$(cat "$ev/invocation-id.txt")
 [[ $invocation =~ ^[0-9a-f]{32}$ && $owner == *":$master" ]]
 unit=${owner%:*}
 [[ $unit =~ ^borsuk-sq8-panel-[0-9a-f-]+-(B0|C0|B1|C1|C2|B2|reducer)\.service$ ]]
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
jq -se 'length==1 and (.[0].Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId=="i-09055af56258dcb86" and .[0].State.Name=="terminated"))' "$out/instance-final.json" >/dev/null
[[ $(cat "$out/volume-final.exit") == 254 ]]
grep -Fq InvalidVolume.NotFound "$out/volume-final.stderr"
grep -Fq vol-0344e18e966af31d5 "$out/volume-final.stderr"
grep -Fxq ActiveState=inactive "$out/external-timer-final.txt"
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
printf '%s\n' SIX_CELL_EXECUTION_VERIFIED_SCREEN_SEPARATE
