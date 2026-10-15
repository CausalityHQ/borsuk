#!/bin/bash
# Completed compiler/test metadata only; no scientific trace or vector parsing.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-reader-gates-a0001
source_commit=4fe2c1349922b7f5862e9b2568ae2fb70fab2417
base=$out/collected
ev=$base/evidence/sq8-panel-reader
manager=$base/evidence/sq8-panel-reader-manager
instance=$(cat "$out/instance.id")
command=$(cat "$out/native-command.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ && $command =~ ^[0-9a-f-]{36}$ ]]
jq -e --arg i "$instance" --arg c "$command" '.InstanceId==$i and .CommandId==$c and .Status=="Success" and .ResponseCode==0' "$out/run-terminal-observation.json" > /dev/null
jq -e --arg s "$source_commit" '.source_commit==$s and .manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false and .native_qualified==true and .scientific_performance_claim==false' "$manager/terminal.json" > /dev/null
jq -e --arg s "$source_commit" '.source_commit==$s and .setup_exit==0 and .source_authenticated==true and .native_qualified==false' "$base/evidence/setup/terminal.json" > /dev/null
cmp "$base/source-files.sha256" "$out/inputs/source-files.sha256"
cmp "$base/native-source.json" "$out/inputs/native-source.json"
jq -e 'length==445' "$base/native-source.json" > /dev/null
cmp "$base/required-exact-tests.txt" "$out/inputs/required-exact-tests.txt"
for f in qualify-reader.sh supervise-reader.sh required-exact-tests.txt; do cmp "$base/evidence/setup/$f" "$out/inputs/$f"; done
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(grep -c ': OK$' "$ev/source-before.log") == 478 ]]
[[ $(cat "$ev/source-commit.txt") == "$source_commit" ]]
stages=(compile inventory example-regressions workspace-clippy workspace-test-build release retain-archive)
for profile in debug release; do
 n=0
 while IFS= read -r name; do
  n=$((n+1))
  stage=$profile-exact-$n
  stages+=("$stage")
  [[ $(cat "$ev/$stage-exit.txt") == 0 ]]
  [[ $(grep -Fxc "test $name ... ok" "$ev/$stage.log") == 1 ]]
  grep -Fq 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$ev/$stage.log"
  flags=()
  [[ $profile != release ]] || flags=(--release)
  expected=$(printf '%q ' cargo test --locked "${flags[@]}" -p borsuk --example compare_native_replay "$name" -- --test-threads=1 --exact)
  [[ $(cat "$ev/$stage-command.txt") == "$expected" ]]
 done < "$out/inputs/required-exact-tests.txt"
 (( n == 9 ))
done
for stage in "${stages[@]}"; do
 [[ $(cat "$ev/$stage-exit.txt") == 0 ]]
 for suffix in command.txt time.txt; do [[ -s $ev/$stage-$suffix ]]; done
 [[ -f $ev/$stage.log && ! -L $ev/$stage.log ]]
done
[[ ${#stages[@]} == 25 ]]
[[ $(grep -c ': test$' "$ev/inventory.log") == 270 ]]
summary=$(grep '^test result:' "$ev/example-regressions.log")
pattern='^test result: ok\. ([0-9]+) passed; 0 failed; ([0-9]+) ignored; 0 measured; 0 filtered out; finished in .+$'
[[ $summary =~ $pattern ]]
passed=${BASH_REMATCH[1]}
ignored=${BASH_REMATCH[2]}
(( passed + ignored == 270 ))
[[ $(cat "$ev/cpu.max.before") == '800000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0-7 ]]
[[ $(cat "$ev/memory.max.before") == 30064771072 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 512 ]]
awk '$1=="oom" {a=1;if($2!=0)exit 1} $1=="oom_kill" {b=1;if($2!=0)exit 1} END {if(!a||!b)exit 1}' "$ev/memory.events.after"
peak=$(cat "$ev/pids.peak.after")
[[ $peak =~ ^[0-9]+$ ]] && (( peak <= 512 ))
if [[ -f $ev/memory.swap.peak.after ]]; then [[ $(cat "$ev/memory.swap.peak.after") == 0 ]]; fi
actual=$(sha256sum "$out/release.tar"); actual=${actual%% *}
read -r expected _ < "$ev/release-archive.sha256"
[[ $actual == "$expected" && $(stat -c%s "$out/release.tar") == "$(cat "$ev/release-archive.bytes")" ]]
member=target/release/examples/compare_native_replay
[[ $(tar -tf "$out/release.tar") == "$member" ]]
read -r expected recorded_member < "$ev/binaries.sha256"
[[ $recorded_member == "$member" ]]
actual=$(tar -xOf "$out/release.tar" "$member" | sha256sum); actual=${actual%% *}
[[ $actual == "$expected" ]]
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e --arg i "$instance" '.Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated")' "$out/instance-final.json" > /dev/null
[[ $(cat "$out/volume-final.exit") == 254 ]]
grep -Fq InvalidVolume.NotFound "$out/volume-final.stderr"
grep -Fq "$(cat "$out/volume.id")" "$out/volume-final.stderr"
grep -Fxq ActiveState=inactive "$out/external-timer-final.txt"
jq -n --arg source "$source_commit" --arg instance "$instance" --arg command "$command" --argjson passed "$passed" --argjson ignored "$ignored" --argjson pids "$peak" \
 '{schema:"borsuk-sq8-panel-reader-root-verification-v1",source_commit:$source,instance_id:$instance,native_command_id:$command,verifier_exit:0,stages:25,exact_tests:18,example_inventory:270,example_passed:$passed,example_ignored:$ignored,source_files_unchanged:478,native_source_files:445,pids_peak:$pids,compute_closed:true,scope:"compiler and synthetic correctness only",scientific_performance_claim:false}'
