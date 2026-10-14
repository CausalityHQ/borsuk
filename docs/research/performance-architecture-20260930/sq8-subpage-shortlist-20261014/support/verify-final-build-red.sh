#!/bin/bash
set -euo pipefail
r=/data/target/borsuk-sq8-shortlist.pending
base=$r/launch-final-build-admission-red-a0001
[[ $(cat "$base/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
grep -qx THIRTEEN_PASSED_FINAL_TEST_BUILD_DEADLINE_SOURCE_RESOURCES_COMPUTE_CLOSURE_VERIFIED "$r/launch-falsifier-full-driver-a0001/independent-partial-verification.log"
for stage in workspace-test-build admission-compile admission-payload-red; do
 out=$base/$stage-collected
 source=fde98a39778c5fb76277327cd2b4bb224838cdc7
 package='package-falsifier-full-driver'
 expected=0
 if [[ $stage != workspace-test-build ]]; then source=89ef873773194f8c8c9fd1aacc28a79313681995; package='package-input-admission-red'; fi
 [[ $stage != admission-payload-red ]] || expected=101
 (cd "$out" && sha256sum --strict -c SHA256SUMS) > "$out/independent-manifest-check.log"
 jq -e --arg s "$source" --arg stage "$stage" --argjson e "$expected" '.source_commit==$s and .stage==$stage and .native_exit==$e and .manager_exit==$e and .owned_cgroup_drained==true' "$out/terminal.json" >/dev/null
 [[ $(wc -l < "$out/source-before.log") == 471 ]]
 cmp "$out/source-before.log" "$out/source-after.log"
 cmp "$out/source-before.log" "$r/$package/current-check.log"
 [[ $(cat "$out/cpu.max.before") == '400000 100000' && $(cat "$out/cpuset.cpus.effective.before") == 0-3 && $(cat "$out/memory.max.before") == 15032385536 && $(cat "$out/memory.swap.max.before") == 0 && $(cat "$out/pids.max.before") == 512 ]]
 [[ $(cat "$out/memory.swap.peak.after") == 0 ]]
 awk '$1=="oom"||$1=="oom_kill"||$1=="oom_group_kill" {n++;if($2!=0)exit 1} END{if(n!=3)exit 1}' "$out/memory.events.after"
 case $stage in
  workspace-test-build) cmd='env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh'; grep -Eq '^rust-test-build status=0 elapsed_seconds=[0-9]+ jobs=1$' "$out/native.log";;
  admission-compile) cmd='cargo test --locked -p borsuk --example compare_native_replay --no-run';;
  admission-payload-red)
   name=sq8_subpage_shortlist::admission_tests::shortlist_admission_validates_all_query_and_truth_rows
   cmd="cargo test --locked -p borsuk --example compare_native_replay $name -- --exact --test-threads=1"
   [[ $(grep -c ': test$' "$out/native-list.log") == 1 && $(grep -Fxc "$name: test" "$out/native-list.log") == 1 ]]
   grep -F 'shortlist input admission implementation missing' "$out/native.log" >/dev/null
   grep -Eq '^test result: FAILED[.] 0 passed; 1 failed; 0 ignored; 0 measured;' "$out/native.log";;
 esac
 [[ $(cat "$out/native-command.txt") == "$cmd" ]]
 printf '%s VERIFIED source=%s expected_native=%s\n' "$stage" "$source" "$expected"
done
instance=$(cat "$base/instance.id"); command=$(cat "$base/native-command.id"); setup=$(cat "$base/setup-command.id"); volume=$(cat "$base/volume.id")
jq -e --arg i "$instance" --arg c "$command" '.InstanceId==$i and .CommandId==$c and .Status=="Success" and .ResponseCode==0' "$base/run-terminal-observation.json" >/dev/null
jq -e --arg i "$instance" --arg c "$setup" '.InstanceId==$i and .CommandId==$c and .Status=="Success" and .ResponseCode==0' "$base/setup-terminal-observation.json" >/dev/null
jq -e --arg i "$instance" '.Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated")' "$base/instance-final.json" >/dev/null
[[ $(cat "$base/volume-final.exit") == 254 ]]
grep -F InvalidVolume.NotFound "$base/volume-final.stderr" >/dev/null
grep -F "$volume" "$base/volume-final.stderr" >/dev/null
grep -qx ActiveState=inactive "$base/external-timer-final.txt"
printf '%s\n' EXACT_PARENT_FINAL_BUILD_AND_EXPECTED_ADMISSION_RED_SOURCE_RESOURCE_COMPUTE_CLOSURE_VERIFIED
