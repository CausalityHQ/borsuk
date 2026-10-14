#!/bin/bash
set -euo pipefail
r=/data/target/borsuk-sq8-shortlist.pending
base=$r/launch-falsifier-full-driver-a0001
source=fde98a39778c5fb76277327cd2b4bb224838cdc7
[[ $(cat "$base/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
[[ $(cat "$base/collection.closed") == ORIGINAL_CLOSED_STAGE_COLLECTION_FINISHED ]]
[[ $(jq length "$base/closed-stage-manifests.json") == 14 ]]
[[ ! -s $base/collection-remaining.txt ]]
count=0
while IFS= read -r stage; do
 out=$base/$stage-collected
 (cd "$out" && sha256sum --strict -c SHA256SUMS) > "$out/independent-manifest-check.log"
 expected=0
 [[ $stage != workspace-test-build ]] || expected=124
 jq -e --arg s "$source" --arg stage "$stage" --argjson e "$expected" '.source_commit==$s and .stage==$stage and .native_exit==$e and .manager_exit==$e and .owned_cgroup_drained==true' "$out/terminal.json" >/dev/null

 [[ $(wc -l < "$out/source-before.log") == 471 ]]
 cmp "$out/source-before.log" "$out/source-after.log"
 cmp "$out/source-before.log" "$r/package-falsifier-full-driver/current-check.log"
 [[ $(cat "$out/cpu.max.before") == '400000 100000' && $(cat "$out/cpuset.cpus.effective.before") == 0-3 && $(cat "$out/memory.max.before") == 15032385536 && $(cat "$out/memory.swap.max.before") == 0 && $(cat "$out/pids.max.before") == 512 ]]
 [[ $(cat "$out/memory.swap.peak.after") == 0 ]]
 awk '$1=="oom"||$1=="oom_kill"||$1=="oom_group_kill" {n++;if($2!=0)exit 1} END{if(n!=3)exit 1}' "$out/memory.events.after"
 selected=$(jq -er --arg stage "$stage" '.[]|select(.stage==$stage)|.expected_count' "$r/assets-falsifier-full-driver/stage-spec.json")
 case $stage in
  driver-compile) cmd='cargo test --locked -p borsuk --example compare_native_replay --no-run';;
  exact-*) name=$(jq -er --arg stage "$stage" '.[]|select(.stage==$stage)|.exact_name' "$r/assets-falsifier-full-driver/stage-spec.json"); cmd="cargo test --locked -p borsuk --example compare_native_replay $name -- --exact --test-threads=1"; [[ $(grep -Fxc "$name: test" "$out/native-list.log") == 1 ]];;
  example-regressions) cmd='cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1';;
  release-shortlist-regressions) cmd='cargo test --locked -p borsuk --release --example compare_native_replay sq8_subpage_shortlist:: -- --test-threads=1';;
  release-example) cmd='cargo build --release --locked -p borsuk --example compare_native_replay'; [[ $(cat "$out/release-bytes.txt") =~ ^[1-9][0-9]*$ ]]; grep -Eq '^[a-f0-9]{64}  target/release/examples/compare_native_replay$' "$out/release-sha256.txt";;
  workspace-clippy) cmd='cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious';;
  workspace-test-build) cmd='env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh';;
  *) exit 1;;
 esac
 [[ $(cat "$out/native-command.txt") == "$cmd" ]]
 if (( selected>0 )); then
  [[ $(grep -c ': test$' "$out/native-list.log") == "$selected" ]]
  grep -Eq "^test result: ok[.] $selected passed; 0 failed; 0 ignored; 0 measured;" "$out/native.log"
 fi
 count=$(( count+1 )); printf '%s VERIFIED\n' "$stage"
done < "$r/assets-falsifier-full-driver/stage-roster.txt"
[[ $count == 14 ]]
instance=$(cat "$base/instance.id"); command=$(cat "$base/native-command.id"); setup=$(cat "$base/setup-command.id"); volume=$(cat "$base/volume.id")
jq -e --arg i "$instance" --arg c "$command" '.InstanceId==$i and .CommandId==$c and .Status=="Failed" and .ResponseCode==1' "$base/run-terminal-observation.json" >/dev/null
jq -e --arg i "$instance" --arg c "$setup" '.InstanceId==$i and .CommandId==$c and .Status=="Success" and .ResponseCode==0' "$base/setup-terminal-observation.json" >/dev/null
jq -e --arg i "$instance" '.Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated")' "$base/instance-final.json" >/dev/null
[[ $(cat "$base/volume-final.exit") == 254 ]]
grep -F InvalidVolume.NotFound "$base/volume-final.stderr" >/dev/null
grep -F "$volume" "$base/volume-final.stderr" >/dev/null
grep -qx ActiveState=inactive "$base/external-timer-final.txt"
printf '%s\n' THIRTEEN_PASSED_FINAL_TEST_BUILD_DEADLINE_SOURCE_RESOURCES_COMPUTE_CLOSURE_VERIFIED
