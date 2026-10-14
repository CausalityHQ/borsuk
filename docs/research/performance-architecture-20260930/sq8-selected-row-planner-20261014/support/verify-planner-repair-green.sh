#!/bin/bash
set -euo pipefail
root=/data/target/borsuk-sq8-shortlist.pending
base=$root/launch-planner-repair-green-a0002
source=cb34ca25da00b8282e4885639c22b80eef4ef062
[[ $(cat "$base/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
[[ $(wc -l < "$root/assets-planner-repair-green/stage-roster.txt") == 10 ]]
while IFS= read -r stage; do
 out=$base/$stage-collected
 (cd "$out" && sha256sum --strict --check SHA256SUMS) > "$out/independent-manifest-check.log"
 jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$out/terminal.json" >/dev/null
 [[ $(wc -l < "$out/source-before.log") == 470 ]]
 cmp "$out/source-before.log" "$out/source-after.log"
 [[ $(cat "$out/cpu.max.before") == '400000 100000' && $(cat "$out/cpuset.cpus.effective.before") == 0-3 && $(cat "$out/memory.max.before") == 15032385536 && $(cat "$out/memory.swap.max.before") == 0 && $(cat "$out/pids.max.before") == 512 ]]
 [[ $(cat "$out/memory.swap.peak.after") == 0 ]]
 awk '$1=="oom"||$1=="oom_kill"||$1=="oom_group_kill" {n++;if($2!=0)exit 1} END{if(n!=3)exit 1}' "$out/memory.events.after"
 count=0; name=''
 case $stage in
 affected-compile) command='cargo test --locked -p borsuk --lib --no-run';;
 example-compile) command='cargo test --locked -p borsuk --example compare_native_replay --no-run';;
 selected-positive) count=1; name=sq8_page_authority::selected_sq8_rows_contract::selected_sq8_rows_authenticated_geometry_parity_and_bridges; command="cargo test --locked -p borsuk --lib $name -- --exact --test-threads=1";;
 selected-refusal) count=1; name=sq8_page_authority::selected_sq8_rows_contract::selected_sq8_rows_refuse_roster_overflow_and_planner_budget; command="cargo test --locked -p borsuk --lib $name -- --exact --test-threads=1";;
 selected-plane-kind) count=1; name=sq8_page_authority::selected_sq8_plane_kind_contract::selected_sq8_rows_rejects_source_authority; command="cargo test --locked -p borsuk --lib $name -- --exact --test-threads=1";;
 planner-regressions) count=10; command='cargo test --locked -p borsuk --lib budgeted_page_rank:: -- --test-threads=1';;
 authority-regressions) count=5; command='cargo test --locked -p borsuk --lib sq8_page_authority:: -- --test-threads=1';;
 release-lib) command='cargo build --release --locked -p borsuk --lib';;
 workspace-clippy) command='cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious';;
 workspace-test-build) command='env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh';;
 *) exit 1;;
 esac
 [[ $(cat "$out/native-command.txt") == "$command" ]]
 if (( count>0 )); then
  [[ $(grep -c ': test$' "$out/native-list.log") == "$count" ]]
  [[ $(grep -Ec "^test result: ok[.] $count passed; 0 failed; 0 ignored; 0 measured;" "$out/native.log") == 1 ]]
  [[ -z $name ]] || [[ $(grep -Fxc "$name: test" "$out/native-list.log") == 1 ]]
  while IFS= read -r test_name; do [[ $(grep -Fxc "test $test_name ... ok" "$out/native.log") == 1 ]]; done < <(sed -n 's/: test$//p' "$out/native-list.log")
 fi
 printf '%s VERIFIED\n' "$stage"
done < "$root/assets-planner-repair-green/stage-roster.txt"
instance=$(cat "$base/instance.id"); command=$(cat "$base/native-command.id"); volume=$(cat "$base/volume.id")
jq -e --arg i "$instance" --arg c "$command" '.InstanceId==$i and .CommandId==$c and .Status=="Success" and .ResponseCode==0' "$base/run-terminal-observation.json" >/dev/null
jq -e --arg i "$instance" '.Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated")' "$base/instance-final.json" >/dev/null
[[ $(cat "$base/volume-final.exit") == 254 ]]
grep -F InvalidVolume.NotFound "$base/volume-final.stderr" >/dev/null
grep -F "$volume" "$base/volume-final.stderr" >/dev/null
grep -qx ActiveState=inactive "$base/external-timer-final.txt"
printf '%s\n' TEN_NATIVE_PLANNER_REPAIR_GATES_SOURCE_RESOURCES_AND_COMPUTE_CLOSURE_VERIFIED
