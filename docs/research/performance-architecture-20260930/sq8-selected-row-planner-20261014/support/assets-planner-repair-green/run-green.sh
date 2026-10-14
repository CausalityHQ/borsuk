#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
mkdir "$root/green-stages" "$root/evidence/green-outcome"
check_list() {
 local ev=$1 expected=$2 count name
 count=$(grep -c ': test$' "$ev/native-list.log")
 [[ $count == "$expected" ]]
 [[ $(grep -Ec "^test result: ok[.] $count passed; 0 failed; 0 ignored; 0 measured;" "$ev/native.log") == 1 ]]
 while IFS= read -r name; do [[ $(grep -Fxc "test $name ... ok" "$ev/native.log") == 1 ]]; done < <(sed -n 's/: test$//p' "$ev/native-list.log")
}
source=cb34ca25da00b8282e4885639c22b80eef4ef062
prefix=research/semantic-router/20261014/sq8-selected-row-planner-repair-green-a0001
stage=affected-compile
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '21df78681b87c7be80df95f45eecc9746463b49106ee436e95adb4fbe80a7988' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
stage=example-compile
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '61a23480b44d856f89abcc56296b8afeb99d055868a949ca2ff6e18ddceb07c6' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
stage=selected-positive
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '451afd08ca216457e5c09934176987cb37eb2a4cd8bdb99004688899f6986f59' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
check_list "$ev" 1
stage=selected-refusal
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '1f0371934461ac18ba68de38df3e99d572db0a87ef7a90ed2906a8fa9c6b4fd4' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
check_list "$ev" 1
stage=selected-plane-kind
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '507c17410ef4e376c86706fd732a4b96ee2f0656e03b007a566092273ed120eb' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
check_list "$ev" 1
stage=planner-regressions
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'd8c24095faee3ca1fdb31e118039a02521765638fd127b1026d802e41c7ed073' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
check_list "$ev" 10
stage=authority-regressions
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'e09b54339f8633602321c19635eef3a362e1e820b09e090a6dbfe2d16a37e355' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
check_list "$ev" 5
stage=release-lib
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'd9c73c0dd7fd9566aaa9e4639d55705101b0df6c12db98ad2092e7eab8f630bf' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
stage=workspace-clippy
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'b208d0a47cc60536b2f49fcedbc56d5d5b82446b22a411b8aee44bf11184b3b3' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
stage=workspace-test-build
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/green-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'bcf5036df0bdf62094c7c7f45002c2c5b8b5c35dab43708a17920ef2d5e56ce4' "$root/green-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/green-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 470 ]]
out=$root/evidence/green-outcome
jq -n --arg s "$source" '{schema:"borsuk-selected-sq8-planner-green-v1",status:"TEN_NATIVE_GATES_CLOSED",source_commit:$s,stage_count:10,native_qualified:true,performance_claim:false}' > "$out/terminal.json"
(cd "$out" && sha256sum terminal.json > SHA256SUMS)
for file in terminal.json SHA256SUMS; do timeout -k2 90 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/green-outcome/$file" --body "$out/$file" --if-none-match '*' >/dev/null; done
