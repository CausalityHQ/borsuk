#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
prefix=research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
mkdir "$root/final-build-stages" "$root/evidence/final-build-red-outcome"
source=fde98a39778c5fb76277327cd2b4bb224838cdc7
stage=workspace-test-build
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/final-build-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '50988049a1a46027eec364d5d8dbd11a41648043d9540eead396d2cef1358883' "$root/final-build-stages/$stage.sh" | sha256sum --strict -c -
set +e
bash "$root/final-build-stages/$stage.sh"
rc=$?
set -e
[[ $rc == 0 ]]
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/red-source.tar.gz" "$root/red-source.tar.gz" >/dev/null
[[ $(stat -c %s "$root/red-source.tar.gz") == 4805524 ]]
printf '%s  %s\n' 'f839d3a626d4d827b4baacf671a34e8fedd9aad460c0fd2e6670806a927a59e6' "$root/red-source.tar.gz" | sha256sum --strict -c -
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/red-source-files.sha256" "$root/red-source-files.sha256" >/dev/null
[[ $(stat -c %s "$root/red-source-files.sha256") == 51976 ]]
printf '%s  %s\n' 'b4b4fe281568e444cbd7b183b3c335deacba93cad342ec79d83d6351b42640bf' "$root/red-source-files.sha256" | sha256sum --strict -c -
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/red-native-source.json" "$root/red-native-source.json" >/dev/null
[[ $(stat -c %s "$root/red-native-source.json") == 51539 ]]
printf '%s  %s\n' '7d39ac52591efd560ac374620924ec05949adb06edda27e1a4f9fb4957d3c4b0' "$root/red-native-source.json" | sha256sum --strict -c -
# Both archives are root-created, committed-source packages authenticated above.
# Overwrite source only; retain Cargo target cache from the passing parent build.
tar -xzf "$root/red-source.tar.gz" -C "$root/source"
cp "$root/red-source-files.sha256" "$root/source-files.sha256"
cp "$root/red-native-source.json" "$root/native-source.json"
(cd "$root/source" && sha256sum --strict -c "$root/source-files.sha256") > "$root/evidence/red-overlay-check.log"
source=89ef873773194f8c8c9fd1aacc28a79313681995
stage=admission-compile
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/final-build-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'f409330dff53d64a3588b424356f56bf6d5236ecb460eace5243992207775566' "$root/final-build-stages/$stage.sh" | sha256sum --strict -c -
set +e
bash "$root/final-build-stages/$stage.sh"
rc=$?
set -e
[[ $rc == 0 ]]
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
source=89ef873773194f8c8c9fd1aacc28a79313681995
stage=admission-payload-red
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/final-build-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '00bca2e3c92a91fc6ebb933e2e087592e59a115c619c2f2b3675a2fb83710374' "$root/final-build-stages/$stage.sh" | sha256sum --strict -c -
set +e
bash "$root/final-build-stages/$stage.sh"
rc=$?
set -e
[[ $rc == 1 ]]
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==101 and .manager_exit==101 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
[[ $(grep -Fxc 'sq8_subpage_shortlist::admission_tests::shortlist_admission_validates_all_query_and_truth_rows: test' "$ev/native-list.log") == 1 ]]
grep -F 'shortlist input admission implementation missing' "$ev/native.log" >/dev/null
grep -Eq '^test result: FAILED[.] 0 passed; 1 failed; 0 ignored; 0 measured;' "$ev/native.log"
out=$root/evidence/final-build-red-outcome
jq -n --arg p 'fde98a39778c5fb76277327cd2b4bb224838cdc7' --arg r '89ef873773194f8c8c9fd1aacc28a79313681995' '{schema:"borsuk-final-build-admission-red-v1",status:"FINAL_WORKSPACE_BUILD_AND_EXPECTED_ADMISSION_RED_CLOSED",parent_source:$p,red_source:$r,stage_count:3,parent_test_build_passed:true,parent_qualification_requires_prior_13:true,admission_implemented:false,actual_input_admitted:false,performance_claim:false}' > "$out/terminal.json"
(cd "$out" && sha256sum terminal.json > SHA256SUMS)
for file in terminal.json SHA256SUMS; do timeout -k2 90 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/final-build-red-outcome/$file" --body "$out/$file" --if-none-match '*' >/dev/null; done
