#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
mkdir "$root/full-driver-stages" "$root/evidence/full-driver-outcome"
source=fde98a39778c5fb76277327cd2b4bb224838cdc7
prefix=research/semantic-router/20261014/sq8-shortlist-full-driver-a0001
stage=driver-compile
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'a27307842e4662eb48181018bc81c04313c38645911f435528e1d57479316664' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
stage=exact-01
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'fb5d83f166e0fcc9a9720de9a4390bafab491f4a8281fd8c0e8700565f9d43f8' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::tests::shortlist_captures_production_scored_rows_excludes_gap_and_breaks_ties: test' "$ev/native-list.log") == 1 ]]
stage=exact-02
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '748c48a6b847616b3a7466c2e2892c270b258b61bfef30f492d7a9d379bf04ce' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::tests::shortlist_authenticated_block_expansion_and_bridges_recover_distinct_rows: test' "$ev/native-list.log") == 1 ]]
stage=exact-03
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'a35ba4738f52ef6fdc5a31514f09df10da5e778c1dbacda0a13e764e4558ee31' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::tests::shortlist_refuses_invalid_scores_ordinals_and_workspace_boundary: test' "$ev/native-list.log") == 1 ]]
stage=exact-04
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'd05021e10122ed77e34646a54497ec93baa587bd6935d60349a8356f970f26ab' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::tests::shortlist_block_only_control_preserves_all_baseline_bytes_ids_and_score_bits: test' "$ev/native-list.log") == 1 ]]
stage=exact-05
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '106c55e9d552b3c5a18fe066410144b3b8575a23dbf3efdcf174a997bbd26c9a' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::digest_tests::shortlist_streamed_sidecar_authenticates_whole_object_and_partial_tail: test' "$ev/native-list.log") == 1 ]]
stage=exact-06
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '1d389317f1e8c052cec562ad294d095c69682ed1b208a765b12abc4d10c34da6' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::driver_tests::shortlist_driver_fixed_input_and_unknown_field_refusals_precede_baseline_open: test' "$ev/native-list.log") == 1 ]]
stage=exact-07
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'deae4f18194a41947ffd5d970ecb3738ed476f646249e9e00f0c6660be561ea4' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::driver_tests::shortlist_driver_invalid_report_is_durable_and_existing_output_is_preserved: test' "$ev/native-list.log") == 1 ]]
stage=exact-08
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '9c8d6439202ba99631fb66adebb87c3fff2b5fe78044b4a07efc0051edf5ba7a' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 1 ]]
grep -Eq '^test result: ok[.] 1 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
[[ $(grep -Fxc 'sq8_subpage_shortlist::report_tests::shortlist_overcap_geometry_has_unknown_quality_and_bad_mapping_is_refused: test' "$ev/native-list.log") == 1 ]]
stage=example-regressions
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'ae49d9744872161e7a8b0719a845c73c46e890c685da5e6003e8f4f551834151' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 206 ]]
grep -Eq '^test result: ok[.] 206 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
stage=release-shortlist-regressions
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'fbbc39e8a882192a8bef7722bbf72e37e414d7993d19a5c3409408ff6e4140e7' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
[[ $(grep -c ': test$' "$ev/native-list.log") == 8 ]]
grep -Eq '^test result: ok[.] 8 passed; 0 failed; 0 ignored; 0 measured;' "$ev/native.log"
stage=release-example
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'f56e30c4a25af67d71e8ec4a5309492a7ba18eca3cd4b2b4cbd2b91e6a999c34' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
stage=workspace-clippy
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' '04190ae531a9515a63a739e51dde95b367e48f72d829ff72ff0b8a9695438506' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
stage=workspace-test-build
timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$stage.sh" "$root/full-driver-stages/$stage.sh" >/dev/null
printf '%s  %s\n' 'ab3e052897c840f78c22d9091b6f82b4ef730b6143a6cc365091cce93bf83719' "$root/full-driver-stages/$stage.sh" | sha256sum --strict -c -
bash "$root/full-driver-stages/$stage.sh"
ev=$root/evidence/$stage
jq -e --arg s "$source" --arg stage "$stage" '.source_commit==$s and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" >/dev/null
cmp "$ev/source-before.log" "$ev/source-after.log"
[[ $(wc -l < "$ev/source-before.log") == 471 ]]
out=$root/evidence/full-driver-outcome
jq -n --arg s "$source" '{schema:"borsuk-sq8-shortlist-full-driver-native-v1",status:"FOURTEEN_NATIVE_DRIVER_GATES_CLOSED",source_commit:$s,stage_count:14,native_qualified:true,actual_input_admitted:false,scientific_qualified:false,performance_claim:false}' > "$out/terminal.json"
(cd "$out" && sha256sum terminal.json > SHA256SUMS)
for file in terminal.json SHA256SUMS; do timeout -k2 90 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/full-driver-outcome/$file" --body "$out/$file" --if-none-match '*' >/dev/null; done
