#!/usr/bin/env bash
set -euo pipefail
root=/data/target/borsuk-source-cover-curve.pending
out=$root/launch-a0001
source=13eb0bdc84c55db3f3e9433f56a1ee7d6bed7d8c
jq -e 'length==437 and all(.[]; type=="string" and test("^[0-9a-f]{64}$"))' "$root/package/native-source.json" > /dev/null
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e '.Status=="Success" and .ResponseCode==0 and .CommandId=="f0bc4c4e-d45a-4f77-b1ea-dc5e1b9abe53" and .InstanceId=="i-0082d3e6d1dbfed04"' "$out/run-terminal-observation.json" > /dev/null
batch=$out/batch-collected
(cd "$batch" && sha256sum --strict -c SHA256SUMS) > /dev/null
jq -e --arg source "$source" '.source_commit==$source and .batch_exit==0 and .completed_stages==9 and .required_stages==9 and .failed_stage==""' "$batch/terminal.json" > /dev/null
cmp "$root/assets-a0001/required-tests.json" "$batch/required-tests.json"
sed 's/^[0-9a-f]\{64\}  / /; s/^ //; s/$/: OK/' "$root/package/source-files.sha256" > "$out/expected-source-check.txt"
[[ $(wc -l < "$out/expected-source-check.txt") == 470 ]]
for stage in compile-example curve-closure curve-byte-cap curve-status curve-full release-elf workspace-clippy workspace-test-build curve-real; do
  ev=$out/$stage-collected
  (cd "$ev" && sha256sum --strict -c SHA256SUMS) > /dev/null
  jq -e --arg source "$source" --arg stage "$stage" '.source_commit==$source and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" > /dev/null
  cmp "$out/expected-source-check.txt" "$ev/source-before.log"
  cmp "$ev/source-before.log" "$ev/source-after.log"
  [[ $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/memory.swap.peak.after") == 0 ]]
  awk '$1=="oom" || $1=="oom_kill" || $1=="oom_group_kill" {if ($2!=0) exit 1; n++} END {if(n<2) exit 1}' "$ev/memory.events.after"
  if jq -e --arg stage "$stage" 'has($stage)' "$batch/required-tests.json" > /dev/null; then
    cmp "$batch/$stage.expected" "$batch/$stage.listed"
    cmp "$batch/$stage.expected" "$batch/$stage.actual"
  fi
done
jq -e '.schema=="borsuk-source-cover-cost-curve-v1" and .status=="COUNTERFACTUAL_COST_ONLY" and .complete==true and .count==1000 and .counterfactual_cost_only==true and .native_ann_called==false and .corpus_queries_truth_bodies_opened==false and .claims_latency==false and .claims_quality==false and .claims_bill==false and .production_change==false and [.curves[].get_cap]==[128,96,64,48,32] and all(.curves[]; .predicted_queries==1000)' "$out/curve-real-collected/curve.json" > /dev/null
jq -e --slurpfile native "$root/package/native-source.json" '.generation_source_sha256==$native[0]["crates/borsuk/src/two_bit_generation.rs"] and .planner_source_sha256==$native[0]["crates/borsuk/src/budgeted_page_rank.rs"] and .reducer_source_sha256==$native[0]["crates/borsuk/examples/compare_native_replay.rs"]' "$out/curve-real-collected/curve.json" > /dev/null
printf '%s\n' 'NINE_NATIVE_STAGES_SOURCE_ROSTERS_RESOURCES_AND_CLOSED_INPUT_COST_REPORT_VERIFIED'
