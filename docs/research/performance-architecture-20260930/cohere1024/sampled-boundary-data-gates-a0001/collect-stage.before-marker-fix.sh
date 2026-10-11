#!/bin/bash
set -euo pipefail
d=/data/target/borsuk-sampled-boundary-data-gate-a0001.pending
stage=$1
[[ "$stage" == admission || "$stage" == smoke ]]
jq -e '.Status=="Success" or .Status=="Failed" or .Status=="TimedOut" or .Status=="Cancelled"' "$d/$stage.ssm-terminal.json" >/dev/null
mkdir "$d/$stage-collected"
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
prefix=research/semantic-router/20261011/sampled-boundary-data-gates-a0001/$stage/
timeout -k 2 25 aws --profile causality --region eu-central-1 s3api list-objects-v2 --no-paginate --bucket borsuk-bench-453182569524-euc1 --prefix "$prefix" > "$d/$stage.objects.json"
jq -e 'has("Contents") and (.Contents|length)>0 and (.IsTruncated==false)' "$d/$stage.objects.json" >/dev/null
while IFS= read -r key; do
 name=${key##*/}
 [[ "$key" == "$prefix$name" && "$name" != .. && "$name" != '' ]]
 timeout -k 2 25 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$d/$stage-collected/$name" > "$d/$stage-collected/$name.get.json"
done < <(jq -r '.Contents[].Key' "$d/$stage.objects.json")
(cd "$d/$stage-collected" && sha256sum --check SHA256SUMS) > "$d/$stage.artifacts-verified.log"
jq -e --arg s "$stage" '.stage==$s and .schema=="borsuk-boundary-data-stage-v1" and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .scientific_measurement==false' "$d/$stage-collected/terminal.json" >/dev/null
test "$(cat "$d/$stage-collected/cpu.max.before")" = '100000 100000'
test "$(cat "$d/$stage-collected/cpuset.cpus.effective.before")" = 0
test "$(cat "$d/$stage-collected/memory.max.before")" = 536870912
test "$(cat "$d/$stage-collected/memory.swap.max.before")" = 0
test "$(cat "$d/$stage-collected/memory.swap.peak.after")" = 0
test "$(cat "$d/$stage-collected/pids.max.before")" = 128
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$d/$stage-collected/memory.events.after"
rg -qx 'REAL_INPUT_AND_DURABLE_SEALS_AUTHENTICATED' "$d/$stage-collected/native.log"
printf '%s authenticated; candidate quality remains opaque\n' "$stage"
