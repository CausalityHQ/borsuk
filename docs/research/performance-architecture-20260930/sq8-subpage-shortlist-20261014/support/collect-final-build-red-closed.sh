#!/bin/bash
set -euo pipefail
r=/data/target/borsuk-sq8-shortlist.pending
base=$r/launch-final-build-admission-red-a0001
[[ $(cat "$base/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
# shellcheck disable=SC2016 # Literal JMESPath backticks.
timeout -k2 25 aws --profile causality --region eu-central-1 s3api list-objects-v2 --bucket borsuk-bench-453182569524-euc1 --prefix research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001/stages/ --query 'Contents[?ends_with(Key, `/SHA256SUMS`)].Key' > "$base/closed-stage-manifests.json"
jq -e 'type=="array" and length>0 and length<=3' "$base/closed-stage-manifests.json" >/dev/null
while IFS= read -r stage; do
 key=research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001/stages/$stage/SHA256SUMS
 if jq -e --arg k "$key" 'index($k)!=null' "$base/closed-stage-manifests.json" >/dev/null; then
  bash "$base/collect-closed-stage-v2.sh" "$stage"
 else
  printf 'UNRUN_OR_UNPUBLISHED %s\n' "$stage" >> "$base/collection-remaining.txt"
 fi
done < "$r/assets-final-build-admission-red/stage-roster.txt"
printf '%s\n' ORIGINAL_CLOSED_STAGE_COLLECTION_FINISHED > "$base/collection.closed"
