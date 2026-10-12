#!/usr/bin/env bash
# A successful empty LIST is evidence; an AWS error is not absence.
set -euo pipefail
[[ $# == 3 && $1 == /mnt/borsuk-retained-returned-publication ]] || exit 125
root=$1; phase=$2; config=$3
case "$phase" in negative-approval|negative-membership) ;; *) exit 125;; esac
[[ $config == "$root/inputs/"* && -f $config && ! -L $config ]] || exit 125
ev=$root/evidence/$phase
jq -e --arg phase "$phase" '.phase==$phase and .execution_closed==true and .manager_exit==0 and .native_exit==2 and .owned_cgroup_drained==true and .forced_cleanup==false' "$ev/supervision.json" > "$ev/negative-closure-check.txt"
bucket=$(jq -er .backend.bucket "$config")
namespace=$(jq -er .backend.namespace "$config")
dest=$(jq -er .destination_prefix "$config")
[[ $bucket == borsuk-bench-453182569524-euc1 && $namespace == research/semantic-router/20261010/full1000-local-admission-a0004/retained-source ]] || exit 125
if [[ $phase == negative-approval ]]; then
 [[ $dest == semantic/publication-admission-spatial-a0001 ]] || exit 125
else
 [[ $dest == semantic/publication-negative-membership-a0001 ]] || exit 125
fi
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
/usr/bin/timeout -k 2 25 aws --region eu-central-1 s3api list-objects-v2 --bucket "$bucket" --prefix "$namespace/$dest/" --no-paginate > "$ev/negative-destination-list.json"
jq -e '.IsTruncated==false and .KeyCount==0 and ((.Contents//[])|length)==0' "$ev/negative-destination-list.json" > "$ev/negative-destination-check.txt"
printf '%s\n' NEGATIVE_DESTINATION_EMPTY > "$ev/negative-destination.status"
