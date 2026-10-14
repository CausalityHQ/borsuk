#!/usr/bin/env bash
set -euo pipefail
stage=${1:?stage}
[[ $stage =~ ^[a-z][a-z0-9_-]+$ ]]
root=/data/target/borsuk-sq8-shortlist.pending/launch-falsifier-full-driver-a0001
[[ $(cat "$root/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
out=$root/$stage-collected
mkdir -m700 "$out"
prefix=research/semantic-router/20261014/sq8-shortlist-full-driver-a0001/stages/$stage
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
timeout -k2 25 /usr/local/bin/aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/SHA256SUMS" "$out/SHA256SUMS" > "$out/manifest.get.json"
[[ $(stat -c%s "$out/SHA256SUMS") -le 16384 ]]
args=(--recursive --exclude '*')
count=0
while read -r hash path; do
 [[ $hash =~ ^[0-9a-f]{64}$ && $path == ./* ]]
 file=${path#./}; [[ $file =~ ^[a-zA-Z0-9_.-]+$ && $file != .* ]]
 args+=(--include "$file")
 count=$((count+1)); (( count<=64 ))
done < "$out/SHA256SUMS"
(( count>0 ))
timeout -k2 180 /usr/local/bin/aws --profile causality --region eu-central-1 s3 cp "s3://borsuk-bench-453182569524-euc1/$prefix/" "$out/" "${args[@]}" > "$out/opaque-download.log" 2> "$out/opaque-download.stderr"
(cd "$out" && sha256sum --strict -c SHA256SUMS) > "$out/full-manifest-check.log"
jq -e --arg stage "$stage" '.source_commit=="fde98a39778c5fb76277327cd2b4bb224838cdc7" and .stage==$stage and .owned_cgroup_drained==true' "$out/terminal.json" > "$out/terminal-identity-check.txt"
