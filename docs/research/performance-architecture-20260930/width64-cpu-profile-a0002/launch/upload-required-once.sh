#!/usr/bin/env bash
set -euo pipefail
profile_dir=/data/target/borsuk-source-width64.pending/cpu-profile-a0002
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
set -o noclobber
date -u +%FT%TZ > "$profile_dir/launch/input-upload-required.claim"
set +o noclobber
for file in "$profile_dir"/inputs/*.json "$profile_dir"/runtime/*.sh "$profile_dir"/launch/perf-admission.sh "$profile_dir/transport.tsv"; do
 base=${file##*/}; target=$base
 if [[ $file == */inputs/* ]]; then target=inputs/$base; fi
 [[ -n $base && -n $target && $target != */ ]]
 timeout -k 2 40 aws --profile causality --region eu-central-1 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261013/source-width64-cpu-profile-a0002/inputs/$target" --body "$file" --if-none-match '*' > "$profile_dir/launch/upload-required-$base.json" 2> "$profile_dir/launch/upload-required-$base.stderr"
done
printf '%s\n' ALL_CREATE_ONLY_PROFILE_INPUTS_STAGED > "$profile_dir/launch/input-upload.closed"
