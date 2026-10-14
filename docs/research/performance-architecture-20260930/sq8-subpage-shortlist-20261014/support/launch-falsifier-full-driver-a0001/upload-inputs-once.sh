#!/bin/bash
set -euo pipefail
root=/data/target/borsuk-sq8-shortlist.pending
out=$root/launch-falsifier-full-driver-a0001
prefix=research/semantic-router/20261014/sq8-shortlist-full-driver-a0001/inputs
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ ! -e $out/input-upload.claim ]]
set -o noclobber
date -u +%FT%TZ > "$out/input-upload.claim"
set +o noclobber
for file in "$root/package-falsifier-full-driver/source.tar.gz" "$root/package-falsifier-full-driver/source-files.sha256" "$root/package-falsifier-full-driver/native-source.json" "$root/assets-falsifier-full-driver"/*.sh; do
 name=${file##*/}
 timeout -k2 60 aws --profile causality --region eu-central-1 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$name" --body "$file" --if-none-match '*' > "$out/upload-$name.json" 2> "$out/upload-$name.stderr"
done
date -u +%FT%TZ > "$out/input-upload.closed"
