#!/bin/bash
set -euo pipefail
r=/data/target/borsuk-sq8-shortlist.pending
out=$r/launch-final-build-admission-red-a0001
prefix=research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001/inputs
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ ! -e $out/input-upload.claim ]]
set -o noclobber
date -u +%FT%TZ > "$out/input-upload.claim"
set +o noclobber
for file in "$r/package-falsifier-full-driver/source.tar.gz" "$r/package-falsifier-full-driver/source-files.sha256" "$r/package-falsifier-full-driver/native-source.json" "$r/assets-final-build-admission-red"/*.sh; do
 name=${file##*/}
 timeout -k2 60 aws --profile causality --region eu-central-1 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$name" --body "$file" --if-none-match '*' > "$out/upload-$name.json" 2> "$out/upload-$name.stderr"
done
for name in source.tar.gz source-files.sha256 native-source.json; do
 timeout -k2 60 aws --profile causality --region eu-central-1 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/red-$name" --body "$r/package-input-admission-red/$name" --if-none-match '*' > "$out/upload-red-$name.json" 2> "$out/upload-red-$name.stderr"
done
date -u +%FT%TZ > "$out/input-upload.closed"
