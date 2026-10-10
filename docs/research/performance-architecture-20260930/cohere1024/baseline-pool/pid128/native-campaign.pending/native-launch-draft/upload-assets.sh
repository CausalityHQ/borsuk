#!/usr/bin/env bash
# Root-side control-source upload only. At most seven small bodies per scope.
set -Eeuo pipefail
umask 077
export AWS_PROFILE=causality AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
[[ $# == 2 && $1 == /tmp/borsuk-pid128-native-ec2-* && $2 =~ ^[012]$ ]] || exit 125
D=$1; batch=$2
[[ -d $D && ! -L $D && $(realpath -e "$D") == "$D" ]] || exit 125
prefix=$(jq -er .assets "$D/asset-plan.json")
[[ $prefix == research/semantic-router/20261010/borsuk-pid128-native-a0001-*/assets ]] || exit 125
mkdir "$D/upload-batch-$batch.claim"
for ((i=batch*7; i<batch*7+7 && i<19; i++)); do
    name=$(jq -er ".[$i].name" "$D/support-roster.json")
    size=$(jq -er ".[$i].bytes" "$D/support-roster.json")
    hash=$(jq -er ".[$i].sha256" "$D/support-roster.json")
    [[ $name =~ ^[a-z0-9.-]+$ && $hash =~ ^[0-9a-f]{64}$ && $size =~ ^[1-9][0-9]*$ && $size -le 100000 ]]
    body=$D/assets/$name
    [[ -f $body && ! -L $body && $(stat -c %s "$body") == "$size" ]]
    printf '%s  %s\n' "$hash" "$body" | sha256sum -c -
    timeout -k 1 10 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 \
      --key "$prefix/$hash" --body "$body" --if-none-match '*' > "$D/$name.upload.json"
    jq -e '.ETag|type=="string" and length>0' "$D/$name.upload.json" >/dev/null
done
sync -f "$D"
