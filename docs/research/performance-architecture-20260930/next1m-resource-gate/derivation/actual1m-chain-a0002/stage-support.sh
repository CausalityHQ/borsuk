#!/usr/bin/env bash
set -Eeuo pipefail
export AWS_PROFILE=causality AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
D=/tmp/borsuk-next1m-native-a0002-603fd5f9
prefix=$(< "$D/run-prefix")
[[ $prefix == research/semantic-router/20261010/actual1m-pid128-chain-a0002 ]]
for name in $(jq -er '.assets[].name' "$D/source-roster.json") support.sha256; do
 [[ $name =~ ^[a-zA-Z0-9._-]+$ && -f $D/assets/$name && ! -L $D/assets/$name ]]
 key=$prefix/inputs/$name
 sha=$(sha256sum "$D/assets/$name");sha=${sha%% *};bytes=$(stat -c %s "$D/assets/$name")
 if [[ -e $D/$name.upload.json ]]; then
  jq -e '.ETag|type=="string" and length>0' "$D/$name.upload.json" >/dev/null
 else
  timeout -k 1 15 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --body "$D/assets/$name" --if-none-match '*' > "$D/$name.upload.json"
 fi
 if [[ -e $D/$name.head.json ]]; then
  [[ ! -e $D/$name.head.previous.json ]]
  mv "$D/$name.head.json" "$D/$name.head.previous.json"
 fi
 timeout -k 1 15 aws s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$D/$name.head.json"
 [[ $(jq -er .ContentLength "$D/$name.head.json") == "$bytes" ]]
 etag=$(jq -er .ETag "$D/$name.head.json")
 [[ $etag == "$(jq -er .ETag "$D/$name.upload.json")" ]]
 [[ ! -e $D/$name.readback ]] || mv "$D/$name.readback" "$D/$name.readback.previous"
 timeout -k 1 15 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag" "$D/$name.readback" > "$D/$name.readback.json"
 cmp "$D/assets/$name" "$D/$name.readback"
 printf '%s  %s\n' "$sha" "$D/$name.readback" | sha256sum -c -
done
timeout -k 1 15 aws ec2 describe-spot-price-history --instance-types c7i.2xlarge --product-descriptions 'Linux/UNIX' --availability-zone eu-central-1c --start-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --max-items 1 > "$D/spot-quote.json"
date +%s > "$D/spot-quote-observed.epoch"
