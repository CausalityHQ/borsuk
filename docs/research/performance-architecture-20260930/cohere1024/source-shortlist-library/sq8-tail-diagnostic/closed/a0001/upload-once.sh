#!/bin/bash
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-diagnostic-a0001
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
jq -e '.controller_launch_authorized==true and .support_archive_bytes==215040 and .support_archive_sha256=="2eec6389c96198d1acfeef273777073a2217f73d9f344f79961eec987b044313"' "$out/freeze.json" >/dev/null
[[ ! -e $out/input-upload.claim && ! -e $out/input-upload.closed ]]
jq -e '.KeyCount==0' "$out/prior-prefix.json" >/dev/null
printf '%s  %s\n' 2eec6389c96198d1acfeef273777073a2217f73d9f344f79961eec987b044313 "$out/support.tar" | sha256sum --strict -c -
set -o noclobber
date -u +%FT%TZ > "$out/input-upload.claim"
set +o noclobber
timeout -k2 30 aws --profile causality --region eu-central-1 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261015/sq8-panel-diagnostic-a0001/inputs/support.tar --body "$out/support.tar" --if-none-match '*' > "$out/input-upload.put.json"
timeout -k2 30 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261015/sq8-panel-diagnostic-a0001/inputs/support.tar "$out/support.readback.tar" > "$out/input-upload.get.json"
printf '%s  %s\n' 2eec6389c96198d1acfeef273777073a2217f73d9f344f79961eec987b044313 "$out/support.readback.tar" | sha256sum --strict -c - > "$out/input-upload.readback.log"
printf '%s\n' SOURCE_PACKET_CREATED_ONCE_AND_READBACK_SHA_VERIFIED > "$out/input-upload.closed"
