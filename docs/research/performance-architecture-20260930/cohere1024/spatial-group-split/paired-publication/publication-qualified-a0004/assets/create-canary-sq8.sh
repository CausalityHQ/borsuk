#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 ]] || exit 125
root=$1
[[ $root == /mnt/borsuk-retained-returned-publication ]] || exit 125
source=$root/inputs/canary-sq8-source.bin
sha=cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1
key=research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/publication-canary-assets-a0001/objects/$sha
[[ -f $source && ! -L $source && $(stat -c %s "$source") == 1036000000 ]] || exit 125
out=$root/evidence/canary-sq8-copy
mkdir -m 700 "$out"
printf '%s  %s\n' "$sha" "$source" | sha256sum -c - > "$out/source-sha-check.log"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
# A lost response is ambiguous; preserve original exit, never automatically retry.
set +e
/usr/bin/timeout -k 2 180 aws --region eu-central-1 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --body "$source" --metadata "sha256=$sha" --if-none-match '*' > "$out/put.stdout" 2> "$out/put.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$out/put.exit"
((status==0)) || exit 1
/usr/bin/timeout -k 2 25 aws --region eu-central-1 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$out/head.json"
jq -e --arg sha "$sha" '.ContentLength==1036000000 and .Metadata.sha256==$sha and (.ETag|type=="string" and length>0 and length<=4096)' "$out/head.json" > "$out/head-check.txt"
etag=$(jq -er .ETag "$out/head.json")
version=$(jq -r '.VersionId//empty' "$out/head.json")
[[ ${#version} -le 4096 && $version != *$'\n'* && $version != *$'\t'* ]] || exit 1
args=(get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --if-match "$etag")
if [[ -n $version ]]; then args+=(--version-id "$version"); fi
/usr/bin/timeout -k 2 180 aws --region eu-central-1 s3api "${args[@]}" "$out/copy.readback.bin" > "$out/readback.get.json"
[[ $(stat -c %s "$out/copy.readback.bin") == 1036000000 ]] || exit 1
printf '%s  %s\n' "$sha" "$out/copy.readback.bin" | sha256sum -c - > "$out/readback-sha-check.log"
jq -e --arg etag "$etag" --arg version "$version" '.ContentLength==1036000000 and .ETag==$etag and ((.VersionId//"")==$version)' "$out/readback.get.json" > "$out/readback-response-check.txt"
/usr/bin/timeout -k 2 25 aws --region eu-central-1 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$out/head.after.json"
jq -S '{ContentLength,ETag,VersionId,LastModified,Metadata}' "$out/head.json" > "$out/head.before.identity"
jq -S '{ContentLength,ETag,VersionId,LastModified,Metadata}' "$out/head.after.json" > "$out/head.after.identity"
cmp "$out/head.before.identity" "$out/head.after.identity"
# Native B additionally authenticates the complete distinct object before head.
jq -n --arg key semantic/publication-canary-assets-a0001/objects/$sha --slurpfile head "$out/head.json" '{schema:"borsuk-owned-canary-sq8-transport-v1",key:$key,bytes:1036000000,sha256:"cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1",etag:$head[0].ETag,version:$head[0].VersionId,native_body_authentication:"PENDING_CANARY",performance_claim:false}' > "$out/transport.json"
