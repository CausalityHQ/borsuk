#!/usr/bin/env bash
# EC2-only opaque body transport; no vector/query/truth decoding.
set -euo pipefail
[[ $# == 3 ]] || exit 125
root=$1; config=$2; ev=$3
[[ $root == /mnt/borsuk-retained-returned-publication && $config == "$root/"* && $ev == "$root/evidence/"* ]] || exit 125
receipt=$ev/native-receipt.json
[[ -f $receipt && ! -L $receipt && $(stat -c %s "$receipt") -le 65536 ]] || exit 125
jq -e '.execution_closed==true and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/supervision.json" > /dev/null
bucket=$(jq -er '.backend.bucket' "$config")
namespace=$(jq -er '.backend.namespace' "$config")
dest=$(jq -er '.destination_prefix' "$config")
meta=$(jq -er '.metadata_prefix' "$receipt")
sha=$(jq -er '.root_sha256' "$receipt")
[[ $bucket == borsuk-bench-453182569524-euc1 && $namespace == research/semantic-router/20261010/full1000-local-admission-a0004/retained-source && $sha =~ ^[0-9a-f]{64}$ && $meta == "$dest/generations/$sha" ]] || exit 125
case "$dest" in semantic/publication-admission-spatial-a0001|semantic/publication-canary-spatial-a0001|semantic/rebound-radial-returned-a0001|semantic/rebound-spatial-returned-a0001) ;; *) exit 125;; esac
out=$ev/readback
mkdir -m 700 "$out"
mkdir -m 700 "$out/generation" "$out/generation/plane" "$out/generation/router" "$out/heads"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
deadline=$(( $(date +%s)+170 ))
aws_call() { local left; left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=60)) || left=60; /usr/bin/timeout -k 2 "$left" aws --region eu-central-1 s3api "$@"; }
aws_call list-objects-v2 --bucket "$bucket" --prefix "$namespace/$dest/" --no-paginate > "$out/remote-roster.json"
# Exact remote names include a separate head plus ten generation objects.
printf '%s\n' "$namespace/$dest/head.json" > "$out/expected-remote-keys.txt"
for path in manifest.json page_digests.bin page_manifest.json plane/manifest.json plane/mean.bin plane/page_digests.bin plane/records.bin router/leaves.bin router/membership.bin router/root.bin; do printf '%s\n' "$namespace/$meta/$path"; done >> "$out/expected-remote-keys.txt"
LC_ALL=C sort -o "$out/expected-remote-keys.txt" "$out/expected-remote-keys.txt"
jq -e '.IsTruncated==false and (.Contents|length)==11 and ([.Contents[].Key]|unique|length)==11' "$out/remote-roster.json" > "$out/remote-roster-shape-check.txt"
jq -er '.Contents[].Key' "$out/remote-roster.json" | LC_ALL=C sort > "$out/actual-remote-keys.txt"
cmp "$out/expected-remote-keys.txt" "$out/actual-remote-keys.txt"
fetch() {
 local label=$1 key=$2 target=$3 cap=$4 bytes etag version
 aws_call head-object --bucket "$bucket" --key "$key" > "$out/heads/$label.before.json"
 bytes=$(jq -er '.ContentLength|select(type=="number" and floor==. and .>0)' "$out/heads/$label.before.json")
 ((bytes<=cap)) || return 1
 etag=$(jq -er '.ETag|select(type=="string" and length>0 and length<=4096)' "$out/heads/$label.before.json")
 version=$(jq -r '.VersionId//empty' "$out/heads/$label.before.json")
 args=(get-object --bucket "$bucket" --key "$key" --if-match "$etag")
 if [[ -n $version ]]; then [[ ${#version} -le 4096 ]] || return 1; args+=(--version-id "$version"); fi
 aws_call "${args[@]}" "$target" > "$out/heads/$label.get.json"
 [[ $(stat -c %s "$target") == "$bytes" ]] || return 1
 jq -e --arg etag "$etag" --argjson bytes "$bytes" --arg version "$version" '.ContentLength==$bytes and .ETag==$etag and ((.VersionId//"")==$version)' "$out/heads/$label.get.json" > "$out/heads/$label.response-check.txt"
 aws_call head-object --bucket "$bucket" --key "$key" > "$out/heads/$label.after.json"
 jq -S '{ContentLength,ETag,VersionId,LastModified}' "$out/heads/$label.before.json" > "$out/heads/$label.before.identity"
 jq -S '{ContentLength,ETag,VersionId,LastModified}' "$out/heads/$label.after.json" > "$out/heads/$label.after.identity"
 cmp "$out/heads/$label.before.identity" "$out/heads/$label.after.identity"
}
fetch head "$namespace/$dest/head.json" "$out/head.json" 4096
for path in manifest.json page_digests.bin page_manifest.json plane/manifest.json plane/mean.bin plane/page_digests.bin plane/records.bin router/leaves.bin router/membership.bin router/root.bin; do
 label=${path//\//_}; cap=300000000
 case "$path" in manifest.json|page_manifest.json|plane/manifest.json) cap=65536;; plane/mean.bin) cap=4096;; esac
 fetch "$label" "$namespace/$meta/$path" "$out/generation/$path" "$cap"
done
aws_call list-objects-v2 --bucket "$bucket" --prefix "$namespace/$dest/" --no-paginate > "$out/remote-roster.after.json"
jq -e '.IsTruncated==false and (.Contents|length)==11 and ([.Contents[].Key]|unique|length)==11' "$out/remote-roster.after.json" > "$out/remote-roster-after-shape-check.txt"
jq -S '.Contents|sort_by(.Key)|map({Key,Size,ETag,LastModified})' "$out/remote-roster.json" > "$out/remote-roster.before.identity"
jq -S '.Contents|sort_by(.Key)|map({Key,Size,ETag,LastModified})' "$out/remote-roster.after.json" > "$out/remote-roster.after.identity"
cmp "$out/remote-roster.before.identity" "$out/remote-roster.after.identity"
printf '%s\n' 'REMOTE_EXACT_ROSTER_AND_PINNED_BODY_READBACK_COMPLETE_INVARIANTS_PENDING' > "$out/transport.status"
