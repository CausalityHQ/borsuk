#!/usr/bin/env bash
# Metadata only; EC2 lane, finite exact read-only source roster.
set -euo pipefail
[[ $# == 3 ]] || exit 125
root=$1; keys=$2; out=$3
[[ $root == /mnt/borsuk-retained-returned-publication && $keys == "$root/"* && $out == "$root/evidence/"* && ! -e $out && ! -L $out ]] || exit 125
[[ -f $keys && ! -L $keys && $(stat -c %s "$keys") -le 65536 ]] || exit 125
jq -e 'type=="array" and length==13 and ([.[].key]|unique|length)==13 and all(.[];keys==["bytes","key"] and (.key|type=="string" and startswith("research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/") and (contains("\n")|not) and (contains("\t")|not)) and (.bytes|type=="number" and floor==. and .>0))' "$keys" > /dev/null
mkdir -m 700 "$out"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
deadline=$(( $(date +%s)+110 ))
jq -er '.[]|[.key,.bytes]|@tsv' "$keys" > "$out/roster.tsv"
index=0
: > "$out/identities.jsonl"
while IFS=$'\t' read -r key bytes; do
 index=$((index+1)); left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=25)) || left=25
 /usr/bin/timeout -k 2 "$left" aws --region eu-central-1 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key" > "$out/$index.head.json"
 jq -e --argjson bytes "$bytes" '.ContentLength==$bytes and (.ETag|type=="string" and length>0 and length<=4096)' "$out/$index.head.json" > "$out/$index.check.txt"
 jq -c --arg key "$key" '{key:$key,bytes:.ContentLength,etag:.ETag,version:.VersionId,last_modified:.LastModified}' "$out/$index.head.json" >> "$out/identities.jsonl"
done < "$out/roster.tsv"
[[ $index == 13 ]]
jq -sS 'sort_by(.key)' "$out/identities.jsonl" > "$out/identities.json"
printf '%s\n' 'EXACT_ORIGINAL_SOURCE_METADATA_FENCE_CAPTURED_NOT_BODY_AUTHENTICATION' > "$out/status.txt"
