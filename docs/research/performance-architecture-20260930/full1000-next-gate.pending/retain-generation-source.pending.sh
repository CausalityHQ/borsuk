#!/usr/bin/env bash
# EC2-only source retention; no native/publication/performance acceptance.
# ROOT BUCKET FRESH_CAMPAIGN_PREFIX DEADLINE_EPOCH
set -Eeuo pipefail
umask 077
[[ $# == 4 && $1 == /mnt/borsuk-scale1m && $2 == borsuk-bench-453182569524-euc1 && $3 =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ && $4 =~ ^[0-9]+$ ]] || exit 94
root=$1 bucket=$2 prefix=$3 deadline=$4
store=$root/prepared-parent/store
receipt=$root/prepared-parent/publication-receipt.json
out=$root/evidence-root/retained-generation
[[ -d $store && ! -L $store && ! -e $out && ! -L $out ]] || exit 94
mkdir -m 0700 "$out"
run() {
 local cap=$1 now left; shift
 now=$(date +%s); left=$((deadline-now-5))
 (( left > 0 )) || return 125
 (( cap <= left )) || cap=$left
 timeout -k 5 "$cap" "$@"
}
small() { [[ -f $1 && ! -L $1 && $(stat -c %h "$1") == 1 && $(stat -c %s "$1") -gt 0 && $(stat -c %s "$1") -le 65536 ]]; }
small "$receipt"
root_sha=$(jq -er '.root_sha256 | select(test("^[0-9a-f]{64}$"))' "$receipt")
meta=semantic/index/generations/$root_sha
jq -e --arg meta "$meta" '.generation==1 and .prefix=="semantic/index" and .metadata_prefix==$meta and (.control_epoch|type=="number") and .control_epoch>0' "$receipt" > "$out/publication-bind.txt"
manifest=$store/$meta/manifest.json
small "$manifest"
digest=$(run 30 sha256sum "$manifest"); [[ ${digest%% *} == "$root_sha" ]]
plane=$store/$meta/plane/manifest.json
page=$store/$meta/page_manifest.json
for pair in "$plane:plane_manifest_sha256" "$page:page_manifest_sha256"; do
 IFS=: read -r file field <<< "$pair"
 small "$file"
 expected=$(jq -er --arg field "$field" '.[$field]|select(test("^[0-9a-f]{64}$"))' "$manifest")
 digest=$(run 30 sha256sum "$file"); [[ ${digest%% *} == "$expected" ]]
done
# Only authenticated small manifests are parsed. Payloads remain opaque streams.
jq -er --slurpfile p "$plane" --slurpfile g "$page" --arg meta "$meta" --arg root "$root_sha" '
 def row($name;$sha): [$meta+"/"+$name,$sha] | @tsv;
 row("manifest.json";$root),row("plane/manifest.json";.plane_manifest_sha256),
 row("page_manifest.json";.page_manifest_sha256),row("page_digests.bin";$g[0].page_digest_sha256),
 row("plane/mean.bin";$p[0].mean_sha256),row("plane/records.bin";$p[0].records_sha256),
 row("plane/page_digests.bin";$p[0].page_digest_sha256),row("router/root.bin";.discovery.root_sha256),
 row("router/membership.bin";.discovery.membership_sha256),row("router/leaves.bin";.discovery.leaves_sha256),
 ([.sq8_object_key,.sq8_object_sha256]|@tsv),([.canonical.object_key,.canonical.sha256]|@tsv)
 ' "$manifest" > "$out/objects.tsv"
[[ $(wc -l < "$out/objects.tsv") == 12 ]]
[[ $(cut -f1 "$out/objects.tsv" | LC_ALL=C sort -u | wc -l) == 12 ]]
put() {
 local key=$1 expected=$2 file bytes actual stamp remote
 [[ $key == "$meta/"* || $key =~ ^semantic/objects/[0-9a-f]{64}$ || $key == semantic/index/head.json ]]
 [[ $key != *'..'* && $expected =~ ^[0-9a-f]{64}$ ]]
 file=$store/$key
 [[ -f $file && ! -L $file && $(stat -c %h "$file") == 1 ]]
 bytes=$(stat -c %s "$file"); (( bytes > 0 && bytes <= 5368709120 ))
 stamp=$(stat -c '%d:%i:%s:%y:%z' "$file")
 actual=$(run 120 sha256sum "$file"); [[ ${actual%% *} == "$expected" ]]
 remote=$prefix/retained-source/$key
 n=$((n+1))
 run 540 aws --region eu-central-1 s3api put-object --bucket "$bucket" --key "$remote" --body "$file" --if-none-match '*' > "$out/put-$n.json"
 [[ $(stat -c '%d:%i:%s:%y:%z' "$file") == "$stamp" ]]
 run 30 aws --region eu-central-1 s3api head-object --bucket "$bucket" --key "$remote" > "$out/head-$n.json"
 jq -e --argjson bytes "$bytes" '.ContentLength==$bytes' "$out/head-$n.json" > "$out/head-$n.validated.txt"
 jq -cn --arg key "$key" --arg remote "$remote" --argjson bytes "$bytes" --arg sha "$expected" --slurpfile h "$out/head-$n.json" \
  '{logical_key:$key,s3_key:$remote,bytes:$bytes,sha256:$sha,etag:$h[0].ETag}' >> "$out/objects.jsonl"
}
n=0
while IFS=$'\t' read -r key expected; do put "$key" "$expected"; done < "$out/objects.tsv"
# Preserve the exact native head LAST; never synthesize or rewrite source authority.
head=$store/semantic/index/head.json
small "$head"
jq -e --arg sha "$root_sha" --slurpfile r "$receipt" '.root_sha256==$sha and .generation==1 and .epoch==$r[0].control_epoch and .mutation==null and .fence==null' "$head" > "$out/head-bind.txt"
digest=$(run 30 sha256sum "$head"); put semantic/index/head.json "${digest%% *}"
[[ $n == 13 ]]
run 20 sync -f "$out"
jq -n --arg root "$root_sha" --arg namespace "$prefix/retained-source" --slurpfile objects "$out/objects.jsonl" \
 '{schema:"borsuk-retained-generation-source-transport-v1",root_sha256:$root,namespace:$namespace,objects:$objects,head_last:true,native_retained_publication_required:true,performance_claim:false}' > "$out/complete.json"
run 20 sync -f "$out/complete.json"
run 20 sync -f "$out"
run 60 aws --region eu-central-1 s3api put-object --bucket "$bucket" --key "$prefix/retained-source-complete.json" --body "$out/complete.json" --if-none-match '*' > "$out/complete.put.json"
