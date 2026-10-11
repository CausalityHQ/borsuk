#!/bin/bash
set -euo pipefail
out=/data/target/borsuk-source-neighborhood-data-gates.pending
stage=${1:?stage}
case "$stage" in admission|smoke|batch|failed-admission|failed-smoke) ;; *) exit 125;; esac
# Administrative collection is allowed only after the original instance is terminated.
instance=$(cat "$out/instance.id")
jq -e --arg instance "$instance" '[.Reservations[].Instances[]]|length==1 and .[0].InstanceId==$instance and .[0].State.Name=="terminated"' "$out/instance.terminated.json" > /dev/null
prefix="research/semantic-router/20261011/source-neighborhood-data-gates-a0001/$stage/"
dest="$out/$stage-collected"
mkdir "$dest" "$out/$stage-get-receipts"
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
deadline=$(( $(date +%s)+240 ))
timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 s3api list-objects-v2 --bucket borsuk-bench-453182569524-euc1 --prefix "$prefix" --output json > "$out/$stage-objects.json"
jq -e '(.IsTruncated==false) and (.Contents|length>0 and length<=64) and ([.Contents[].Size]|add<=16777216)' "$out/$stage-objects.json" > /dev/null
jq -r '.Contents[]|[.Key,(.Size|tostring)]|@tsv' "$out/$stage-objects.json" > "$out/$stage-roster.tsv"
while IFS=$'\t' read -r key bytes; do
 name=${key##*/}
 [[ $key == "$prefix$name" && $name =~ ^[a-zA-Z0-9._-]+$ && $name != . && $name != .. && $bytes =~ ^[0-9]+$ ]]
 (( bytes <= 8388608 ))
 left=$(( deadline-$(date +%s)-2 )); (( left > 0 )); (( left <= 25 )) || left=25
 timeout -k 2 "$left" /usr/local/bin/aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --checksum-mode ENABLED "$dest/$name" > "$out/$stage-get-receipts/$name.json"
 test "$(stat -c %s "$dest/$name")" = "$bytes"
 sha=$(sha256sum "$dest/$name"); sha=${sha%% *}
 checksum=$(printf '%s' "$sha" | xxd -r -p | base64 -w0)
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$out/$stage-get-receipts/$name.json" > /dev/null
done < "$out/$stage-roster.tsv"
if [[ $stage == admission || $stage == smoke || $stage == batch ]]; then
 test -s "$dest/SHA256SUMS"
 : > "$out/$stage-manifest-names.txt"
 while read -r sha name extra; do
  [[ $sha =~ ^[0-9a-f]{64}$ && $name =~ ^\./[a-zA-Z0-9._-]+$ && -z $extra ]]
  name=${name#./}
  [[ $name != . && $name != .. && $name != SHA256SUMS ]]
  printf '%s\n' "$name" >> "$out/$stage-manifest-names.txt"
 done < "$dest/SHA256SUMS"
 LC_ALL=C sort "$out/$stage-manifest-names.txt" > "$out/$stage-manifest-names.sorted"
 awk -F '\t' '{n=split($1,a,"/"); if(a[n]!="SHA256SUMS") print a[n]}' "$out/$stage-roster.tsv" | LC_ALL=C sort > "$out/$stage-object-names.sorted"
 cmp "$out/$stage-manifest-names.sorted" "$out/$stage-object-names.sorted"
 (cd "$dest" && sha256sum -c SHA256SUMS) > "$out/$stage-collected.verified.log"
fi
# Geometry reports, anchor seals and native stdout are intentionally never interpreted here.
printf 'OPAQUE_COLLECTION_COMPLETE stage=%s\n' "$stage"
