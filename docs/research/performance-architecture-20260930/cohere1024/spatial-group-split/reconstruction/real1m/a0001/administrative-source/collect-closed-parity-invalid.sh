#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
stage=${1:?stage}
case $stage in admission|smoke|parity) ;; *) exit 125;; esac
instance=$(cat "$out/instance.id")
command_id=$(cat "$out/$stage.ssm-command.id")
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
timeout -k 2 25 aws --profile causality --region eu-central-1 ssm get-command-invocation --command-id "$command_id" --instance-id "$instance" > "$out/$stage.ssm-original-terminal.json"
jq -e --arg command "$command_id" --arg instance "$instance" '.CommandId==$command and .InstanceId==$instance and .Status=="Failed" and .ResponseCode==1' "$out/$stage.ssm-original-terminal.json" > /dev/null
prefix=research/semantic-router/20261011/reconstruction-real1m-a0001/$stage/
dest=$out/$stage-collected
mkdir "$dest" "$out/$stage-get-receipts"
deadline=$(( $(date +%s)+240 ))
timeout -k 2 25 aws --profile causality --region eu-central-1 s3api list-objects-v2 --bucket borsuk-bench-453182569524-euc1 --prefix "$prefix" --no-paginate --output json > "$out/$stage-objects.json"
jq -e '.IsTruncated==false and (.Contents|length>0 and length<=64) and ([.Contents[].Size]|add<=16777216)' "$out/$stage-objects.json" > /dev/null
jq -r '.Contents[]|[.Key,(.Size|tostring)]|@tsv' "$out/$stage-objects.json" > "$out/$stage-roster.tsv"
while IFS=$'\t' read -r key bytes; do
 name=${key##*/}
 [[ $key == "$prefix$name" && $name =~ ^[a-zA-Z0-9._-]+$ && $name != . && $name != .. && $bytes =~ ^[0-9]+$ ]]
 (( bytes <= 2097152 ))
 left=$(( deadline-$(date +%s)-2 )); (( left > 0 )); (( left <= 25 )) || left=25
 timeout -k 2 "$left" aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" --checksum-mode ENABLED "$dest/$name" > "$out/$stage-get-receipts/$name.json"
 test "$(stat -c %s "$dest/$name")" = "$bytes"
 sha=$(sha256sum "$dest/$name"); sha=${sha%% *}
 checksum=$(printf '%s' "$sha" | xxd -r -p | base64 -w0)
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$out/$stage-get-receipts/$name.json" > /dev/null
done < "$out/$stage-roster.tsv"
test -s "$dest/SHA256SUMS"
: > "$out/$stage-manifest-names.txt"
while read -r sha name extra; do
 [[ $sha =~ ^[0-9a-f]{64}$ && $name =~ ^\./[a-zA-Z0-9._-]+$ && -z $extra ]]
 name=${name#./}
 [[ $name != . && $name != .. && $name != SHA256SUMS ]]
 printf '%s\n' "$name" >> "$out/$stage-manifest-names.txt"
done < "$dest/SHA256SUMS"
LC_ALL=C sort "$out/$stage-manifest-names.txt" > "$out/$stage-manifest-names.sorted"
LC_ALL=C sort -u "$out/$stage-manifest-names.txt" > "$out/$stage-manifest-names.unique"
cmp "$out/$stage-manifest-names.sorted" "$out/$stage-manifest-names.unique"
awk -F '\t' '{n=split($1,a,"/"); if(a[n]!="SHA256SUMS") print a[n]}' "$out/$stage-roster.tsv" | LC_ALL=C sort > "$out/$stage-object-names.sorted"
cmp "$out/$stage-manifest-names.sorted" "$out/$stage-object-names.sorted"
(cd "$dest" && sha256sum -c SHA256SUMS) > "$out/$stage-collected.verified.log"
