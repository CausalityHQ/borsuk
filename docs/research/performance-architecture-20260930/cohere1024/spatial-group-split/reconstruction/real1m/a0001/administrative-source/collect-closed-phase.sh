#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
stage=${1:?stage}
case $stage in admission|smoke|parity) ;; *) exit 125;; esac
instance=$(cat "$out/instance.id")
command_id=$(cat "$out/$stage.ssm-command.id")
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
timeout -k 2 25 aws --profile causality --region eu-central-1 ssm get-command-invocation --command-id "$command_id" --instance-id "$instance" > "$out/$stage.ssm-original-terminal.json"
jq -e --arg command "$command_id" --arg instance "$instance" '.CommandId==$command and .InstanceId==$instance and .Status=="Success" and .ResponseCode==0' "$out/$stage.ssm-original-terminal.json" > /dev/null
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
config_sha=$(cat "$out/package/$stage.config.sha256")
approval_sha=$(cat "$out/package/approval.sha256")
receipt_sha=$(sha256sum "$dest/native-receipt.json"); receipt_sha=${receipt_sha%% *}
jq -e --arg phase "$stage" --arg config "$config_sha" --arg approval "$approval_sha" --slurpfile approved "$out/package/approval.json" '
 .schema=="borsuk-partition-reconstruction-receipt-v1" and .complete==true
 and .config.sha256==$config and .approval.sha256==$approval and .original==$approved[0]
 and .requests_opened==false and .truth_opened==false
 and .centroids_bytes==64000032 and .centroids_sha256=="720b98899031e1aeeeabe0fb2aa0d938485b43141c2438c251b3383a6f0315d8"
 and .binary.sha256=="739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52" and .binary.bytes==8387384
 and .implementation.example_sha256=="d71ba7717c57d498f3c68c7976a0d2c824ea26472c644c97cbfe676f6a0652cb"
 and .implementation.module_sha256=="715b8a1d028403a361f83bea0e70b1fe4a4f5aabcceae06512faa900d23547c5"
 and .requires_successful_file_sync==true and .requires_successful_directory_sync==true
 and (if $phase=="parity" then .status=="HISTORICAL_RADIAL_PARITY" and .historical_root_opened==true and .historical_membership_opened==true else .status=="RECONSTRUCTED" and .historical_root_opened==false and .historical_membership_opened==false end)
' "$dest/native-receipt.json" > "$out/$stage-root-native-receipt-check.log"
jq -e --arg phase "$stage" --arg instance "$instance" --arg config "$config_sha" --arg approval "$approval_sha" --arg receipt "$receipt_sha" '.schema=="borsuk-reconstruction-real1m-stage-v1" and .stage==$phase and .status=="GATE_ACCEPTED" and .execution.instance_id==$instance and .config_sha256==$config and .approval_sha256==$approval and .native_receipt_sha256==$receipt and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .scientific_measurement==false and .cold_s3_claim==false and .vendor_win==false' "$dest/terminal.json" > /dev/null
terminal_sha=$(sha256sum "$dest/terminal.json"); terminal_sha=${terminal_sha%% *}
jq -n --arg stage "$stage" --arg instance "$instance" --arg command "$command_id" --arg terminal "$terminal_sha" --arg receipt "$receipt_sha" '{schema:"borsuk-reconstruction-root-phase-acceptance-v1",stage:$stage,instance_id:$instance,original_ssm_command_id:$command,terminal_sha256:$terminal,native_receipt_sha256:$receipt,all_closed_body_hashes_verified:true,original_ssm_success:true,performance_claim:false}' > "$out/$stage.root-accepted.json"
printf 'CLOSED_PHASE_COLLECTED_AND_AUTHENTICATED %s\n' "$stage"
