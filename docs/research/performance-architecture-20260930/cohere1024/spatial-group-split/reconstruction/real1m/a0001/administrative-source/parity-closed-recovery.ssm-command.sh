#!/bin/bash
set -euo pipefail
p=/opt/borsuk-reconstruction-gate/verify-retain-closed-parity.sh
test ! -e "$p"
cat > "$p" <<'BORSUK_CLOSED_PARITY_END'
#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
root=/mnt/borsuk-reconstruction-real1m
phase=parity
status=HISTORICAL_RADIAL_PARITY
ev=$root/evidence/parity
receipt=$ev/published/receipt.json
config=$root/parity.config.json
approval=$root/approval.json
config_sha=$(cat "$support/parity.config.sha256")
approval_sha=$(cat "$support/approval.sha256")
recovery=$support/parity-closed-recovery
mkdir "$recovery"
(cd "$support" && sha256sum -c support.sha256) > "$recovery/package-check.log"
test "$(cat "$ev/native-exit.txt")" = 0
test "$(cat "$ev/manager-exit.txt")" = 1
test "$(cat "$ev/memory.swap.peak.after")" = 0
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/memory.events.after"
printf '%s  %s\n' a85b49fa008dc3feb699abb96d498dafb872de870cd804bc77aff38785a33771 "$receipt" "$config_sha" "$config" "$approval_sha" "$approval" | sha256sum -c - > "$recovery/authority-check.log"
jq -e --arg status "$status" --arg config "$config_sha" --arg approval "$approval_sha" --arg phase "$phase" --slurpfile cfg "$config" --slurpfile approved "$approval" '
 .schema=="borsuk-partition-reconstruction-receipt-v1"
 and .complete==true and .status==$status and .requests_opened==false and .truth_opened==false
 and .config.sha256==$config and .approval.sha256==$approval and .original==$approved[0]
 and .generation==$cfg[0].generation_manifest and .plane==$cfg[0].plane_manifest and .leaves==$cfg[0].leaves
 and .binary.bytes==8387384 and .binary.sha256=="739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52"
 and .implementation.example_sha256=="d71ba7717c57d498f3c68c7976a0d2c824ea26472c644c97cbfe676f6a0652cb"
 and .implementation.module_sha256=="715b8a1d028403a361f83bea0e70b1fe4a4f5aabcceae06512faa900d23547c5"
 and .implementation.router_sha256=="60538073b26d5893d1d75e689ebe4b223e575875b5a4d45e5be02f56cd2a3e2f"
 and .centroids_bytes==64000032 and .centroids_sha256=="720b98899031e1aeeeabe0fb2aa0d938485b43141c2438c251b3383a6f0315d8"
 and .centroids.bytes==64000032 and .centroids.sha256==.centroids_sha256
 and .unit_count==31250 and .final_unit_rows==32 and .max_payload_bytes==536870912
 and .requires_successful_file_sync==true and .requires_successful_directory_sync==true
 and .requires_original_successful_terminal_receipt==true
 and (if $phase=="parity" then
 .historical_root_opened==true and .historical_membership_opened==true
 and .historical_parity.old_root==($cfg[0].historical.root|{bytes,sha256})
 and .historical_parity.old_membership==($cfg[0].historical.membership|{bytes,sha256})
 and .historical_parity.old_leaves==($cfg[0].leaves|{bytes,sha256})
 and .radial_outputs.membership.sha256==$cfg[0].historical.membership.sha256
 and .radial_outputs.leaves.sha256==$cfg[0].leaves.sha256
 else .historical_root_opened==false and .historical_membership_opened==false and .historical_parity==null and .radial_outputs==null end)
 ' "$receipt" > "$recovery/validator.log"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
prefix=research/semantic-router/20261011/reconstruction-real1m-a0001/retained-derived
upload() {
 local file=$1 name=$2 hash checksum
 hash=$(sha256sum "$file"); hash=${hash%% *}
 checksum=$(printf '%s' "$hash" | xxd -r -p | base64 -w0)
 timeout -k 2 30 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$name" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 --checksum-sha256 "$checksum" > "$recovery/$name.put.json"
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$recovery/$name.put.json" > /dev/null
}
jq -r '([.centroids,.radial_outputs.root,.radial_outputs.membership,.radial_outputs.leaves])[]|[.path,(.bytes|tostring),.sha256]|@tsv' "$receipt" > "$recovery/output-pins.tsv"
while IFS=$'\t' read -r path bytes sha; do
 [[ $path == "$ev/published/"* && $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[0-9]+$ ]]
 test -f "$path" && test ! -L "$path"
 test "$(realpath "$path")" = "$path"
 test "$(stat -c %s "$path")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path" | sha256sum -c - >> "$recovery/output-authentication.log"
 upload "$path" "${path##*/}"
done < "$recovery/output-pins.tsv"
cp "$receipt" "$recovery/native-receipt.json"
upload "$recovery/native-receipt.json" native-receipt.json
jq -n '{schema:"borsuk-reconstruction-closed-parity-recovery-v1",status:"CLOSED_NATIVE_PARITY_VALIDATED",original_native_exit:0,original_manager_exit:1,original_stage_disposition:"INVALID_RECEIPT_VALIDATOR",original_disposition_preserved:true,native_reexecuted:false,original_receipt_sha256:"a85b49fa008dc3feb699abb96d498dafb872de870cd804bc77aff38785a33771",descriptor_validation:"exact bytes and SHA; descriptor deliberately has no local path",derived_outputs_full_body_hash_verified:true,derived_outputs_retained_create_only:true,performance_claim:false}' > "$recovery/terminal.json"
(cd "$recovery" && find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$recovery/SHA256SUMS"
sync -f "$recovery/terminal.json"
sync -f "$recovery/SHA256SUMS"
sync -f "$recovery"
for f in "$recovery"/*; do
 [[ ${f##*/} == terminal.json ]] && continue
 [[ ${f##*/} == *.put.json ]] && continue
 upload "$f" "recovery-${f##*/}"
done
upload "$recovery/terminal.json" recovery-terminal.json
cat "$recovery/terminal.json"
BORSUK_CLOSED_PARITY_END
systemd-run --unit=borsuk-reconstruction-closed-recovery --slice=borsukreconstruction.slice --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=6G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=180 -p TimeoutStopSec=15 /bin/bash "$p"
