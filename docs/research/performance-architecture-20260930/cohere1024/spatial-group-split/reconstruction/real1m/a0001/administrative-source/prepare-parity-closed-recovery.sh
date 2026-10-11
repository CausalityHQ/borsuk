#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
test ! -e "$out/verify-retain-closed-parity.sh"
{
 cat <<'HEADER'
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
HEADER
 awk '/^jq -e --arg status/{p=1} p{print} p && /receipt-validator.log/{exit}' "$out/native-reconstruction-inner.sh" \
 | sed -e 's/\.historical_parity.old_root==\$cfg\[0\].historical.root/.historical_parity.old_root==($cfg[0].historical.root|{bytes,sha256})/' \
 -e 's/\.historical_parity.old_membership==\$cfg\[0\].historical.membership/.historical_parity.old_membership==($cfg[0].historical.membership|{bytes,sha256})/' \
 -e 's/\.historical_parity.old_leaves==\$cfg\[0\].leaves/.historical_parity.old_leaves==($cfg[0].leaves|{bytes,sha256})/' \
 -e 's@"$ev/receipt-validator.log"@"$recovery/validator.log"@'
 cat <<'FOOTER'
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
FOOTER
} > "$out/verify-retain-closed-parity.sh"
bash -n "$out/verify-retain-closed-parity.sh"
shellcheck -S error "$out/verify-retain-closed-parity.sh"
{
 printf '%s\n' '#!/bin/bash' 'set -euo pipefail' 'p=/opt/borsuk-reconstruction-gate/verify-retain-closed-parity.sh' 'test ! -e "$p"' 'cat > "$p" <<\x27BORSUK_CLOSED_PARITY_END\x27'
 cat "$out/verify-retain-closed-parity.sh"
 printf '%s\n' BORSUK_CLOSED_PARITY_END
 printf '%s\n' 'systemd-run --unit=borsuk-reconstruction-closed-recovery --slice=borsukreconstruction.slice --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=6G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=180 -p TimeoutStopSec=15 /bin/bash "$p"'
} > "$out/parity-closed-recovery.ssm-command.sh"
bash -n "$out/parity-closed-recovery.ssm-command.sh"
instance=$(cat "$out/instance.id")
jq -n --arg instance "$instance" --rawfile command "$out/parity-closed-recovery.ssm-command.sh" '{InstanceIds:[$instance],DocumentName:"AWS-RunShellScript",Comment:"borsuk validate sealed original native parity and retain opaque derived outputs; NO native rerun",TimeoutSeconds:210,Parameters:{executionTimeout:["210"],commands:[$command]}}' > "$out/parity-closed-recovery.ssm-request.json"
sha256sum "$out/verify-retain-closed-parity.sh" "$out/parity-closed-recovery.ssm-request.json" > "$out/parity-closed-recovery.sha256"
