#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
root=/mnt/borsuk-reconstruction-real1m
phase=${1:?phase}
case $phase in admission|smoke) seconds=600;; parity) seconds=4800;; *) exit 125;; esac
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
if [[ $phase == smoke || $phase == parity ]]; then
 jq -e '.stage=="admission" and .status=="GATE_ACCEPTED" and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$support/admission.archived-terminal.json" > /dev/null
 test -f "$support/admission.root-accepted.json"
fi
if [[ $phase == parity ]]; then
 jq -e '.stage=="smoke" and .status=="GATE_ACCEPTED" and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$support/smoke.archived-terminal.json" > /dev/null
 test -f "$support/smoke.root-accepted.json"
fi
ev=$root/evidence/$phase
mkdir "$ev"
cp "$support/$phase.config.json" "$root/$phase.config.json"
chmod 0400 "$root/$phase.config.json"
if [[ $phase == admission ]]; then
 cp "$support/approval.json" "$root/approval.json"
 chmod 0400 "$root/approval.json"
fi
cp "$support/execution-identity.json" "$ev/execution-identity.json"
left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-150 ))
(( left >= seconds+60 )) || exit 124
unit=borsuk-reconstruction-$phase
set +e
systemd-run --unit="$unit" --slice=borsukreconstruction.slice --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=6G -p MemorySwapMax=0 -p IOAccounting=yes -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$((seconds+30))" /bin/bash "$support/native-reconstruction-inner.sh" "$phase" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
set +e
timeout -k 2 10 systemctl show "$unit.service" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf '%s\n' "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
cg=/borsukreconstruction.slice/$unit.service
if test ! -d "/sys/fs/cgroup$cg"; then drained=true
elif grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi
if [[ $drained != true ]]; then
 test ! -e "/sys/fs/cgroup$cg/cgroup.kill" || printf '1\n' > "/sys/fs/cgroup$cg/cgroup.kill"
 # A failed drain is never converted to success by cleanup.
 timeout -k 2 10 systemctl stop "$unit.service" > "$ev/failed-drain-stop.log" 2>&1 || true
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
status=INVALID
if [[ $manager == 0 && $native == 0 && $drained == true ]] && test -s "$ev/native-receipt.json"; then status=GATE_ACCEPTED; fi
config_sha=$(cat "$support/$phase.config.sha256")
approval_sha=$(cat "$support/approval.sha256")
receipt_sha=null
if test -f "$ev/native-receipt.json"; then h=$(sha256sum "$ev/native-receipt.json"); receipt_sha=${h%% *}; fi
jq -n --slurpfile execution "$ev/execution-identity.json" --arg stage "$phase" --arg status "$status" --arg config "$config_sha" --arg approval "$approval_sha" --arg receipt "$receipt_sha" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:"borsuk-reconstruction-real1m-stage-v1",execution:$execution[0],stage:$stage,status:$status,source_commit:"91107713f6f7bd700b5b6d92a83e438febac8069",elf_sha256:"739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52",config_sha256:$config,approval_sha256:$approval,native_receipt_sha256:$receipt,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,scientific_measurement:false,cold_s3_claim:false,vendor_win:false}' > "$ev/terminal.json"
(cd "$ev" && find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
sync -f "$ev/terminal.json"
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
prefix=$(cat "$support/campaign-prefix.txt")
[[ $prefix == research/semantic-router/20261011/reconstruction-real1m-a0001 ]]
deadline=$(( $(date +%s)+90 ))
upload() {
 local file=$1 left checksum machine_left
 left=$(( deadline-$(date +%s)-2 ))
 machine_left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-30 ))
 (( left <= machine_left )) || left=$machine_left
 (( left > 0 )) || return 1
 checksum=$(sha256sum "$file"); checksum=${checksum%% *}
 checksum=$(printf '%s' "$checksum" | xxd -r -p | base64 -w0)
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$phase/${file##*/}" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 --checksum-sha256 "$checksum" > "$support/$phase.${file##*/}.put.json"
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$support/$phase.${file##*/}.put.json" > /dev/null
}
for file in "$ev"/*; do
 test -f "$file" && test ! -L "$file" || continue
 [[ ${file##*/} != terminal.json ]] || continue
 test "$(stat -c %s "$file")" -le 2097152
 upload "$file"
done
upload "$ev/terminal.json"
cp "$ev/terminal.json" "$support/$phase.archived-terminal.json"
cat "$ev/terminal.json"
[[ $status == GATE_ACCEPTED ]]
