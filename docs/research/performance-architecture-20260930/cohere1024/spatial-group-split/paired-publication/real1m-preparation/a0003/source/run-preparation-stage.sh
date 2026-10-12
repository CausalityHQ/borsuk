#!/usr/bin/env bash
set -euo pipefail
phase=${1:?canary or preparation}
case $phase in canary) seconds=60;; preparation) seconds=1200;; *) exit 125;; esac
support=/opt/borsuk-paired-preparation
root=/mnt/borsuk-paired-source-screen
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
if [[ $phase == preparation ]]; then
 jq -e '.status=="GATE_ACCEPTED" and .native_exit==2 and .manager_exit==0 and .owned_cgroup_drained==true' "$support/canary.archived-terminal.json" > /dev/null
fi
ev=$root/evidence/$phase
mkdir "$ev"
unit=borsuk-paired-$phase
set +e
systemd-run --unit="$unit" --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=4G -p MemorySwapMax=0 -p TasksMax=256 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$((seconds+30))" /bin/bash "$support/native-preparation-inner.sh" "$phase" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
timeout -k 2 10 systemctl show "$unit.service" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr" || true
drained=false
cg=/sys/fs/cgroup/system.slice/$unit.service
if test ! -d "$cg"; then drained=true; elif grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; fi
if [[ $drained != true ]]; then
 test ! -f "$cg/cgroup.kill" || printf '1\n' > "$cg/cgroup.kill"
 timeout -k 2 10 systemctl stop "$unit.service" > "$ev/cleanup.log" 2>&1 || true
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
status=INVALID
if [[ $manager == 0 && $drained == true ]]; then
 if [[ $phase == canary && $native == 2 ]] && test -s "$ev/canary-verdict.txt"; then status=GATE_ACCEPTED; fi
 if [[ $phase == preparation && $native == 0 ]] && test -s "$ev/native-receipt.json"; then status=GATE_ACCEPTED; fi
fi
jq -n --arg stage "$phase" --arg status "$status" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:"borsuk-paired-real-preparation-stage-v1",stage:$stage,status:$status,source_commit:"043ee8826e485b0e1a1f1f46e11448a742a001de",elf_sha256:"fd6d36f6bfbb4e370a94707941cec396af59492f832f6d1090c24eacbc1ec748",config_sha256:"3779326cba9d54a2da8bf98dd4e284011f87d5968448b87918a23434894c5908",authority_sha256:"8c672fbd059a71b0ba692cbbdbd4a2a4e9a9f01f4a43634ff5d1509d25fd5bcd",manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,requests_staged:false,truth_staged:false,scientific_measurement:false,cold_s3_claim:false,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
sync -f "$ev/terminal.json"
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
prefix=research/semantic-router/20261012/paired-real-preparation-a0003
deadline=$(( $(date +%s)+180 ))
upload() {
 local file=$1 key=$2 left
 left=$((deadline-$(date +%s)-2)); (( left > 0 )) || return 124
 (( left <= 90 )) || left=90
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$key" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 > "$support/$phase.${file##*/}.put.json"
}
for file in "$ev"/*; do [[ ${file##*/} == terminal.json ]] && continue; upload "$file" "$phase/${file##*/}"; done
if [[ $phase == preparation && $native == 0 ]] && test -s "$ev/native-receipt.json" && jq -e '.status=="SPATIAL_PREPARED" and .complete==true' "$ev/native-receipt.json" > /dev/null; then
 for name in spatial-root.bin spatial-membership.bin spatial-leaves.bin receipt.json; do upload "$root/prepared-spatial/$name" "retained-derived/$name"; done
fi
upload "$ev/terminal.json" "$phase/terminal.json"
cp "$ev/terminal.json" "$support/$phase.archived-terminal.json"
cat "$ev/terminal.json"
[[ $status == GATE_ACCEPTED ]]
