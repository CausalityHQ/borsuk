#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
phase=${1:?canary or screen}
case $phase in canary|screen) ;; *) exit 125;; esac
root=/mnt/borsuk-paired-source-screen
support=/opt/borsuk-paired-admission
cd "$support"
sha256sum -c source-support.sha256 > /dev/null
if [[ $phase == screen ]]; then
 jq -e '.phase=="canary" and .execution_closed==true and .native_exit==2 and .manager_exit==0' source-canary.terminal.json > /dev/null
fi
ev=$root/evidence/source-$phase
mkdir "$ev"
unit=borsuk-paired-source-$phase
set +e
systemd-run --unit="$unit" --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=4G -p MemorySwapMax=0 -p TasksMax=256 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec=1260 /bin/bash "$support/source-inner.sh" "$phase" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
cg=/sys/fs/cgroup/system.slice/$unit.service
drained=false
if [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; fi
if [[ $drained == false ]]; then
 [[ ! -f $cg/cgroup.kill ]] || printf '1\n' > "$cg/cgroup.kill"
 timeout -k 2 10 systemctl stop "$unit.service" > "$ev/cleanup.log" 2>&1 || true
 if [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; fi
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
closed=false
if [[ $manager == 0 && $drained == true && ( $native == 0 || $native == 2 ) ]]; then closed=true; fi
jq -n --arg phase "$phase" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" --argjson closed "$closed" '{schema:"borsuk-paired-source-screen-stage-v1",phase:$phase,execution_closed:$closed,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,consumer_source:"043ee8826e485b0e1a1f1f46e11448a742a001de",consumer_elf_sha256:"fd6d36f6bfbb4e370a94707941cec396af59492f832f6d1090c24eacbc1ec748",configuration_sha256:"78c29dc3388f991c5db17fef7883ca9ee503ed2b4da9a1632b6cc19200dfb1a8",authority_sha256:"eae672ca128acd9e67d6c92c93422b264579754ebd00934bf3d911858e6f5737",scope:"consumed32 SOURCE planner coverage only; native result remains authoritative",cold_s3_claim:false,performance_claim:false,candidate_returned_quality_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
sync -f "$ev/terminal.json"
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
deadline=$(( $(date +%s)+180 ))
for file in "$ev"/*; do
 # Terminal is uploaded once, after every other retained body.
 [[ ${file##*/} != terminal.json ]] || continue
 left=$((deadline-$(date +%s)-2)); (( left > 0 ))
 (( left <= 90 )) || left=90
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261012/paired-source-screen-a0001/$phase/${file##*/}" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 > "$support/$phase.${file##*/}.put.json"
done
left=$((deadline-$(date +%s)-2)); (( left > 0 ))
(( left <= 90 )) || left=90
timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261012/paired-source-screen-a0001/$phase/terminal.json" --body "$ev/terminal.json" --if-none-match '*' --checksum-algorithm SHA256 > "$support/$phase.terminal.put.json"
cp "$ev/terminal.json" "$support/source-$phase.terminal.json"
cat "$ev/terminal.json"
[[ $closed == true ]]
