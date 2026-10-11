#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-boundary-data
stage=$1
[[ "$stage" == admission || "$stage" == smoke ]]
unit="borsuk-boundary-data-$stage"
ev="$root/evidence/$stage"
if [[ "$stage" == smoke ]]; then jq -e '.manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true' "$root/evidence/admission/terminal.json"; fi
mkdir "$ev"
set +e
systemd-run --unit="$unit" --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p RuntimeMaxSec=930 /bin/bash /tmp/leaf-data-inner.sh "$stage" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
timeout -k 2 10 systemctl show "$unit.service" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr" || true
drained=false
if test -s "$ev/cgroup-path.txt"; then
 cg=$(cat "$ev/cgroup-path.txt")
 if [[ "$cg" == "/system.slice/$unit.service" ]]; then
  if test ! -d "/sys/fs/cgroup$cg"; then drained=true
  elif grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true
  fi
 fi
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ "$native" =~ ^[0-9]+$ ]] || native=null; fi
jq -n --arg stage "$stage" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:"borsuk-boundary-data-stage-v1",stage:$stage,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,scientific_measurement:false,cold_s3_claim:false,vendor_win:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
sync -f "$ev/terminal.json"
sync -f "$ev"
deadline=$(( $(date +%s) + 90 ))
for file in "$ev"/*; do
 left=$(( deadline - $(date +%s) - 2 )); (( left > 0 )) || exit 124
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261011/sampled-boundary-data-gates-a0001/$stage/$(basename "$file")" --body "$file" --if-none-match '*' > /dev/null
done
cat "$ev/terminal.json"
[[ "$manager" == 0 && "$native" == 0 && "$drained" == true ]]
