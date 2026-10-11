#!/bin/bash
set -euo pipefail
support=/opt/borsuk-neighborhood-gate
root=/mnt/borsuk-neighborhood-data
stage=${1:?stage}
[[ $stage == admission || $stage == smoke ]]
unit="borsuk-neighborhood-$stage"
ev="$root/evidence/$stage"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
if [[ $stage == smoke ]]; then
 jq -e '.stage=="admission" and .status=="GATE_ACCEPTED" and .manager_exit==0 and .native_exit==0 and .owned_cgroup_drained==true' "$support/admission.archived-terminal.json" > /dev/null
 test ! -e "$root/evidence/admission"
 test ! -e "$root/admission.config.json"
fi
mkdir "$ev"
cp "$support/execution-identity.json" "$ev/execution-identity.json"
jq -e '.schema=="borsuk-source-neighborhood-execution-v1" and .attempt=="source-neighborhood-data-gates-a0001" and (.instance_id|test("^i-[0-9a-f]{17}$")) and .source_commit=="fc60c18e780063804c625a72f722d3e697fb4b61"' "$ev/execution-identity.json" > /dev/null
cp "$support/$stage.config.json" "$root/$stage.config.json"
chmod 0400 "$root/$stage.config.json"
left=$(( $(cat /run/borsuk-neighborhood-machine-stop.epoch)-$(date +%s)-120 ))
(( left >= 1260 )) || exit 124
set +e
systemd-run --unit="$unit" --slice=borsukneighborhood.slice --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p IOAccounting=yes -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec=1230 /bin/bash "$support/native-data-inner.sh" "$stage" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
set +e
timeout -k 2 10 systemctl show "$unit.service" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf '%s\n' "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
if test -s "$ev/cgroup-path.txt"; then
 cg=$(cat "$ev/cgroup-path.txt")
 if [[ $cg == "/borsukneighborhood.slice/$unit.service" ]]; then
  if test ! -d "/sys/fs/cgroup$cg"; then drained=true
  elif grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi
 fi
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
slice_ok=true
for f in memory.peak memory.events memory.stat memory.swap.peak cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
 cat "/sys/fs/cgroup/borsukneighborhood.slice/$f" > "$ev/owning-slice.$f" || slice_ok=false
done
test "$(cat "$ev/owning-slice.memory.swap.peak")" = 0 || slice_ok=false
peak=$(cat "$ev/owning-slice.memory.peak") || slice_ok=false
if [[ $peak =~ ^[0-9]{1,10}$ ]]; then
 (( 10#$peak <= 1073741824 )) || slice_ok=false
else slice_ok=false; fi
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/owning-slice.memory.events" || slice_ok=false
status=INVALID
if [[ $manager == 0 && $native == 0 && $drained == true && $slice_ok == true ]]; then status=GATE_ACCEPTED; fi
config_sha=$(cat "$support/$stage.config.sha256")
seal_sha=null; report_sha=null
if test -f "$ev/anchors.json"; then seal_sha=$(sha256sum "$ev/anchors.json"); seal_sha=${seal_sha%% *}; fi
if test -f "$ev/report.json"; then report_sha=$(sha256sum "$ev/report.json"); report_sha=${report_sha%% *}; fi
jq -n --slurpfile identity "$ev/execution-identity.json" --arg stage "$stage" --arg status "$status" --arg config "$config_sha" --arg seal "$seal_sha" --arg report "$report_sha" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" --argjson slice_ok "$slice_ok" '{schema:"borsuk-source-neighborhood-data-stage-v1",execution:$identity[0],stage:$stage,status:$status,source_commit:"fc60c18e780063804c625a72f722d3e697fb4b61",source_sha256:"239c80dbe7d0a978a88791d98b9cf0827b10d91105367103d312c89987eca29f",elf_sha256:"77a8e582e426f1d8500f26b66741ddb978ce7f0d1ef5250b5111c7a3e1da1010",config_sha256:$config,anchor_seal_sha256:$seal,report_sha256:$report,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,owning_slice_resources_ok:$slice_ok,scientific_measurement:false,cold_s3_claim:false,vendor_win:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
metadata_bytes=0
for file in "$ev"/*; do
 case ${file##*/} in anchors.json|report.json) ;; *) metadata_bytes=$(( metadata_bytes+$(stat -c %s "$file") ));; esac
done
(( metadata_bytes <= 2097152 )) || exit 125
sync -f "$ev/terminal.json"
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
deadline=$(( $(date +%s)+90 ))
prefix=$(cat "$support/campaign-prefix.txt")
[[ $prefix == research/semantic-router/20261011/source-neighborhood-data-gates-a0001 ]]
# Terminal is uploaded last; every PutObject has an explicit content checksum.
upload() {
 local file=$1 left machine_left checksum
 left=$(( deadline-$(date +%s)-2 ))
 machine_left=$(( $(cat /run/borsuk-neighborhood-machine-stop.epoch)-$(date +%s)-30 ))
 (( left <= machine_left )) || left=$machine_left
 (( left > 0 )) || exit 124
 checksum=$(sha256sum "$file"); checksum=${checksum%% *}
 checksum=$(printf '%s' "$checksum" | xxd -r -p | base64 -w0)
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$stage/${file##*/}" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 --checksum-sha256 "$checksum" > "$support/$stage.${file##*/}.put.json"
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$support/$stage.${file##*/}.put.json" > /dev/null
}
for file in "$ev"/*; do
 [[ ${file##*/} != terminal.json ]] || continue
 upload "$file"
done
upload "$ev/terminal.json"
cp "$ev/terminal.json" "$support/$stage.archived-terminal.json"
cat "$ev/terminal.json"
[[ $status == GATE_ACCEPTED ]]
# Exact owned outputs can be removed only after the complete checksummed archive.
for file in "$ev"/*; do test -f "$file" && test ! -L "$file"; rm -- "$file"; done
rmdir "$ev"
rm -- "$root/$stage.config.json"
