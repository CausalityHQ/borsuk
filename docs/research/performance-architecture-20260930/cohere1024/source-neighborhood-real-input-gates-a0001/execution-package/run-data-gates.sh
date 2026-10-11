#!/bin/bash
set -euo pipefail
support=/opt/borsuk-neighborhood-gate
root=/mnt/borsuk-neighborhood-data
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
phase=package
original=1
completed=0
test -d "$support"
cd "$support"
sha256sum -c support.sha256 > support-check.log
test -f /run/borsuk-neighborhood-machine-stop.epoch
machine=$(cat /run/borsuk-neighborhood-machine-stop.epoch)
[[ $machine =~ ^[0-9]{10}$ ]]
systemctl is-active --quiet borsuk-neighborhood-data-shutdown.timer
mkdir batch
finish() {
 driver=$?
 original=$driver
 trap - EXIT
 set +e
 cleanup=0
 for unit in borsuk-neighborhood-install borsuk-neighborhood-hydrate borsuk-neighborhood-admission borsuk-neighborhood-smoke; do
  timeout -k 1 4 systemctl stop "$unit.service" > "batch/$unit.stop.log" 2>&1
  cg="/sys/fs/cgroup/borsukneighborhood.slice/$unit.service"
  [[ $unit != borsuk-neighborhood-install ]] || cg="/sys/fs/cgroup/system.slice/$unit.service"
  if test -d "$cg"; then
   test ! -e "$cg/cgroup.kill" || printf '1\n' > "$cg/cgroup.kill" || cleanup=1
   drain_deadline=$(( $(date +%s)+2 ))
   while test -d "$cg" && ! grep -qx 'populated 0' "$cg/cgroup.events" && (( $(date +%s) < drain_deadline )); do sleep 0.1; done
   if test -d "$cg" && ! grep -qx 'populated 0' "$cg/cgroup.events"; then cleanup=1; fi
  fi
 done
 if (( original == 0 && cleanup != 0 )); then original=125; fi
 printf '%s\n' "$driver" > batch/original-exit.txt || original=125
 printf '%s\n' "$cleanup" > batch/cleanup-exit.txt || original=125
 # JSON may be unavailable when installation fails: raw phase/exit/SSM evidence remains.
 printf '%s\n' "$phase" > batch/last-phase.txt || original=125
 if command -v jq > /dev/null; then
  jq -n --slurpfile identity execution-identity.json --arg phase "$phase" --argjson driver "$driver" --argjson exit "$original" --argjson cleanup "$cleanup" --argjson completed "$completed" '{schema:"borsuk-source-neighborhood-data-batch-v1",execution:$identity[0],last_phase:$phase,original_driver_exit:$driver,batch_exit:$exit,cleanup_exit:$cleanup,completed_stages:$completed,native_stage_only:false,scientific_measurement:false,cold_s3_claim:false,vendor_win:false}' > batch/terminal.json || original=125
 fi
 # Keep failure logs opaque too; only final administrative status is printed.
 if command -v aws > /dev/null && command -v jq > /dev/null && command -v xxd > /dev/null; then
  archive_ok=1
  cp support-check.log execution-identity.json support.sha256 batch/ || { original=125; archive_ok=0; }
  for file in "$support"/*.put.json "$support"/hydration.* "$support"/*.phase.log; do
   test ! -f "$file" || cp "$file" batch/ || { original=125; archive_ok=0; }
  done
  (cd batch && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > batch/SHA256SUMS || { original=125; archive_ok=0; }
  deadline=$(( $(date +%s)+90 ))
  upload() {
   local file=$1 key=$2 left checksum
   left=$(( deadline-$(date +%s)-2 ))
   (( left > 0 )) || return 1
   checksum=$(sha256sum "$file") || return 1; checksum=${checksum%% *}
   [[ $checksum =~ ^[0-9a-f]{64}$ ]] || return 1
   checksum=$(printf '%s' "$checksum" | xxd -r -p | base64 -w0) || return 1
   timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261011/source-neighborhood-data-gates-a0001/$key" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 --checksum-sha256 "$checksum" > "$support/${key//\//_}.batch-put.json" || return 1
   jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$support/${key//\//_}.batch-put.json" > /dev/null
  }
  # Failed-stage native evidence is archived directly, without parsing/copying vector bodies.
  if (( driver != 0 )) && [[ $phase == admission || $phase == smoke ]] && test -d "$root/evidence/$phase"; then
   for file in "$root/evidence/$phase"/*; do
    test -f "$file" && test ! -L "$file" || { archive_ok=0; original=125; break; }
    upload "$file" "failed-$phase/${file##*/}" || { archive_ok=0; original=125; break; }
   done
  fi
  for file in batch/*; do
   [[ ${file##*/} != terminal.json ]] || continue
   upload "$file" "batch/${file##*/}" || { archive_ok=0; original=125; break; }
  done
  if (( archive_ok == 1 )); then
   upload batch/terminal.json batch/terminal.json || original=125
  fi
 else
  (( original != 0 )) || original=125
  if [[ $phase == install ]] && test -f install.phase.log; then
   printf 'INSTALL_FAILURE_EXCERPT_BEGIN\n' || original=125
   tail -c 8192 install.phase.log || original=125
   printf '\nINSTALL_FAILURE_EXCERPT_END\n' || original=125
  fi
 fi
 printf 'DRIVER_EXIT=%s BATCH_EXIT=%s COMPLETED_STAGES=%s CLEANUP_EXIT=%s LAST_PHASE=%s\n' "$driver" "$original" "$completed" "$cleanup" "$phase" || original=125
 exit "$original"
}
trap finish EXIT
phase=install
left=$(( machine-$(date +%s)-120 )); (( left >= 600 ))
timeout -k 5 595 systemd-run --unit=borsuk-neighborhood-install --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=2G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=570 -p TimeoutStopSec=25 /bin/bash "$support/setup-data.sh" install > install.phase.log 2>&1
phase=identity
token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document > batch/instance-identity.json
jq -e --slurpfile expected execution-identity.json '.instanceId==$expected[0].instance_id and .imageId==$expected[0].image_id and .region=="eu-central-1" and .instanceType=="c7i.large"' batch/instance-identity.json > /dev/null
uname -a > batch/kernel.txt
systemd --version > batch/systemd.txt
aws --version > batch/aws-version.txt
phase=hydrate
left=$(( machine-$(date +%s)-120 )); (( left >= 900 ))
timeout -k 5 895 systemd-run --unit=borsuk-neighborhood-hydrate --slice=borsukneighborhood.slice --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=1G -p MemorySwapMax=0 -p IOAccounting=yes -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=870 -p TimeoutStopSec=25 /bin/bash "$support/setup-data.sh" hydrate > hydrate.phase.log 2>&1
for stage in admission smoke; do
 phase=$stage
 left=$(( machine-$(date +%s)-120 )); (( left >= 1400 ))
 timeout -k 5 1395 /bin/bash "$support/run-data-stage.sh" "$stage" > "$stage.phase.log" 2>&1
 completed=$((completed+1))
done
phase=complete
