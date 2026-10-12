#!/usr/bin/env bash
set -euo pipefail
phase=${1:?canary, prefix, or corrupt}
case $phase in canary) previous=;; prefix) previous=canary;; corrupt) previous=prefix;; *) exit 125;; esac
support=/opt/borsuk-paired-admission
root=/mnt/borsuk-paired-source-screen
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
cd "$support"
sha256sum --check support.sha256 > "$phase.package-check.log"
# shellcheck source=/dev/null
source "$support/admission.env"
: "${CONFIG_SHA256:?}" "${AUTHORITY_SHA256:?}" "${STRACE_REALPATH:?}" "${STRACE_SHA256:?}" "${EVIDENCE_PREFIX:?}"
[[ $EVIDENCE_PREFIX =~ ^research/semantic-router/20261012/paired-source-admission-a[0-9]{4}$ ]]
exec 9> "$support/serial-stage.lock"
flock -n 9
if [[ -n $previous ]]; then
 jq -e --arg phase "$previous" --arg config "$CONFIG_SHA256" --arg authority "$AUTHORITY_SHA256" --arg tracer "$STRACE_SHA256" --arg prefix "$EVIDENCE_PREFIX" '.stage==$phase and .status=="WRAPPER_GATE_ACCEPTED" and .native_exit==2 and .manager_exit==0 and .owned_cgroup_drained==true and .config_sha256==$config and .authority_sha256==$authority and .tracer_sha256==$tracer and .evidence_prefix==$prefix' "$support/$previous.archived-terminal.json" > /dev/null
fi
ev=$root/evidence/$phase
mkdir "$ev"
unit=borsuk-paired-admission-$phase
set +e
systemd-run --unit="$unit" --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=4G -p MemorySwapMax=0 -p TasksMax=256 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec=1200 /bin/bash "$support/native-admission-inner.sh" "$phase" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
timeout -k 2 10 systemctl show "$unit.service" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr" || true
cg=/sys/fs/cgroup/system.slice/$unit.service
drained=false
for check in before after; do
 if test ! -d "$cg" || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; break; fi
 if [[ $check == before ]]; then
  test ! -f "$cg/cgroup.kill" || printf '1\n' > "$cg/cgroup.kill"
  timeout -k 2 10 systemctl stop "$unit.service" > "$ev/cleanup.log" 2>&1 || true
 fi
done
# A killed inner shell cannot run its EXIT trap; restore only after its cgroup drains.
restored=true
if [[ $phase == corrupt ]]; then
 membership=$(jq -r '.spatial.membership.path' "$root/paired.config.json")
 membership_sha=$(jq -r '.spatial.membership.sha256' "$root/paired.config.json")
 restored=false
 if [[ $drained == true ]]; then
  if test -f "$membership.approved-original"; then
   if printf '%s  %s\n' "$membership_sha" "$membership.approved-original" | sha256sum -c - > "$ev/manager-backup-authentication.log" 2>&1; then
    mv -f "$membership.approved-original" "$membership" > "$ev/manager-restore-move.log" 2>&1 || true
   fi
  fi
  if printf '%s  %s\n' "$membership_sha" "$membership" | sha256sum -c - > "$ev/manager-restoration.log" 2>&1; then
   sync -f "$membership"
   sync -f "${membership%/*}"
   restored=true
  fi
 fi
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
traced=null
if test -s "$ev/traced-command-exit.txt"; then traced=$(cat "$ev/traced-command-exit.txt"); [[ $traced =~ ^[0-9]+$ ]] || traced=null; fi
native_status=null
if test -s "$ev/report.json"; then native_status=$(jq -r '.status // "null"' "$ev/report.json" 2> "$ev/native-status.stderr") || native_status=null; fi
if [[ $phase == canary && $native == 2 ]] && test -s "$ev/refusal-witness.txt"; then native_status=INVALID; fi
status=INVALID
gate=UNQUALIFIED
if [[ $manager == 0 && $native == 2 && $drained == true && $restored == true ]] && test -s "$ev/wrapper-prefix-gate.txt"; then
 status=WRAPPER_GATE_ACCEPTED
 gate=$(cat "$ev/wrapper-prefix-gate.txt")
fi
jq -n --arg stage "$phase" --arg status "$status" --arg gate "$gate" --arg config "$CONFIG_SHA256" --arg authority "$AUTHORITY_SHA256" --arg tracer "$STRACE_SHA256" --arg prefix "$EVIDENCE_PREFIX" --arg native_status "$native_status" --argjson manager "$manager" --argjson native "$native" --argjson traced "$traced" --argjson drained "$drained" --argjson restored "$restored" '{schema:"borsuk-paired-import-admission-stage-v1",stage:$stage,status:$status,wrapper_prefix_gate:$gate,source_commit:"043ee8826e485b0e1a1f1f46e11448a742a001de",delivery_base:"4b524677783c4101bbd22e627e69297aa50c60a5",elf_sha256:"fd6d36f6bfbb4e370a94707941cec396af59492f832f6d1090c24eacbc1ec748",config_sha256:$config,authority_sha256:$authority,tracer_sha256:$tracer,evidence_prefix:$prefix,manager_exit:$manager,native_exit:$native,traced_command_exit:$traced,native_status:(if $native_status=="null" then null else $native_status end),owned_cgroup_drained:$drained,approved_membership_restored:$restored,requests_staged:false,truth_staged:false,sq8_staged:false,scientific_measurement:false,cold_s3_claim:false,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
sync -f "$ev/terminal.json"
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
deadline=$(( $(date +%s)+180 ))
upload() {
 local file=$1 left
 left=$((deadline-$(date +%s)-2)); (( left > 0 )) || return 124
 (( left <= 90 )) || left=90
 timeout -k 2 "$left" aws --cli-connect-timeout 10 --cli-read-timeout 60 s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$EVIDENCE_PREFIX/$phase/${file##*/}" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 > "$support/$phase.${file##*/}.put.json"
}
for file in "$ev"/*; do [[ ${file##*/} == terminal.json ]] && continue; upload "$file"; done
upload "$ev/terminal.json"
cp "$ev/terminal.json" "$support/$phase.archived-terminal.json"
sync -f "$support/$phase.archived-terminal.json"
cat "$ev/terminal.json"
[[ $status == WRAPPER_GATE_ACCEPTED ]]
