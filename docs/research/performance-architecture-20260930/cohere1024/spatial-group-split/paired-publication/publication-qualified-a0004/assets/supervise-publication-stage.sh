#!/usr/bin/env bash
# One exact stage; no retry or publication/quality acceptance.
set -euo pipefail
[[ $# == 3 ]] || exit 125
root=$1; spec=$2; spec_sha=$3
[[ $root == /mnt/borsuk-retained-returned-publication && $spec == "$root/"* && $spec_sha =~ ^[0-9a-f]{64}$ ]] || exit 125
[[ -f $spec && ! -L $spec && $(stat -c %s "$spec") -le 65536 ]] || exit 125
printf '%s  %s\n' "$spec_sha" "$spec" | sha256sum -c -
phase=$(jq -er .phase "$spec")
case "$phase" in negative-approval|negative-membership|admission-spatial|canary-copy|canary-replay|final-radial|final-spatial) ;; *) exit 125;; esac
cap=$(jq -er '.cap_seconds|select(type=="number" and floor==. and .>=1 and .<=720)' "$spec")
expected=$(jq -er '.expected_exit|select(.==0 or .==2)' "$spec")
ev=$root/evidence/$phase
unit=borsuk-publication-$phase.service
mkdir -m 700 "$ev"
[[ -f $root/input-files.sha256 && ! -L $root/input-files.sha256 ]] || exit 125
sha256sum -c "$root/input-files.sha256" > "$ev/input-supervisor-before.log"
cp -- "$spec" "$ev/stage-spec.json"
# The global machine watchdog remains independent. Exact unit only.
set +e
/usr/bin/timeout -k 20 "$((cap+90))" systemd-run --expand-environment=no --unit="$unit" --wait --pipe -p WorkingDirectory="$root" -p CPUQuota=400% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0-3 -p MemoryMax=4G -p MemorySwapMax=0 -p TasksMax=256 -p KillMode=control-group -p TimeoutStopSec=20 -p LimitCORE=0 -p RuntimeMaxSec="$((cap+40))" /bin/bash "$root/owned-publication-stage.sh" "$root" "$spec" "$spec_sha" "$ev" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
# Drain even if the launcher timed out or failed after creating the unit.
cg=/system.slice/$unit
set +e
/usr/bin/timeout -k 2 10 systemctl show "$unit" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_rc=$?
set -e
printf '%s\n' "$show_rc" > "$ev/show-exit.txt"
drained=false
forced_cleanup=false
if [[ ! -d /sys/fs/cgroup$cg ]]; then
 drained=true
elif grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then
 drained=true
else
 forced_cleanup=true
 set +e
 if [[ -f /sys/fs/cgroup$cg/cgroup.kill ]]; then printf '1\n' > "/sys/fs/cgroup$cg/cgroup.kill"; kill_rc=$?; else kill_rc=125; fi
 printf '%s\n' "$kill_rc" > "$ev/cleanup-kill.exit"
 /usr/bin/timeout -k 2 10 systemctl stop "$unit" > "$ev/cleanup-stop.stdout" 2> "$ev/cleanup-stop.stderr"
 cleanup_rc=$?
 set -e
 printf '%s\n' "$cleanup_rc" > "$ev/cleanup-stop.exit"
 if [[ ! -d /sys/fs/cgroup$cg ]] || grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi
fi
native=null
if [[ -f $ev/native-exit.txt && ! -L $ev/native-exit.txt ]]; then
 native=$(cat "$ev/native-exit.txt")
 [[ $native =~ ^[0-9]+$ ]] || native=null
fi
stage_execution=false
if [[ $manager == 0 && $native == "$expected" && $drained == true && $forced_cleanup == false ]]; then stage_execution=true; fi
jq -n --arg phase "$phase" --argjson manager "$manager" --argjson native "$native" --argjson expected "$expected" --argjson drained "$drained" --argjson forced "$forced_cleanup" --argjson closed "$stage_execution" '{schema:"borsuk-retained-publication-stage-supervision-v1",phase:$phase,manager_exit:$manager,native_exit:$native,expected_native_exit:$expected,owned_cgroup_drained:$drained,forced_cleanup:$forced,execution_closed:$closed,publication_accepted:false,quality_claim:false,performance_claim:false}' > "$ev/supervision.json.tmp"
mv "$ev/supervision.json.tmp" "$ev/supervision.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
/usr/bin/sync -f "$ev/supervision.json"
/usr/bin/sync -f "$ev/SHA256SUMS"
/usr/bin/sync -f "$ev"
cat "$ev/supervision.json"
[[ $stage_execution == true ]]
