#!/usr/bin/env bash
set -euo pipefail
[[ $# == 6 && $1 == /mnt/borsuk-fetch-width-scale ]] || exit 125
root=$1; phase=$2; kind=$3; config=$4; sha=$5; cap=$6
[[ $phase =~ ^(admission|canary)-width(16|32)$ || $phase =~ ^formal-width(16|32)-(a1|a2|b1|b2)$ || $phase =~ ^completed-((admission|canary)-width(16|32)|formal-width(16|32)-(a1|a2|b1|b2))$ || $phase =~ ^paired-(1|2)$ ]] || exit 125
case "$phase" in
 admission-*|canary-*) [[ $kind == baseline && $cap == 60 ]] || exit 125;;
 formal-*) [[ $kind == baseline && $cap == 1800 ]] || exit 125;;
 completed-*) [[ $kind == completed && $cap == 180 ]] || exit 125;;
 paired-*) [[ $kind == paired && $cap == 180 ]] || exit 125;;
esac
memory=1G
ev=$root/evidence/$phase
mkdir -m 700 "$ev"
unit=borsuk-fetch-width-$phase.service
set +e
/usr/bin/timeout -k 20 "$((cap+90))" systemd-run --unit="$unit" --expand-environment=no --wait --pipe -p WorkingDirectory="$root" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax="$memory" -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=20 -p LimitCORE=0 -p RuntimeMaxSec="$((cap+40))" /bin/bash "$root/owned-native-stage.sh" "$root" "$phase" "$kind" "$config" "$sha" "$cap" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
forced=false; drained=false
cg=/sys/fs/cgroup/system.slice/$unit
if [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; else
 forced=true
 set +e
 printf '1\n' > "$cg/cgroup.kill"; printf '%s\n' "$?" > "$ev/kill.exit"
 /usr/bin/timeout -k 2 10 systemctl stop "$unit" > "$ev/stop.stdout" 2> "$ev/stop.stderr"; printf '%s\n' "$?" > "$ev/stop.exit"
 set -e
 if [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; fi
fi
native=null
if [[ -f $ev/native-exit.txt ]]; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
closed=false
if [[ $manager == 0 && $drained == true && $forced == false && $native == 0 ]]; then closed=true; fi
jq -n --arg phase "$phase" --arg kind "$kind" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" --argjson forced "$forced" --argjson closed "$closed" '{schema:"borsuk-paired-returned-stage-supervision-v1",phase:$phase,kind:$kind,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,forced_cleanup:$forced,execution_closed:$closed,quality_decision_pending:true,performance_claim:false}' > "$ev/supervision.json"
sync -f "$ev/supervision.json"
[[ $closed == true ]]
