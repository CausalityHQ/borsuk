#!/bin/bash
# EC2-only paired cold cell glue. Rust owns result validation/statistics.
set -euo pipefail
root=/mnt/borsuk-v9-panel
owned=false
if [[ ${1:-} == --owned ]]; then owned=true; shift; fi
[[ $# == 3 && $EUID == 0 ]] || exit 125
phase=$1; config=$2; sha=$3
[[ $config == "$root/config/"* && -f $config && ! -L $config && $sha =~ ^[0-9a-f]{64}$ ]] || exit 125
[[ $(stat -c %s "$config") -le 65536 ]] || exit 125
if grep -q 'PENDING_' "$config"; then exit 125; fi
args=()
binary=$root/bin/compare_native_replay
pin=f456ea8a94f983a2266d3df661a7b4dfe120e607b79b13f968687d2af510880a
output=$root/results/$phase.json
case $phase in
 admission-original|canary-original|panel-a0|panel-a1|panel-a2|canary-panel-a0|canary-panel-a1|canary-panel-a2)
  cap=60; [[ $phase != *panel-* ]] || cap=120
  binary=$root/bin/original; pin=59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083
  output=$root/results/$phase.jsonl; extra=("$output");;
 admission-candidate|canary-candidate|panel-b0|panel-b1|panel-b2|canary-panel-b0|canary-panel-b1|canary-panel-b2)
  cap=60; [[ $phase != *panel-* ]] || cap=120
  binary=$root/bin/candidate; pin=912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995
  output=$root/results/$phase.jsonl; extra=("$output");;
 completed-admission-original|completed-admission-candidate|completed-canary-original|completed-canary-candidate)
  cap=180; args=(--completed-scale); extra=("$output");;
 canary-panel-attribution|panel-attribution)
  cap=180; args=(--sq8-panel-attribution); extra=("$output")
  [[ $(stat -c %s "$config") -le 32768 ]];;
 *) exit 125;;
esac
[[ -f $binary && ! -L $binary && -x $binary && ! -e $output && ! -L $output ]] || exit 125
printf '%s  %s\n%s  %s\n' "$pin" "$binary" "$sha" "$config" | sha256sum --strict -c - >/dev/null
stop=$(cat "$root/machine-stop.epoch")
[[ $stop =~ ^[0-9]+$ && $stop -gt $(( $(date +%s)+cap+360 )) ]] || exit 125
ev=$root/evidence/$phase
unit=borsuk-v9-cold-$phase.service
cg=/sys/fs/cgroup/system.slice/$unit
if [[ $owned == true ]]; then
 [[ $(sed -n 's/^0:://p' /proc/self/cgroup) == /system.slice/"$unit" && -d $ev && ! -L $ev ]] || exit 125
 for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "$cg/$f" > "$ev/$f.before"; done
 [[ $(cat "$ev/cpu.max.before") == '100000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0 && $(cat "$ev/memory.max.before") == 2147483648 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 128 ]] || exit 125
 printf '%s\0' "$binary" "${args[@]}" "$config" "$sha" "${extra[@]}" > "$ev/native.argv0"
 set +e
 # shellcheck disable=SC2016 # Expansion belongs to the child shell.
 /usr/bin/time -v -o "$ev/native-time.txt" /usr/bin/timeout -k 20 "$cap" /bin/bash -c '
   evidence=$1; shift
   "$@"; original=$?
   printf "%s\n" "$original" > "$evidence/native-exit.txt" || exit 126
   exit "$original"
 ' bash "$ev" "$binary" "${args[@]}" "$config" "$sha" "${extra[@]}" > "$ev/native.log" 2>&1
 wrapper=$?
 set -e
 printf '%s\n' "$wrapper" > "$ev/timeout-wrapper-exit.txt"
 for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do cat "$cg/$f" > "$ev/$f.after"; done
 printf '%s  %s\n%s  %s\n' "$pin" "$binary" "$sha" "$config" | sha256sum --strict -c - > "$ev/artifacts-after.log"
 [[ $wrapper == 0 && -f $ev/native-exit.txt && $(cat "$ev/native-exit.txt") == 0 && -f $output && ! -L $output ]] || exit 1
 for event in oom oom_kill oom_group_kill; do grep -qx "$event 0" "$ev/memory.events.after" || exit 1; done
 [[ $(cat "$ev/memory.swap.peak.after") == 0 ]] || exit 1
 sha256sum "$output" > "$ev/result.sha256"
 stat -c %s "$output" > "$ev/result.bytes"
 exit 0
fi
[[ -d $root/results && ! -L $root/results && -d $root/evidence && ! -L $root/evidence ]] || exit 125
[[ $phase != fixture || ( ! -e $root/new-generation && ! -L $root/new-generation ) ]] || exit 125
[[ ! -d $cg ]] || exit 125
[[ $(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p LoadState --value) == not-found ]] || exit 125
mkdir -m 700 "$ev"
self=$(realpath -e "$0")
sha256sum "$self" "$binary" "$config" > "$ev/artifacts-before.sha256"
token=$(cat /proc/sys/kernel/random/uuid)
[[ $token =~ ^[a-f0-9-]{36}$ ]] || exit 125
printf '%s\n' "$token" > "$ev/unit-token.txt"
set +e
/usr/bin/timeout -k 20 "$((cap+90))" systemd-run --unit="$unit" --expand-environment=no --wait --pipe -p WorkingDirectory="$root" -p Environment="BORSUK_V9_STAGE_TOKEN=$token" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=2147483648 -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStartSec=10 -p TimeoutStopSec=20 -p LimitCORE=0 -p RuntimeMaxSec="$((cap+40))" /bin/bash "$self" --owned "$phase" "$config" "$sha" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
forced=false; drained=false
state=$(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p ActiveState --value) || { touch "$root/evidence/cleanup-unverified"; exit 125; }
if { [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; } && [[ $state == inactive || $state == failed ]]; then drained=true; else
 forced=true
 invocation=$(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p InvocationID --value) || { touch "$root/evidence/cleanup-unverified"; exit 125; }
 [[ $invocation =~ ^[a-f0-9]{32}$ && $(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p Environment --value) == "BORSUK_V9_STAGE_TOKEN=$token" ]] || { touch "$root/evidence/cleanup-unverified"; exit 125; }
 printf '%s\n' "$invocation" > "$ev/unit-invocation-id.txt"
 [[ $(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p InvocationID --value) == "$invocation" ]] || { touch "$root/evidence/cleanup-unverified"; exit 125; }
 set +e
 printf '1\n' > "$cg/cgroup.kill"; printf '%s\n' "$?" > "$ev/kill.exit"
 if [[ $(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p InvocationID --value) == "$invocation" ]]; then
  /usr/bin/timeout -k 2 10 systemctl stop "$unit" > "$ev/stop.stdout" 2> "$ev/stop.stderr"; printf '%s\n' "$?" > "$ev/stop.exit"
 fi
 set -e
 if [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; then drained=true; fi
fi
state=$(/usr/bin/timeout -k 2 10 systemctl show "$unit" -p ActiveState --value) || { touch "$root/evidence/cleanup-unverified"; exit 125; }
[[ $state == inactive || $state == failed ]] || drained=false
[[ $drained == true ]] || { touch "$root/evidence/cleanup-unverified"; exit 125; }
native=null; wrapper=null
if [[ -f $ev/native-exit.txt ]]; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]{1,3}$ ]] || native=null; fi
if [[ -f $ev/timeout-wrapper-exit.txt ]]; then wrapper=$(cat "$ev/timeout-wrapper-exit.txt"); [[ $wrapper =~ ^[0-9]{1,3}$ ]] || wrapper=null; fi
closed=false
if [[ $manager == 0 && $native == 0 && $wrapper == 0 && $drained == true && $forced == false ]]; then closed=true; fi
jq -n --arg phase "$phase" --arg config "$sha" --arg binary "$pin" --argjson manager "$manager" --argjson native "$native" --argjson wrapper "$wrapper" --argjson drained "$drained" --argjson forced "$forced" --argjson closed "$closed" '{schema:"borsuk-v9-correctness-stage-v1",phase:$phase,config_sha256:$config,binary_sha256:$binary,manager_exit:$manager,native_exit:$native,timeout_wrapper_exit:$wrapper,owned_cgroup_drained:$drained,forced_cleanup:$forced,execution_closed:$closed,performance_claim:false}' > "$ev/supervision.json"
sync -f "$ev/supervision.json"
sync -f "$ev"
[[ $closed == true ]]
