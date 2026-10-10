#!/usr/bin/env bash
# SOURCE ONLY. EC2 root entrypoint; real systemd, no BORSUK ELF/data.
# API: authenticated support directory, NEW evidence parent already exists.
# shellcheck disable=SC2329
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
[[ $# == 1 && $EUID == 0 ]] || exit 98
support=$1
[[ $support == /mnt/borsuk-platform-support && $(realpath -e "$support") == "$support" ]] || exit 98
E=/mnt/borsuk-pid-evidence
[[ -d $E && ! -L $E && $(realpath -e "$E") == "$E" && ! -e $E/platform.started ]] || exit 98
printf '%s\n' SOURCE_BOUND_PLATFORM_ONLY > "$E/platform.started"
deadline=$(( $(date +%s)+300 )); owned_unit='' owned_id=''
manager() {
    local cap=$1 left; shift
    left=$((deadline-$(date +%s)-2)); ((left>0)) || return 98
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {
    local rc=$? cleanup=0 cg
    trap - EXIT; set +e
    if [[ -n $owned_unit ]]; then
        timeout -k 1 5 systemctl show "$owned_unit" -p InvocationID -p ControlGroup > "$E/failure-owned.show"
        if [[ -n $owned_id && $(sed -n 's/^InvocationID=//p' "$E/failure-owned.show") == "$owned_id" &&
              $(sed -n 's/^ControlGroup=//p' "$E/failure-owned.show") == "/system.slice/$owned_unit" ]]; then
            timeout -k 1 10 systemctl stop "$owned_unit" > "$E/failure-stop.log" 2>&1 || cleanup=1
            cg=/sys/fs/cgroup/system.slice/$owned_unit
            if [[ -e $cg/cgroup.events ]] && ! grep -Fx 'populated 0' "$cg/cgroup.events" >/dev/null; then
                printf '1\n' > "$cg/cgroup.kill" || cleanup=1
                timeout -k 1 10 systemctl stop "$owned_unit" >> "$E/failure-stop.log" 2>&1 || cleanup=1 || cleanup=1
            fi
            [[ ! -e $cg/cgroup.events ]] || grep -Fx 'populated 0' "$cg/cgroup.events" > "$E/failure-drain.events" || cleanup=1
        else cleanup=1; fi
    fi
    printf 'original=%s cleanup=%s\n' "$rc" "$cleanup" > "$E/platform.exit" || rc=97
    sync -f "$E" || rc=97
    ((cleanup==0)) || exit 97
    ((rc==0)) || exit "$rc"
    exit 0
}
trap finish EXIT
for mode in positive negative; do
    cfg=$E/$mode.config.json
    jq -n --arg mode "$mode" '{schema:"borsuk-native-pid128-platform-canary-v1",mode:$mode,
      scratch:{evidence_root:"/mnt/borsuk-pid-evidence",phase_transient_reserve_bytes:{diagnostic:67108864},
        evidence_reserve_bytes:67108864,phase_peak_bytes:{diagnostic:1073741824},
        total_cap_bytes:1073741824,minimum_free_bytes:134217728}}' > "$cfg"
    sha=$(sha256sum < "$cfg"); sha=${sha%% *}
    token=$(< /proc/sys/kernel/random/uuid); owned_unit=borsuk-pid128-observer-$token-platform-$mode.service
    owned_id=''
    [[ $(manager 5 systemctl show "$owned_unit" -p LoadState --value) == not-found ]] || exit 98
    printf '%s\n' "$owned_unit" > "$E/$mode.unit"
    start_rc=0
    manager 10 systemd-run --quiet --unit="$owned_unit" --description="$owned_unit" --service-type=exec \
      -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=268435456 -p MemorySwapMax=0 \
      -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec=90 -p LimitCORE=0 \
      /usr/bin/taskset -c 0 /bin/bash "$support/observer-command.sh" "$support/run-platform-canary.sh" "$cfg" "$sha" "$E/$mode" "$E/$mode.actual-exit" \
      > "$E/$mode.start.stdout" 2> "$E/$mode.start.stderr" || start_rc=$?
    printf '%s\n' "$start_rc" > "$E/$mode.start.exit"
    owned_id=$(manager 5 systemctl show "$owned_unit" -p InvocationID --value)
    [[ $owned_id =~ ^[0-9a-f]{32}$ && $start_rc == 0 ]] || exit 98
    printf '%s\n' "$owned_id" > "$E/$mode.invocation"
    collector_mode=platform; [[ $mode != negative ]] || collector_mode=negative
    recipe_sha=$(sha256sum < "$support/run-platform-canary.sh"); recipe_sha=${recipe_sha%% *}
    manager 110 /bin/bash "$support/collect-stage-outer.sh" "$owned_unit" "$owned_id" "$E/$mode" "$E/$mode-outer" "$E/$mode.actual-exit" "$recipe_sha" "$sha" "$deadline" "$collector_mode"
    owned_unit='' owned_id=''
done
# Expected INVALID/98 is insufficient: the payload must have timed out and drained.
N=$E/negative/phases/diagnostic
[[ $(< "$N/timeout.exit") =~ ^(124|137)$ ]] || exit 98
identity=/mnt/borsuk-pool-pid/staging-platform-negative/native/descendant.identity
unit=$(< "$N/unit")
[[ -f $identity && ! -L $identity && $(sed -n 's/^pid=//p' "$identity") =~ ^[1-9][0-9]*$ &&
   $(sed -n 's/^0:://p' "$identity") == "/system.slice/$unit" ]] || exit 98
[[ ! -e /sys/fs/cgroup/system.slice/$unit/cgroup.events ]] || grep -Fx 'populated 0' /sys/fs/cgroup/system.slice/"$unit"/cgroup.events > "$E/negative-final-drain.events"
if [[ -f $N/cleanup.kill.txt ]]; then
    [[ $(< "$N/cleanup.kill.txt") == cgroup.kill=1 && $(< "$N/cleanup.drained.txt") == populated=0 && $(< "$N/cleanup.stop.exit") == 0 ]] || exit 98
else
    [[ $(< "$N/drained.txt") == populated=0 && $(< "$N/manager.stop.exit") == 0 ]] || exit 98
    grep -Fx Result=exit-code "$N/manager.final.txt" >/dev/null
    grep -Fx ExecMainCode=1 "$N/manager.final.txt" >/dev/null
    grep -Fx ExecMainStatus=98 "$N/manager.final.txt" >/dev/null
fi
jq -n '{schema:"borsuk-native-pid128-platform-pair-v1",status:"PLATFORM_PAIR_ROOT_REPLAY_REQUIRED",ann_executed:false,
 actual_systemd:true,actual_timeout:true,performance_claim:false,production_qualification:false}' > "$E/platform-pair.json"
exit 0
