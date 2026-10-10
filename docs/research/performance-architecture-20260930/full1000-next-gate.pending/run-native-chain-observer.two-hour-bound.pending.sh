#!/usr/bin/env bash
# Exact scale1M chain launcher. Launch status is not the native command's exit.
# ROOT CONFIG_SHA; all support bytes are admitted by bootstrap support.sha256.
set -Eeuo pipefail
set -o noclobber
umask 077
[[ $# == 2 && $EUID == 0 && $1 == /mnt/borsuk-scale1m && $2 =~ ^[0-9a-f]{64}$ ]] || exit 98
root=$1 config_sha=$2
[[ $(realpath -e "$root") == "$root" && -d $root/evidence-root &&
   ! -e $root/evidence-chain && ! -L $root/evidence-chain ]] || exit 98
unit=borsuk-pid128-observer-$(cat /proc/sys/kernel/random/uuid).service
[[ $unit =~ ^borsuk-pid128-observer-[0-9a-f-]+\.service$ ]] || exit 98
id=''
done_ok=0
# ShellCheck does not follow the EXIT trap registration below.
# shellcheck disable=SC2329
cleanup() {
    local rc=$? show
    trap - EXIT
    set +e
    if (( done_ok == 0 )); then
        show=$(timeout -k 1 5 systemctl show "$unit" -p Id -p Description -p InvocationID -p ControlGroup)
        if [[ -n $id ]] && grep -Fx "Id=$unit" <<< "$show" >/dev/null &&
           grep -Fx "Description=$unit" <<< "$show" >/dev/null &&
           grep -Fx "InvocationID=$id" <<< "$show" >/dev/null &&
           grep -Fx "ControlGroup=/system.slice/$unit" <<< "$show" >/dev/null; then
            cg=/sys/fs/cgroup/system.slice/$unit
            if [[ -d $cg && ! -L $cg && $(realpath -e "$cg") == "$cg" ]]; then
                printf '1\n' >| "$cg/cgroup.kill"
            fi
            timeout -k 1 5 systemctl stop "$unit" > "$root/evidence-root/chain-failure-stop.stdout" 2> "$root/evidence-root/chain-failure-stop.stderr"
        fi
        (( rc != 0 )) || rc=98
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 98' HUP INT TERM
printf '%s\n' "$unit" > "$root/evidence-root/chain-observer.unit"
recipe_sha=$(sha256sum "$root/run_native_scale_build_gate.sh"); recipe_sha=${recipe_sha%% *}
[[ $recipe_sha =~ ^[0-9a-f]{64}$ ]]
config_actual=$(sha256sum "$root/gate-config.json"); config_actual=${config_actual%% *}
[[ $config_actual == "$config_sha" ]]
deadline=$(( $(date +%s) + 3680 ))
launch=0
timeout -k 1 10 systemd-run --unit="$unit" --description="$unit" \
  -p Type=exec -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 \
  -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 \
  -p RuntimeMaxSec=3680 -p TimeoutStopSec=30 -p KillMode=control-group -p LimitCORE=0 \
  /bin/bash "$root/observer-command.sh" "$root/run_native_scale_build_gate.sh" \
  "$root/gate-config.json" "$config_sha" "$root/evidence-chain" \
  "$root/evidence-root/chain-actual.exit" \
  > "$root/evidence-root/chain-launch.stdout" 2> "$root/evidence-root/chain-launch.stderr" || launch=$?
printf '%s\n' "$launch" > "$root/evidence-root/chain-launch.exit"
# Capture identity even on a failed launcher response; never retry the launch.
identity_end=$((SECONDS+10))
while :; do
    timeout -k 1 2 systemctl show "$unit" -p Id -p Description -p InvocationID -p ControlGroup \
      >| "$root/evidence-root/chain-launch.identity" || exit 98
    id=$(sed -n 's/^InvocationID=//p' "$root/evidence-root/chain-launch.identity")
    [[ -z $id ]] || break
    (( SECONDS < identity_end )) || exit 98
    sleep 0.1
done
[[ $id =~ ^[0-9a-f]{32}$ ]]
grep -Fx "Id=$unit" "$root/evidence-root/chain-launch.identity" >/dev/null
grep -Fx "Description=$unit" "$root/evidence-root/chain-launch.identity" >/dev/null
grep -Fx "ControlGroup=/system.slice/$unit" "$root/evidence-root/chain-launch.identity" >/dev/null
(( launch == 0 )) || exit 98
bash "$root/collect-native-chain-outer.sh" "$unit" "$id" "$root/evidence-chain" \
  "$root/evidence-root/chain-outer" "$root/evidence-root/chain-actual.exit" \
  "$recipe_sha" "$config_sha" "$deadline"
rc=$(< "$root/evidence-root/chain-actual.exit")
[[ $rc == 0 || $rc == 2 || $rc == 3 ]]
printf '%s\n' "$rc" > "$root/evidence-root/chain-unit.exit"
sync -f "$root/evidence-root/chain-unit.exit"
sync -f "$root/evidence-root"
done_ok=1
exit "$rc"
