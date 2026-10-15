#!/bin/bash
# Root-owned SSM template. Freeze the manifest pin before submitting once.
set -euo pipefail
root=/mnt/borsuk-v9-panel
manifest=PENDING_RUNTIME_MANIFEST_SHA256
[[ $manifest =~ ^[0-9a-f]{64}$ ]] || exit 125
printf '%s  %s\n' "$manifest" "$root/runtime-code.sha256" | sha256sum --strict -c -
(cd "$root" && sha256sum --strict -c runtime-code.sha256)
ev=$root/evidence/deadline-admission
mkdir -m 700 "$ev"
timer=borsuk-v9-runtime-shutdown.timer
service=borsuk-v9-runtime-shutdown.service
timeout -k 2 10 systemctl is-active --quiet "$timer"
timeout -k 2 10 systemctl cat "$timer" "$service" > "$ev/units.txt"
timeout -k 2 10 systemctl show "$timer" -p Unit --value > "$ev/timer-unit.txt"
[[ $(cat "$ev/timer-unit.txt") == "$service" ]] || exit 125
timeout -k 2 10 systemctl show "$service" -p ExecStart --value > "$ev/service-exec.txt"
[[ $(wc -l < "$ev/service-exec.txt") == 1 ]] || exit 125
grep -Fq 'path=/usr/sbin/shutdown ; argv[]=/usr/sbin/shutdown -h now ; ignore_errors=no ;' "$ev/service-exec.txt"
timeout -k 2 10 busctl --system get-property org.freedesktop.systemd1 /org/freedesktop/systemd1/unit/borsuk_2dv9_2druntime_2dshutdown_2etimer org.freedesktop.systemd1.Timer NextElapseUSecMonotonic > "$ev/timer-next-us.txt"
read -r signature next rest < "$ev/timer-next-us.txt"
[[ $signature == t && $next =~ ^[0-9]{1,10}$ && -z ${rest:-} ]] || exit 125
(( next > 0 && next <= 3600000000 )) || exit 125
now_us=$(awk '{printf "%.0f\n", $1*1000000}' /proc/uptime)
(( next > now_us )) || exit 125
stop=$(cat "$root/machine-stop.epoch")
[[ $stop =~ ^[0-9]{10}$ ]] || exit 125
stored_left=$((stop-$(date +%s)))
timer_left=$(((next-now_us)/1000000))
delta=$((stored_left-timer_left))
(( delta >= -5 && delta <= 5 )) || exit 125
printf 'stored_remaining_seconds=%s\ntimer_remaining_seconds=%s\ndelta_seconds=%s\n' "$stored_left" "$timer_left" "$delta" > "$ev/deadline-comparison.txt"
sync -f "$ev/deadline-comparison.txt"
sync -f "$ev"
left=$((stop-$(date +%s)-180))
((left>60)) || exit 125
((left<=2800)) || left=2800
/usr/bin/timeout -k 20 "$left" systemd-run --unit=borsuk-sq8-panel-controller --expand-environment=no --wait --pipe -p Environment="BORSUK_RUNTIME_MANIFEST_SHA=$manifest" -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=536870912 -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=20 -p LimitCORE=0 -p RuntimeMaxSec="$((left-20))" /bin/bash "$root/run-sq8-panel-chain.sh"
