#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
printf '%s  %s\n' c37b35867891659227d61f53d43d90fa16cb7994981dbf828fc074aba0308f3c "$root/supervise-gc-session.sh" | sha256sum --strict -c -
ev=$root/evidence/deadline-admission
mkdir -m 700 "$ev"
timer=borsuk-paired-qualification-shutdown.timer
service=borsuk-paired-qualification-shutdown.service
timeout -k 2 10 systemctl is-active --quiet "$timer"
timeout -k 2 10 systemctl cat "$timer" "$service" > "$ev/units.txt"
timeout -k 2 10 systemctl show "$timer" -p Unit --value > "$ev/timer-unit.txt"
[[ $(cat "$ev/timer-unit.txt") == "$service" ]] || exit 125
timeout -k 2 10 systemctl show "$service" -p ExecStart --value > "$ev/service-exec.txt"
[[ $(wc -l < "$ev/service-exec.txt") == 1 ]] || exit 125
grep -Fq 'path=/usr/sbin/shutdown ; argv[]=/usr/sbin/shutdown -h now ; ignore_errors=no ;' "$ev/service-exec.txt"
timeout -k 2 10 busctl --system get-property org.freedesktop.systemd1 /org/freedesktop/systemd1/unit/borsuk_2dpaired_2dqualification_2dshutdown_2etimer org.freedesktop.systemd1.Timer NextElapseUSecMonotonic > "$ev/timer-next-us.txt"
read -r signature next rest < "$ev/timer-next-us.txt"
[[ $signature == t && $next =~ ^[0-9]{1,10}$ && -z ${rest:-} ]] || exit 125
(( next > 0 && next <= 5520000000 )) || exit 125
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
bash "$root/supervise-gc-session.sh" "$root" 103f49c7f6384b8acd27271134a8cc86c7877247 31e528998da425b24e8631e552a62a314c1f33e5617476d289e103bfa260ceeb 24 research/semantic-router/20261016/gc-session-gates-a0001
