#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-source-width64
[[ -f $root/input-files.sha256 && -f $root/machine-stop.epoch ]] || exit 125
stop=$(cat "$root/machine-stop.epoch")
[[ $stop =~ ^[0-9]+$ && $stop -gt $(( $(date +%s)+10000 )) ]] || exit 125
sha256sum -c "$root/input-files.sha256" > "$root/pre-campaign-input-check.log"
trap '/sbin/shutdown -h now' EXIT
set +e
/usr/bin/timeout -k 20 12600 /bin/bash "$root/close-byte-window-campaign.sh" "$root" > "$root/campaign.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$root/campaign.exit"
exit "$status"
