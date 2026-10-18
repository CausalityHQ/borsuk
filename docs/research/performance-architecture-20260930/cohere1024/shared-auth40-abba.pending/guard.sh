#!/bin/bash
guard() {
 [[ $(cat /var/lib/cloud/data/instance-id) == i-0518bef1e30d66437 ]] || return 73
 grep -qx 'USEC=1792288279918914' /run/systemd/shutdown/scheduled || return 74
 (( $(date +%s) + $1 + 60 < 1792288279 )) || return 75
 overlap=$(systemctl list-units --type=service --state=active,activating --no-legend --plain |
  awk '$1 ~ /(borsuk|cohere|sdk|native|abba|falsifier|benchmark)/ && $1 !~ /^shared-auth40-abba-(gates|run|collect)-a0001.service$/ {print $1}')
 [[ -z $overlap ]] || return 76
 mountpoint -q /mnt/borsuk-workspace && mountpoint -q /mnt/borsuk-sq8-shortlist
}
