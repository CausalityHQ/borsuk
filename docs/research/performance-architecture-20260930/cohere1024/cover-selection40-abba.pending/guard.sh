#!/bin/bash
guard() {
 [[ $(cat /var/lib/cloud/data/instance-id) == i-0a540555f138f1b66 ]] || return 73
 grep -qx 'USEC=1792286800636192' /run/systemd/shutdown/scheduled || return 74
 (( $(date +%s) + $1 + 60 < 1792286800 )) || return 75
 overlap=$(systemctl list-units --type=service --state=active,activating --no-legend --plain |
  awk '$1 ~ /(borsuk|cohere|sdk|native|abba|falsifier|benchmark)/ && $1 !~ /^cover-selection40-abba-(gates|run|collect)-a0001.service$/ {print $1}')
 [[ -z $overlap ]] || return 76
 mountpoint -q /mnt/borsuk-workspace && mountpoint -q /mnt/borsuk-sq8-shortlist
}
