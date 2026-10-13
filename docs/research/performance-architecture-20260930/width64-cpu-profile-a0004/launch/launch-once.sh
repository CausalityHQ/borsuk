#!/usr/bin/env bash
# Logs belong to the unprivileged caller; only manager commands need sudo.
# shellcheck disable=SC2024
set -euo pipefail
out=/data/target/borsuk-source-width64.pending/cpu-profile-a0004/launch
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
(cd "$out/.." && sha256sum -c frozen.sha256) > "$out/prelaunch-check.log"
[[ -s $out/input-upload.closed && ! -e $out/instance.id ]]
jq -e '.SpotPriceHistory|sort_by(.Timestamp)|last|.AvailabilityZone=="eu-central-1c" and .InstanceType=="c7i.2xlarge" and (.SpotPrice|tonumber)<=0.25' "$out/spot-quote.json" > "$out/quote-admission.txt"
jq -e '.status=="FROZEN_CPU_PROFILE" and .source=="f5a0c36b0bd00b550c53690402577a2012b5b2b4" and .machine_seconds==2400 and .cost_ceiling_usd==0.50' "$out/freeze.json" > /dev/null
set -o noclobber
date -u +%FT%TZ > "$out/launch-once.claim"
set +o noclobber
date +%s > "$out/launch-started.epoch"
# Ambiguous call outcome: preserve claim/token; never automatically call again.
timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 run-instances --cli-input-json "file://$out/launch-request.json" --user-data "file://$out/user-data.sh" > "$out/launch.stdout" 2> "$out/launch.stderr"
jq -er '.Instances|if length==1 then .[0].InstanceId else error("exactly one instance") end' "$out/launch.stdout" > "$out/instance.id"
instance=$(cat "$out/instance.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
emergency() { timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/emergency-terminate.json" 2> "$out/emergency-terminate.stderr"; }
left=$(( $(cat "$out/launch-started.epoch")+2520-$(date +%s) ))
if ((left<=0)); then emergency; exit 1; fi
if ! sudo -n systemd-run --unit=borsuk-width64-cpu-profile-a0004-external --on-active="${left}s" -p User=rb -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 /usr/bin/timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/external-timer-start.log" 2>&1; then emergency; exit 1; fi
if ! sudo -n systemctl is-active borsuk-width64-cpu-profile-a0004-external.timer > "$out/external-timer-active.txt"; then emergency; exit 1; fi
printf '%s\n' "$instance"
