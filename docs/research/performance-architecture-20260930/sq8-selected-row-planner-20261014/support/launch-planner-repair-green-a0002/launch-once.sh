#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-sq8-shortlist.pending/launch-planner-repair-green-a0002
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
test -s "/data/target/borsuk-sq8-shortlist.pending/launch-planner-repair-green-a0001/input-upload.closed"
jq -e '.Reservations|length==0' "/data/target/borsuk-sq8-shortlist.pending/launch-planner-repair-green-a0001/failed-launch-instances.json" >/dev/null
[[ $(cat /data/target/borsuk-sq8-shortlist.pending/launch-plane-kind-red-a0001/compute.closed) == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e '.SpotPriceHistory | sort_by(.Timestamp) | last | .AvailabilityZone == "eu-central-1b" and .InstanceType == "m7i.2xlarge" and (.SpotPrice|tonumber) <= 0.25' "$out/spot-quote.json" > "$out/quote-admission.txt"
(cd "$out" && sha256sum --strict --check launch-inputs.sha256) > "$out/prelaunch-inputs.verified.log"
test ! -e "$out/instance.id"
set -o noclobber
date -u +%FT%TZ > "$out/launch-once.claim"
set +o noclobber
date +%s > "$out/launch-started.epoch"
timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 run-instances \
 --cli-input-json "file://$out/launch-request.json" --user-data "file://$out/launch-user-data.txt" \
 > "$out/launch.stdout" 2> "$out/launch.stderr"
jq -er '.Instances|if length==1 then .[0].InstanceId else error("one instance required") end' "$out/launch.stdout" > "$out/instance.id"
instance=$(cat "$out/instance.id")
[[ "$instance" =~ ^i-[0-9a-f]{17}$ ]]
emergency() { timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/emergency-terminate.json" 2> "$out/emergency-terminate.stderr"; }
left=$(( $(cat "$out/launch-started.epoch") + 7320 - $(date +%s) ))
if (( left <= 0 )); then emergency; exit 1; fi
if ! sudo -n systemd-run --unit=borsuk-sq8-selected-row-planner-repair-green-a0002-external --on-active="${left}s" -p User=rb -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 \
 /usr/bin/timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" \
 > "$out/external-timer-start.log" 2>&1; then emergency; exit 1; fi
if ! sudo -n systemctl is-active borsuk-sq8-selected-row-planner-repair-green-a0002-external.timer > "$out/external-timer-active.txt"; then emergency; exit 1; fi
printf '%s\n' "$instance"
