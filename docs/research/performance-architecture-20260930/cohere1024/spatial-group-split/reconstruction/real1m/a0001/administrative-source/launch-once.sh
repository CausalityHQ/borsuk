#!/bin/bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
(cd "$out" && sha256sum -c launch-inputs.sha256) > "$out/prelaunch-inputs.verified.log"
test ! -e "$out/instance.id"
set -o noclobber
date -u +%FT%TZ > "$out/launch-once.claim"
set +o noclobber
date +%s > "$out/launch-started.epoch"
left=$(( $(cat "$out/launch-started.epoch")+7320-$(date +%s) ))
(( left > 0 ))
sudo -n systemd-run --unit=borsuk-reconstruction-real1m-a0001-external --on-active="${left}s" --timer-property=AccuracySec=1us --timer-property=RandomizedDelaySec=0 -p User=rb -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=5 /usr/bin/timeout -k 5 115 /bin/bash "$out/stop-original-token.sh" > "$out/external-timer-start.log" 2>&1
sudo -n systemctl is-active borsuk-reconstruction-real1m-a0001-external.timer > "$out/external-timer-active.txt"
launch_failure() {
 status=$?
 trap - EXIT
 if (( status != 0 )); then
  printf '%s\n' "$status" > "$out/launch-original-exit.txt"
  timeout -k 5 115 /bin/bash "$out/stop-original-token.sh" > "$out/launch-failure-stop.log" 2>&1 || true
 fi
 exit "$status"
}
trap launch_failure EXIT
timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 run-instances --cli-input-json "file://$out/launch-request.json" --user-data "file://$out/launch-user-data.txt" > "$out/launch.stdout" 2> "$out/launch.stderr"
jq -er '.Instances|if length==1 then .[0].InstanceId else error("one instance required") end' "$out/launch.stdout" > "$out/instance.id"
instance=$(cat "$out/instance.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
trap - EXIT
printf '%s\n' "$instance"
