#!/usr/bin/env bash
# shellcheck disable=SC2024 # Caller-owned evidence redirects are intentional.
# One observer of original setup/run handles; terminal markers only.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
instance=$(cat "$out/instance.id"); volume=$(cat "$out/volume.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ && $volume =~ ^vol-[0-9a-f]{17}$ ]]
deadline=$(( $(cat "$out/launch-started.epoch")+3720 ))
command=$(cat "$out/setup-command.id"); phase=setup
while (( $(date +%s)<deadline )); do
 if timeout -k 2 25 aws --profile causality --region eu-central-1 ssm get-command-invocation --command-id "$command" --instance-id "$instance" --query '{CommandId:CommandId,InstanceId:InstanceId,Status:Status,ResponseCode:ResponseCode}' > "$out/watch-observation.tmp" 2> "$out/watch-observation.stderr"; then
  mv "$out/watch-observation.tmp" "$out/watch-observation.json"
  jq -e --arg c "$command" --arg i "$instance" '.CommandId==$c and .InstanceId==$i' "$out/watch-observation.json" > /dev/null
  status=$(jq -r .Status "$out/watch-observation.json")
  case "$status" in
   Success|Failed|Cancelled|TimedOut)
    cp "$out/watch-observation.json" "$out/$phase-terminal-observation.json"
    if [[ $phase == setup && $status == Success ]]; then
     while [[ ! -f $out/native-command.id ]] && (( $(date +%s)<deadline )); do sleep 10; done
     if [[ -f $out/native-command.id ]]; then command=$(cat "$out/native-command.id"); phase=run; continue; fi
    fi
    break;;
   Pending|InProgress|Delayed|Cancelling) ;;
   *) printf '%s\n' "UNRECOGNIZED_STATUS:$status" > "$out/watch-invalid-status.txt"; break;;
  esac
 fi
 sleep 20
done
printf '%s\n' "$phase" > "$out/closure-phase.txt"
set -o noclobber
date -u +%FT%TZ > "$out/termination-once.claim"
set +o noclobber
timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/terminate.stdout" 2> "$out/terminate.stderr"
timeout -k 2 120 aws --profile causality --region eu-central-1 ec2 wait instance-terminated --instance-ids "$instance" > "$out/termination-wait.stdout" 2> "$out/termination-wait.stderr"
timeout -k 2 45 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$instance" > "$out/instance-final.json"
jq -e --arg i "$instance" '.Reservations|length==1 and (.[0].Instances|length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated")' "$out/instance-final.json" > /dev/null
set +e
timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-volumes --volume-ids "$volume" > "$out/volume-final.stdout" 2> "$out/volume-final.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$out/volume-final.exit"
[[ $rc == 254 ]]
grep -F 'InvalidVolume.NotFound' "$out/volume-final.stderr"
grep -F "$volume" "$out/volume-final.stderr"
sudo -n systemctl stop borsuk-sq8-split-screen-a0001-external.timer
sudo -n systemctl show borsuk-sq8-split-screen-a0001-external.timer -p ActiveState -p SubState > "$out/external-timer-final.txt"
grep -qx 'ActiveState=inactive' "$out/external-timer-final.txt"
printf '%s\n' INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE > "$out/compute.closed"
devbox-agent bg borsuk/prod-ready-v9 off
