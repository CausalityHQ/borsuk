#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-diagnostic-a0001
instance=$(cat "$out/instance.id")
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
end=$(( $(cat "$out/launch-started.epoch")+600 ))
while (( $(date +%s)<end )); do
 if timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$instance" > "$out/instance-current.tmp" 2> "$out/instance-current.stderr"; then
  mv "$out/instance-current.tmp" "$out/instance-current.json"
  state=$(jq -r '.Reservations[0].Instances[0].State.Name' "$out/instance-current.json")
  [[ $state != terminated && $state != shutting-down && $state != stopped ]] || exit 1
  if jq -e '.Reservations[0].Instances[0].BlockDeviceMappings|length==1 and .[0].DeviceName=="/dev/sda1" and .[0].Ebs.DeleteOnTermination==true' "$out/instance-current.json" > /dev/null; then
   jq -er '.Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' "$out/instance-current.json" > "$out/volume.id"
  fi
 fi
 if timeout -k 2 25 aws --profile causality --region eu-central-1 ssm describe-instance-information --filters "Key=InstanceIds,Values=$instance" > "$out/ssm-current.tmp" 2> "$out/ssm-current.stderr"; then
  mv "$out/ssm-current.tmp" "$out/ssm-current.json"
  if [[ -f $out/volume.id ]] && jq -e --arg i "$instance" '.InstanceInformationList|length==1 and .[0].InstanceId==$i and .[0].PingStatus=="Online"' "$out/ssm-current.json" > /dev/null; then printf '%s\n' INSTANCE_AND_SSM_READY > "$out/readiness.closed"; exit 0; fi
 fi
 sleep 10
done
exit 1
