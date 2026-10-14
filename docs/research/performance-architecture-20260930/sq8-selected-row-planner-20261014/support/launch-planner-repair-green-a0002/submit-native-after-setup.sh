#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-sq8-shortlist.pending/launch-planner-repair-green-a0002
instance=$(cat "$out/instance.id")
command=$(cat "$out/setup-command.id")
end=$(( $(cat "$out/launch-started.epoch")+2100 ))
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
while (( $(date +%s)<end )); do
 timeout -k2 25 aws --profile causality --region eu-central-1 ssm get-command-invocation --command-id "$command" --instance-id "$instance" --query '{CommandId:CommandId,InstanceId:InstanceId,Status:Status,ResponseCode:ResponseCode}' > "$out/setup-submit-observation.tmp"
 mv "$out/setup-submit-observation.tmp" "$out/setup-submit-observation.json"
 jq -e --arg c "$command" --arg i "$instance" '.CommandId==$c and .InstanceId==$i' "$out/setup-submit-observation.json" >/dev/null
 state=$(jq -r .Status "$out/setup-submit-observation.json")
 case $state in
 Success) [[ $(jq -r .ResponseCode "$out/setup-submit-observation.json") == 0 ]]; break;;
 Failed|Cancelled|TimedOut) exit 1;;
 Pending|InProgress|Delayed|Cancelling) sleep 20;;
 *) exit 1;;
 esac
done
[[ $state == Success && ! -e $out/native-send.claim ]]
jq -Rs '{commands:[.],executionTimeout:["6000"]}' "$out/native-command.sh" > "$out/native-parameters.json"
set -o noclobber
date -u +%FT%TZ > "$out/native-send.claim"
set +o noclobber
timeout -k2 25 aws --profile causality --region eu-central-1 ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript --parameters "file://$out/native-parameters.json" --timeout-seconds 600 --comment borsuk-selected-sq8-planner-native-green > "$out/native-send.stdout" 2> "$out/native-send.stderr"
jq -er '.Command.CommandId' "$out/native-send.stdout" > "$out/native-command.id"
cat "$out/native-command.id"
