#!/bin/bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
token=borsuk-reconstruction-real1m-a0001-91107713
stamp=$(date +%s)
audit="$out/token-stop-$stamp-$$"
mkdir "$audit"
deadline=$(( stamp+115 ))
found=false
for observation in 1 2 3; do
 left=$(( deadline-$(date +%s)-30 )); (( left > 2 )) || break
 (( left <= 25 )) || left=25
 set +e
 timeout -k 2 "$left" /usr/local/bin/aws --profile causality --region eu-central-1 ec2 describe-instances --filters "Name=client-token,Values=$token" > "$audit/describe-$observation.json" 2> "$audit/describe-$observation.stderr"
 status=$?
 set -e
 printf '%s\n' "$status" > "$audit/describe-$observation.exit"
 if (( status == 0 )); then
  jq '[.Reservations[].Instances[]]' "$audit/describe-$observation.json" > "$audit/instances.json"
  count=$(jq -r 'length' "$audit/instances.json")
  if [[ $count == 1 ]]; then found=true; break; fi
  [[ $count == 0 ]] || exit 125
 fi
 (( observation == 3 )) || sleep 2
done
if [[ $found != true ]]; then
 devbox-tell "BORSUK exact-token stop could not prove identity: $token. No repeated RunInstances; inspect $audit and preserve the original claim." || true
 exit 125
fi
jq -e --arg token "$token" 'length==1 and .[0].ClientToken==$token and .[0].ImageId=="ami-0b8a830d6339a9758" and .[0].InstanceType=="c7i.xlarge" and (.[0].InstanceId|test("^i-[0-9a-f]{17}$"))' "$audit/instances.json" > /dev/null
instance=$(jq -r '.[0].InstanceId' "$audit/instances.json")
state=$(jq -r '.[0].State.Name' "$audit/instances.json")
printf '%s\n' "$instance" > "$audit/instance.id"
if [[ $state == terminated || $state == shutting-down ]]; then
 printf 'ORIGINAL_ALREADY_CLOSING_OR_CLOSED\n' > "$audit/disposition.txt"
 exit 0
fi
case "$state" in pending|running|stopping|stopped) ;; *) exit 125;; esac
left=$(( deadline-$(date +%s)-2 )); (( left > 0 )); (( left <= 25 )) || left=25
timeout -k 2 "$left" /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$audit/terminate.json" 2> "$audit/terminate.stderr"
printf 'EXACT_ORIGINAL_TERMINATION_REQUESTED\n' > "$audit/disposition.txt"
# A requested stop is not confirmed termination; root must collect state/volume/timer closure.
