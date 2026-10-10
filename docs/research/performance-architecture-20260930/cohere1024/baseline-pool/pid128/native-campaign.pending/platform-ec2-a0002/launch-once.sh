#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
export AWS_PROFILE=causality AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
D=/tmp/borsuk-pid128-platform-ec2-a0002.final
S=/tmp/borsuk-pid128-native-campaign-next/review-repair-r7
mkdir "$D/launch.claim"
freeze=$D/launch-freeze.json
token=$(jq -er .client_token "$freeze")
prefix=$(jq -er .prefix "$freeze")
assets=$(jq -er .assets "$freeze")
bootstrap=$(jq -er .bootstrap_sha256 "$freeze")
printf '%s  %s\n' "$bootstrap" "$D/bootstrap.sh" | sha256sum -c -
printf '%s  %s\n' "$(jq -er .watch_sha256 "$freeze")" "$S/platform-watch.sh" | sha256sum -c -
cp "$S/platform-watch.sh" "$D/watch.sh"
timeout -k 1 5 aws sts get-caller-identity > "$D/account.json"
[[ $(jq -er .Account "$D/account.json") == 453182569524 ]]
timeout -k 1 8 aws ec2 describe-instances --filters "Name=client-token,Values=$token" > "$D/preexisting.json"
jq -e '[.Reservations[].Instances[]]|length==0' "$D/preexisting.json" >/dev/null
while IFS=$'\t' read -r name size sha; do
    [[ $(stat -c %s "$S/$name") == "$size" ]]
    printf '%s  %s\n' "$sha" "$S/$name" | sha256sum -c -
    timeout -k 1 8 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$assets/$sha" --body "$S/$name" --if-none-match '*' > "$D/$name.upload.json"
done < <(jq -r '.support_roster[]|@tsv' "$freeze")
date +%s > "$D/launched.epoch"
rc=0
timeout -k 1 15 aws ec2 run-instances --cli-input-json "file://$D/request.json" --user-data "file://$D/bootstrap.sh" > "$D/run-instances.json" 2> "$D/run-instances.stderr" || rc=$?
printf '%s\n' "$rc" > "$D/run-instances.exit"
if ((rc!=0)); then
    # Reconcile only this original token. Never dispatch a second RunInstances.
    timeout -k 1 8 aws ec2 describe-instances --filters "Name=client-token,Values=$token" > "$D/reconciled.json"
    instance=$(jq -er '[.Reservations[].Instances[]]|if length==1 then .[0].InstanceId else error("ambiguous original launch") end' "$D/reconciled.json")
else
    instance=$(jq -er '.Instances|if length==1 then .[0].InstanceId else error("launch identity") end' "$D/run-instances.json")
fi
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
printf '%s\n' "$instance" > "$D/instance.id"
launched=$(< "$D/launched.epoch")
watch=borsuk-pid128-platform-ec2-a0002.service
watch_rc=0
systemd-run --user --expand-environment=no --unit="$watch" --description="$instance $token" --service-type=exec \
    -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
    -p TasksMax=128 -p RuntimeMaxSec=1560 -p TimeoutStopSec=20 \
    /usr/bin/taskset -c 0 /bin/bash "$D/watch.sh" "$instance" "$prefix" "$D/collection" "$launched" "$bootstrap" || watch_rc=$?
printf '%s\n' "$watch_rc" > "$D/watch.start.exit"
if ((watch_rc!=0)); then
    timeout -k 1 15 aws ec2 terminate-instances --instance-ids "$instance" > "$D/emergency-terminate.json"
    exit 94
fi
systemctl --user show "$watch" -p InvocationID -p ActiveState -p MainPID -p ExecMainCode -p ExecMainStatus > "$D/watch.initial.show"
sync -f "$D"
printf 'ORIGINAL_INSTANCE=%s WATCH=%s\n' "$instance" "$watch"
