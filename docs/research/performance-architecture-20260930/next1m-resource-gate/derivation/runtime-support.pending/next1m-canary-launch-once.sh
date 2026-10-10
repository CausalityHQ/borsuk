#!/usr/bin/env bash
# One root-owned request. Never retry RunInstances; reconcile its original token.
set -Eeuo pipefail
umask 077
export AWS_PROFILE=causality AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
[[ $# == 1 && $1 == /tmp/borsuk-next1m-supervisor-canary-* ]] || exit 125
D=$1; [[ -d $D && ! -L $D && $(realpath -e "$D") == "$D" ]] || exit 125
mkdir "$D/launch.claim"
freeze=$D/launch-freeze.json
token=$(jq -er .client_token "$freeze"); prefix=$(jq -er .prefix "$freeze")
bootstrap=$(jq -er .bootstrap_sha256 "$freeze"); launched=$(jq -er .launched_epoch "$freeze")
watch=$(jq -er .watch_unit "$freeze")
[[ $token =~ ^borsuk-next1m-canary-a0001-[0-9a-f]{16}$ && $watch == "$token.service" &&
   $prefix == "research/semantic-router/20261010/$token/run" && $bootstrap =~ ^[0-9a-f]{64}$ ]]
instance='' watch_started=false
cleanup_unwatched() {
    local rc=$?; trap - EXIT
    if [[ -n $instance && $watch_started == false ]]; then
        timeout -k 1 15 aws ec2 terminate-instances --instance-ids "$instance" > "$D/emergency-terminate.json" || rc=94
    fi
    exit "$rc"
}
trap cleanup_unwatched EXIT
printf '%s  %s\n' "$bootstrap" "$D/bootstrap.sh" | sha256sum -c -
printf '%s  %s\n' "$(jq -er .watch_sha256 "$freeze")" "$D/next1m-canary-watch.sh" | sha256sum -c -
printf '%s  %s\n' "$(jq -er .request_sha256 "$freeze")" "$D/request.json" | sha256sum -c -
jq -e '.launch_authority==true and (.support_roster|length==3)' "$freeze" >/dev/null
timeout -k 1 5 aws sts get-caller-identity > "$D/account.json"
[[ $(jq -er .Account "$D/account.json") == 453182569524 ]]
timeout -k 1 8 aws ec2 describe-instances --filters "Name=client-token,Values=$token" > "$D/preexisting.json"
jq -e '[.Reservations[].Instances[]]|length==0' "$D/preexisting.json" >/dev/null
age=$(($(date +%s)-launched)); ((age>=0 && age<=30)) || exit 125
date +%s > "$D/run-request.epoch"
rc=0
timeout -k 1 15 aws ec2 run-instances --cli-input-json "file://$D/request.json" --user-data "file://$D/bootstrap.sh" \
  > "$D/run-instances.json" 2> "$D/run-instances.stderr" || rc=$?
printf '%s\n' "$rc" > "$D/run-instances.exit"
if ((rc==0)); then
    instance=$(jq -er '.Instances|if length==1 then .[0].InstanceId else error("launch identity") end' "$D/run-instances.json")
else
    # Read-only eventual-state reconciliation, never a second request.
    for n in 1 2 3; do
        timeout -k 1 8 aws ec2 describe-instances --filters "Name=client-token,Values=$token" > "$D/reconciled.$n.json"
        count=$(jq -er '[.Reservations[].Instances[]]|length' "$D/reconciled.$n.json")
        ((count<=1)) || exit 94
        if ((count==1)); then instance=$(jq -er '.Reservations[0].Instances[0].InstanceId' "$D/reconciled.$n.json"); break; fi
        sleep 2
    done
    if [[ -z $instance ]]; then
        devbox-tell "Original causality EC2 request outcome is unresolved: $token. No second request was sent. Preserve $D and reconcile this token."
        exit 94
    fi
fi
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
printf '%s\n' "$instance" > "$D/instance.id"
systemd-run --user --expand-environment=no --unit="$watch" --description="$instance $token" --service-type=exec \
  -p RemainAfterExit=yes -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 \
  -p TasksMax=128 -p RuntimeMaxSec=2800 -p TimeoutStopSec=20 \
  /usr/bin/taskset -c 0 /bin/bash "$D/next1m-canary-watch.sh" "$instance" "$prefix" "$D/collection" "$launched" "$bootstrap"
watch_started=true
systemctl --user show "$watch" -p InvocationID -p ActiveState -p MainPID -p ExecMainCode -p ExecMainStatus > "$D/watch.initial.show"
sync -f "$D"
printf 'ORIGINAL_INSTANCE=%s WATCH=%s\n' "$instance" "$watch"
