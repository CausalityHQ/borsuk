#!/bin/bash
set -Eeuo pipefail
umask 077
cd /tmp/borsuk-next1m-replay-a0003
set -o noclobber
printf '%s\n' "$(date -u +%FT%TZ)" > launch.claim
set +o noclobber
instance=
terminate() {
 [[ -n $instance ]] || return 1
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" >terminate.json
 timeout -k 2 120 aws --profile causality --region eu-central-1 ec2 wait instance-terminated --instance-ids "$instance"
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$instance" >terminated.json
}
abort() { rc=$?; trap - EXIT; set +e; [[ -z $instance ]] || terminate; exit "$rc"; }
trap abort EXIT
set +e
timeout -k 2 40 aws --profile causality --region eu-central-1 ec2 run-instances --cli-input-json file://launch-request.json >launch-response.json 2>launch.stderr
launch_rc=$?
set -e
printf '%s\n' "$launch_rc" >launch.exit
instance=$(jq -er '.Instances | select(length==1) | .[0].InstanceId' launch-response.json)
[[ $instance =~ ^i-[0-9a-f]+$ ]]
printf '%s\n' "$instance" >instance-id
[[ $launch_rc == 0 ]]
deadline=$(( $(date +%s) + 600 ))
key=research/semantic-router/20261010/actual1m-a0002-replay-a0003/output
mkdir collected
found=0
while (( $(date +%s) < deadline )); do
 if timeout -k 2 15 aws --profile causality --region eu-central-1 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key/terminal.json" >terminal.head.json 2>terminal.head.stderr; then found=1; break; fi
 sleep 10
done
terminate
trap - EXIT
for name in replay.stdout replay.stderr replay.exit replay.manager.show replay.launcher.stdout replay.launcher.stderr terminal.json; do
 timeout -k 2 15 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key/$name" "collected/$name" >"collected/$name.get.json" 2>"collected/$name.get.stderr" || exit 10
done
[[ $found == 1 ]]
sha256sum collected/replay.stdout collected/replay.stderr collected/replay.exit collected/replay.manager.show collected/terminal.json >collected/SHA256SUMS
printf '%s\n' CLOSED_REPLAY_COLLECTED >state.txt
