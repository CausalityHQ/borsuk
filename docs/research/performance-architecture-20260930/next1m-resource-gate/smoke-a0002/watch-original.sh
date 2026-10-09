#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
instance=$1
started=$2
[[ $instance =~ ^i-[0-9a-f]+$ && $started =~ ^[0-9]+$ ]]
mkdir -m 0700 -p remote-results
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261009/actual1m-q32-admission-smoke-a0002
aws_call() { timeout -k 2 25 aws --profile causality --region eu-central-1 --cli-connect-timeout 5 --cli-read-timeout 15 "$@"; }
reason=deadline
while (( $(date +%s) - started < 900 )); do
  if aws_call s3api head-object --bucket "$bucket" --key "$prefix/terminal.json" > remote-results/terminal.head.json 2>remote-results/terminal.head.stderr; then
    reason=terminal
    break
  fi
  if aws_call ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text > remote-results/state.txt 2>remote-results/state.stderr; then
    if [[ $(cat remote-results/state.txt) == terminated ]]; then reason=instance_terminated; break; fi
  fi
  sleep 15
done
printf '%s\n' "$reason" > remote-results/watcher-reason.txt
# Provisional S3 terminal cannot race termination ahead of the manager's final exit.
if [[ $reason == terminal ]]; then
 for (( proof_attempt=0; proof_attempt<12 && $(date +%s)-started<900; proof_attempt++ )); do
  if aws_call s3api head-object --bucket "$bucket" --key "$prefix/bootstrap-manager.json" > remote-results/manager.head.json 2>remote-results/manager.head.stderr; then break; fi
  sleep 5
 done
fi
# Terminate only this original instance; no replacement or retry launch.
aws_call ec2 terminate-instances --instance-ids "$instance" > remote-results/terminate.json
terminated=false
for (( attempt=0; attempt<40; attempt++ )); do
  if aws_call ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text > remote-results/state.txt 2>remote-results/state.stderr && [[ $(cat remote-results/state.txt) == terminated ]]; then
    terminated=true
    break
  fi
  sleep 5
done
[[ $terminated == true ]] || { printf 'termination unconfirmed\n' >&2; exit 3; }
aws_call ec2 get-console-output --instance-id "$instance" --latest > remote-results/console.json 2>remote-results/console.stderr || true
aws_call s3api get-object --bucket "$bucket" --key "$prefix/terminal.json" remote-results/terminal.json > remote-results/terminal.get.json 2>remote-results/terminal.get.stderr || { printf 'terminated; terminal missing: execution INVALID\n' >&2; exit 2; }
jq -e --arg instance "$instance" '.instance_id == $instance and .evidence.bytes <= 67108864 and .evidence.bytes > 0 and (.evidence.sha256|test("^[0-9a-f]{64}$"))' remote-results/terminal.json >/dev/null
aws_call s3api get-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" remote-results/evidence.tar.gz > remote-results/evidence.get.json
[[ $(stat -c %s remote-results/evidence.tar.gz) == "$(jq -er .evidence.bytes remote-results/terminal.json)" ]]
[[ $(sha256sum remote-results/evidence.tar.gz | cut -d' ' -f1) == "$(jq -er .evidence.sha256 remote-results/terminal.json)" ]]
(ulimit -f 64; aws_call s3api get-object --bucket "$bucket" --key "$prefix/artifacts.sha256" remote-results/artifacts.sha256 > remote-results/manifest.get.json)
[[ $(stat -c %s remote-results/artifacts.sha256) -le 65536 ]]
jq -n --arg instance "$instance" --arg reason "$reason" '{instance_id:$instance,terminated:true,reason:$reason,archive_authenticated:true,independent_replay_pending:true,performance_claim:false}' > remote-results/collection.json
jq -e '.schema=="borsuk-parity-smoke-closed-v1" and .original_unit_exit==0 and .acceptance=="ROOT_REPLAY_REQUIRED" and .performance_claim==false' remote-results/terminal.json >/dev/null || { printf 'Original terminal is not successful parity closure: INVALID\n' >&2; exit 2; }
(ulimit -f 4; aws_call s3api get-object --bucket "$bucket" --key "$prefix/bootstrap-manager.json" remote-results/bootstrap-manager.json > remote-results/manager.get.json)
[[ $(stat -c %s remote-results/bootstrap-manager.json) -le 4096 ]]
jq -e --arg instance "$instance" --arg sha "$(sha256sum remote-results/terminal.json | cut -d' ' -f1)" '.schema=="borsuk-parity-bootstrap-exit-v1" and .instance_id==$instance and .terminal_sha256==$sha and .exit_code=="exited" and .exit_status=="0" and .service_result=="success" and .final_exit=="0"' remote-results/bootstrap-manager.json >/dev/null || { printf 'Original main exit unverified or nonzero: INVALID\n' >&2; exit 2; }
printf 'Original instance terminated; archive and actual bootstrap exit authenticated; independent replay pending\n'
