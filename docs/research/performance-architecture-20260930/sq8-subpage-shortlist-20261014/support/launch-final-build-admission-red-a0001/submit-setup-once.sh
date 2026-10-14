#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-sq8-shortlist.pending/launch-final-build-admission-red-a0001
[[ -s $out/readiness.closed && ! -e $out/setup-send.claim ]]
instance=$(cat "$out/instance.id")
jq -Rs '{commands:[.],executionTimeout:["1800"]}' "$out/setup-command.sh" > "$out/setup-parameters.json"
set -o noclobber
date -u +%FT%TZ > "$out/setup-send.claim"
set +o noclobber
AWS_MAX_ATTEMPTS=1 timeout -k2 25 aws --profile causality --region eu-central-1 ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript --parameters "file://$out/setup-parameters.json" --timeout-seconds 600 --comment borsuk-sq8-shortlist-full-driver-setup > "$out/setup-send.stdout" 2> "$out/setup-send.stderr"
jq -er '.Command.CommandId' "$out/setup-send.stdout" > "$out/setup-command.id"
cat "$out/setup-command.id"
