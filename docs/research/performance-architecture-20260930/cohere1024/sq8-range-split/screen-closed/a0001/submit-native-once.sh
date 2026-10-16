#!/bin/bash
set -euo pipefail
set -C
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ ! -e $out/native-send.claim && ! -e $out/native-command.id ]]
instance=$(cat "$out/instance.id")
setup=$(cat "$out/setup-command.id")
[[ $instance == i-09055af56258dcb86 && $setup == 7f2d9951-fea3-4301-99cc-64672539d2c3 ]]
jq -e --arg i "$instance" --arg c "$setup" '.CommandId==$c and .InstanceId==$i and .Status=="Success" and .ResponseCode==0' "$out/setup-original.json" >/dev/null
jq -e '.native_authorized==true and .scientific_admission==true and .setup_verified==true and .scope=="FROZEN_SIX_CELL_DIAGNOSTIC" and .campaign_cap_usd==0.30' "$out/native-admission.json" >/dev/null
master=$(sha256sum "$out/master.json"); master=${master%% *}
jq -e --arg sha "$master" '.master_sha256==$sha' "$out/native-admission.json" >/dev/null
(cd "$out"; sha256sum --strict --check native-inputs.sha256) > "$out/native-preflight.verified.log"
cmp "$out/master.json" "$out/embedded-master.check.json"
start=$(cat "$out/launch-started.epoch")
(( start + 3600 - $(date +%s) >= 1200 ))
jq -Rs '{commands:[.],executionTimeout:["1200"]}' "$out/native-command.sh" > "$out/native-parameters.json"
date -u +%FT%TZ > "$out/native-send.claim"
timeout -k2 25 aws --profile causality --region eu-central-1 ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript --parameters "file://$out/native-parameters.json" --timeout-seconds 600 --comment borsuk-sq8-split-screen-a0001 > "$out/native-send.stdout" 2> "$out/native-send.stderr"
jq -er '.Command.CommandId' "$out/native-send.stdout" > "$out/native-command.id"
cat "$out/native-command.id"
