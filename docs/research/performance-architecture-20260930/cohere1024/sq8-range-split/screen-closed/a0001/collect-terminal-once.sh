#!/usr/bin/env bash
# Collect completed opaque evidence only; do not extract scientific JSONL locally.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
prefix=research/semantic-router/20261016/sq8-split-screen-a0001/terminal
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ ! -e $out/collection-once.claim ]]
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
timeout -k 2 25 aws --profile causality --region eu-central-1 ssm get-command-invocation --command-id 98e42169-f7ba-4b61-b10e-e613d7df6b46 --instance-id i-09055af56258dcb86 > "$out/native-original.json"
jq -e '.InstanceId=="i-09055af56258dcb86" and .CommandId=="98e42169-f7ba-4b61-b10e-e613d7df6b46" and (.Status=="Success" or .Status=="Failed" or .Status=="Cancelled" or .Status=="TimedOut")' "$out/native-original.json" >/dev/null
(set -o noclobber; date -u +%FT%TZ > "$out/collection-once.claim")
get() {
 local key=$1 file=$2 cap=$3
 [[ ! -e $file && ! -L $file ]]
 timeout -k 2 45 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$key" --range "bytes=0-$cap" "$file" > "$file.get.json" 2> "$file.get.stderr"
 [[ -f $file && ! -L $file && $(stat -c %s "$file") -gt 0 && $(stat -c %s "$file") -le $cap ]]
}
get terminal.json "$out/terminal.json" 1048576
jq -e '.schema=="borsuk-sq8-split-panel-publication-v1" and .scope=="SCIENTIFIC_DIAGNOSTIC_PANEL" and .master_sha256=="21a824b3ed20ef266d7834c4f3e200ccdbc05fe61f34ffbc83fa8c9d2b57e1a8" and .driver_sha256=="a65283d5291f660b99c8c96773054b373c0cbeb9a0dd227b3e95b13904f5f644" and (.archive_sha256|test("^[0-9a-f]{64}$"))' "$out/terminal.json" >/dev/null
get evidence.sha256 "$out/evidence.sha256" 1024
get evidence.tar.gz "$out/evidence.tar.gz" 67108864
digest=$(jq -er .archive_sha256 "$out/terminal.json")
[[ $(cat "$out/evidence.sha256") == "$digest  evidence.tar.gz" ]]
printf '%s  %s\n' "$digest" "$out/evidence.tar.gz" | sha256sum --strict --check > "$out/archive-authenticated.log"
printf '%s\n' OPAQUE_ARCHIVE_COLLECTED_NOT_VERIFIED > "$out/collection.closed"
