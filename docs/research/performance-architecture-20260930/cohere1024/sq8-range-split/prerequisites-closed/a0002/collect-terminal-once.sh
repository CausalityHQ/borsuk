#!/usr/bin/env bash
# Collect completed opaque evidence only; do not extract scientific JSONL locally.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-prerequisites-a0002
prefix=research/semantic-router/20261016/sq8-split-prerequisites-a0002/terminal
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ ! -e $out/collection-once.claim ]]
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
timeout -k 2 25 aws --profile causality --region eu-central-1 ssm get-command-invocation --command-id b2184862-5483-4914-934a-942c199b0fde --instance-id i-05113071a227077e5 > "$out/native-original.json"
jq -e '.InstanceId=="i-05113071a227077e5" and .CommandId=="b2184862-5483-4914-934a-942c199b0fde" and (.Status=="Success" or .Status=="Failed" or .Status=="Cancelled" or .Status=="TimedOut")' "$out/native-original.json" >/dev/null
(set -o noclobber; date -u +%FT%TZ > "$out/collection-once.claim")
get() {
 local key=$1 file=$2 cap=$3
 [[ ! -e $file && ! -L $file ]]
 timeout -k 2 45 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$key" --range "bytes=0-$cap" "$file" > "$file.get.json" 2> "$file.get.stderr"
 [[ -f $file && ! -L $file && $(stat -c %s "$file") -gt 0 && $(stat -c %s "$file") -le $cap ]]
}
get terminal.json "$out/terminal.json" 1048576
jq -e '.schema=="borsuk-split-prerequisites-publication-v1" and .scope=="UNMEASURED_PREREQUISITES" and .master_sha256=="e75ebcf5149ff51b359137352b2eaad7874b5cf3fbc8878f18b27a05c415a51b" and .runner_sha256=="cbc0f0749d1a54d89ef1545f95a82b7065a7b964e6f9b71cf698ca306c50bfa4" and (.archive_sha256|test("^[0-9a-f]{64}$"))' "$out/terminal.json" >/dev/null
get evidence.sha256 "$out/evidence.sha256" 1024
get evidence.tar.gz "$out/evidence.tar.gz" 67108864
digest=$(jq -er .archive_sha256 "$out/terminal.json")
[[ $(cat "$out/evidence.sha256") == "$digest  evidence.tar.gz" ]]
printf '%s  %s\n' "$digest" "$out/evidence.tar.gz" | sha256sum --strict --check > "$out/archive-authenticated.log"
printf '%s\n' OPAQUE_ARCHIVE_COLLECTED_NOT_VERIFIED > "$out/collection.closed"
