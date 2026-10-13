#!/usr/bin/env bash
set -euo pipefail
base=/data/target/borsuk-source-width128.pending/cold-launch-a0002
out=$base/collected
prefix=research/semantic-router/20261013/source-width128-scale-cold-a0001/terminal
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
[[ $(cat "$base/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
mkdir "$out"
get() { timeout -k 2 60 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$1" "$out/$1" > "$out/$1.get.json"; }
get terminal.exit
get evidence.tar.gz.sha256
get evidence.tar.gz
[[ $(stat -c %s "$out/evidence.tar.gz") -le 335544320 ]]
read -r hash path < "$out/evidence.tar.gz.sha256"
[[ $hash =~ ^[0-9a-f]{64}$ && $path == /mnt/borsuk-source-width64/evidence.tar.gz ]]
printf '%s  %s\n' "$hash" "$out/evidence.tar.gz" | sha256sum -c - > "$out/archive-check.log"
tar -tzf "$out/evidence.tar.gz" > "$out/archive-members.txt"
extract() {
 local member=$1 target=$2
 [[ $(grep -Fxc "$member" "$out/archive-members.txt") == 1 ]]
 tar -xOzf "$out/evidence.tar.gz" "$member" > "$out/$target"
}
extract evidence/terminal/terminal.json campaign-terminal.json
if [[ $(cat "$out/terminal.exit") != 0 ]]; then
 printf '%s\n' CLOSED_INVALID_OPAQUE_ARCHIVE_AUTHENTICATED_DIAGNOSIS_PENDING > "$out/collection.status"
 exit 0
fi
extract evidence/stages.tsv stages.tsv
mkdir "$out/stage-metadata"
while IFS=$'\t' read -r phase kind pin; do
 [[ $phase =~ ^[a-z0-9-]+$ && $pin =~ ^[0-9a-f]{64}$ ]]
 for file in supervision.json manager-exit.txt native-exit.txt memory.peak.after memory.events.after memory.swap.peak.after cpu.max.before cpuset.cpus.effective.before memory.max.before memory.swap.max.before pids.max.before binary-before.log binary-after.log config-before.log config-after.log input-before.log input-after.log native-time.txt; do
  extract "evidence/$phase/$file" "stage-metadata/$phase.$file"
 done
done < "$out/stages.tsv"
# Only Rust-produced aggregate reports. Never extract formal raw query rows.
for cell in original-a1 candidate-b1 candidate-b2 original-a2; do
 extract "results/completed-formal-$cell.jsonl" "$cell.native-completed-report.json"
done
for repetition in 1 2; do
 extract "results/paired-$repetition.jsonl" "paired-$repetition.native-report.json"
done
printf '%s\n' CLOSED_OPAQUE_ARCHIVE_AUTHENTICATED_AGGREGATES_EXTRACTED_VERIFICATION_PENDING > "$out/collection.status"
