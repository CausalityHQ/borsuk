#!/usr/bin/env bash
# Native rows and perf.data remain opaque; extract completed native summaries only.
set -euo pipefail
out=/data/target/borsuk-source-width64.pending/cpu-profile-a0004/launch
[[ $(cat "$out/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
[[ ! -e $out/collected ]]
mkdir -m 700 "$out/collected"
ev=$out/collected
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
prefix=research/semantic-router/20261013/source-width64-cpu-profile-a0004/terminal
for file in terminal.exit evidence.tar.gz.sha256 evidence.tar.gz; do
 timeout -k 2 90 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$file" "$ev/$file" > "$ev/$file.get.json" 2> "$ev/$file.get.stderr"
done
[[ $(cat "$ev/terminal.exit") == 0 ]]
[[ $(stat -c %s "$ev/evidence.tar.gz") -le 335544320 ]]
read -r sha path < "$ev/evidence.tar.gz.sha256"
[[ $sha =~ ^[0-9a-f]{64}$ && $path == /mnt/borsuk-source-width64/evidence.tar.gz ]]
printf '%s  %s\n' "$sha" "$ev/evidence.tar.gz" | sha256sum -c - > "$ev/archive-sha-check.txt"
tar -tzf "$ev/evidence.tar.gz" > "$ev/archive-members.txt"
extract() {
 local member=$1 destination=$2 count
 count=$(grep -Fxc "$member" "$ev/archive-members.txt")
 [[ $count == 1 && ! -e $ev/$destination ]]
 timeout -k 2 30 tar -xOzf "$ev/evidence.tar.gz" "$member" > "$ev/$destination"
 [[ -s $ev/$destination && $(stat -c %s "$ev/$destination") -le 16777216 ]]
}
extract evidence/terminal/terminal.json terminal.json
extract evidence/stages.tsv stages.tsv
extract perf-admission/canary-report.txt canary-report.txt
extract perf-admission/version.txt perf-version.txt
extract perf-admission/package-version.txt perf-package-version.txt
extract perf-admission/kernel.txt kernel.txt
extract perf.path perf.path
extract perf.sha256 perf.sha256
extract evidence/formal-candidate-b1/perf-self-report.txt perf-self-report.txt
extract evidence/formal-candidate-b1/perf-inclusive-report.txt perf-inclusive-report.txt
extract evidence/formal-candidate-b1/native.log profile-native.log
extract evidence/formal-candidate-b1/native-time.txt profile-native-time.txt
extract evidence/formal-candidate-b1/perf.data.sha256 perf.data.sha256
phases=(admission-candidate completed-admission-candidate canary-candidate completed-canary-candidate formal-candidate-b1 completed-formal-candidate-b1)
for phase in "${phases[@]}"; do
 extract "evidence/$phase/supervision.json" "$phase.supervision.json"
 extract "evidence/$phase/memory.peak.after" "$phase.memory.peak"
 extract "evidence/$phase/memory.events.after" "$phase.memory.events"
 extract "evidence/$phase/cpu.max.before" "$phase.cpu.max"
 extract "evidence/$phase/cpuset.cpus.effective.before" "$phase.cpuset"
 extract "evidence/$phase/memory.max.before" "$phase.memory.max"
 extract "evidence/$phase/memory.swap.max.before" "$phase.memory.swap.max"
 extract "evidence/$phase/pids.max.before" "$phase.pids.max"
 extract "evidence/$phase/binary-before.log" "$phase.binary-before.log"
 extract "evidence/$phase/binary-after.log" "$phase.binary-after.log"
 extract "evidence/$phase/config-before.log" "$phase.config-before.log"
 extract "evidence/$phase/config-after.log" "$phase.config-after.log"
 extract "evidence/$phase/input-before.log" "$phase.input-before.log"
 extract "evidence/$phase/input-after.log" "$phase.input-after.log"
 if [[ $phase == completed-* ]]; then
  extract "results/$phase.jsonl" "$phase.native-report.json"
 fi
done
sha256sum "$ev"/*.json "$ev"/*.txt > "$ev/selected-artifacts.sha256"
printf '%s\n' CLOSED_SMALL_NATIVE_AND_PERF_EVIDENCE_COLLECTED > "$ev/collection.status"
