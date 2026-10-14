#!/usr/bin/env bash
set -euo pipefail
base=/data/target/borsuk-byte-window-interval.pending
root=$base/launch-green-a0002
source=5d85c7577da4a10b7a3907b40f03bfb8eb7181f0
[[ $(cat "$root/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
awk '{print $2 ": OK"}' "$base/package-green-a0002/source-files.sha256" > "$root/expected-source.log"
while IFS=$'\t' read -r stage kind command; do
 out=$root/$stage-collected
 (cd "$out" && sha256sum --strict -c SHA256SUMS) > "$out/independent-manifest-check.log"
 jq -e --arg source "$source" --arg stage "$stage" '.source_commit==$source and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true and .performance_claim==false' "$out/terminal.json"
 cmp "$root/expected-source.log" "$out/source-before.log"
 cmp "$out/source-before.log" "$out/source-after.log"
 [[ $(cat "$out/native-command.txt") == "$command" ]]
 grep -qx '0' "$out/memory.swap.peak.after"
 grep -qx 'oom 0' "$out/memory.events.after"
 grep -qx 'oom_kill 0' "$out/memory.events.after"
 grep -qx '400000 100000' "$out/cpu.max.before"
 grep -qx '0-3' "$out/cpuset.cpus.effective.before"
 grep -qx '15032385536' "$out/memory.max.before"
 grep -qx '0' "$out/memory.swap.max.before"
 grep -qx '512' "$out/pids.max.before"
 if [[ $kind == listed ]]; then
  count=$(grep -c ': test$' "$out/native-list.log")
  (( count>0 ))
  case "$stage" in interval-max) [[ $count == 1 ]];; interval-report) [[ $count == 3 ]];; esac
  [[ $(grep -Ec "^test result: ok[.] $count passed; 0 failed; 0 ignored; 0 measured;" "$out/native.log") == 1 ]]
  while IFS= read -r name; do [[ $(grep -Fxc "test $name ... ok" "$out/native.log") == 1 ]]; done < <(sed -n 's/: test$//p' "$out/native-list.log")
 fi
done < "$base/assets-green-a0002/stage-roster.tsv"
[[ $(wc -l < "$root/release-binaries-collected/release-artifacts.tsv") == 1 ]]
while IFS=$'\t' read -r role bytes sha; do
 [[ $role == compare_native_replay ]]
 [[ $bytes =~ ^[1-9][0-9]*$ && $sha =~ ^[0-9a-f]{64}$ ]]
done < "$root/release-binaries-collected/release-artifacts.tsv"
[[ $(cut -f1 "$root/release-binaries-collected/release-artifacts.tsv") == compare_native_replay ]]
printf '%s\n' SEVEN_NATIVE_GREEN_GATES_AND_SOURCE_RESOURCE_COMPUTE_CLOSURE_VERIFIED
