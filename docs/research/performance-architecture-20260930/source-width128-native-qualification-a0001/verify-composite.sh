#!/usr/bin/env bash
set -euo pipefail
root=/data/target/borsuk-source-width128.pending
first=$root/green-launch-a0002
last=$root/remaining-launch-a0001
rev=32bce58e1d981b26d9fa3bb5b2052dad7d6dc7bb
pkg=$root/package-green-repaired
[[ $(cat "$first/partial.verified") == THREE_EXACT_SOURCE_NATIVE_GATES_VERIFIED_ROSTER_DEFECT_PRESERVED_COMPUTE_CLOSED ]]
[[ $(cat "$first/instance.id") == i-011f3d091b10cce0d && $(cat "$first/volume.id") == vol-00338cd97f1215798 ]]
[[ $(cat "$last/instance.id") == i-0f09236f59f9eb80b && $(cat "$last/volume.id") == vol-095c0dcf096290021 ]]
jq -e '.CommandId=="c109f670-89f7-42cf-9ee0-0dcec105bd11" and .InstanceId=="i-011f3d091b10cce0d" and .Status=="Failed" and .ResponseCode==42' "$first/native-terminal.json" > /dev/null
for run in "$first" "$last"; do
 [[ $(cat "$run/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
 grep -qx ActiveState=inactive "$run/external-timer-final.txt"
 jq -e '.Reservations|length==1 and .[0].Instances[0].State.Name=="terminated"' "$run/instance-final.json" > /dev/null
 [[ $(cat "$run/volume-final.exit") == 254 ]]
 grep -Fq "$(cat "$run/volume.id")" "$run/volume-final.stderr"
 grep -Fq InvalidVolume.NotFound "$run/volume-final.stderr"
 (cd "$run/batch-collected" && sha256sum -c SHA256SUMS) > "$run/batch.rechecked.log"
done
jq -e '.CommandId=="99e49bec-8749-4087-911e-0e153254469b" and .InstanceId=="i-0f09236f59f9eb80b" and .Status=="Success" and .ResponseCode==0' "$last/native-terminal.json" > /dev/null
jq -e --arg rev "$rev" '.source_commit==$rev and .batch_exit==0 and .completed_stages==5 and .required_stages==5 and .failed_stage==""' "$last/batch-collected/terminal.json" > /dev/null
cut -f1 "$last/batch-collected/stages.tsv" > "$last/actual-stage-order.txt"
tail -5 "$root/composite-gates.tsv" | cut -f1 > "$last/expected-stage-order.txt"
cmp "$last/actual-stage-order.txt" "$last/expected-stage-order.txt"
awk -F '\t' 'NF!=2||$2!=0{bad=1}END{exit(bad||NR!=5)}' "$last/batch-collected/stages.tsv"
: > "$root/qualified-stages.jsonl"
while IFS=$'\t' read -r stage command; do
 run=$last
 case "$stage" in compile-affected|range-regressions|source-regressions) run=$first;; esac
 ev=$run/$stage-collected
 (cd "$ev" && sha256sum -c SHA256SUMS) > "$run/verify-$stage.manifest.log"
 jq -e --arg rev "$rev" --arg stage "$stage" '.source_commit==$rev and .stage==$stage and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$ev/terminal.json" > /dev/null
 [[ $(cat "$ev/native-command.txt") == "$command" ]]
 [[ $(cat "$ev/cpu.max.before") == '400000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0-3 && $(cat "$ev/memory.max.before") == 15032385536 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 512 ]]
 [[ $(cat "$ev/memory.peak.after") -le 15032385536 && $(cat "$ev/memory.swap.peak.after") == 0 && $(cat "$ev/pids.peak.after") -le 512 ]]
 awk '$1=="oom"||$1=="oom_kill"||$1=="oom_group_kill"{if($2!=0)bad=1;n++}END{exit(bad||n!=3)}' "$ev/memory.events.after"
 cmp "$ev/source-before.log" "$ev/source-after.log"
 cmp "$ev/source-before.log" "$pkg/source-verified.log"
 [[ $(wc -l < "$ev/source-before.log") == 470 ]]
 grep -Fxq 'CARGO_BUILD_JOBS=1; BORSUK_CPU_THREADS=1; RAYON_NUM_THREADS=1' "$ev/build-configuration.txt"
 case "$stage" in
  range-regressions|source-regressions|baseline-regressions|example-regressions)
   count=22
   case "$stage" in source-regressions) count=32;; baseline-regressions) count=44;; example-regressions) count=174;; esac
   sed -n 's/: test$//p' "$ev/native-list.log" | LC_ALL=C sort > "$run/$stage.compiled.txt"
   awk '$1=="test"&&$3=="..."&&$4=="ok"{print $2}' "$ev/native.log" | LC_ALL=C sort > "$run/$stage.passed.txt"
   [[ $(wc -l < "$run/$stage.compiled.txt") == "$count" ]]
   cmp "$run/$stage.compiled.txt" "$run/$stage.passed.txt"
   [[ $(grep -Ec "^test result: ok[.] $count passed; 0 failed; 0 ignored; 0 measured;" "$ev/native.log") == 1 ]]
   case "$stage" in
    range-regressions) jq -r '."range-regressions"[]' "$root/green-assets-a0002/required-tests.json" | LC_ALL=C sort > "$run/$stage.expected.txt"; cmp "$run/$stage.expected.txt" "$run/$stage.compiled.txt";;
    source-regressions) cmp "$first/corrected-source-roster.txt" "$run/$stage.compiled.txt";;
    baseline-regressions) grep -Fxq tests::cold_fetch_parallelism128_preserves_caps_and_one_source_wave "$run/$stage.compiled.txt";;
    example-regressions) grep -Fxq tests::paired_width128_scale_requires_same_source_and_ordered_widths "$run/$stage.compiled.txt";;
   esac;;
 esac
 sha=$(sha256sum "$ev/terminal.json"); sha=${sha%% *}
 jq -cn --arg name "$stage" --arg command "$command" --arg path "$ev/terminal.json" --arg sha "$sha" --arg instance "$(cat "$run/instance.id")" --arg session "$(cat "$run/native-command.id")" --argjson bytes "$(stat -c %s "$ev/terminal.json")" '{name:$name,exit:0,command:$command,manager_exit:0,owned_cgroup_drained:true,instance_id:$instance,command_id:$session,receipt:{path:$path,bytes:$bytes,sha256:$sha}}' >> "$root/qualified-stages.jsonl"
done < "$root/composite-gates.tsv"
[[ $(wc -l < "$root/qualified-stages.jsonl") == 8 ]]
jq -e 'length==437' "$pkg/native-source.json" > /dev/null
release=$last/release-elf-collected
for binary in check_cohere_native_baseline compare_native_replay; do
 pin=$release/release.sha256; bytes=$release/release.bytes
 if [[ $binary == compare_native_replay ]]; then pin=$release/compare_native_replay.sha256; bytes=$release/compare_native_replay.bytes; fi
 read -r sha path < "$pin"
 [[ $sha =~ ^[0-9a-f]{64}$ && $path == /mnt/borsuk-paired-qualification/evidence/release-elf/"$binary" ]]
 [[ $(stat -c %s "$release/$binary") == "$(cat "$bytes")" ]]
 printf '%s  %s\n' "$sha" "$release/$binary" | sha256sum -c - > "$last/$binary.verified"
done
printf '%s\n' ALL_EIGHT_EXACT_SOURCE_NATIVE_GATES_BOTH_ARTIFACTS_AND_COMPUTE_CLOSURES_VERIFIED > "$root/composite.native.verified"
