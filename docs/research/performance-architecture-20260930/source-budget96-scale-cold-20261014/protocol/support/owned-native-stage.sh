#!/usr/bin/env bash
# EC2-only experiment glue; validation and result parsing belong to Rust.
set -euo pipefail
[[ $# == 6 && $1 == /mnt/borsuk-source-width64 ]] || exit 125
root=$1; phase=$2; kind=$3; config=$4; sha=$5; cap=$6
[[ $phase =~ ^(admission|canary)-(original|candidate)$ || $phase =~ ^formal-(original|candidate)-(a1|a2|b1|b2)$ || $phase =~ ^completed-((admission|canary)-(original|candidate)|formal-(original|candidate)-(a1|a2|b1|b2))$ || $phase =~ ^source-budget-abba$ ]] || exit 125
[[ $config == "$root/"* && -f $config && ! -L $config && $sha =~ ^[0-9a-f]{64}$ ]] || exit 125
[[ $cap =~ ^[0-9]+$ && $cap -ge 1 && $cap -le 1800 ]] || exit 125
case "$phase" in
 admission-*|canary-*) [[ $kind == baseline && $cap == 60 ]] || exit 125;;
 formal-*) [[ $kind == baseline && $cap == 1800 ]] || exit 125;;
 completed-*) [[ $kind == completed && $cap == 180 ]] || exit 125;;
 source-budget-abba) [[ $kind == abba && $cap == 180 ]] || exit 125;;
esac
case "$kind" in
 baseline)
  binary=$root/bin/check_cohere_native_baseline; pin=59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083
  memory=1073741824; args=();;
 completed) binary=$root/bin/compare_native_replay; pin=aad72c4f32de6f266590b392610288e02bab38bd209b4c0c34307da959782571; memory=1073741824; args=(--completed-scale);;
 abba) [[ $phase =~ ^source-budget-abba$ ]] || exit 125; binary=$root/bin/compare_native_replay; pin=aad72c4f32de6f266590b392610288e02bab38bd209b4c0c34307da959782571; memory=1073741824; args=(--source-budget-abba-scale);;
 *) exit 125;;
esac
[[ $pin =~ ^[0-9a-f]{64}$ ]] || exit 125
ev=$root/evidence/$phase
[[ -d $ev && ! -L $ev ]] || exit 125
cg=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-source-width64-"$phase".service ]] || exit 125
for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$file" > "$ev/$file.before"; done
[[ $(cat "$ev/cpu.max.before") == '100000 100000' && $(cat "$ev/cpuset.cpus.effective.before") == 0 && $(cat "$ev/memory.max.before") == "$memory" && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 128 ]] || exit 125
printf '%s  %s\n' "$pin" "$binary" | sha256sum -c - > "$ev/binary-before.log"
printf '%s  %s\n' "$sha" "$config" | sha256sum -c - > "$ev/config-before.log"
sha256sum -c "$root/input-files.sha256" > "$ev/input-before.log"
[[ -f $root/machine-stop.epoch ]] || exit 125
stop=$(cat "$root/machine-stop.epoch"); [[ $stop =~ ^[0-9]+$ && $stop -gt $(( $(date +%s)+cap+180 )) ]] || exit 125
output=$root/results/$phase.jsonl
[[ ! -e $output && ! -L $output ]] || exit 125
set +e
/usr/bin/time -v -o "$ev/native-time.txt" /usr/bin/timeout -k 20 "$cap" "$binary" "${args[@]}" "$config" "$sha" "$output" > "$ev/native.log" 2>&1
native=$?
set -e
printf '%s\n' "$native" > "$ev/native-exit.txt"
for file in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if [[ -f /sys/fs/cgroup$cg/$file ]]; then cat "/sys/fs/cgroup$cg/$file" > "$ev/$file.after"; fi; done
printf '%s  %s\n' "$pin" "$binary" | sha256sum -c - > "$ev/binary-after.log"
printf '%s  %s\n' "$sha" "$config" | sha256sum -c - > "$ev/config-after.log"
sha256sum -c "$root/input-files.sha256" > "$ev/input-after.log"
[[ $native == 0 ]] || exit 1
[[ -f $output && ! -L $output ]] || exit 1
sha256sum "$output" > "$ev/result.sha256"
stat -c %s "$output" > "$ev/result.bytes"
