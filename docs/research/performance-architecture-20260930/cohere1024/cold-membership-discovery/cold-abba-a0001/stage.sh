#!/bin/bash
set -euo pipefail
test -f /mnt/borsuk-retained-s3/FROZEN_MEMBERSHIP_ABBA || exit 98
root=/mnt/borsuk-retained-s3
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
    if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > "$evidence/$field.after"; fi
  done
  status=$original
  sha256sum -c "$root/pins.sha256" > "$evidence/pins-after.txt" || { if (( status == 0 )); then status=96; fi; }
  find "$root/membership-abba-a0001/query-scratch" -mindepth 1 -printf '%p\n' > "$evidence/scratch-files.txt" || { if (( status == 0 )); then status=96; fi; }
  if test -s "$evidence/scratch-files.txt" && (( status == 0 )); then status=96; fi
  printf '%s\n' "$original" > "$evidence/original-exit"
  printf '%s\n' "$status" > "$evidence/final-exit"
  exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM
for field in cpu.max memory.max memory.swap.max pids.max; do cat /sys/fs/cgroup${cg}/$field > $evidence/$field.before; done
test "$(cat $evidence/cpu.max.before)" = '100000 100000'
test "$(cat $evidence/memory.max.before)" = 536870912
test "$(cat $evidence/memory.swap.max.before)" = 0
test "$(cat $evidence/pids.max.before)" = 256
sha256sum -c $root/pins.sha256 > $evidence/pins-before.txt
export RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1 BORSUK_CPU_THREADS=1
status=0
sha256sum -c $root/pins.sha256 > $evidence/A1.pins-before.txt
date -u +%FT%T.%NZ > "$evidence/A1.started"
set +e
/usr/bin/time -v -o $evidence/A1.time timeout --kill-after=10 1200 $root/control $root/config.json 0fd76aacea19b51e221f5dfb5d7d42ab0190fca51fe5f95a4ecf2942abad4f92 $evidence/A1.jsonl > $evidence/A1.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/A1.native-exit
date -u +%FT%T.%NZ > "$evidence/A1.finished"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/A1.pins-after.txt
sha256sum -c $root/pins.sha256 > $evidence/B1.pins-before.txt
date -u +%FT%T.%NZ > "$evidence/B1.started"
set +e
/usr/bin/time -v -o $evidence/B1.time timeout --kill-after=10 1200 $root/candidate $root/config.json 0fd76aacea19b51e221f5dfb5d7d42ab0190fca51fe5f95a4ecf2942abad4f92 $evidence/B1.jsonl > $evidence/B1.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/B1.native-exit
date -u +%FT%T.%NZ > "$evidence/B1.finished"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/B1.pins-after.txt
sha256sum -c $root/pins.sha256 > $evidence/B2.pins-before.txt
date -u +%FT%T.%NZ > "$evidence/B2.started"
set +e
/usr/bin/time -v -o $evidence/B2.time timeout --kill-after=10 1200 $root/candidate $root/config.json 0fd76aacea19b51e221f5dfb5d7d42ab0190fca51fe5f95a4ecf2942abad4f92 $evidence/B2.jsonl > $evidence/B2.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/B2.native-exit
date -u +%FT%T.%NZ > "$evidence/B2.finished"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/B2.pins-after.txt
sha256sum -c $root/pins.sha256 > $evidence/A2.pins-before.txt
date -u +%FT%T.%NZ > "$evidence/A2.started"
set +e
/usr/bin/time -v -o $evidence/A2.time timeout --kill-after=10 1200 $root/control $root/config.json 0fd76aacea19b51e221f5dfb5d7d42ab0190fca51fe5f95a4ecf2942abad4f92 $evidence/A2.jsonl > $evidence/A2.log 2>&1
status=$?
set -e
printf '%s\n' "$status" > $evidence/A2.native-exit
date -u +%FT%T.%NZ > "$evidence/A2.finished"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c $root/pins.sha256 > $evidence/A2.pins-after.txt
printf "%s\n" "$status" > $evidence/native-exit
# Root must freeze the repaired native contract and exact template before enabling this draft.
cp "$root/reduction-template.json" "$evidence/reduction.config.json"
labels=(A1 B1 B2 A2)
for index in 0 1 2 3; do
  label=${labels[$index]}
  path="$evidence/$label.jsonl"
  bytes=$(stat -c %s "$path")
  sha=$(sha256sum "$path" | cut -d' ' -f1)
  jq --argjson i "$index" --arg path "$path" --argjson bytes "$bytes" --arg sha "$sha" '.runs[$i].input = {path:$path,bytes:$bytes,sha256:$sha}' "$evidence/reduction.config.json" > "$evidence/reduction.config.tmp"
  mv "$evidence/reduction.config.tmp" "$evidence/reduction.config.json"
done
config_sha=$(sha256sum "$evidence/reduction.config.json" | cut -d' ' -f1)
date -u +%FT%T.%NZ > "$evidence/reduction.started"
set +e
/usr/bin/time -v -o "$evidence/reduction.time" timeout --kill-after=10 180 "$root/reducer" --membership-abba-v2 "$evidence/reduction.config.json" "$config_sha" "$evidence/reduction.report.json" > "$evidence/reduction.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/reduction.native-exit"
date -u +%FT%T.%NZ > "$evidence/reduction.finished"
if (( status != 0 )); then exit "$status"; fi
jq -e '.schema == "borsuk-membership-abba-native-reduction-v1" and .status == "MEASURED" and .complete == true and .semantic_parity == true and .nomination_trace_parity == true and .source_sq8_charge_parity == true and (.runs|length)==4 and (.comparisons|length)==2 and (.performance_screen_passed|type)=="boolean"' "$evidence/reduction.report.json" > "$evidence/reduction.check.txt"
exit 0
