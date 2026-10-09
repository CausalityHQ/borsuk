#!/bin/bash
set -euo pipefail
test -f /mnt/borsuk-retained-s3/FROZEN_SQ8_ATTRIBUTION_ADMISSION || exit 98
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
  : > "$evidence/scratch-files.txt"
  for arm in A_first-untraced A_first-trace B_first-untraced B_first-trace B_last-untraced B_last-trace A_last-untraced A_last-trace; do
    scratch="$root/sq8-attribution-admission-a0001/$arm/query-scratch"
    if test -d "$scratch"; then
      find "$scratch" -mindepth 1 -printf '%p\n' >> "$evidence/scratch-files.txt" || { if (( status == 0 )); then status=96; fi; }
    fi
  done
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

sha256sum -c "$root/pins.sha256" > "$evidence/A_first-untraced.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/A_first-untraced.started"
set +e
/usr/bin/time -v -o "$evidence/A_first-untraced.time" timeout --kill-after=10 300 "$root/runner" "$root/A_first-untraced.config.json" 456a2245ff414d497b81427c5cc29e931d07bc2456ebc921f6db4d1db4a2e9a5 "$evidence/A_first-untraced.jsonl" > "$evidence/A_first-untraced.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/A_first-untraced.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/A_first-untraced.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/A_first-untraced.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/A_first-trace.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/A_first-trace.started"
set +e
/usr/bin/time -v -o "$evidence/A_first-trace.time" timeout --kill-after=10 300 "$root/runner" "$root/A_first-trace.config.json" 1798d86af0fa04e1ba5ae73bb90bcd0eab77a1b8bd2df0761da925ee12162ee8 "$evidence/A_first-trace.jsonl" > "$evidence/A_first-trace.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/A_first-trace.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/A_first-trace.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/A_first-trace.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/B_first-untraced.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/B_first-untraced.started"
set +e
/usr/bin/time -v -o "$evidence/B_first-untraced.time" timeout --kill-after=10 300 "$root/runner" "$root/B_first-untraced.config.json" c640852f29a8d3e90135775c031387feb84afb37a51e9445dc7f8731a030cdbe "$evidence/B_first-untraced.jsonl" > "$evidence/B_first-untraced.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/B_first-untraced.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/B_first-untraced.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/B_first-untraced.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/B_first-trace.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/B_first-trace.started"
set +e
/usr/bin/time -v -o "$evidence/B_first-trace.time" timeout --kill-after=10 300 "$root/runner" "$root/B_first-trace.config.json" cf4fdf981b279865d5d5949bbea63f8c01df3f27355b15e02f22039cfb5f9acd "$evidence/B_first-trace.jsonl" > "$evidence/B_first-trace.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/B_first-trace.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/B_first-trace.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/B_first-trace.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/B_last-untraced.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/B_last-untraced.started"
set +e
/usr/bin/time -v -o "$evidence/B_last-untraced.time" timeout --kill-after=10 300 "$root/runner" "$root/B_last-untraced.config.json" 7dca28380675a84fe56c2031819b546b2eab5951e6673288bfe28a6739330e71 "$evidence/B_last-untraced.jsonl" > "$evidence/B_last-untraced.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/B_last-untraced.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/B_last-untraced.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/B_last-untraced.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/B_last-trace.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/B_last-trace.started"
set +e
/usr/bin/time -v -o "$evidence/B_last-trace.time" timeout --kill-after=10 300 "$root/runner" "$root/B_last-trace.config.json" 0c150b56af62b134cea6c0d19e75493f9105f7cbd52e8a63a4c435005b77b93d "$evidence/B_last-trace.jsonl" > "$evidence/B_last-trace.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/B_last-trace.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/B_last-trace.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/B_last-trace.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/A_last-untraced.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/A_last-untraced.started"
set +e
/usr/bin/time -v -o "$evidence/A_last-untraced.time" timeout --kill-after=10 300 "$root/runner" "$root/A_last-untraced.config.json" 4bf9a3eb7f8da4f374cd6d001add82ddba57ce0e103050bb80fda44dda06c4af "$evidence/A_last-untraced.jsonl" > "$evidence/A_last-untraced.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/A_last-untraced.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/A_last-untraced.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/A_last-untraced.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/A_last-trace.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/A_last-trace.started"
set +e
/usr/bin/time -v -o "$evidence/A_last-trace.time" timeout --kill-after=10 300 "$root/runner" "$root/A_last-trace.config.json" 39851dae1f57b97f51135d5e63f24a01c376b8f8520dde6ccac2ebb4c97041a2 "$evidence/A_last-trace.jsonl" > "$evidence/A_last-trace.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/A_last-trace.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/A_last-trace.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/A_last-trace.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
exit 0
