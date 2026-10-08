# UNFROZEN draft: no binary, real admission, canary or runtime launch authority.
#!/bin/bash
set -euo pipefail
test -f /mnt/borsuk-retained-s3/FROZEN_DIRECT_CLOSURE_RUNTIME || exit 98
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
  for arm in A B; do
    scratch="$root/direct-closure-admission-a0001/$arm/query-scratch"
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

sha256sum -c "$root/pins.sha256" > "$evidence/A.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/A.started"
set +e
/usr/bin/time -v -o "$evidence/A.time" timeout --kill-after=10 1200 "$root/runner" "$root/A.config.json" 691918ec91f67e14a3bbd23ae720c300a77328663c3ba58c600716534c1361fb "$evidence/A.jsonl" > "$evidence/A.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/A.native-exit"
date -u +%FT%T.%NZ > "$evidence/A.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/A.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/B.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/B.started"
set +e
/usr/bin/time -v -o "$evidence/B.time" timeout --kill-after=10 1200 "$root/runner" "$root/B.config.json" beac89c95274f9c33b863cbbff7f9be0dbb20626c31c1f242f755c1b8bf775c0 "$evidence/B.jsonl" > "$evidence/B.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/B.native-exit"
date -u +%FT%T.%NZ > "$evidence/B.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/B.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
exit 0
