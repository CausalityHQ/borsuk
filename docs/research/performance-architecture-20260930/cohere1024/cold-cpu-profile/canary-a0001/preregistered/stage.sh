#!/bin/bash
set -euo pipefail
# Disposable profiler/CLI admission only; no ANN or performance measurement.
root=/mnt/borsuk-retained-s3
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  status=$original
  for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
    if test -f /sys/fs/cgroup${cg}/$field; then cat /sys/fs/cgroup${cg}/$field > "$evidence/$field.after" || status=96; fi
  done
  sha256sum -c "$root/pins.sha256" > "$evidence/pins-after.txt" || status=96
  find "$root/membership-admission-a0001/query-scratch" -mindepth 1 -printf '%p\n' > "$evidence/scratch-files.txt" || status=96
  if test -s "$evidence/scratch-files.txt"; then status=96; fi
  printf '%s\n' "$original" > "$evidence/original-exit"
  printf '%s\n' "$status" > "$evidence/final-exit"
  exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM
for field in cpu.max memory.max memory.swap.max pids.max; do
  cat "/sys/fs/cgroup${cg}/$field" > "$evidence/$field.before"
done
test "$(cat "$evidence/cpu.max.before")" = '100000 100000'
test "$(cat "$evidence/memory.max.before")" = 536870912
test "$(cat "$evidence/memory.swap.max.before")" = 0
test "$(cat "$evidence/pids.max.before")" = 256
sha256sum -c "$root/pins.sha256" > "$evidence/pins-before.txt"
perf version > "$evidence/perf-version.txt"
uname -a > "$evidence/kernel.txt"
cat /proc/sys/kernel/perf_event_paranoid > "$evidence/perf-event-paranoid.txt"
test ! -e "$evidence/perf.data"
# File limit stops excessive profiler output; an interrupted/partial profile fails.
(
  ulimit -f 65536
  timeout --kill-after=5 60 perf record -e cpu-clock:u -F 99 --strict-freq \
    --call-graph dwarf,8192 --mmap-pages=64 -o "$evidence/perf.data" -- \
    /bin/bash -c 'for ((i=0;i<128;i++)); do sha256sum "$1" >/dev/null || exit; done' _ "$root/runner"
) > "$evidence/perf-record.log" 2>&1
test "$(stat -c %s "$evidence/perf.data")" -le 67108864
timeout --kill-after=5 30 perf report -i "$evidence/perf.data" --stdio --no-children \
  --sort dso,symbol --percent-limit 0 > "$evidence/perf-report.txt" 2> "$evidence/perf-report.stderr"
timeout --kill-after=5 30 perf script -i "$evidence/perf.data" > "$evidence/perf-samples.txt" 2> "$evidence/perf-script.stderr"
grep -q sha256sum "$evidence/perf-samples.txt"
# Record-only platform success is insufficient; prove samples decode to userspace symbols.
grep -Eq 'libcrypto|libc|sha256sum|bash' "$evidence/perf-report.txt"
export RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1 BORSUK_CPU_THREADS=1
set +e
timeout --kill-after=5 20 "$root/runner" "$root/config.json" \
  0000000000000000000000000000000000000000000000000000000000000000 \
  "$evidence/cli-invalid.jsonl" > "$evidence/cli-invalid.log" 2>&1
native_exit=$?
set -e
printf '%s\n' "$native_exit" > "$evidence/native-exit"
test "$native_exit" = 2
jq -se 'length == 2 and .[0].phase == "identity" and .[1].phase == "terminal" and .[1].summary.status == "INVALID" and .[1].summary.stage == "config" and .[1].summary.error == "artifact identity/SHA" and .[1].summary.completed_queries == 0 and .[1].summary.truth_opened == false and .[1].summary.all_queries_sealed == false and .[1].summary.sum.submitted_gets == 0 and .[1].summary.binding_charge.submitted_gets == 0' \
  "$evidence/cli-invalid.jsonl" > "$evidence/cli-invalid.check"
sha256sum -c "$root/pins.sha256" > "$evidence/pins-after.txt"
printf 'PROFILER_PLATFORM_AND_CLI_CANARY_PASS_NO_ANN\n' > "$evidence/canary.status"
