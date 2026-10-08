#!/bin/bash
set -euo pipefail
# Candidate-only CPU diagnosis; instrumented latency is not comparison evidence.
root=/mnt/borsuk-retained-s3
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  status=$original
  if test -f "$evidence/perf.data"; then
    sha256sum "$evidence/perf.data" > "$evidence/perf-data.sha256" || status=96
    stat -c %s "$evidence/perf.data" > "$evidence/perf-data.bytes" || status=96
    rm -f -- "$evidence/perf.data" || status=96
  fi
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
export RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1 BORSUK_CPU_THREADS=1
set +e
(
  ulimit -f 131072
  /usr/bin/time -v -o "$evidence/profile.time" timeout --kill-after=10 1200 \
    perf record -e cpu-clock:u -F 99 --strict-freq --call-graph dwarf,8192 \
    --mmap-pages=64 -o "$evidence/perf.data" -- /bin/bash -c \
    'set +e; "$1" "$2" "$3" "$4"; code=$?; printf "%s\n" "$code" > "$5"; exit "$code"' \
    _ "$root/runner" "$root/config.json" \
    e604a1ca02644329113e6c11a935d67115f784c40008859966f7e332fae53a3a \
    "$evidence/profile-result.jsonl" "$evidence/native-exit"
) > "$evidence/profile.log" 2>&1
record_exit=$?
set -e
printf '%s\n' "$record_exit" > "$evidence/perf-record.exit"
test "$record_exit" = 0
test "$(cat "$evidence/native-exit")" = 0
test "$(stat -c %s "$evidence/perf.data")" -le 134217728
# Bound analysis files too; retain raw CPU samples for independently checked attribution.
(
  ulimit -f 131072
  timeout --kill-after=5 180 perf report -i "$evidence/perf.data" --stdio \
    --no-children --sort dso,symbol --percent-limit 0.1 > "$evidence/perf-self.txt" 2> "$evidence/perf-self.stderr"
  timeout --kill-after=5 180 perf report -i "$evidence/perf.data" --stdio \
    --children --sort symbol --percent-limit 0.1 > "$evidence/perf-children.txt" 2> "$evidence/perf-children.stderr"
  timeout --kill-after=5 180 perf script -i "$evidence/perf.data" > "$evidence/perf-samples.txt" 2> "$evidence/perf-script.stderr"
)
grep -q '# Total Lost Samples: 0' "$evidence/perf-self.txt"
# Reuse the already qualified full native closure reducer on all 1000 results.
cp "$root/reduction-template.json" "$evidence/reduction.config.json"
bytes=$(stat -c %s "$evidence/profile-result.jsonl")
sha=$(sha256sum "$evidence/profile-result.jsonl" | cut -d' ' -f1)
jq --arg path "$evidence/profile-result.jsonl" --argjson bytes "$bytes" --arg sha "$sha" \
  '.input = {path:$path,bytes:$bytes,sha256:$sha}' "$evidence/reduction.config.json" > "$evidence/reduction.config.tmp"
mv "$evidence/reduction.config.tmp" "$evidence/reduction.config.json"
config_sha=$(sha256sum "$evidence/reduction.config.json" | cut -d' ' -f1)
set +e
timeout --kill-after=10 180 "$root/reducer" --completed-v2 \
  "$evidence/reduction.config.json" "$config_sha" "$evidence/reduction.report.json" > "$evidence/reduction.log" 2>&1
reduction_exit=$?
set -e
printf '%s\n' "$reduction_exit" > "$evidence/reduction.native-exit"
test "$reduction_exit" = 0
jq -e '.status == "MEASURED" and .complete == true and .native_terminal.queries == 1000 and .native_terminal.recall_numerator == 9723 and .native_terminal.underfilled_queries == 0' \
  "$evidence/reduction.report.json" > "$evidence/reduction.check.txt"
printf 'CANDIDATE_CPU_PROFILE_CLOSED_NOT_COMPARISON_LATENCY\n' > "$evidence/profile.status"
