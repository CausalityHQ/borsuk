#!/bin/bash
set -euo pipefail
test -f /mnt/borsuk-retained-s3/FROZEN_SQ8_ATTRIBUTION_CANARY || exit 98
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
    scratch="$root/sq8-attribution-canary-a0001/$arm/query-scratch"
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

cat /sys/devices/system/clocksource/clocksource0/current_clocksource > "$evidence/clocksource.before" 2>/dev/null || printf 'unavailable\n' > "$evidence/clocksource.before"
awk '/^Cpus_allowed_list:/ {print}' /proc/self/status > "$evidence/allowed-cpus.before"
cat /proc/sys/kernel/osrelease > "$evidence/kernel.before"
set +e
 timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 0000000000000000000000000000000000000000000000000000000000000000 "$evidence/negative-cli.jsonl" > "$evidence/negative-cli.log" 2>&1
negative=$?
set -e
printf '%s\n' "$negative" > "$evidence/negative-cli.native-exit"
if (( negative != 2 )); then exit 95; fi
if ! jq -es 'length==2 and .[0].phase=="identity" and .[0].config_sha256=="0000000000000000000000000000000000000000000000000000000000000000" and .[1].phase=="terminal" and (.[1].summary | .status=="INVALID" and .stage=="config" and .error=="artifact identity/SHA" and .completed_queries==0 and .truth_opened==false and .all_queries_sealed==false and ([.charges | .. | numbers] | all(.==0)))' "$evidence/negative-cli.jsonl" > "$evidence/negative-cli-validated.txt"; then exit 95; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q00-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q00-1.started"
set +e
/usr/bin/time -v -o "$evidence/q00-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q00-1.jsonl" > "$evidence/q00-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q00-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q00-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q00-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q00-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q00-2.started"
set +e
/usr/bin/time -v -o "$evidence/q00-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q00-2.jsonl" > "$evidence/q00-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q00-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q00-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q00-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q00-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q00-3.started"
set +e
/usr/bin/time -v -o "$evidence/q00-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q00-3.jsonl" > "$evidence/q00-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q00-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q00-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q00-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q00-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q00-4.started"
set +e
/usr/bin/time -v -o "$evidence/q00-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q00-4.jsonl" > "$evidence/q00-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q00-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q00-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q00-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q01-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q01-1.started"
set +e
/usr/bin/time -v -o "$evidence/q01-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q01-1.jsonl" > "$evidence/q01-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q01-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q01-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q01-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q01-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q01-2.started"
set +e
/usr/bin/time -v -o "$evidence/q01-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q01-2.jsonl" > "$evidence/q01-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q01-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q01-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q01-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q01-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q01-3.started"
set +e
/usr/bin/time -v -o "$evidence/q01-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q01-3.jsonl" > "$evidence/q01-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q01-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q01-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q01-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q01-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q01-4.started"
set +e
/usr/bin/time -v -o "$evidence/q01-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q01-4.jsonl" > "$evidence/q01-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q01-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q01-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q01-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q02-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q02-1.started"
set +e
/usr/bin/time -v -o "$evidence/q02-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q02-1.jsonl" > "$evidence/q02-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q02-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q02-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q02-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q02-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q02-2.started"
set +e
/usr/bin/time -v -o "$evidence/q02-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q02-2.jsonl" > "$evidence/q02-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q02-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q02-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q02-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q02-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q02-3.started"
set +e
/usr/bin/time -v -o "$evidence/q02-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q02-3.jsonl" > "$evidence/q02-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q02-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q02-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q02-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q02-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q02-4.started"
set +e
/usr/bin/time -v -o "$evidence/q02-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q02-4.jsonl" > "$evidence/q02-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q02-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q02-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q02-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q03-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q03-1.started"
set +e
/usr/bin/time -v -o "$evidence/q03-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q03-1.jsonl" > "$evidence/q03-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q03-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q03-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q03-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q03-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q03-2.started"
set +e
/usr/bin/time -v -o "$evidence/q03-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q03-2.jsonl" > "$evidence/q03-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q03-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q03-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q03-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q03-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q03-3.started"
set +e
/usr/bin/time -v -o "$evidence/q03-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q03-3.jsonl" > "$evidence/q03-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q03-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q03-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q03-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q03-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q03-4.started"
set +e
/usr/bin/time -v -o "$evidence/q03-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q03-4.jsonl" > "$evidence/q03-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q03-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q03-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q03-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q04-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q04-1.started"
set +e
/usr/bin/time -v -o "$evidence/q04-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q04-1.jsonl" > "$evidence/q04-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q04-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q04-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q04-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q04-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q04-2.started"
set +e
/usr/bin/time -v -o "$evidence/q04-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q04-2.jsonl" > "$evidence/q04-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q04-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q04-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q04-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q04-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q04-3.started"
set +e
/usr/bin/time -v -o "$evidence/q04-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q04-3.jsonl" > "$evidence/q04-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q04-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q04-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q04-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q04-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q04-4.started"
set +e
/usr/bin/time -v -o "$evidence/q04-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q04-4.jsonl" > "$evidence/q04-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q04-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q04-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q04-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q05-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q05-1.started"
set +e
/usr/bin/time -v -o "$evidence/q05-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q05-1.jsonl" > "$evidence/q05-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q05-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q05-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q05-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q05-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q05-2.started"
set +e
/usr/bin/time -v -o "$evidence/q05-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q05-2.jsonl" > "$evidence/q05-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q05-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q05-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q05-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q05-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q05-3.started"
set +e
/usr/bin/time -v -o "$evidence/q05-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q05-3.jsonl" > "$evidence/q05-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q05-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q05-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q05-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q05-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q05-4.started"
set +e
/usr/bin/time -v -o "$evidence/q05-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q05-4.jsonl" > "$evidence/q05-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q05-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q05-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q05-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q06-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q06-1.started"
set +e
/usr/bin/time -v -o "$evidence/q06-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q06-1.jsonl" > "$evidence/q06-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q06-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q06-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q06-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q06-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q06-2.started"
set +e
/usr/bin/time -v -o "$evidence/q06-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q06-2.jsonl" > "$evidence/q06-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q06-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q06-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q06-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q06-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q06-3.started"
set +e
/usr/bin/time -v -o "$evidence/q06-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q06-3.jsonl" > "$evidence/q06-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q06-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q06-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q06-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q06-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q06-4.started"
set +e
/usr/bin/time -v -o "$evidence/q06-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q06-4.jsonl" > "$evidence/q06-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q06-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q06-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q06-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q07-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q07-1.started"
set +e
/usr/bin/time -v -o "$evidence/q07-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q07-1.jsonl" > "$evidence/q07-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q07-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q07-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q07-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q07-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q07-2.started"
set +e
/usr/bin/time -v -o "$evidence/q07-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q07-2.jsonl" > "$evidence/q07-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q07-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q07-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q07-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q07-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q07-3.started"
set +e
/usr/bin/time -v -o "$evidence/q07-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q07-3.jsonl" > "$evidence/q07-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q07-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q07-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q07-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q07-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q07-4.started"
set +e
/usr/bin/time -v -o "$evidence/q07-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q07-4.jsonl" > "$evidence/q07-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q07-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q07-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q07-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q08-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q08-1.started"
set +e
/usr/bin/time -v -o "$evidence/q08-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q08-1.jsonl" > "$evidence/q08-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q08-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q08-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q08-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q08-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q08-2.started"
set +e
/usr/bin/time -v -o "$evidence/q08-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q08-2.jsonl" > "$evidence/q08-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q08-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q08-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q08-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q08-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q08-3.started"
set +e
/usr/bin/time -v -o "$evidence/q08-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q08-3.jsonl" > "$evidence/q08-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q08-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q08-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q08-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q08-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q08-4.started"
set +e
/usr/bin/time -v -o "$evidence/q08-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q08-4.jsonl" > "$evidence/q08-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q08-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q08-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q08-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q09-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q09-1.started"
set +e
/usr/bin/time -v -o "$evidence/q09-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q09-1.jsonl" > "$evidence/q09-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q09-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q09-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q09-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q09-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q09-2.started"
set +e
/usr/bin/time -v -o "$evidence/q09-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q09-2.jsonl" > "$evidence/q09-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q09-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q09-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q09-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q09-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q09-3.started"
set +e
/usr/bin/time -v -o "$evidence/q09-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q09-3.jsonl" > "$evidence/q09-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q09-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q09-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q09-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q09-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q09-4.started"
set +e
/usr/bin/time -v -o "$evidence/q09-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q09-4.jsonl" > "$evidence/q09-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q09-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q09-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q09-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q10-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q10-1.started"
set +e
/usr/bin/time -v -o "$evidence/q10-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q10-1.jsonl" > "$evidence/q10-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q10-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q10-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q10-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q10-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q10-2.started"
set +e
/usr/bin/time -v -o "$evidence/q10-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q10-2.jsonl" > "$evidence/q10-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q10-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q10-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q10-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q10-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q10-3.started"
set +e
/usr/bin/time -v -o "$evidence/q10-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q10-3.jsonl" > "$evidence/q10-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q10-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q10-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q10-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q10-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q10-4.started"
set +e
/usr/bin/time -v -o "$evidence/q10-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q10-4.jsonl" > "$evidence/q10-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q10-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q10-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q10-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q11-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q11-1.started"
set +e
/usr/bin/time -v -o "$evidence/q11-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q11-1.jsonl" > "$evidence/q11-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q11-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q11-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q11-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q11-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q11-2.started"
set +e
/usr/bin/time -v -o "$evidence/q11-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q11-2.jsonl" > "$evidence/q11-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q11-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q11-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q11-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q11-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q11-3.started"
set +e
/usr/bin/time -v -o "$evidence/q11-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q11-3.jsonl" > "$evidence/q11-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q11-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q11-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q11-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q11-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q11-4.started"
set +e
/usr/bin/time -v -o "$evidence/q11-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q11-4.jsonl" > "$evidence/q11-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q11-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q11-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q11-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q12-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q12-1.started"
set +e
/usr/bin/time -v -o "$evidence/q12-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q12-1.jsonl" > "$evidence/q12-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q12-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q12-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q12-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q12-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q12-2.started"
set +e
/usr/bin/time -v -o "$evidence/q12-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q12-2.jsonl" > "$evidence/q12-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q12-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q12-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q12-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q12-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q12-3.started"
set +e
/usr/bin/time -v -o "$evidence/q12-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q12-3.jsonl" > "$evidence/q12-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q12-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q12-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q12-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q12-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q12-4.started"
set +e
/usr/bin/time -v -o "$evidence/q12-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q12-4.jsonl" > "$evidence/q12-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q12-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q12-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q12-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q13-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q13-1.started"
set +e
/usr/bin/time -v -o "$evidence/q13-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q13-1.jsonl" > "$evidence/q13-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q13-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q13-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q13-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q13-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q13-2.started"
set +e
/usr/bin/time -v -o "$evidence/q13-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q13-2.jsonl" > "$evidence/q13-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q13-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q13-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q13-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q13-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q13-3.started"
set +e
/usr/bin/time -v -o "$evidence/q13-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q13-3.jsonl" > "$evidence/q13-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q13-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q13-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q13-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q13-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q13-4.started"
set +e
/usr/bin/time -v -o "$evidence/q13-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q13-4.jsonl" > "$evidence/q13-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q13-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q13-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q13-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q14-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q14-1.started"
set +e
/usr/bin/time -v -o "$evidence/q14-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q14-1.jsonl" > "$evidence/q14-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q14-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q14-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q14-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q14-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q14-2.started"
set +e
/usr/bin/time -v -o "$evidence/q14-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q14-2.jsonl" > "$evidence/q14-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q14-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q14-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q14-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q14-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q14-3.started"
set +e
/usr/bin/time -v -o "$evidence/q14-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q14-3.jsonl" > "$evidence/q14-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q14-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q14-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q14-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q14-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q14-4.started"
set +e
/usr/bin/time -v -o "$evidence/q14-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q14-4.jsonl" > "$evidence/q14-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q14-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q14-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q14-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q15-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q15-1.started"
set +e
/usr/bin/time -v -o "$evidence/q15-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q15-1.jsonl" > "$evidence/q15-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q15-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q15-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q15-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q15-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q15-2.started"
set +e
/usr/bin/time -v -o "$evidence/q15-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q15-2.jsonl" > "$evidence/q15-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q15-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q15-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q15-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q15-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q15-3.started"
set +e
/usr/bin/time -v -o "$evidence/q15-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q15-3.jsonl" > "$evidence/q15-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q15-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q15-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q15-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q15-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q15-4.started"
set +e
/usr/bin/time -v -o "$evidence/q15-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q15-4.jsonl" > "$evidence/q15-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q15-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q15-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q15-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q16-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q16-1.started"
set +e
/usr/bin/time -v -o "$evidence/q16-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q16-1.jsonl" > "$evidence/q16-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q16-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q16-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q16-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q16-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q16-2.started"
set +e
/usr/bin/time -v -o "$evidence/q16-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q16-2.jsonl" > "$evidence/q16-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q16-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q16-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q16-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q16-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q16-3.started"
set +e
/usr/bin/time -v -o "$evidence/q16-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q16-3.jsonl" > "$evidence/q16-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q16-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q16-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q16-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q16-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q16-4.started"
set +e
/usr/bin/time -v -o "$evidence/q16-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q16-4.jsonl" > "$evidence/q16-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q16-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q16-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q16-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q17-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q17-1.started"
set +e
/usr/bin/time -v -o "$evidence/q17-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q17-1.jsonl" > "$evidence/q17-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q17-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q17-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q17-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q17-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q17-2.started"
set +e
/usr/bin/time -v -o "$evidence/q17-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q17-2.jsonl" > "$evidence/q17-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q17-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q17-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q17-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q17-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q17-3.started"
set +e
/usr/bin/time -v -o "$evidence/q17-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q17-3.jsonl" > "$evidence/q17-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q17-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q17-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q17-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q17-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q17-4.started"
set +e
/usr/bin/time -v -o "$evidence/q17-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q17-4.jsonl" > "$evidence/q17-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q17-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q17-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q17-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q18-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q18-1.started"
set +e
/usr/bin/time -v -o "$evidence/q18-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q18-1.jsonl" > "$evidence/q18-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q18-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q18-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q18-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q18-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q18-2.started"
set +e
/usr/bin/time -v -o "$evidence/q18-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q18-2.jsonl" > "$evidence/q18-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q18-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q18-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q18-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q18-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q18-3.started"
set +e
/usr/bin/time -v -o "$evidence/q18-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q18-3.jsonl" > "$evidence/q18-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q18-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q18-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q18-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q18-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q18-4.started"
set +e
/usr/bin/time -v -o "$evidence/q18-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q18-4.jsonl" > "$evidence/q18-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q18-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q18-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q18-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q19-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q19-1.started"
set +e
/usr/bin/time -v -o "$evidence/q19-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q19-1.jsonl" > "$evidence/q19-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q19-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q19-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q19-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q19-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q19-2.started"
set +e
/usr/bin/time -v -o "$evidence/q19-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q19-2.jsonl" > "$evidence/q19-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q19-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q19-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q19-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q19-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q19-3.started"
set +e
/usr/bin/time -v -o "$evidence/q19-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q19-3.jsonl" > "$evidence/q19-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q19-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q19-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q19-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q19-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q19-4.started"
set +e
/usr/bin/time -v -o "$evidence/q19-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q19-4.jsonl" > "$evidence/q19-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q19-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q19-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q19-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q20-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q20-1.started"
set +e
/usr/bin/time -v -o "$evidence/q20-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q20-1.jsonl" > "$evidence/q20-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q20-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q20-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q20-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q20-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q20-2.started"
set +e
/usr/bin/time -v -o "$evidence/q20-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q20-2.jsonl" > "$evidence/q20-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q20-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q20-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q20-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q20-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q20-3.started"
set +e
/usr/bin/time -v -o "$evidence/q20-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q20-3.jsonl" > "$evidence/q20-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q20-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q20-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q20-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q20-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q20-4.started"
set +e
/usr/bin/time -v -o "$evidence/q20-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q20-4.jsonl" > "$evidence/q20-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q20-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q20-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q20-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q21-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q21-1.started"
set +e
/usr/bin/time -v -o "$evidence/q21-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q21-1.jsonl" > "$evidence/q21-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q21-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q21-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q21-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q21-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q21-2.started"
set +e
/usr/bin/time -v -o "$evidence/q21-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q21-2.jsonl" > "$evidence/q21-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q21-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q21-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q21-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q21-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q21-3.started"
set +e
/usr/bin/time -v -o "$evidence/q21-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q21-3.jsonl" > "$evidence/q21-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q21-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q21-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q21-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q21-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q21-4.started"
set +e
/usr/bin/time -v -o "$evidence/q21-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q21-4.jsonl" > "$evidence/q21-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q21-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q21-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q21-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q22-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q22-1.started"
set +e
/usr/bin/time -v -o "$evidence/q22-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q22-1.jsonl" > "$evidence/q22-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q22-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q22-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q22-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q22-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q22-2.started"
set +e
/usr/bin/time -v -o "$evidence/q22-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q22-2.jsonl" > "$evidence/q22-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q22-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q22-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q22-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q22-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q22-3.started"
set +e
/usr/bin/time -v -o "$evidence/q22-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q22-3.jsonl" > "$evidence/q22-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q22-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q22-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q22-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q22-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q22-4.started"
set +e
/usr/bin/time -v -o "$evidence/q22-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q22-4.jsonl" > "$evidence/q22-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q22-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q22-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q22-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q23-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q23-1.started"
set +e
/usr/bin/time -v -o "$evidence/q23-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q23-1.jsonl" > "$evidence/q23-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q23-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q23-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q23-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q23-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q23-2.started"
set +e
/usr/bin/time -v -o "$evidence/q23-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q23-2.jsonl" > "$evidence/q23-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q23-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q23-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q23-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q23-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q23-3.started"
set +e
/usr/bin/time -v -o "$evidence/q23-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q23-3.jsonl" > "$evidence/q23-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q23-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q23-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q23-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q23-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q23-4.started"
set +e
/usr/bin/time -v -o "$evidence/q23-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q23-4.jsonl" > "$evidence/q23-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q23-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q23-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q23-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q24-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q24-1.started"
set +e
/usr/bin/time -v -o "$evidence/q24-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q24-1.jsonl" > "$evidence/q24-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q24-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q24-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q24-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q24-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q24-2.started"
set +e
/usr/bin/time -v -o "$evidence/q24-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q24-2.jsonl" > "$evidence/q24-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q24-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q24-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q24-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q24-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q24-3.started"
set +e
/usr/bin/time -v -o "$evidence/q24-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q24-3.jsonl" > "$evidence/q24-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q24-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q24-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q24-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q24-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q24-4.started"
set +e
/usr/bin/time -v -o "$evidence/q24-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q24-4.jsonl" > "$evidence/q24-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q24-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q24-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q24-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q25-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q25-1.started"
set +e
/usr/bin/time -v -o "$evidence/q25-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q25-1.jsonl" > "$evidence/q25-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q25-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q25-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q25-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q25-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q25-2.started"
set +e
/usr/bin/time -v -o "$evidence/q25-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q25-2.jsonl" > "$evidence/q25-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q25-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q25-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q25-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q25-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q25-3.started"
set +e
/usr/bin/time -v -o "$evidence/q25-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q25-3.jsonl" > "$evidence/q25-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q25-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q25-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q25-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q25-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q25-4.started"
set +e
/usr/bin/time -v -o "$evidence/q25-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q25-4.jsonl" > "$evidence/q25-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q25-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q25-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q25-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q26-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q26-1.started"
set +e
/usr/bin/time -v -o "$evidence/q26-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q26-1.jsonl" > "$evidence/q26-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q26-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q26-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q26-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q26-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q26-2.started"
set +e
/usr/bin/time -v -o "$evidence/q26-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q26-2.jsonl" > "$evidence/q26-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q26-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q26-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q26-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q26-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q26-3.started"
set +e
/usr/bin/time -v -o "$evidence/q26-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q26-3.jsonl" > "$evidence/q26-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q26-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q26-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q26-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q26-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q26-4.started"
set +e
/usr/bin/time -v -o "$evidence/q26-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q26-4.jsonl" > "$evidence/q26-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q26-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q26-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q26-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q27-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q27-1.started"
set +e
/usr/bin/time -v -o "$evidence/q27-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q27-1.jsonl" > "$evidence/q27-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q27-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q27-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q27-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q27-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q27-2.started"
set +e
/usr/bin/time -v -o "$evidence/q27-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q27-2.jsonl" > "$evidence/q27-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q27-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q27-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q27-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q27-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q27-3.started"
set +e
/usr/bin/time -v -o "$evidence/q27-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q27-3.jsonl" > "$evidence/q27-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q27-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q27-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q27-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q27-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q27-4.started"
set +e
/usr/bin/time -v -o "$evidence/q27-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q27-4.jsonl" > "$evidence/q27-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q27-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q27-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q27-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q28-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q28-1.started"
set +e
/usr/bin/time -v -o "$evidence/q28-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q28-1.jsonl" > "$evidence/q28-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q28-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q28-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q28-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q28-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q28-2.started"
set +e
/usr/bin/time -v -o "$evidence/q28-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q28-2.jsonl" > "$evidence/q28-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q28-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q28-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q28-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q28-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q28-3.started"
set +e
/usr/bin/time -v -o "$evidence/q28-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q28-3.jsonl" > "$evidence/q28-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q28-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q28-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q28-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q28-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q28-4.started"
set +e
/usr/bin/time -v -o "$evidence/q28-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q28-4.jsonl" > "$evidence/q28-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q28-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q28-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q28-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q29-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q29-1.started"
set +e
/usr/bin/time -v -o "$evidence/q29-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q29-1.jsonl" > "$evidence/q29-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q29-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q29-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q29-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q29-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q29-2.started"
set +e
/usr/bin/time -v -o "$evidence/q29-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q29-2.jsonl" > "$evidence/q29-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q29-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q29-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q29-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q29-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q29-3.started"
set +e
/usr/bin/time -v -o "$evidence/q29-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q29-3.jsonl" > "$evidence/q29-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q29-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q29-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q29-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q29-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q29-4.started"
set +e
/usr/bin/time -v -o "$evidence/q29-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q29-4.jsonl" > "$evidence/q29-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q29-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q29-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q29-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q30-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q30-1.started"
set +e
/usr/bin/time -v -o "$evidence/q30-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q30-1.jsonl" > "$evidence/q30-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q30-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q30-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q30-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q30-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q30-2.started"
set +e
/usr/bin/time -v -o "$evidence/q30-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q30-2.jsonl" > "$evidence/q30-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q30-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q30-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q30-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q30-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q30-3.started"
set +e
/usr/bin/time -v -o "$evidence/q30-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q30-3.jsonl" > "$evidence/q30-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q30-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q30-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q30-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q30-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q30-4.started"
set +e
/usr/bin/time -v -o "$evidence/q30-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q30-4.jsonl" > "$evidence/q30-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q30-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q30-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q30-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q31-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q31-1.started"
set +e
/usr/bin/time -v -o "$evidence/q31-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q31-1.jsonl" > "$evidence/q31-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q31-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q31-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q31-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q31-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q31-2.started"
set +e
/usr/bin/time -v -o "$evidence/q31-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q31-2.jsonl" > "$evidence/q31-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q31-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q31-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q31-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q31-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q31-3.started"
set +e
/usr/bin/time -v -o "$evidence/q31-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q31-3.jsonl" > "$evidence/q31-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q31-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q31-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q31-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q31-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q31-4.started"
set +e
/usr/bin/time -v -o "$evidence/q31-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q31-4.jsonl" > "$evidence/q31-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q31-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q31-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q31-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q32-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q32-1.started"
set +e
/usr/bin/time -v -o "$evidence/q32-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q32-1.jsonl" > "$evidence/q32-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q32-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q32-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q32-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q32-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q32-2.started"
set +e
/usr/bin/time -v -o "$evidence/q32-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q32-2.jsonl" > "$evidence/q32-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q32-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q32-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q32-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q32-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q32-3.started"
set +e
/usr/bin/time -v -o "$evidence/q32-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q32-3.jsonl" > "$evidence/q32-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q32-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q32-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q32-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q32-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q32-4.started"
set +e
/usr/bin/time -v -o "$evidence/q32-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q32-4.jsonl" > "$evidence/q32-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q32-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q32-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q32-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q33-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q33-1.started"
set +e
/usr/bin/time -v -o "$evidence/q33-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q33-1.jsonl" > "$evidence/q33-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q33-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q33-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q33-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q33-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q33-2.started"
set +e
/usr/bin/time -v -o "$evidence/q33-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q33-2.jsonl" > "$evidence/q33-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q33-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q33-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q33-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q33-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q33-3.started"
set +e
/usr/bin/time -v -o "$evidence/q33-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q33-3.jsonl" > "$evidence/q33-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q33-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q33-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q33-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q33-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q33-4.started"
set +e
/usr/bin/time -v -o "$evidence/q33-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q33-4.jsonl" > "$evidence/q33-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q33-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q33-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q33-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q34-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q34-1.started"
set +e
/usr/bin/time -v -o "$evidence/q34-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q34-1.jsonl" > "$evidence/q34-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q34-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q34-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q34-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q34-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q34-2.started"
set +e
/usr/bin/time -v -o "$evidence/q34-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q34-2.jsonl" > "$evidence/q34-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q34-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q34-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q34-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q34-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q34-3.started"
set +e
/usr/bin/time -v -o "$evidence/q34-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q34-3.jsonl" > "$evidence/q34-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q34-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q34-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q34-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q34-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q34-4.started"
set +e
/usr/bin/time -v -o "$evidence/q34-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q34-4.jsonl" > "$evidence/q34-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q34-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q34-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q34-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q35-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q35-1.started"
set +e
/usr/bin/time -v -o "$evidence/q35-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q35-1.jsonl" > "$evidence/q35-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q35-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q35-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q35-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q35-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q35-2.started"
set +e
/usr/bin/time -v -o "$evidence/q35-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q35-2.jsonl" > "$evidence/q35-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q35-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q35-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q35-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q35-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q35-3.started"
set +e
/usr/bin/time -v -o "$evidence/q35-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q35-3.jsonl" > "$evidence/q35-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q35-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q35-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q35-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q35-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q35-4.started"
set +e
/usr/bin/time -v -o "$evidence/q35-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q35-4.jsonl" > "$evidence/q35-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q35-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q35-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q35-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q36-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q36-1.started"
set +e
/usr/bin/time -v -o "$evidence/q36-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q36-1.jsonl" > "$evidence/q36-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q36-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q36-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q36-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q36-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q36-2.started"
set +e
/usr/bin/time -v -o "$evidence/q36-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q36-2.jsonl" > "$evidence/q36-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q36-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q36-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q36-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q36-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q36-3.started"
set +e
/usr/bin/time -v -o "$evidence/q36-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q36-3.jsonl" > "$evidence/q36-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q36-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q36-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q36-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q36-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q36-4.started"
set +e
/usr/bin/time -v -o "$evidence/q36-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q36-4.jsonl" > "$evidence/q36-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q36-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q36-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q36-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q37-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q37-1.started"
set +e
/usr/bin/time -v -o "$evidence/q37-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q37-1.jsonl" > "$evidence/q37-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q37-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q37-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q37-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q37-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q37-2.started"
set +e
/usr/bin/time -v -o "$evidence/q37-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q37-2.jsonl" > "$evidence/q37-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q37-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q37-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q37-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q37-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q37-3.started"
set +e
/usr/bin/time -v -o "$evidence/q37-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q37-3.jsonl" > "$evidence/q37-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q37-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q37-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q37-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q37-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q37-4.started"
set +e
/usr/bin/time -v -o "$evidence/q37-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q37-4.jsonl" > "$evidence/q37-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q37-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q37-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q37-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q38-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q38-1.started"
set +e
/usr/bin/time -v -o "$evidence/q38-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q38-1.jsonl" > "$evidence/q38-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q38-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q38-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q38-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q38-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q38-2.started"
set +e
/usr/bin/time -v -o "$evidence/q38-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q38-2.jsonl" > "$evidence/q38-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q38-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q38-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q38-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q38-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q38-3.started"
set +e
/usr/bin/time -v -o "$evidence/q38-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q38-3.jsonl" > "$evidence/q38-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q38-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q38-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q38-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q38-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q38-4.started"
set +e
/usr/bin/time -v -o "$evidence/q38-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q38-4.jsonl" > "$evidence/q38-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q38-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q38-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q38-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q39-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q39-1.started"
set +e
/usr/bin/time -v -o "$evidence/q39-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q39-1.jsonl" > "$evidence/q39-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q39-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q39-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q39-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q39-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q39-2.started"
set +e
/usr/bin/time -v -o "$evidence/q39-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q39-2.jsonl" > "$evidence/q39-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q39-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q39-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q39-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q39-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q39-3.started"
set +e
/usr/bin/time -v -o "$evidence/q39-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q39-3.jsonl" > "$evidence/q39-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q39-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q39-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q39-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q39-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q39-4.started"
set +e
/usr/bin/time -v -o "$evidence/q39-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q39-4.jsonl" > "$evidence/q39-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q39-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q39-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q39-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q40-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q40-1.started"
set +e
/usr/bin/time -v -o "$evidence/q40-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q40-1.jsonl" > "$evidence/q40-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q40-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q40-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q40-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q40-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q40-2.started"
set +e
/usr/bin/time -v -o "$evidence/q40-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q40-2.jsonl" > "$evidence/q40-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q40-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q40-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q40-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q40-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q40-3.started"
set +e
/usr/bin/time -v -o "$evidence/q40-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q40-3.jsonl" > "$evidence/q40-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q40-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q40-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q40-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q40-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q40-4.started"
set +e
/usr/bin/time -v -o "$evidence/q40-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q40-4.jsonl" > "$evidence/q40-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q40-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q40-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q40-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q41-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q41-1.started"
set +e
/usr/bin/time -v -o "$evidence/q41-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q41-1.jsonl" > "$evidence/q41-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q41-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q41-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q41-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q41-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q41-2.started"
set +e
/usr/bin/time -v -o "$evidence/q41-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q41-2.jsonl" > "$evidence/q41-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q41-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q41-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q41-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q41-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q41-3.started"
set +e
/usr/bin/time -v -o "$evidence/q41-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q41-3.jsonl" > "$evidence/q41-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q41-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q41-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q41-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q41-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q41-4.started"
set +e
/usr/bin/time -v -o "$evidence/q41-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q41-4.jsonl" > "$evidence/q41-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q41-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q41-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q41-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q42-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q42-1.started"
set +e
/usr/bin/time -v -o "$evidence/q42-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q42-1.jsonl" > "$evidence/q42-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q42-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q42-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q42-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q42-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q42-2.started"
set +e
/usr/bin/time -v -o "$evidence/q42-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q42-2.jsonl" > "$evidence/q42-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q42-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q42-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q42-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q42-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q42-3.started"
set +e
/usr/bin/time -v -o "$evidence/q42-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q42-3.jsonl" > "$evidence/q42-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q42-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q42-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q42-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q42-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q42-4.started"
set +e
/usr/bin/time -v -o "$evidence/q42-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q42-4.jsonl" > "$evidence/q42-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q42-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q42-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q42-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q43-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q43-1.started"
set +e
/usr/bin/time -v -o "$evidence/q43-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q43-1.jsonl" > "$evidence/q43-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q43-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q43-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q43-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q43-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q43-2.started"
set +e
/usr/bin/time -v -o "$evidence/q43-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q43-2.jsonl" > "$evidence/q43-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q43-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q43-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q43-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q43-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q43-3.started"
set +e
/usr/bin/time -v -o "$evidence/q43-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q43-3.jsonl" > "$evidence/q43-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q43-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q43-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q43-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q43-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q43-4.started"
set +e
/usr/bin/time -v -o "$evidence/q43-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q43-4.jsonl" > "$evidence/q43-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q43-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q43-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q43-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q44-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q44-1.started"
set +e
/usr/bin/time -v -o "$evidence/q44-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q44-1.jsonl" > "$evidence/q44-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q44-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q44-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q44-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q44-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q44-2.started"
set +e
/usr/bin/time -v -o "$evidence/q44-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q44-2.jsonl" > "$evidence/q44-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q44-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q44-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q44-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q44-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q44-3.started"
set +e
/usr/bin/time -v -o "$evidence/q44-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q44-3.jsonl" > "$evidence/q44-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q44-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q44-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q44-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q44-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q44-4.started"
set +e
/usr/bin/time -v -o "$evidence/q44-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q44-4.jsonl" > "$evidence/q44-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q44-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q44-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q44-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q45-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q45-1.started"
set +e
/usr/bin/time -v -o "$evidence/q45-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q45-1.jsonl" > "$evidence/q45-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q45-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q45-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q45-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q45-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q45-2.started"
set +e
/usr/bin/time -v -o "$evidence/q45-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q45-2.jsonl" > "$evidence/q45-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q45-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q45-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q45-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q45-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q45-3.started"
set +e
/usr/bin/time -v -o "$evidence/q45-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q45-3.jsonl" > "$evidence/q45-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q45-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q45-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q45-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q45-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q45-4.started"
set +e
/usr/bin/time -v -o "$evidence/q45-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q45-4.jsonl" > "$evidence/q45-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q45-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q45-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q45-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q46-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q46-1.started"
set +e
/usr/bin/time -v -o "$evidence/q46-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q46-1.jsonl" > "$evidence/q46-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q46-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q46-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q46-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q46-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q46-2.started"
set +e
/usr/bin/time -v -o "$evidence/q46-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q46-2.jsonl" > "$evidence/q46-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q46-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q46-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q46-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q46-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q46-3.started"
set +e
/usr/bin/time -v -o "$evidence/q46-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q46-3.jsonl" > "$evidence/q46-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q46-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q46-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q46-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q46-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q46-4.started"
set +e
/usr/bin/time -v -o "$evidence/q46-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q46-4.jsonl" > "$evidence/q46-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q46-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q46-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q46-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q47-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q47-1.started"
set +e
/usr/bin/time -v -o "$evidence/q47-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q47-1.jsonl" > "$evidence/q47-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q47-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q47-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q47-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q47-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q47-2.started"
set +e
/usr/bin/time -v -o "$evidence/q47-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q47-2.jsonl" > "$evidence/q47-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q47-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q47-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q47-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q47-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q47-3.started"
set +e
/usr/bin/time -v -o "$evidence/q47-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q47-3.jsonl" > "$evidence/q47-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q47-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q47-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q47-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q47-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q47-4.started"
set +e
/usr/bin/time -v -o "$evidence/q47-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q47-4.jsonl" > "$evidence/q47-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q47-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q47-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q47-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q48-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q48-1.started"
set +e
/usr/bin/time -v -o "$evidence/q48-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q48-1.jsonl" > "$evidence/q48-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q48-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q48-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q48-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q48-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q48-2.started"
set +e
/usr/bin/time -v -o "$evidence/q48-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q48-2.jsonl" > "$evidence/q48-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q48-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q48-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q48-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q48-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q48-3.started"
set +e
/usr/bin/time -v -o "$evidence/q48-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q48-3.jsonl" > "$evidence/q48-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q48-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q48-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q48-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q48-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q48-4.started"
set +e
/usr/bin/time -v -o "$evidence/q48-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q48-4.jsonl" > "$evidence/q48-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q48-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q48-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q48-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q49-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q49-1.started"
set +e
/usr/bin/time -v -o "$evidence/q49-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q49-1.jsonl" > "$evidence/q49-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q49-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q49-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q49-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q49-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q49-2.started"
set +e
/usr/bin/time -v -o "$evidence/q49-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q49-2.jsonl" > "$evidence/q49-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q49-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q49-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q49-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q49-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q49-3.started"
set +e
/usr/bin/time -v -o "$evidence/q49-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q49-3.jsonl" > "$evidence/q49-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q49-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q49-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q49-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q49-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q49-4.started"
set +e
/usr/bin/time -v -o "$evidence/q49-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q49-4.jsonl" > "$evidence/q49-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q49-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q49-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q49-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q50-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q50-1.started"
set +e
/usr/bin/time -v -o "$evidence/q50-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q50-1.jsonl" > "$evidence/q50-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q50-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q50-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q50-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q50-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q50-2.started"
set +e
/usr/bin/time -v -o "$evidence/q50-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q50-2.jsonl" > "$evidence/q50-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q50-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q50-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q50-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q50-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q50-3.started"
set +e
/usr/bin/time -v -o "$evidence/q50-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q50-3.jsonl" > "$evidence/q50-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q50-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q50-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q50-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q50-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q50-4.started"
set +e
/usr/bin/time -v -o "$evidence/q50-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q50-4.jsonl" > "$evidence/q50-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q50-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q50-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q50-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q51-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q51-1.started"
set +e
/usr/bin/time -v -o "$evidence/q51-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q51-1.jsonl" > "$evidence/q51-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q51-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q51-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q51-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q51-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q51-2.started"
set +e
/usr/bin/time -v -o "$evidence/q51-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q51-2.jsonl" > "$evidence/q51-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q51-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q51-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q51-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q51-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q51-3.started"
set +e
/usr/bin/time -v -o "$evidence/q51-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q51-3.jsonl" > "$evidence/q51-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q51-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q51-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q51-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q51-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q51-4.started"
set +e
/usr/bin/time -v -o "$evidence/q51-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q51-4.jsonl" > "$evidence/q51-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q51-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q51-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q51-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q52-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q52-1.started"
set +e
/usr/bin/time -v -o "$evidence/q52-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q52-1.jsonl" > "$evidence/q52-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q52-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q52-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q52-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q52-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q52-2.started"
set +e
/usr/bin/time -v -o "$evidence/q52-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q52-2.jsonl" > "$evidence/q52-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q52-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q52-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q52-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q52-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q52-3.started"
set +e
/usr/bin/time -v -o "$evidence/q52-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q52-3.jsonl" > "$evidence/q52-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q52-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q52-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q52-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q52-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q52-4.started"
set +e
/usr/bin/time -v -o "$evidence/q52-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q52-4.jsonl" > "$evidence/q52-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q52-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q52-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q52-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q53-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q53-1.started"
set +e
/usr/bin/time -v -o "$evidence/q53-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q53-1.jsonl" > "$evidence/q53-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q53-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q53-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q53-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q53-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q53-2.started"
set +e
/usr/bin/time -v -o "$evidence/q53-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q53-2.jsonl" > "$evidence/q53-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q53-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q53-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q53-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q53-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q53-3.started"
set +e
/usr/bin/time -v -o "$evidence/q53-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q53-3.jsonl" > "$evidence/q53-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q53-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q53-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q53-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q53-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q53-4.started"
set +e
/usr/bin/time -v -o "$evidence/q53-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q53-4.jsonl" > "$evidence/q53-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q53-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q53-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q53-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q54-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q54-1.started"
set +e
/usr/bin/time -v -o "$evidence/q54-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q54-1.jsonl" > "$evidence/q54-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q54-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q54-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q54-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q54-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q54-2.started"
set +e
/usr/bin/time -v -o "$evidence/q54-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q54-2.jsonl" > "$evidence/q54-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q54-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q54-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q54-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q54-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q54-3.started"
set +e
/usr/bin/time -v -o "$evidence/q54-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q54-3.jsonl" > "$evidence/q54-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q54-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q54-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q54-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q54-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q54-4.started"
set +e
/usr/bin/time -v -o "$evidence/q54-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q54-4.jsonl" > "$evidence/q54-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q54-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q54-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q54-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q55-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q55-1.started"
set +e
/usr/bin/time -v -o "$evidence/q55-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q55-1.jsonl" > "$evidence/q55-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q55-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q55-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q55-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q55-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q55-2.started"
set +e
/usr/bin/time -v -o "$evidence/q55-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q55-2.jsonl" > "$evidence/q55-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q55-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q55-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q55-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q55-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q55-3.started"
set +e
/usr/bin/time -v -o "$evidence/q55-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q55-3.jsonl" > "$evidence/q55-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q55-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q55-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q55-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q55-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q55-4.started"
set +e
/usr/bin/time -v -o "$evidence/q55-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q55-4.jsonl" > "$evidence/q55-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q55-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q55-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q55-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q56-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q56-1.started"
set +e
/usr/bin/time -v -o "$evidence/q56-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q56-1.jsonl" > "$evidence/q56-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q56-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q56-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q56-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q56-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q56-2.started"
set +e
/usr/bin/time -v -o "$evidence/q56-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q56-2.jsonl" > "$evidence/q56-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q56-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q56-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q56-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q56-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q56-3.started"
set +e
/usr/bin/time -v -o "$evidence/q56-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q56-3.jsonl" > "$evidence/q56-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q56-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q56-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q56-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q56-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q56-4.started"
set +e
/usr/bin/time -v -o "$evidence/q56-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q56-4.jsonl" > "$evidence/q56-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q56-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q56-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q56-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q57-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q57-1.started"
set +e
/usr/bin/time -v -o "$evidence/q57-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q57-1.jsonl" > "$evidence/q57-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q57-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q57-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q57-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q57-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q57-2.started"
set +e
/usr/bin/time -v -o "$evidence/q57-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q57-2.jsonl" > "$evidence/q57-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q57-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q57-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q57-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q57-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q57-3.started"
set +e
/usr/bin/time -v -o "$evidence/q57-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q57-3.jsonl" > "$evidence/q57-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q57-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q57-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q57-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q57-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q57-4.started"
set +e
/usr/bin/time -v -o "$evidence/q57-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q57-4.jsonl" > "$evidence/q57-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q57-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q57-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q57-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q58-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q58-1.started"
set +e
/usr/bin/time -v -o "$evidence/q58-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q58-1.jsonl" > "$evidence/q58-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q58-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q58-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q58-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q58-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q58-2.started"
set +e
/usr/bin/time -v -o "$evidence/q58-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q58-2.jsonl" > "$evidence/q58-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q58-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q58-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q58-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q58-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q58-3.started"
set +e
/usr/bin/time -v -o "$evidence/q58-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q58-3.jsonl" > "$evidence/q58-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q58-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q58-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q58-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q58-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q58-4.started"
set +e
/usr/bin/time -v -o "$evidence/q58-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q58-4.jsonl" > "$evidence/q58-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q58-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q58-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q58-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q59-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q59-1.started"
set +e
/usr/bin/time -v -o "$evidence/q59-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q59-1.jsonl" > "$evidence/q59-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q59-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q59-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q59-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q59-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q59-2.started"
set +e
/usr/bin/time -v -o "$evidence/q59-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q59-2.jsonl" > "$evidence/q59-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q59-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q59-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q59-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q59-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q59-3.started"
set +e
/usr/bin/time -v -o "$evidence/q59-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q59-3.jsonl" > "$evidence/q59-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q59-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q59-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q59-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q59-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q59-4.started"
set +e
/usr/bin/time -v -o "$evidence/q59-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q59-4.jsonl" > "$evidence/q59-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q59-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q59-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q59-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q60-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q60-1.started"
set +e
/usr/bin/time -v -o "$evidence/q60-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q60-1.jsonl" > "$evidence/q60-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q60-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q60-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q60-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q60-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q60-2.started"
set +e
/usr/bin/time -v -o "$evidence/q60-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q60-2.jsonl" > "$evidence/q60-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q60-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q60-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q60-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q60-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q60-3.started"
set +e
/usr/bin/time -v -o "$evidence/q60-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-trace.config.json" 97f977bf2cd6d1da508d7ef800c8385529b656d4c9032c4693beda862d14b936 "$evidence/q60-3.jsonl" > "$evidence/q60-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q60-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q60-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q60-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q60-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q60-4.started"
set +e
/usr/bin/time -v -o "$evidence/q60-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_first-untraced.config.json" 2febc7205e3fb4a78c8e125572d9e26a3ca6421968fe7bd4d3f659711db020f3 "$evidence/q60-4.jsonl" > "$evidence/q60-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q60-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q60-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q60-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q61-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q61-1.started"
set +e
/usr/bin/time -v -o "$evidence/q61-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q61-1.jsonl" > "$evidence/q61-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q61-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q61-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q61-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q61-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q61-2.started"
set +e
/usr/bin/time -v -o "$evidence/q61-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q61-2.jsonl" > "$evidence/q61-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q61-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q61-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q61-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q61-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q61-3.started"
set +e
/usr/bin/time -v -o "$evidence/q61-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-untraced.config.json" 587654bb92844486bbc3de3d1add19a6d618605efef52c1629277700c72a501f "$evidence/q61-3.jsonl" > "$evidence/q61-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q61-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q61-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q61-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q61-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q61-4.started"
set +e
/usr/bin/time -v -o "$evidence/q61-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_first-trace.config.json" dbcc24fc0a21ebf96c257d2927ce6931146c6add3d56f3e6b840c10fa74d4411 "$evidence/q61-4.jsonl" > "$evidence/q61-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q61-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q61-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q61-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q62-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q62-1.started"
set +e
/usr/bin/time -v -o "$evidence/q62-1.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q62-1.jsonl" > "$evidence/q62-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q62-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q62-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q62-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q62-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q62-2.started"
set +e
/usr/bin/time -v -o "$evidence/q62-2.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q62-2.jsonl" > "$evidence/q62-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q62-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q62-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q62-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q62-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q62-3.started"
set +e
/usr/bin/time -v -o "$evidence/q62-3.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-trace.config.json" 7230ecb8f274ea38073f6eb4e4078399a5a729067dbfeb1fcf508a84a7870425 "$evidence/q62-3.jsonl" > "$evidence/q62-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q62-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q62-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q62-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q62-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q62-4.started"
set +e
/usr/bin/time -v -o "$evidence/q62-4.time" timeout --kill-after=5 30 "$root/runner" "$root/A_last-untraced.config.json" b7b5588566b00dad5118dc94a5c56ddf5edaa48e04a3db3ddbc81e0352be52b0 "$evidence/q62-4.jsonl" > "$evidence/q62-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q62-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q62-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q62-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q63-1.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q63-1.started"
set +e
/usr/bin/time -v -o "$evidence/q63-1.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q63-1.jsonl" > "$evidence/q63-1.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q63-1.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q63-1.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q63-1.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q63-2.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q63-2.started"
set +e
/usr/bin/time -v -o "$evidence/q63-2.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q63-2.jsonl" > "$evidence/q63-2.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q63-2.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q63-2.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q63-2.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q63-3.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q63-3.started"
set +e
/usr/bin/time -v -o "$evidence/q63-3.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-trace.config.json" 9b8f9639d6e6cd4feec8bdb94ce82f197dd5e6e696165b451c78ce5a13403c8d "$evidence/q63-3.jsonl" > "$evidence/q63-3.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q63-3.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q63-3.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q63-3.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
sha256sum -c "$root/pins.sha256" > "$evidence/q63-4.pins-before.txt"
date -u +%FT%T.%NZ > "$evidence/q63-4.started"
set +e
/usr/bin/time -v -o "$evidence/q63-4.time" timeout --kill-after=5 30 "$root/runner" "$root/B_last-untraced.config.json" 3c889cb1a9a269b015d1f813fe2a67879f491ddcb6bdc8fe5507aacc0967a3fc "$evidence/q63-4.jsonl" > "$evidence/q63-4.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" > "$evidence/q63-4.native-exit"
printf '%s\n' "$status" > "$evidence/last-native-exit"
date -u +%FT%T.%NZ > "$evidence/q63-4.finished"
sha256sum -c "$root/pins.sha256" > "$evidence/q63-4.pins-after.txt"
if (( status != 0 )); then exit "$status"; fi
exit 0
