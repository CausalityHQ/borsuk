#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-http
evidence=$root/evidence
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
for f in cpu.max memory.max memory.swap.max pids.max; do
  cat "/sys/fs/cgroup${cg}/$f" > "$evidence/replay-$f.before"
done
test "$(cat "$evidence/replay-cpu.max.before")" = '100000 100000'
test "$(cat "$evidence/replay-memory.max.before")" = 268435456
test "$(cat "$evidence/replay-memory.swap.max.before")" = 0
test "$(cat "$evidence/replay-pids.max.before")" = 128
test ! -e "$evidence/source-utilization.json"
# A disposable CLI/loader smoke has no input arguments and cannot open B1.
set +e
"$root/retained/compare_native_replay" --source-utilization > "$evidence/replay-cli-smoke.stdout" 2> "$evidence/replay-cli-smoke.stderr"
smoke_exit=$?
set -e
printf '%s\n' "$smoke_exit" > "$evidence/replay-cli-smoke.exit"
test "$smoke_exit" = 2
test ! -s "$evidence/replay-cli-smoke.stdout"
grep -Fq 'usage: compare_native_replay --source-utilization CONFIG CONFIG_SHA256 NEW_REPORT_JSON' "$evidence/replay-cli-smoke.stderr"
config_sha=$(sha256sum "$root/replay-config.json" | cut -d' ' -f1)
set +e
"$root/retained/compare_native_replay" --source-utilization "$root/replay-config.json" "$config_sha" "$evidence/source-utilization.json"
native_exit=$?
set -e
printf '%s\n' "$native_exit" > "$evidence/replay-original-exit"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do
  if [[ -f "/sys/fs/cgroup${cg}/$f" ]]; then
    cat "/sys/fs/cgroup${cg}/$f" > "$evidence/replay-$f.after"
  fi
done
exit "$native_exit"
