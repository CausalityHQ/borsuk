#!/bin/bash
# One-shot platform commands; native Rust validates data. No result reduction here.
set -euo pipefail
root=/mnt/borsuk-retained-admission
cd "$root"
mkdir evidence scratch
printf '%s  inputs.json\n' cb4a475b5987c1562384f73a73692b8e6b7ad9ffb4a5d10d295b7787e7c6d829 | sha256sum -c -
test "$(jq '.objects | length' inputs.json)" = 15
jq -r '.objects[] | [.sha256,.path] | @tsv' inputs.json |
while IFS=$'\t' read -r sha path; do
  printf '%s  %s\n' "$sha" "$path" | sha256sum -c -
done > evidence/inputs-before.sha256
jq -r '.objects[] | [.bytes,.path] | @tsv' inputs.json |
while IFS=$'\t' read -r bytes path; do test "$(stat -c %s "$path")" = "$bytes"; done
printf '%s  %s\n' ce43842caeea9dbb722f3497b237265e71c7d829cbaf0243a81a2a352fcf1221 bin/A 3911839ba9ef68604e2c487d802a3b3e125bdc1121aee9af268db3ba8a9ca8cf bin/B 733cf976db05b8482ce192251ad25ec203fae9aec4cf8cf519f1dc6658bf0806 bin/publisher | sha256sum -c - > evidence/binaries.sha256
test "$(stat -c %s bin/A)" = 6665104
test "$(stat -c %s bin/B)" = 6764512
test "$(stat -c %s bin/publisher)" = 6323912
stage() {
  name=$1 expected=$2 seconds=$3; shift 3
  dir="$root/evidence/$name"; mkdir "$dir"
  printf '%s\n' "$expected" > "$dir/expected-exit"
  printf '%q ' "$@" > "$dir/command"
  stop_receipt="/bin/sh -c 'printf \"%s\\n\" \"\$\${SERVICE_RESULT}\" \"\$\${EXIT_CODE}\" \"\$\${EXIT_STATUS}\" > $dir/systemd-terminal'"
  set +e
  systemd-run --unit="borsuk-retained-$name" --slice=borsuk-retained-admission.slice --wait --pipe -p "ExecStopPost=$stop_receipt" -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec="$seconds" --setenv=RAYON_NUM_THREADS=1 --setenv=TOKIO_WORKER_THREADS=1 --setenv=BORSUK_CPU_THREADS=1 /bin/bash -c '
    set -euo pipefail
    dir=$1; shift
    cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
    printf "%s\n" "$cg" > "$dir/cgroup-path"
    for field in cpu.max memory.max memory.swap.max pids.max cpuset.cpus.effective; do cat "/sys/fs/cgroup$cg/$field" > "$dir/$field.before"; done
    test "$(cat "$dir/cpu.max.before")" = "100000 100000"
    test "$(cat "$dir/memory.max.before")" = 536870912
    test "$(cat "$dir/memory.swap.max.before")" = 0
    test "$(cat "$dir/pids.max.before")" = 128
    test "$(cat "$dir/cpuset.cpus.effective.before")" = 0
    set +e
    /usr/bin/time -v -o "$dir/time.txt" "$@"
    status=$?
    set -e
    printf "%s\n" "$status" > "$dir/native-exit"
    for field in memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do if [[ -f "/sys/fs/cgroup$cg/$field" ]]; then cat "/sys/fs/cgroup$cg/$field" > "$dir/$field.after"; fi; done
    test "$(cat "$dir/memory.swap.current.after")" = 0
    test "$(cat "$dir/memory.swap.peak.after")" = 0
    test "$(cat "$dir/memory.peak.after")" -le 536870912
    awk '\''$1=="oom" || $1=="oom_kill" {seen++; if ($2!=0) bad=1} END {exit (seen!=2 || bad)}'\'' "$dir/memory.events.after"
    exit "$status"
  ' bash "$dir" "$@" > "$dir/log" 2>&1
  service_exit=$?
  set -e
  printf '%s\n' "$service_exit" > "$dir/service-exit"
  systemctl show "borsuk-retained-$name.service" -p LoadState -p MainPID -p ActiveState -p SubState -p Result -p ExecMainStatus > "$dir/systemd-after"
  test "$(cat "$dir/native-exit")" = "$expected"
  test "$service_exit" = "$expected"
  service_result=success
  if [[ "$expected" != 0 ]]; then service_result=exit-code; fi
  printf '%s\n' "$service_result" exited "$expected" > "$dir/systemd-terminal-expected"
  cmp "$dir/systemd-terminal-expected" "$dir/systemd-terminal"
  test "$(systemctl show "borsuk-retained-$name.service" --value -p MainPID)" = 0
  grep -Fx ActiveState=inactive "$dir/systemd-after"
  grep -Fx SubState=dead "$dir/systemd-after"
  cg_after="/sys/fs/cgroup$(cat "$dir/cgroup-path")"
  if [[ -d "$cg_after" ]]; then test "$(cat "$cg_after/pids.current")" = 0; fi
  printf '%s\n' NO_REMAINING_CGROUP_PROCESSES > "$dir/drained"
}
stage canary-A 2 15 "$root/bin/A"
stage canary-B 2 15 "$root/bin/B"
stage canary-publisher 2 15 "$root/bin/publisher" --retained
grep -F 'usage: check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT' evidence/canary-A/log
grep -F 'usage: check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT' evidence/canary-B/log
grep -F 'usage: publish_two_bit_generation [--retained] CONFIG CONFIG_SHA NEW_RECEIPT' evidence/canary-publisher/log
jq -r '.objects[] | [.sha256,.path] | @tsv' inputs.json |
while IFS=$'\t' read -r sha path; do printf '%s  %s\n' "$sha" "$path" | sha256sum -c -; done > evidence/inputs-after.sha256
cmp evidence/inputs-before.sha256 evidence/inputs-after.sha256
test "$(du -sb "$root" | cut -f1)" -le 4294967296
