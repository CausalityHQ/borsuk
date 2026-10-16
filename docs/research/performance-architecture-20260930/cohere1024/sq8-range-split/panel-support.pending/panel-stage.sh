#!/bin/bash
# UNVERIFIED source-only companion. Root must qualify every execution path.
set -euo pipefail
set -C
umask 077
export PATH=/usr/sbin:/usr/bin:/sbin:/bin LC_ALL=C
mode=${1:?}
ev=${2:?}
unit=${3:?}
owner=${4:?}
shift 4
[[ $ev =~ ^/[A-Za-z0-9_./-]+$ && -d $ev && ! -L $ev ]]
[[ $unit =~ ^borsuk-sq8-panel-[0-9a-f-]+-(B0|C0|B1|C1|C2|B2|reducer)\.service$ ]]
cg=/system.slice/$unit
[[ $(awk -F: '$1==0 {print $3}' /proc/self/cgroup) == "$cg" ]]
[[ ${INVOCATION_ID:-} =~ ^[0-9a-f]{32}$ ]]
self=$(realpath -e -- "$0")

snapshot() {
 local suffix=$1 f
 for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.events memory.peak memory.swap.peak pids.peak cpu.stat cgroup.events; do
  cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.$suffix"
 done
}

case $mode in
 stop)
  # Reuse qualification-support's ExecStopPost capture before successful unload.
  systemctl show "$unit" -p Description -p InvocationID -p ControlGroup -p Result -p ExecMainCode -p ExecMainStatus > "$ev/unit-stop-status.txt"
  grep -Fxq "Description=$owner" "$ev/unit-stop-status.txt"
  grep -Fxq "InvocationID=$INVOCATION_ID" "$ev/unit-stop-status.txt"
  grep -Fxq "ControlGroup=$cg" "$ev/unit-stop-status.txt"
  printf '%s\n' "${SERVICE_RESULT:-unset}" > "$ev/service-result.txt"
  printf '%s\n' "${EXIT_CODE:-unset}" > "$ev/service-exit-code.txt"
  printf '%s\n' "${EXIT_STATUS:-unset}" > "$ev/service-exit-status.txt"
  snapshot after
  ;;
 native)
  binary=$1
  shift
  date -u +%FT%TZ > "$ev/native-started.txt"
  "$binary" "$@" && rc=0 || rc=$?
  printf '%s\n' "$rc" > "$ev/native-exit.txt"
  date -u +%FT%TZ > "$ev/native-finished.txt"
  exit "$rc"
  ;;
 timed)
  /usr/bin/time -v -o "$ev/native-time.txt" /bin/bash "$self" native "$ev" "$unit" "$owner" "$@" && rc=0 || rc=$?
  printf '%s\n' "$rc" > "$ev/time-exit.txt"
  exit "$rc"
  ;;
 run)
  deadline=$1
  binary=$2
  binary_sha=$3
  binary_bytes=$4
  shift 4
  [[ $deadline =~ ^[1-9][0-9]{9}$ && $binary_sha =~ ^[0-9a-f]{64}$ && $binary_bytes =~ ^[1-9][0-9]*$ ]]
  printf '%s\n' "$INVOCATION_ID" > "$ev/invocation-id.txt"
  systemctl show "$unit" -p Description -p InvocationID -p ControlGroup > "$ev/unit-start-status.txt"
  grep -Fxq "Description=$owner" "$ev/unit-start-status.txt"
  grep -Fxq "InvocationID=$INVOCATION_ID" "$ev/unit-start-status.txt"
  grep -Fxq "ControlGroup=$cg" "$ev/unit-start-status.txt"
  snapshot before
  [[ $(cat "$ev/cpu.max.before") == '100000 100000' ]]
  [[ $(cat "$ev/cpuset.cpus.effective.before") == 0 ]]
  [[ $(cat "$ev/memory.max.before") == 2147483648 ]]
  [[ $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 128 ]]
  [[ $(cat "$ev/memory.swap.peak.before") == 0 ]]
  grep -Fxq 'oom 0' "$ev/memory.events.before"
  grep -Fxq 'oom_kill 0' "$ev/memory.events.before"
  [[ -f $binary && ! -L $binary && -x $binary && $(stat -c %s -- "$binary") == "$binary_bytes" ]]
  printf '%s  %s\n' "$binary_sha" "$binary" | sha256sum --strict --check > "$ev/binary-before.txt"
  readelf -h "$binary" > "$ev/binary-elf.txt"
  left=$((deadline - $(date +%s) - 5))
  (( left <= 115 )) || left=115
  printf '%s\n' "$left" > "$ev/native-timeout-seconds.txt"
  (( left > 0 )) || exit 124
  printf '%q ' "$binary" "$@" > "$ev/native-command.txt"
  printf '\n' >> "$ev/native-command.txt"
  # Keep timeout, time and actual ELF exits independently; missing means unknown.
  timeout -k 2 "$left" /bin/bash "$self" timed "$ev" "$unit" "$owner" "$binary" "$@" && rc=0 || rc=$?
  printf '%s\n' "$rc" > "$ev/timeout-exit.txt"
  printf '%s  %s\n' "$binary_sha" "$binary" | sha256sum --strict --check > "$ev/binary-after.txt" && pin_rc=0 || pin_rc=$?
  printf '%s\n' "$pin_rc" > "$ev/binary-after-exit.txt"
  (( rc != 0 || pin_rc == 0 )) || rc=98
  printf '%s\n' "$rc" > "$ev/worker-exit.txt"
  exit "$rc"
  ;;
 *) exit 98 ;;
esac
