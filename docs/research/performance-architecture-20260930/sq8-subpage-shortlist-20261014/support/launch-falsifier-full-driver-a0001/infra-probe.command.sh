#!/bin/bash
set -euo pipefail
printf '%s\n' INFRASTRUCTURE_ONLY_NO_TEST_LOGS_OR_INPUT_READS
unit=borsuk-loss-example-regressions.service
timeout -k1 10 systemctl show "$unit" -p ActiveState -p SubState -p Result -p MainPID -p ControlGroup
cg=/sys/fs/cgroup/system.slice/$unit
if [[ -d $cg ]]; then
 for file in cpu.stat memory.current memory.peak memory.events pids.current cgroup.events; do printf '%s\n' "$file"; cat "$cg/$file"; done
 for pid in $(cat "$cg/cgroup.procs"); do
  [[ $pid =~ ^[1-9][0-9]*$ ]] || exit 1
  if [[ -r /proc/$pid/comm ]]; then printf 'PID %s\n' "$pid"; cat "/proc/$pid/comm"; grep -E '^(State|VmRSS|VmHWM|Threads):' "/proc/$pid/status" || true; fi
 done
 sleep 1
 printf '%s\n' CPU_STAT_AFTER_ONE_SECOND
 cat "$cg/cpu.stat"
fi
