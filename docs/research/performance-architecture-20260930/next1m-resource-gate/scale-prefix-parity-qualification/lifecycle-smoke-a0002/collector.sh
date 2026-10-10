#!/bin/bash
set -uo pipefail
[[ $# == 1 && $1 == /* && -d $1 ]] || exit 125
cd "$1" || exit 125
code=0
 owned_cgroup=$(cat evidence/cgroup-path.txt) || code=96
 zero_receipts() {
  [[ $(cat evidence/launcher.exit) == 0 && $(cat evidence/final-exit) == 0 ]]
 }
 exact_absent() {
  [[ $owned_cgroup == /system.slice/native-parity-gates.service && ! -e /sys/fs/cgroup$owned_cgroup ]]
 }
 if [[ $owned_cgroup != /system.slice/native-parity-gates.service ]]; then
  code=96
 else
  if [[ -d /sys/fs/cgroup$owned_cgroup ]]; then
   timeout -k 2 30 systemctl stop native-parity-gates >evidence/stop.stdout 2>evidence/stop.stderr
   stop_rc=$?
   printf '%s\n' "$stop_rc" >evidence/stop.exit || code=96
   if (( stop_rc != 0 )); then
    if (( stop_rc != 124 && stop_rc != 137 )) &&
       [[ ! -s evidence/stop.stdout ]] &&
       [[ $(cat evidence/stop.stderr) == 'Failed to stop native-parity-gates.service: Unit native-parity-gates.service not loaded.' ]] &&
       exact_absent && zero_receipts; then
     printf '%s\n' unloaded-during-stop >evidence/stop-reconciled || code=96
    else code=96; fi
   fi
  fi
  if [[ -e /sys/fs/cgroup$owned_cgroup/cgroup.events ]]; then
   if cat "/sys/fs/cgroup$owned_cgroup/cgroup.events" >evidence/drain.events; then
    grep -Fxq 'populated 0' evidence/drain.events || code=96
   elif exact_absent && zero_receipts; then
    printf '%s\n' owned-cgroup-disappeared-during-read >evidence/drain.events || code=96
   else code=96; fi
  elif exact_absent && zero_receipts; then
   printf '%s\n' owned-cgroup-absent >evidence/drain.events || code=96
  else code=96; fi
  # A successful closure always requires both payload receipts, including after stop.
  if (( code == 0 )); then zero_receipts || code=96; fi
 fi
printf '%s\n' "$code" > collector.exit || exit 96
exit "$code"
