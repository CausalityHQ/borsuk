#!/bin/bash
set -euo pipefail
stage=$1
prep=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-preparation-a0001
gates=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-gates-a0001
out=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-run-a0001
control=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-$stage-control-a0001
case "$stage" in
 gates) command=(bash "$prep/run-abba.sh" gates "$prep" "$prep" "$gates");;
 run)
  [[ $(cat /mnt/borsuk-workspace/evidence/leaf-prefix40-abba-gates-control-a0001/runtime.exit) == 0 ]]
  command=(bash "$prep/run-abba.sh" run "$prep" "$gates" "$out");;
 collect)
  [[ $(cat /mnt/borsuk-workspace/evidence/leaf-prefix40-abba-run-control-a0001/runtime.exit) == 0 ]]
  command=(bash "$prep/collect-closed.sh");;
 *) exit 64;;
esac
source "$prep/guard.sh"
guard 2400
mkdir "$control"
unit=leaf-prefix40-abba-$stage-a0001
set +e
systemd-run --unit="$unit" --wait --property=AllowedCPUs=4-7 --property=CPUQuota=400% \
 --property=MemoryMax=8G --property=MemorySwapMax=0 --property=TasksMax=512 \
 --property=RuntimeMaxSec=2400 --property=TimeoutStopSec=60 --property=KillMode=control-group \
 --property=OOMPolicy=stop --property=Type=exec \
 /usr/bin/env OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 RAYON_NUM_THREADS=4 \
 "${command[@]}" >"$control/runtime.stdout" 2>"$control/runtime.stderr"
rc=$?
set -e
echo "$rc" >"$control/runtime.exit"
systemctl show "$unit" -p Result -p ExecMainCode -p ExecMainStatus -p ActiveState \
 -p MemoryPeak -p CPUUsageNSec -p ControlGroup >"$control/runtime.properties" 2>"$control/runtime.properties.stderr" || echo UNIT_GARBAGE_COLLECTED >"$control/runtime.properties.unavailable"
journalctl -u "$unit" --no-pager >"$control/runtime.journal"
if ((rc!=0)); then echo EXECUTION_INVALID >"$control/terminal"; exit "$rc"; fi
echo CLOSED >"$control/terminal"
