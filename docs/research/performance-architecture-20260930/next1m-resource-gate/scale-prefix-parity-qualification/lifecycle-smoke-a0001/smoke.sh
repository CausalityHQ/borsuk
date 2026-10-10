#!/bin/bash
set -Eeuo pipefail
[[ $# == 2 && $1 == /* && $2 == /* ]] || exit 125
collector=$1; root=$2
[[ ! -e $root && $(sha256sum "$collector" | cut -d ' ' -f1) == 1b090217e0927eb44c63df6fc0f465205531050cc27d3f80dd98dc4586986e60 ]] || exit 125
mkdir "$root"
exec >"$root/smoke.log" 2>&1
cleanup() {
 local original=$? cg=/sys/fs/cgroup/system.slice/native-parity-gates.service
 trap - EXIT
 set +e
 timeout -k 2 35 /usr/bin/systemctl stop native-parity-gates >"$root/cleanup.stdout" 2>"$root/cleanup.stderr"
 if [[ -e $cg/cgroup.events ]]; then
  cat "$cg/cgroup.events" >"$root/cleanup.events"
  grep -Fxq 'populated 0' "$root/cleanup.events" || original=71
 elif [[ -e $cg ]]; then original=71
 else printf 'owned-cgroup-absent\n' >"$root/cleanup.events"; fi
 if (( original != 0 )); then printf 'INVALID exit=%s\n' "$original" >"$root/result.txt"; fi
 printf '%s\n' "$original" >"$root/smoke.exit" || original=74
 # SHA256SUMS is explicitly excluded from its own input roster.
 # shellcheck disable=SC2094
 (cd "$root" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS) || original=74
 sync -f "$root/result.txt" && sync -f "$root/SHA256SUMS" && sync -f "$root" || original=74
 exit "$original"
}
trap cleanup EXIT
[[ $(id -u) == 0 && -f /sys/fs/cgroup/cgroup.controllers ]] || exit 125
systemd --version >"$root/systemd-version.txt"
newcase() { case_root=$root/$1; mkdir -p "$case_root/evidence"; printf '%s\n' /system.slice/native-parity-gates.service >"$case_root/evidence/cgroup-path.txt"; printf '0\n' >"$case_root/evidence/launcher.exit"; printf '0\n' >"$case_root/evidence/final-exit"; }
check() {
 local want=$1 rc
 set +e
 /bin/bash "$collector" "$case_root"
 rc=$?
 set -e
 printf '%s\n' "$rc" >"$case_root/observed.exit"
 [[ $rc == "$want" ]] || exit 70
 [[ ! -e /sys/fs/cgroup/system.slice/native-parity-gates.service ]] || { grep -Fxq 'populated 0' /sys/fs/cgroup/system.slice/native-parity-gates.service/cgroup.events || exit 71; }
 printf '%s %s\n' "${case_root##*/}" "$rc" >>"$root/cases.txt"
}
start() {
 /usr/bin/systemd-run --unit=native-parity-gates --wait --pipe -p CPUQuota=100% -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=30 -p TimeoutStopSec=5 -p KillMode=control-group -p LimitCORE=0 /bin/bash -c "$1" >"$case_root/launcher.stdout" 2>"$case_root/launcher.stderr"
}
newcase success-unloaded
start 'exit 0'
[[ ! -e /sys/fs/cgroup/system.slice/native-parity-gates.service ]] || exit 72
check 0
newcase failure17
set +e; start 'exit 17'; rc=$?; set -e
[[ $rc == 17 ]] || exit 73
printf '%s\n' "$rc" >"$case_root/evidence/launcher.exit"; printf '17\n' >"$case_root/evidence/final-exit"
check 96
/usr/bin/systemctl reset-failed native-parity-gates || true
newcase malformed-path
printf '/unexpected\n' >"$case_root/evidence/cgroup-path.txt"; check 96
newcase missing-path
rm "$case_root/evidence/cgroup-path.txt"; check 96
newcase missing-final
rm "$case_root/evidence/final-exit"; check 96
newcase nonzero-final
printf '17\n' >"$case_root/evidence/final-exit"; check 96
newcase live-descendant
/usr/bin/systemd-run --unit=native-parity-gates -p CPUQuota=100% -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=30 -p TimeoutStopSec=5 -p KillMode=control-group -p LimitCORE=0 /bin/bash -c 'trap "" TERM; sleep 25 & wait' >"$case_root/start.stdout" 2>"$case_root/start.stderr"
for _ in $(seq 1 20); do [[ -e /sys/fs/cgroup/system.slice/native-parity-gates.service/cgroup.events ]] && break; sleep .1; done
cat /sys/fs/cgroup/system.slice/native-parity-gates.service/cgroup.events >"$case_root/populated.before"
grep -Fxq 'populated 1' "$case_root/populated.before"
cat /sys/fs/cgroup/system.slice/native-parity-gates.service/cgroup.procs >"$case_root/procs.before"
[[ -s $case_root/procs.before ]]
check 0
newcase present-missing-final
/usr/bin/systemd-run --unit=native-parity-gates -p CPUQuota=100% -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=30 -p TimeoutStopSec=5 -p KillMode=control-group /bin/sleep 25 >"$case_root/start.stdout" 2>"$case_root/start.stderr"
rm "$case_root/evidence/final-exit"
check 96
printf 'LIFECYCLE_CORE_SMOKE_PASSED\n' >"$root/result.txt"
printf 'scope=real-systemd-core-only; race-injections=NOT_RUN; compiler=false; performance=false\n' >"$root/scope.txt"
