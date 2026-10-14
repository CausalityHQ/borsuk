#!/usr/bin/env bash
set -euo pipefail
# Prospective draft: root must freeze the final reviewed source before execution.
[[ cb34ca25da00b8282e4885639c22b80eef4ef062 =~ ^[0-9a-f]{40}$ ]] || { echo 'source freeze pending' >&2; exit 125; }
root=/mnt/borsuk-paired-qualification
ev="$root/evidence/workspace-test-build"
mkdir "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
set +e
systemd-run --expand-environment=no --unit=borsuk-loss-workspace-test-build --wait --pipe -p WorkingDirectory="$root/source" -p CPUQuota=400% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0-3 -p MemoryMax=14G -p MemorySwapMax=0 -p TasksMax=512 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$(( left + 120 ))" --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" /bin/bash -c 'set -euo pipefail
trap '\''exit 97'\'' TERM HUP INT
root=/mnt/borsuk-paired-qualification
ev="$root/evidence/workspace-test-build"
cd "$root/source"
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER=
unset BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_TARGET
rustc -vV > "$ev/rustc-version.txt"
cargo -vV > "$ev/cargo-version.txt"
printf "%s\n" "features=manifest-default; no scalar-control" "target=rustc-host; CARGO_BUILD_TARGET unset" "RUSTFLAGS unset; CARGO_ENCODED_RUSTFLAGS unset" "CARGO_BUILD_JOBS=1; BORSUK_CPU_THREADS=1; RAYON_NUM_THREADS=1" > "$ev/build-configuration.txt"
cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
printf "%s\n" "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = "400000 100000"
test "$(cat "$ev/cpuset.cpus.effective.before")" = "0-3"
test "$(cat "$ev/memory.max.before")" = 15032385536
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 512
sha256sum --strict --check "$root/source-files.sha256" > "$ev/source-before.log"
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
printf "%s\n" '\''env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh'\'' > "$ev/native-command.txt"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout --kill-after=30 "$left" env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh > "$ev/native.log" 2>&1
status=$?
set -e
printf "%s\n" "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
sha256sum --strict --check "$root/source-files.sha256" > "$ev/source-after.log"
cmp "$ev/source-before.log" "$ev/source-after.log"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
(( status == 0 )) || exit "$status"
exit "$status"' > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf "%s\n" "$manager" > "$ev/manager-exit.txt"
set +e
timeout --kill-after=2 10 systemctl show borsuk-loss-workspace-test-build.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf "%s\n" "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
if test -s "$ev/cgroup-path.txt"; then cg=$(cat "$ev/cgroup-path.txt"); if [[ "$cg" == "/system.slice/borsuk-loss-workspace-test-build.service" ]]; then if test ! -d "/sys/fs/cgroup$cg"; then drained=true; elif grep -qx "populated 0" "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi; fi; fi
native=null; if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ "$native" =~ ^[0-9]+$ ]] || native=null; fi
jq -n --arg schema borsuk-sealed-loss-native-stage-v1 --arg stage 'workspace-test-build' --arg source cb34ca25da00b8282e4885639c22b80eef4ef062 --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:$schema,stage:$stage,source_commit:$source,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261014/sq8-selected-row-planner-repair-green-a0001/stages/workspace-test-build/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
cat "$ev/terminal.json"
[[ "$native" == 0 && "$manager" == 0 && "$drained" == true ]]
