#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-scale-closure-qualification
ev="$root/evidence/green-codec-r2"
mkdir "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
set +e
systemd-run --unit=borsuk-scale-green-codec-r2 --wait --pipe -p WorkingDirectory="$root/source" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p RuntimeMaxSec="$(( left + 120 ))" --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" /bin/bash -c 'set -euo pipefail
trap '\''exit 97'\'' TERM HUP INT
root=/mnt/borsuk-scale-closure-qualification
ev="$root/evidence/green-codec-r2"
cd "$root/source"
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER=
unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
printf "%s\n" "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = "100000 100000"
test "$(cat "$ev/cpuset.cpus.effective.before")" = "0"
test "$(cat "$ev/memory.max.before")" = 1073741824
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
sha256sum --check "$root/source-files.sha256" > "$ev/source-before.log"
test_elf=$(jq -Rr '\''fromjson? | select(.reason=="compiler-artifact" and .target.name=="borsuk" and .profile.test==true and .executable!=null) | .executable'\'' "$root/evidence/green-lib-recompile/native.log")
[[ "$test_elf" == "$root/source/target/debug/deps/borsuk-"* && "$test_elf" != *$'\''\n'\''* ]]
test -f "$test_elf" && test ! -L "$test_elf" && test -x "$test_elf"
sha256sum "$test_elf" > "$ev/test-elf-sha.txt"
stat -c %s "$test_elf" > "$ev/test-elf-bytes.txt"
printf "%s\n" "$test_elf" > "$ev/test-elf-path.txt"
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
printf "%s\n" '\''"$test_elf" two_bit_generation::source_walk_tests::scale_scattered_codec_walk_covers_rows_and_partial_tail --exact --test-threads=1'\'' > "$ev/native-command.txt"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout --kill-after=30 "$left" "$test_elf" two_bit_generation::source_walk_tests::scale_scattered_codec_walk_covers_rows_and_partial_tail --exact --test-threads=1 > "$ev/native.log" 2>&1
status=$?
set -e
printf "%s\n" "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
sha256sum --check "$root/source-files.sha256" > "$ev/source-after.log"
cmp "$ev/source-before.log" "$ev/source-after.log"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
(( status == 0 )) || exit "$status"
exit "$status"' > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf "%s\n" "$manager" > "$ev/manager-exit.txt"
set +e
timeout --kill-after=2 10 systemctl show borsuk-scale-green-codec-r2.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf "%s\n" "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
if test -s "$ev/cgroup-path.txt"; then cg=$(cat "$ev/cgroup-path.txt"); if [[ "$cg" == "/system.slice/borsuk-scale-green-codec-r2.service" ]]; then if test ! -d "/sys/fs/cgroup$cg"; then drained=true; elif grep -qx "populated 0" "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi; fi; fi
native=null; if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ "$native" =~ ^[0-9]+$ ]] || native=null; fi
jq -n --arg schema borsuk-scale-closure-native-stage-v1 --arg stage 'green-codec-r2' --arg source 6e78d393b4d7dadf19befeeabe3dc20a77319a26 --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:$schema,stage:$stage,source_commit:$source,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-native-qualification-a0001/stages/green-codec-r2/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
cat "$ev/terminal.json"
[[ "$native" == 0 && "$manager" == 0 && "$drained" == true ]]
