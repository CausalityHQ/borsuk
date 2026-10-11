#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-scale1m
ev="$root/evidence/staging0"
mkdir "$root/closed/s3/staging0-scratch"
mkdir "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
(( left <= 120 )) || left=120
set +e
systemd-run --expand-environment=no --unit=borsuk-rust-replay-staging0 --wait --pipe -p WorkingDirectory="$root" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p RuntimeMaxSec="$(( left + 120 ))" --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" /bin/bash -c 'set -euo pipefail
trap '\''exit 97'\'' TERM HUP INT
root=/mnt/borsuk-scale1m
ev="$root/evidence/staging0"
cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
printf "%s\n" "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = "100000 100000"
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 536870912
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
(( left <= 120 )) || left=120
printf "%s\n" '\''/mnt/borsuk-scale1m/bin/check_cohere_native_baseline /mnt/borsuk-scale1m/closed/s3/canary-one-query-config.json b3bc10dd04c6fbb76ed5336b6a9fd1cd04d530741ed55ad5ac627963fd3583c8 /mnt/borsuk-scale1m/closed/s3/staging0-result.jsonl'\'' > "$ev/native-command.txt"
printf "%s  %s\n" 72ac92e78f0e12ada77f6f6dc8c48d66bc547067a8e12440b469424993fe21f0 "$root/bin/check_cohere_native_baseline" | sha256sum -c - > "$ev/release-before.log"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout --kill-after=30 "$left" /mnt/borsuk-scale1m/bin/check_cohere_native_baseline /mnt/borsuk-scale1m/closed/s3/canary-one-query-config.json b3bc10dd04c6fbb76ed5336b6a9fd1cd04d530741ed55ad5ac627963fd3583c8 /mnt/borsuk-scale1m/closed/s3/staging0-result.jsonl > "$ev/native.log" 2>&1
status=$?
set -e
printf "%s\n" "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
exit "$status"' > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf "%s\n" "$manager" > "$ev/manager-exit.txt"
set +e
timeout --kill-after=2 10 systemctl show borsuk-rust-replay-staging0.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf "%s\n" "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
if test -s "$ev/cgroup-path.txt"; then cg=$(cat "$ev/cgroup-path.txt"); if [[ "$cg" == "/system.slice/borsuk-rust-replay-staging0.service" ]]; then if test ! -d "/sys/fs/cgroup$cg"; then drained=true; elif grep -qx "populated 0" "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi; fi; fi
native=null; if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ "$native" =~ ^[0-9]+$ ]] || native=null; fi
if [[ "$native" == 0 ]]; then cp --no-clobber /mnt/borsuk-scale1m/closed/s3/staging0-result.jsonl "$ev/staging0-result.jsonl"; cp --no-clobber /mnt/borsuk-scale1m/closed/s3/canary-one-query-config.json "$ev/staging0-config.json"; fi
jq -n --arg schema borsuk-staging0-rust-replay-stage-v1 --arg stage 'staging0' --arg source 6e78d393b4d7dadf19befeeabe3dc20a77319a26 --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:$schema,stage:$stage,source_commit:$source,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-query527-a0001/stages/staging0/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
cat "$ev/terminal.json"
[[ "$native" == 0 && "$manager" == 0 && "$drained" == true ]]
