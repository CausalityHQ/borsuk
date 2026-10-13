#!/usr/bin/env bash
set -euo pipefail
# Prospective draft: root must freeze the final reviewed source before execution.
[[ 13eb0bdc84c55db3f3e9433f56a1ee7d6bed7d8c =~ ^[0-9a-f]{40}$ ]] || { echo 'source freeze pending' >&2; exit 125; }
root=/mnt/borsuk-paired-qualification
ev="$root/evidence/curve-real"
mkdir "$ev"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
mkdir -m 700 "$root/closed"
timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width128-scale-cold-a0001/terminal/evidence.tar.gz "$root/closed/evidence.tar.gz" > "$ev/archive-get.json"
[[ $(stat -c %s "$root/closed/evidence.tar.gz") == 30123447 ]]
printf '%s  %s\n' 4c14db39f57d00b6ae783d000779f3d513fb0356213abd027d6cbf981f70ed1b "$root/closed/evidence.tar.gz" | sha256sum --strict -c - > "$ev/archive-check.log"
tar -tzf "$root/closed/evidence.tar.gz" > "$ev/archive-members.txt"
[[ $(grep -Fxc results/formal-original-a1.jsonl "$ev/archive-members.txt") == 1 ]]
tar -xOzf "$root/closed/evidence.tar.gz" results/formal-original-a1.jsonl > "$root/closed/original-a1.jsonl"
[[ $(stat -c %s "$root/closed/original-a1.jsonl") == 35180200 ]]
printf '%s  %s\n' 0855a896f5142b0e84c22fb2f7b1c897909a08f5d680051427ca97de9962ee4b "$root/closed/original-a1.jsonl" | sha256sum --strict -c - > "$ev/result-check.log"
timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-cover-cost-curve-a0001/inputs/curve-real.config.json "$root/curve-real.config.json" > "$ev/config-get.json"
printf '%s  %s\n' 5cd2e59a3f480fc21ebd40c422dfe4be66482af02eeb3c0d0ae175a83d6cb400 "$root/curve-real.config.json" | sha256sum --strict -c - > "$ev/config-check.log"
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
(( left <= 120 )) || left=120
set +e
systemd-run --expand-environment=no --unit=borsuk-loss-curve-real --wait --pipe -p WorkingDirectory="$root/source" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=512 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$(( left + 120 ))" --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" /bin/bash -c 'set -euo pipefail
trap '\''exit 97'\'' TERM HUP INT
root=/mnt/borsuk-paired-qualification
ev="$root/evidence/curve-real"
cd "$root/source"
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1 RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER=
unset BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_TARGET
rustc -vV > "$ev/rustc-version.txt"
cargo -vV > "$ev/cargo-version.txt"
printf "%s\n" "features=manifest-default; no scalar-control" "target=rustc-host; CARGO_BUILD_TARGET unset" "RUSTFLAGS unset; CARGO_ENCODED_RUSTFLAGS unset" "CARGO_BUILD_JOBS=1; BORSUK_CPU_THREADS=1; RAYON_NUM_THREADS=1" > "$ev/build-configuration.txt"
cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
printf "%s\n" "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = "100000 100000"
test "$(cat "$ev/cpuset.cpus.effective.before")" = "0"
test "$(cat "$ev/memory.max.before")" = 1073741824
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 512
sha256sum --strict --check "$root/source-files.sha256" > "$ev/source-before.log"
left=$(( $(cat "$root/gate-deadline.epoch") - $(date +%s) ))
(( left > 30 )) || exit 124
(( left <= 120 )) || left=120
printf "%s\n" '\''/mnt/borsuk-paired-qualification/source/target/release/examples/compare_native_replay --source-cover-scale /mnt/borsuk-paired-qualification/curve-real.config.json 5cd2e59a3f480fc21ebd40c422dfe4be66482af02eeb3c0d0ae175a83d6cb400 /mnt/borsuk-paired-qualification/evidence/curve-real/curve.json'\'' > "$ev/native-command.txt"
date -u +%FT%TZ > "$ev/native-started.txt"

set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout --kill-after=30 "$left" /mnt/borsuk-paired-qualification/source/target/release/examples/compare_native_replay --source-cover-scale /mnt/borsuk-paired-qualification/curve-real.config.json 5cd2e59a3f480fc21ebd40c422dfe4be66482af02eeb3c0d0ae175a83d6cb400 /mnt/borsuk-paired-qualification/evidence/curve-real/curve.json > "$ev/native.log" 2>&1
status=$?
set -e
printf "%s\n" "$status" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
sha256sum --strict --check "$root/source-files.sha256" > "$ev/source-after.log"
cmp "$ev/source-before.log" "$ev/source-after.log"
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
(( status == 0 )) || exit "$status"
jq -e '\''.schema=="borsuk-source-cover-cost-curve-v1" and .status=="COUNTERFACTUAL_COST_ONLY" and .complete==true and .count==1000 and .counterfactual_cost_only==true and .native_ann_called==false and .corpus_queries_truth_bodies_opened==false and .claims_latency==false and .claims_quality==false and .claims_bill==false and .production_change==false and [.curves[].get_cap]==[128,96,64,48,32] and all(.curves[];.predicted_queries==1000)'\'' "$ev/curve.json" > "$ev/report-check.txt"
exit "$status"' > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf "%s\n" "$manager" > "$ev/manager-exit.txt"
set +e
timeout --kill-after=2 10 systemctl show borsuk-loss-curve-real.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf "%s\n" "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
if test -s "$ev/cgroup-path.txt"; then cg=$(cat "$ev/cgroup-path.txt"); if [[ "$cg" == "/system.slice/borsuk-loss-curve-real.service" ]]; then if test ! -d "/sys/fs/cgroup$cg"; then drained=true; elif grep -qx "populated 0" "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi; fi; fi
native=null; if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ "$native" =~ ^[0-9]+$ ]] || native=null; fi
jq -n --arg schema borsuk-sealed-loss-native-stage-v1 --arg stage 'curve-real' --arg source 13eb0bdc84c55db3f3e9433f56a1ee7d6bed7d8c --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:$schema,stage:$stage,source_commit:$source,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,performance_claim:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-cover-cost-curve-a0001/stages/curve-real/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
cat "$ev/terminal.json"
[[ "$native" == 0 && "$manager" == 0 && "$drained" == true ]]
