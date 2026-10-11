#!/bin/bash
set -euo pipefail
support=/opt/borsuk-neighborhood-gate
root=/mnt/borsuk-neighborhood-data
stage=${1:?stage}
[[ $stage == admission || $stage == smoke ]]
ev="$root/evidence/$stage"
config="$root/$stage.config.json"
config_sha=$(cat "$support/$stage.config.sha256")
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == "/borsukneighborhood.slice/borsuk-neighborhood-$stage.service" ]]
cat /sys/fs/cgroup/borsukneighborhood.slice/memory.high > "$ev/slice.memory.high.before"
test "$(cat "$ev/slice.memory.high.before")" = max
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
 cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"
done
test "$(cat "$ev/cpu.max.before")" = '100000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 6442450944
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
test "$(cat /sys/fs/cgroup/borsukneighborhood.slice/memory.max)" = 6442450944
test "$(cat /sys/fs/cgroup/borsukneighborhood.slice/memory.swap.max)" = 0
loop=$(cat "$support/scratch-loop.txt")
[[ $loop =~ ^/dev/loop[0-9]+$ ]]
test "$(findmnt -n -o SOURCE --target "$root")" = "$loop"
test "$(cat "/sys/block/${loop##*/}/loop/backing_file")" = "$support/scratch.img"
test "$(cat "/sys/block/${loop##*/}/loop/dio")" = 1
cat "/sys/fs/cgroup$cg/io.stat" > "$ev/io.stat.before"
printf '%s  %s\n' 77a8e582e426f1d8500f26b66741ddb978ce7f0d1ef5250b5111c7a3e1da1010 "$root/bin/compare_native_replay" | sha256sum -c - > "$ev/elf-check.log"
printf '%s  %s\n' "$config_sha" "$config" | sha256sum -c - > "$ev/config-check.log"
test "$(du -sb "$root" | cut -f1)" -lt 8589934592
left=$(( $(cat /run/borsuk-neighborhood-machine-stop.epoch)-$(date +%s)-120 ))
(( left >= 3600 )) || exit 124
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 15 3600 "$root/bin/compare_native_replay" --source-neighborhood "$config" "$config_sha" > "$ev/native.log" 2>&1
native=$?
set -e
printf '%s\n' "$native" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
for f in memory.high memory.peak memory.events memory.stat memory.swap.peak pids.peak cpu.stat; do
 cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"
 cat "/sys/fs/cgroup/borsukneighborhood.slice/$f" > "$ev/slice.$f.after"
done
test "$(cat "$ev/slice.memory.high.after")" = max
for which in '' slice.; do
 peak=$(cat "$ev/${which}memory.peak.after")
 [[ $peak =~ ^[0-9]{1,12}$ ]]
 test "$(cat "$ev/${which}memory.swap.peak.after")" = 0
 awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/${which}memory.events.after"
done
printf 'RESOURCE_RECEIPTS_VALID\n' > "$ev/resource-evidence-valid.txt"
(( native == 0 )) || exit "$native"
test "$(stat -c %s "$ev/anchors.json")" -le 4194304
test "$(stat -c %s "$ev/report.json")" -le 2097152
test "$(stat -c %s "$ev/native.log")" -le 2097152
seal_sha=$(sha256sum "$ev/anchors.json"); seal_sha=${seal_sha%% *}
jq -e --arg config "$config_sha" --arg seal "$seal_sha" --slurpfile c "$config" '
 .schema=="borsuk-source-neighborhood-v1" and .status=="MEASURED" and .complete==true
 and .interpretation=="EXPLORATORY_INCONCLUSIVE" and .qualified==false
 and .implementation_sha256=="239c80dbe7d0a978a88791d98b9cf0827b10d91105367103d312c89987eca29f"
 and .config_sha256==$config and .generation_sha256==$c[0].generation_manifest.sha256
 and .canonical_sha256==$c[0].canonical.sha256 and .anchor_seal_sha256==$seal
 and .rows==$c[0].rows and .dimensions==$c[0].dimensions and .scope==$c[0].scope
 and .anchor_count==$c[0].anchor_count and .neighbor_count==$c[0].neighbor_count
 and .canonical_full_authenticated_passes==2 and .anchor_seal_durable_before_neighbor_scan==true
 and .source_only==true and .external_gate_required==true and .report_alone_accepted==false
 and .recall_qps_cold_vendor_or_100m_claim==false and .ann_arm_selected==false
 and .allocation.peak_payload_bytes<=$c[0].max_payload_bytes
 ' "$ev/report.json" > /dev/null
jq -e --arg config "$config_sha" --slurpfile c "$config" '
 .schema=="borsuk-source-neighborhood-anchor-seal-v1" and .status=="SEALED"
 and .config_sha256==$config and .config==$c[0]
 and .implementation_sha256=="239c80dbe7d0a978a88791d98b9cf0827b10d91105367103d312c89987eca29f"
 and .canonical_sha256==$c[0].canonical.sha256
 ' "$ev/anchors.json" > /dev/null
printf '%s  %s\n' "$config_sha" "$config" | sha256sum -c - > "$ev/config-final-check.log"
printf '%s  %s\n' 77a8e582e426f1d8500f26b66741ddb978ce7f0d1ef5250b5111c7a3e1da1010 "$root/bin/compare_native_replay" | sha256sum -c - > "$ev/elf-final-check.log"
test "$(du -sb "$root" | cut -f1)" -lt 8589934592
test "$(cat "/sys/block/${loop##*/}/loop/dio")" = 1
printf 'REAL_INPUT_AND_DURABLE_SOURCE_SEAL_AUTHENTICATED\n'
