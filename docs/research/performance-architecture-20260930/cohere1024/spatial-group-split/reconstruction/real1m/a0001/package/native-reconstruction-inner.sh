#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
root=/mnt/borsuk-reconstruction-real1m
phase=${1:?phase}
case $phase in admission|smoke) seconds=600; status=RECONSTRUCTED;; parity) seconds=4800; status=HISTORICAL_RADIAL_PARITY;; *) exit 125;; esac
ev=$root/evidence/$phase
config=$root/$phase.config.json
config_sha=$(cat "$support/$phase.config.sha256")
approval=$root/approval.json
approval_sha=$(cat "$support/approval.sha256")
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == "/borsukreconstruction.slice/borsuk-reconstruction-$phase.service" ]]
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
 cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"
done
test "$(cat "$ev/cpu.max.before")" = '100000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 6442450944
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 128
test "$(cat /sys/fs/cgroup/borsukreconstruction.slice/memory.high)" = max
loop=$(cat "$support/scratch-loop.txt")
[[ $loop =~ ^/dev/loop[0-9]+$ ]]
test "$(findmnt -n -o SOURCE --target "$root")" = "$loop"
test "$(cat "/sys/block/${loop##*/}/loop/dio")" = 1
test "$(cat "/sys/block/${loop##*/}/loop/backing_file")" = "$support/scratch.img"
test "$(du -sb "$root" | cut -f1)" -lt 2147483648
printf '%s  %s\n' 739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52 "$root/bin/compare_native_replay" | sha256sum -c - > "$ev/elf-check.log"
printf '%s  %s\n' "$config_sha" "$config" "$approval_sha" "$approval" | sha256sum -c - > "$ev/authority-check.log"
left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-150 ))
(( left >= seconds+30 )) || exit 124
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 15 "$seconds" "$root/bin/compare_native_replay" --partition-reconstruction "$config" "$config_sha" "$approval" "$approval_sha" > "$ev/native.log" 2>&1
native=$?
set -e
printf '%s\n' "$native" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
for f in memory.high memory.peak memory.events memory.stat memory.swap.peak pids.peak cpu.stat; do
 cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"
 cat "/sys/fs/cgroup/borsukreconstruction.slice/$f" > "$ev/slice.$f.after"
done
for which in '' slice.; do
 test "$(cat "$ev/${which}memory.high.after")" = max
 test "$(cat "$ev/${which}memory.swap.peak.after")" = 0
 awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/${which}memory.events.after"
done
(( native == 0 )) || exit "$native"
receipt=$ev/published/receipt.json
test "$(stat -c %s "$receipt")" -le 65536
test "$(stat -c %s "$ev/native.log")" -le 2097152
jq -e --arg status "$status" --arg config "$config_sha" --arg approval "$approval_sha" --arg phase "$phase" --slurpfile cfg "$config" --slurpfile approved "$approval" '
 .schema=="borsuk-partition-reconstruction-receipt-v1"
 and .complete==true and .status==$status and .requests_opened==false and .truth_opened==false
 and .config.sha256==$config and .approval.sha256==$approval and .original==$approved[0]
 and .generation==$cfg[0].generation_manifest and .plane==$cfg[0].plane_manifest and .leaves==$cfg[0].leaves
 and .binary.bytes==8387384 and .binary.sha256=="739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52"
 and .implementation.example_sha256=="d71ba7717c57d498f3c68c7976a0d2c824ea26472c644c97cbfe676f6a0652cb"
 and .implementation.module_sha256=="715b8a1d028403a361f83bea0e70b1fe4a4f5aabcceae06512faa900d23547c5"
 and .implementation.router_sha256=="60538073b26d5893d1d75e689ebe4b223e575875b5a4d45e5be02f56cd2a3e2f"
 and .centroids_bytes==64000032 and .centroids_sha256=="720b98899031e1aeeeabe0fb2aa0d938485b43141c2438c251b3383a6f0315d8"
 and .centroids.bytes==64000032 and .centroids.sha256==.centroids_sha256
 and .unit_count==31250 and .final_unit_rows==32 and .max_payload_bytes==536870912
 and .requires_successful_file_sync==true and .requires_successful_directory_sync==true
 and .requires_original_successful_terminal_receipt==true
 and (if $phase=="parity" then
 .historical_root_opened==true and .historical_membership_opened==true
 and .historical_parity.old_root==$cfg[0].historical.root
 and .historical_parity.old_membership==$cfg[0].historical.membership
 and .historical_parity.old_leaves==$cfg[0].leaves
 and .radial_outputs.membership.sha256==$cfg[0].historical.membership.sha256
 and .radial_outputs.leaves.sha256==$cfg[0].leaves.sha256
 else .historical_root_opened==false and .historical_membership_opened==false and .historical_parity==null and .radial_outputs==null end)
 ' "$receipt" > "$ev/receipt-validator.log"
# Hash every emitted opaque binary on EC2; collect only receipt/administrative metadata.
jq -r '([.centroids]+(if .radial_outputs==null then [] else [.radial_outputs.root,.radial_outputs.membership,.radial_outputs.leaves] end))[]|[.path,(.bytes|tostring),.sha256]|@tsv' "$receipt" > "$ev/output-pins.tsv"
while IFS=$'\t' read -r path bytes sha; do
 [[ $path == "$ev/published/"* && $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[0-9]+$ ]]
 test "$(stat -c %s "$path")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path" | sha256sum -c - >> "$ev/output-authentication.log"
done < "$ev/output-pins.tsv"
cp "$receipt" "$ev/native-receipt.json"
printf '%s  %s\n' "$config_sha" "$config" "$approval_sha" "$approval" | sha256sum -c - > "$ev/authority-final-check.log"
test "$(du -sb "$root" | cut -f1)" -lt 2147483648
printf 'REAL_NATIVE_RECONSTRUCTION_OUTPUTS_AUTHENTICATED\n'
