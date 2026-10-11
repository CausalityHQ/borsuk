#!/bin/bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
test ! -e "$support"
mkdir "$support"
cat > "$support/admission.config.json" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
{
  "schema": "borsuk-partition-reconstruction-config-v1",
  "generation_manifest": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/generation.json",
    "bytes": 27501,
    "sha256": "cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307"
  },
  "plane_manifest": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/plane.json",
    "bytes": 653,
    "sha256": "e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6"
  },
  "leaves": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/leaves.bin",
    "bytes": 64125000,
    "sha256": "2b302a08d2e583c6470c95f16b46f3db60b86dc782ae555365f201f74d5323c6"
  },
  "historical": null,
  "caller_pinned_bytes": 0,
  "max_payload_bytes": 536870912,
  "output_directory": "/mnt/borsuk-reconstruction-real1m/evidence/admission/published"
}
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/admission.config.sha256" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
c27c81943b7eec931c9b474c121f5f88182a035fec0365a34a934a6e274ce88a
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/approval.json" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
{
  "schema": "borsuk-partition-reconstruction-approval-v1",
  "generation_sha256": "cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307",
  "generation": 1,
  "base_epoch": 0,
  "plane_sha256": "e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6",
  "discovery_sha256": "5641146cd167de6be232a71fd54162a9863337d3bdbbda0d2e4857e88826c2da",
  "input_schema": "borsuk-two-bit-plane-v3",
  "input_root_sha256": "e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6",
  "centroids_sha256": "720b98899031e1aeeeabe0fb2aa0d938485b43141c2438c251b3383a6f0315d8",
  "profile": "scale1m",
  "rows": 1000000,
  "dimensions": 1024
}
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/approval.sha256" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
8f2b7bee0066cd0af9363566c5fb726486f883f4c36d0cdb1024d3fd382a73ff
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/campaign-prefix.txt" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
research/semantic-router/20261011/reconstruction-real1m-a0001
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/execution-identity.json" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
{
  "schema": "borsuk-reconstruction-real1m-execution-v1",
  "attempt": "reconstruction-real1m-a0001",
  "instance_id": "i-0e7ec79ad22db80d1",
  "source_commit": "91107713f6f7bd700b5b6d92a83e438febac8069",
  "published_commit": "319f8c4bbac69f522fc25a0ecd93f2e9232e9b84",
  "native_source_identity_sha256": "bef40caf201f947718554128b544cb77feedf907b897faa59d32e8d36bf62772",
  "request_sha256": "6d1a8a901bf03d3fe7fbf728b314611c2ff8923803f661ec227e7fae5ec2536d",
  "package_manifest_sha256": "a170186260bd411bc95392b929495b8fe3e637c8a1168246f4ae1ccb23760ca3",
  "guest_deadline_seconds": 7200,
  "external_deadline_seconds": 7320,
  "total_cost_ceiling_usd": 0.50
}
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/native-reconstruction-inner.sh" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
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
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/parity.config.json" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
{
  "schema": "borsuk-partition-reconstruction-config-v1",
  "generation_manifest": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/generation.json",
    "bytes": 27501,
    "sha256": "cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307"
  },
  "plane_manifest": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/plane.json",
    "bytes": 653,
    "sha256": "e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6"
  },
  "leaves": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/leaves.bin",
    "bytes": 64125000,
    "sha256": "2b302a08d2e583c6470c95f16b46f3db60b86dc782ae555365f201f74d5323c6"
  },
  "historical": {
    "root": {
      "path": "/mnt/borsuk-reconstruction-real1m/inputs/router-root.bin",
      "bytes": 3041472,
      "sha256": "8fe6f7c690e454002ba7d319353b114d574d5f5cea7e1f8008c7288b41c41492"
    },
    "membership": {
      "path": "/mnt/borsuk-reconstruction-real1m/inputs/membership.bin",
      "bytes": 125000,
      "sha256": "9864f7c207430cda4b5f58798c3957aaf0b8bfa0553920e18f08315e5fe6c463"
    }
  },
  "caller_pinned_bytes": 0,
  "max_payload_bytes": 536870912,
  "output_directory": "/mnt/borsuk-reconstruction-real1m/evidence/parity/published"
}
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/parity.config.sha256" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
2d7887c0d367055a6500cac6031aaa74a7dce07e2fe8179744117d02b5cc9b9f
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/run-reconstruction-stage.sh" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
root=/mnt/borsuk-reconstruction-real1m
phase=${1:?phase}
case $phase in admission|smoke) seconds=600;; parity) seconds=4800;; *) exit 125;; esac
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
if [[ $phase == smoke || $phase == parity ]]; then
 jq -e '.stage=="admission" and .status=="GATE_ACCEPTED" and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$support/admission.archived-terminal.json" > /dev/null
 test -f "$support/admission.root-accepted.json"
fi
if [[ $phase == parity ]]; then
 jq -e '.stage=="smoke" and .status=="GATE_ACCEPTED" and .native_exit==0 and .manager_exit==0 and .owned_cgroup_drained==true' "$support/smoke.archived-terminal.json" > /dev/null
 test -f "$support/smoke.root-accepted.json"
fi
ev=$root/evidence/$phase
mkdir "$ev"
cp "$support/$phase.config.json" "$root/$phase.config.json"
chmod 0400 "$root/$phase.config.json"
if [[ $phase == admission ]]; then
 cp "$support/approval.json" "$root/approval.json"
 chmod 0400 "$root/approval.json"
fi
cp "$support/execution-identity.json" "$ev/execution-identity.json"
left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-150 ))
(( left >= seconds+60 )) || exit 124
unit=borsuk-reconstruction-$phase
set +e
systemd-run --unit="$unit" --slice=borsukreconstruction.slice --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=6G -p MemorySwapMax=0 -p IOAccounting=yes -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$((seconds+30))" /bin/bash "$support/native-reconstruction-inner.sh" "$phase" > "$ev/systemd-run.log" 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > "$ev/manager-exit.txt"
set +e
timeout -k 2 10 systemctl show "$unit.service" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr"
show_status=$?
set -e
printf '%s\n' "$show_status" > "$ev/systemd-show-exit.txt"
drained=false
cg=/borsukreconstruction.slice/$unit.service
if test ! -d "/sys/fs/cgroup$cg"; then drained=true
elif grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi
if [[ $drained != true ]]; then
 test ! -e "/sys/fs/cgroup$cg/cgroup.kill" || printf '1\n' > "/sys/fs/cgroup$cg/cgroup.kill"
 # A failed drain is never converted to success by cleanup.
 timeout -k 2 10 systemctl stop "$unit.service" > "$ev/failed-drain-stop.log" 2>&1 || true
fi
native=null
if test -s "$ev/native-exit.txt"; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
status=INVALID
if [[ $manager == 0 && $native == 0 && $drained == true ]] && test -s "$ev/native-receipt.json"; then status=GATE_ACCEPTED; fi
config_sha=$(cat "$support/$phase.config.sha256")
approval_sha=$(cat "$support/approval.sha256")
receipt_sha=null
if test -f "$ev/native-receipt.json"; then h=$(sha256sum "$ev/native-receipt.json"); receipt_sha=${h%% *}; fi
jq -n --slurpfile execution "$ev/execution-identity.json" --arg stage "$phase" --arg status "$status" --arg config "$config_sha" --arg approval "$approval_sha" --arg receipt "$receipt_sha" --argjson manager "$manager" --argjson native "$native" --argjson drained "$drained" '{schema:"borsuk-reconstruction-real1m-stage-v1",execution:$execution[0],stage:$stage,status:$status,source_commit:"91107713f6f7bd700b5b6d92a83e438febac8069",elf_sha256:"739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52",config_sha256:$config,approval_sha256:$approval,native_receipt_sha256:$receipt,manager_exit:$manager,native_exit:$native,owned_cgroup_drained:$drained,scientific_measurement:false,cold_s3_claim:false,vendor_win:false}' > "$ev/terminal.json"
(cd "$ev" && find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
sync -f "$ev/terminal.json"
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
prefix=$(cat "$support/campaign-prefix.txt")
[[ $prefix == research/semantic-router/20261011/reconstruction-real1m-a0001 ]]
deadline=$(( $(date +%s)+90 ))
upload() {
 local file=$1 left checksum machine_left
 left=$(( deadline-$(date +%s)-2 ))
 machine_left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-30 ))
 (( left <= machine_left )) || left=$machine_left
 (( left > 0 )) || return 1
 checksum=$(sha256sum "$file"); checksum=${checksum%% *}
 checksum=$(printf '%s' "$checksum" | xxd -r -p | base64 -w0)
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/$phase/${file##*/}" --body "$file" --if-none-match '*' --checksum-algorithm SHA256 --checksum-sha256 "$checksum" > "$support/$phase.${file##*/}.put.json"
 jq -e --arg checksum "$checksum" '.ChecksumSHA256==$checksum' "$support/$phase.${file##*/}.put.json" > /dev/null
}
for file in "$ev"/*; do
 test -f "$file" && test ! -L "$file" || continue
 [[ ${file##*/} != terminal.json ]] || continue
 test "$(stat -c %s "$file")" -le 2097152
 upload "$file"
done
upload "$ev/terminal.json"
cp "$ev/terminal.json" "$support/$phase.archived-terminal.json"
cat "$ev/terminal.json"
[[ $status == GATE_ACCEPTED ]]
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/setup-data.sh" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
#!/bin/bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
root=/mnt/borsuk-reconstruction-real1m
phase=${1:?install or hydrate}
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
if [[ $phase == install ]]; then
 timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
 timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time xxd
 cd "$support"
 curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
 test "$(stat -c %s awscliv2.zip)" = 73022935
 printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
 timeout -k 5 60 unzip -q awscliv2.zip
 timeout -k 5 60 ./aws/install
 [[ $(aws --version) == aws-cli/2.36.11\ * ]]
 cat > /etc/systemd/system/borsukreconstruction.slice <<'SLICE'
[Unit]
Description=Owned source-neighborhood hydration and native charge boundary
[Slice]
CPUAccounting=yes
CPUQuota=200%
CPUQuotaPeriodSec=100ms
AllowedCPUs=0-1
MemoryAccounting=yes
IOAccounting=yes
MemoryMax=6442450944
MemorySwapMax=0
TasksAccounting=yes
TasksMax=128
SLICE
 systemctl daemon-reload
 systemctl start borsukreconstruction.slice
 systemctl is-active --quiet borsuk-reconstruction-data-shutdown.timer
 exit 0
fi
[[ $phase == hydrate ]]
deadline=$(( $(date +%s)+900 ))
machine=$(cat /run/borsuk-reconstruction-machine-stop.epoch)
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /borsukreconstruction.slice/borsuk-reconstruction-hydrate.service ]]
cat /sys/fs/cgroup/borsukreconstruction.slice/memory.high > "$support/hydration.slice.memory.high.before"
test "$(cat "$support/hydration.slice.memory.high.before")" = max
test "$(cat /sys/fs/cgroup/borsukreconstruction.slice/memory.max)" = 6442450944
test "$(cat /sys/fs/cgroup/borsukreconstruction.slice/memory.swap.max)" = 0
for dir in /sys/fs/cgroup /sys/fs/cgroup/borsukreconstruction.slice; do
 grep -qw io "$dir/cgroup.subtree_control"
done
for f in cgroup.controllers cgroup.subtree_control io.stat; do
 cat "/sys/fs/cgroup/borsukreconstruction.slice/$f" > "$support/hydration.slice.$f"
done
cat "/sys/fs/cgroup$cg/io.stat" > "$support/hydration.service.io.stat"
test ! -e "$root"
test ! -e "$support/scratch.img"
test "$(df -B1 --output=avail "$support" | tail -n 1)" -gt 2147483648
truncate -s 2147483648 "$support/scratch.img"
timeout -k 2 30 mkfs.ext4 -q -F "$support/scratch.img"
mkdir "$root"
loop=$(timeout -k 2 10 losetup --find --show --direct-io=on "$support/scratch.img")
[[ $loop =~ ^/dev/loop[0-9]+$ ]]
printf '%s\n' "$loop" > "$support/scratch-loop.txt"
test "$(cat "/sys/block/${loop##*/}/loop/dio")" = 1
test "$(cat "/sys/block/${loop##*/}/loop/backing_file")" = "$support/scratch.img"
timeout -k 2 10 mount -o nodev,nosuid "$loop" "$root"
test "$(findmnt -n -o SOURCE --target "$root")" = "$loop"
test "$(findmnt -n -o FSTYPE --target "$root")" = ext4
test "$(stat -f -c '%S' "$root")" = 4096
test "$(( $(stat -f -c '%b' "$root") * 4096 ))" -le 2147483648
mkdir "$root/bin" "$root/inputs" "$root/evidence"
jq -e '.schema=="borsuk-reconstruction-inputs-v1" and .authority_pending==false and (.inputs|length)==5 and ([.inputs[].role]|sort)==["generation_manifest","leaves","membership","plane_manifest","router_root"]' "$support/transport.json" > /dev/null
jq -r '(.inputs+[.binary])[]|[.key,.pin.path,(.pin.bytes|tostring),.pin.sha256]|@tsv' "$support/transport.json" > "$support/transport.tsv"
while IFS=$'\t' read -r key path bytes sha; do
 [[ $path == "$root/inputs/"* || $path == "$root/bin/compare_native_replay" ]]
 [[ ${path##*/} =~ ^[a-zA-Z0-9._-]+$ && $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[0-9]+$ ]]
 test ! -e "$path"
 left=$(( deadline-$(date +%s)-5 ))
 machine_left=$(( machine-$(date +%s)-120 ))
 (( left <= machine_left )) || left=$machine_left
 (( left > 0 )) || exit 124
 (( left <= 600 )) || left=600
 timeout -k 5 "$left" aws s3 cp "s3://borsuk-bench-453182569524-euc1/$key" "$path.part" --only-show-errors
 test "$(stat -c %s "$path.part")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path.part" | sha256sum -c -
 mv "$path.part" "$path"
 chmod 0400 "$path"
done < "$support/transport.tsv"
(( $(date +%s) <= deadline )) || exit 124
chmod 0500 "$root/bin/compare_native_replay"
test "$(du -sb "$root" | cut -f1)" -lt 2147483648
for f in memory.high memory.max memory.swap.max memory.peak memory.events memory.stat memory.swap.peak cpu.max cpuset.cpus.effective pids.max; do
 cat "/sys/fs/cgroup/borsukreconstruction.slice/$f" > "$support/hydration.slice.$f"
done
test "$(cat "$support/hydration.slice.memory.swap.peak")" = 0
test "$(cat "$support/hydration.slice.memory.high")" = max
peak=$(cat "$support/hydration.slice.memory.peak")
[[ $peak =~ ^[0-9]{1,12}$ ]]
peak=$(cat "/sys/fs/cgroup$cg/memory.peak")
printf '%s\n' "$peak" > "$support/hydration.service.memory.peak"
[[ $peak =~ ^[0-9]{1,12}$ ]]
test "$(cat "/sys/block/${loop##*/}/loop/dio")" = 1
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$support/hydration.slice.memory.events"
printf 'SIX_INPUTS_AND_ELF_AUTHENTICATED\n'
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/setup-reconstruction-host.sh" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
cd "$support"
sha256sum -c support.sha256 > package-check.log
systemctl is-active --quiet borsuk-reconstruction-data-shutdown.timer
phase=install
finish() {
 original=$?
 trap - EXIT
 printf 'SETUP_PHASE=%s ORIGINAL_EXIT=%s\n' "$phase" "$original"
 if (( original != 0 )); then
  for f in install.log hydrate.log; do test ! -f "$f" || tail -c 4000 "$f"; done
 fi
 exit "$original"
}
trap finish EXIT
systemd-run --unit=borsuk-reconstruction-install --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=2G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=570 -p TimeoutStopSec=25 /bin/bash "$support/setup-data.sh" install > install.log 2>&1
phase=identity
token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document > instance-identity.json
jq -e --slurpfile expected execution-identity.json '.instanceId==$expected[0].instance_id and .imageId=="ami-0b8a830d6339a9758" and .region=="eu-central-1" and .instanceType=="c7i.xlarge" and .availabilityZone=="eu-central-1c"' instance-identity.json > /dev/null
phase=hydrate
systemd-run --unit=borsuk-reconstruction-hydrate --slice=borsukreconstruction.slice --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=6G -p MemorySwapMax=0 -p IOAccounting=yes -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=870 -p TimeoutStopSec=25 /bin/bash "$support/setup-data.sh" hydrate > hydrate.log 2>&1
phase=complete
for unit in borsuk-reconstruction-install borsuk-reconstruction-hydrate; do
 cg=/sys/fs/cgroup/borsukreconstruction.slice/$unit.service
 [[ $unit != borsuk-reconstruction-install ]] || cg=/sys/fs/cgroup/system.slice/$unit.service
 if test -d "$cg"; then grep -qx 'populated 0' "$cg/cgroup.events"; fi
done
test "$(cat /sys/fs/cgroup/borsukreconstruction.slice/memory.swap.peak)" = 0
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' /sys/fs/cgroup/borsukreconstruction.slice/memory.events
jq -n --slurpfile execution execution-identity.json '{schema:"borsuk-reconstruction-host-setup-v1",status:"ASSETS_READY",execution:$execution[0],native_executed:false,owned_units_drained:true}' > setup-terminal.json
cat setup-terminal.json
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/smoke.config.json" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
{
  "schema": "borsuk-partition-reconstruction-config-v1",
  "generation_manifest": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/generation.json",
    "bytes": 27501,
    "sha256": "cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307"
  },
  "plane_manifest": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/plane.json",
    "bytes": 653,
    "sha256": "e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6"
  },
  "leaves": {
    "path": "/mnt/borsuk-reconstruction-real1m/inputs/leaves.bin",
    "bytes": 64125000,
    "sha256": "2b302a08d2e583c6470c95f16b46f3db60b86dc782ae555365f201f74d5323c6"
  },
  "historical": null,
  "caller_pinned_bytes": 0,
  "max_payload_bytes": 536870912,
  "output_directory": "/mnt/borsuk-reconstruction-real1m/evidence/smoke/published"
}
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/smoke.config.sha256" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
003989243172e81625b9cdd52a451eb3a4f24dd8735f3e1b26b42e3b41f7f442
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/support.sha256" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
775ad56e0002df83fdfdd5c9e7bf9003d1d8d0c8effc3a5dc44981616db5cfa2  native-reconstruction-inner.sh
0b4c025713a4afd9015bd3a28b2b88a23c51506721cb28b9806010b9f7534755  run-reconstruction-stage.sh
3d732de6a4e162bc33f846c4575b280dc816edc307d01fe8552b8a1cb9876424  setup-data.sh
33c581760cc38607918dce5ac783fdb181e48ca1d276b3061651b25e7862653f  setup-reconstruction-host.sh
c27c81943b7eec931c9b474c121f5f88182a035fec0365a34a934a6e274ce88a  admission.config.json
8f2b7bee0066cd0af9363566c5fb726486f883f4c36d0cdb1024d3fd382a73ff  approval.json
1ae0e1659035fcc89f822057084a9c1f93686b7659d8a319881e2b0fd8e64bb9  execution-identity.json
2d7887c0d367055a6500cac6031aaa74a7dce07e2fe8179744117d02b5cc9b9f  parity.config.json
003989243172e81625b9cdd52a451eb3a4f24dd8735f3e1b26b42e3b41f7f442  smoke.config.json
6eb97f7663a0be5a30a507ad04e123a48b1357eec298d699e2f5834f29c2c3af  transport.json
b7e6aff50306adf7b410e36a089aa238acf43b5ad91ba1ed88c0de1d2d860dd2  admission.config.sha256
36eebbd685bf6a62813c4073aea647c12d90608fc740f2b24becffebd55eca40  smoke.config.sha256
5a1b94ecfbb56fbf59162a5a746a71abe5941b7ee8f038e9139c5b8a23a66c20  parity.config.sha256
df9feac4f1756ebaf69438009ff7928335a7b6b95da7117b2a59ba947f786777  approval.sha256
026ceeb3359462d5395f0b85c9196b3e70c145f7b1e205c79700e64bf50de064  campaign-prefix.txt
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
cat > "$support/transport.json" <<'BORSUK_RECONSTRUCTION_FIXED_ASSET_END'
{
  "schema": "borsuk-reconstruction-inputs-v1",
  "authority_pending": false,
  "source_commit": "91107713f6f7bd700b5b6d92a83e438febac8069",
  "binary": {
    "role": "compare_native_replay",
    "key": "research/semantic-router/20261011/reconstruction-native-qualification-a0002/stages/reconstruction-release/compare_native_replay",
    "pin": {
      "path": "/mnt/borsuk-reconstruction-real1m/bin/compare_native_replay",
      "bytes": 8387384,
      "sha256": "739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52"
    }
  },
  "inputs": [
    {
      "role": "generation_manifest",
      "key": "research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-full1000-a0001/generations/cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307/manifest.json",
      "pin": {
        "path": "/mnt/borsuk-reconstruction-real1m/inputs/generation.json",
        "bytes": 27501,
        "sha256": "cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307"
      }
    },
    {
      "role": "plane_manifest",
      "key": "research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-full1000-a0001/generations/cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307/plane/manifest.json",
      "pin": {
        "path": "/mnt/borsuk-reconstruction-real1m/inputs/plane.json",
        "bytes": 653,
        "sha256": "e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6"
      }
    },
    {
      "role": "router_root",
      "key": "research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-full1000-a0001/generations/cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307/router/root.bin",
      "pin": {
        "path": "/mnt/borsuk-reconstruction-real1m/inputs/router-root.bin",
        "bytes": 3041472,
        "sha256": "8fe6f7c690e454002ba7d319353b114d574d5f5cea7e1f8008c7288b41c41492"
      }
    },
    {
      "role": "membership",
      "key": "research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-full1000-a0001/generations/cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307/router/membership.bin",
      "pin": {
        "path": "/mnt/borsuk-reconstruction-real1m/inputs/membership.bin",
        "bytes": 125000,
        "sha256": "9864f7c207430cda4b5f58798c3957aaf0b8bfa0553920e18f08315e5fe6c463"
      }
    },
    {
      "role": "leaves",
      "key": "research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/rebound-full1000-a0001/generations/cb9f3a64f6f709005b897aa1dad7352a93fba2f080aca43b31106613de622307/router/leaves.bin",
      "pin": {
        "path": "/mnt/borsuk-reconstruction-real1m/inputs/leaves.bin",
        "bytes": 64125000,
        "sha256": "2b302a08d2e583c6470c95f16b46f3db60b86dc782ae555365f201f74d5323c6"
      }
    }
  ]
}
BORSUK_RECONSTRUCTION_FIXED_ASSET_END
(cd "$support" && sha256sum -c support.sha256)
left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-150 ))
(( left >= 1200 ))
timeout -k 10 1190 systemd-run --unit=borsuk-reconstruction-setup-driver --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec=1180 /bin/bash "$support/setup-reconstruction-host.sh"
