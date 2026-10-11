#!/bin/bash
set -euo pipefail
support=/opt/borsuk-neighborhood-gate
root=/mnt/borsuk-neighborhood-data
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
 cat > /etc/systemd/system/borsukneighborhood.slice <<'SLICE'
[Unit]
Description=Owned source-neighborhood hydration and native charge boundary
[Slice]
CPUAccounting=yes
CPUQuota=200%
CPUQuotaPeriodSec=100ms
AllowedCPUs=0-1
MemoryAccounting=yes
IOAccounting=yes
MemoryMax=1073741824
MemorySwapMax=0
TasksAccounting=yes
TasksMax=128
SLICE
 systemctl daemon-reload
 systemctl start borsukneighborhood.slice
 systemctl is-active --quiet borsuk-neighborhood-data-shutdown.timer
 exit 0
fi
[[ $phase == hydrate ]]
deadline=$(( $(date +%s)+900 ))
machine=$(cat /run/borsuk-neighborhood-machine-stop.epoch)
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /borsukneighborhood.slice/borsuk-neighborhood-hydrate.service ]]
test "$(cat /sys/fs/cgroup/borsukneighborhood.slice/memory.max)" = 1073741824
test "$(cat /sys/fs/cgroup/borsukneighborhood.slice/memory.swap.max)" = 0
for dir in /sys/fs/cgroup /sys/fs/cgroup/borsukneighborhood.slice; do
 grep -qw io "$dir/cgroup.subtree_control"
done
for f in cgroup.controllers cgroup.subtree_control io.stat; do
 cat "/sys/fs/cgroup/borsukneighborhood.slice/$f" > "$support/hydration.slice.$f"
done
cat "/sys/fs/cgroup$cg/io.stat" > "$support/hydration.service.io.stat"
test ! -e "$root"
test ! -e "$support/scratch.img"
test "$(df -B1 --output=avail "$support" | tail -n 1)" -gt 8589934592
truncate -s 8589934592 "$support/scratch.img"
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
test "$(( $(stat -f -c '%b' "$root") * 4096 ))" -le 8589934592
mkdir "$root/bin" "$root/inputs" "$root/evidence"
jq -e '.schema=="borsuk-source-neighborhood-transport-v1" and .authority_pending==false and (.inputs|length)==6 and ([.inputs[].role]|sort)==["canonical","generation_manifest","leaves","membership","plane_manifest","router_root"]' "$support/transport.json" > /dev/null
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
test "$(du -sb "$root" | cut -f1)" -lt 8589934592
for f in memory.max memory.swap.max memory.peak memory.events memory.stat memory.swap.peak cpu.max cpuset.cpus.effective pids.max; do
 cat "/sys/fs/cgroup/borsukneighborhood.slice/$f" > "$support/hydration.slice.$f"
done
test "$(cat "$support/hydration.slice.memory.swap.peak")" = 0
peak=$(cat "$support/hydration.slice.memory.peak")
[[ $peak =~ ^[0-9]{1,10}$ ]] && (( 10#$peak <= 1073741824 ))
peak=$(cat "/sys/fs/cgroup$cg/memory.peak")
printf '%s\n' "$peak" > "$support/hydration.service.memory.peak"
[[ $peak =~ ^[0-9]{1,10}$ ]] && (( 10#$peak <= 1073741824 ))
test "$(cat "/sys/block/${loop##*/}/loop/dio")" = 1
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$support/hydration.slice.memory.events"
printf 'SIX_INPUTS_AND_ELF_AUTHENTICATED\n'
