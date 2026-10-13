#!/usr/bin/env bash
set -euo pipefail
[[ ac310e2f73cb086237e173b6dca104d77fc9115305d3b2717d3bb2a5f7421530 =~ ^[0-9a-f]{64}$ ]] || exit 125
root=/mnt/borsuk-source-width64
[[ ! -e $root ]] || exit 125
mkdir -m 700 "$root"
read -r uptime ignored < /proc/uptime
now=$(date +%s)
printf '%s\n' "$((now-${uptime%%.*}+14400))" > "$root/machine-stop.epoch"
systemctl is-active --quiet borsuk-source-width64-shutdown.timer
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
cd "$root"
timeout -k 10 180 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 10 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time
curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
# Frozen manifest and bootstrap helper have different final target names.
timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width128-scale-cold-a0001/inputs/transport.tsv transport.tsv > manifest-get.json
printf '%s  transport.tsv\n' ac310e2f73cb086237e173b6dca104d77fc9115305d3b2717d3bb2a5f7421530 | sha256sum -c -
timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width128-scale-cold-a0001/inputs/transport-panel-inputs.sh bootstrap-transport.sh > bootstrap-get.json
# 33471e3cdd6fa813d8752fe060a2323035b2cb502d74d2242cebe0752e973120 must be replaced from the staged immutable row.
printf '%s  bootstrap-transport.sh\n' 33471e3cdd6fa813d8752fe060a2323035b2cb502d74d2242cebe0752e973120 | sha256sum -c -
cat > "$root/transport-entry.sh" <<'TRANSPORT'
#!/usr/bin/env bash
set -euo pipefail
[[ ac310e2f73cb086237e173b6dca104d77fc9115305d3b2717d3bb2a5f7421530 =~ ^[0-9a-f]{64}$ ]] || exit 125
root=/mnt/borsuk-source-width64
cg=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-source-width64-transport.service ]]
mkdir "$root/transport-resources"
for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$file" > "$root/transport-resources/$file"; done
[[ $(cat "$root/transport-resources/cpu.max") == '100000 100000' && $(cat "$root/transport-resources/cpuset.cpus.effective") == 0 && $(cat "$root/transport-resources/memory.max") == 1073741824 && $(cat "$root/transport-resources/memory.swap.max") == 0 && $(cat "$root/transport-resources/pids.max") == 128 ]]
exec /bin/bash "$root/bootstrap-transport.sh" "$root" "$root/transport.tsv" ac310e2f73cb086237e173b6dca104d77fc9115305d3b2717d3bb2a5f7421530
TRANSPORT
timeout -k 20 650 systemd-run --unit=borsuk-source-width64-transport --expand-environment=no --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=20 -p RuntimeMaxSec=620 /bin/bash "$root/transport-entry.sh"
cg=/sys/fs/cgroup/system.slice/borsuk-source-width64-transport.service
[[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"
cp -r "$root/transport-resources" "$root/evidence/setup-transport-resources"
