#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-source-width64
[[ ! -e $root ]] || exit 125
mkdir -m 700 "$root"
read -r uptime _ < /proc/uptime
now=$(date +%s)
printf '%s\n' "$((now-${uptime%%.*}+2400))" > "$root/machine-stop.epoch"
systemctl is-active --quiet borsuk-source-width64-shutdown.timer
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
cd "$root"
timeout -k 10 180 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 10 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time linux-tools-common linux-tools-generic
curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
# Frozen manifest and bootstrap helper have different final target names.
timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width64-cpu-profile-a0001/inputs/transport.tsv transport.tsv > manifest-get.json
printf '%s  transport.tsv\n' a3faa00e02c55891bfb6cf2a9f33575d53480c705b358629bb36658afdc863d2 | sha256sum -c -
timeout -k 2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width64-cpu-profile-a0001/inputs/transport-panel-inputs.sh bootstrap-transport.sh > bootstrap-get.json
# cc59a8a767f31ae48bc213345f6bf0e98afce3e70ce766a5dc39028ce80a6b3d must be replaced from the staged immutable row.
printf '%s  bootstrap-transport.sh\n' cc59a8a767f31ae48bc213345f6bf0e98afce3e70ce766a5dc39028ce80a6b3d | sha256sum -c -
cat > "$root/transport-entry.sh" <<'TRANSPORT'
#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-source-width64
cg=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-source-width64-transport.service ]]
mkdir "$root/transport-resources"
for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$file" > "$root/transport-resources/$file"; done
[[ $(cat "$root/transport-resources/cpu.max") == '100000 100000' && $(cat "$root/transport-resources/cpuset.cpus.effective") == 0 && $(cat "$root/transport-resources/memory.max") == 1073741824 && $(cat "$root/transport-resources/memory.swap.max") == 0 && $(cat "$root/transport-resources/pids.max") == 128 ]]
exec /bin/bash "$root/bootstrap-transport.sh" "$root" "$root/transport.tsv" a3faa00e02c55891bfb6cf2a9f33575d53480c705b358629bb36658afdc863d2
TRANSPORT
timeout -k 20 650 systemd-run --unit=borsuk-source-width64-transport --expand-environment=no --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=20 -p RuntimeMaxSec=620 /bin/bash "$root/transport-entry.sh"
cg=/sys/fs/cgroup/system.slice/borsuk-source-width64-transport.service
[[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"
cp -r "$root/transport-resources" "$root/evidence/setup-transport-resources"
timeout -k 10 60 systemd-run --unit=borsuk-source-width64-perf-admission --expand-environment=no --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=1280M -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec=45 /bin/bash "$root/perf-admission.sh"
cg=/sys/fs/cgroup/system.slice/borsuk-source-width64-perf-admission.service
[[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"
[[ $(cat "$root/perf-admission/status") == SOFTWARE_SAMPLING_CANARY_CLOSED ]]
