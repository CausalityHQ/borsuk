#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
root=/mnt/borsuk-admission-smoke
mkdir "$root"
cd "$root"
boot_epoch=$(date +%s)
shutdown -h +14
exec > bootstrap.log 2>&1
timeout -k 5 90 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 120 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq python3 time util-linux
(ulimit -f 71412; curl -fsS --proto '=https' --connect-timeout 5 --max-time 90 --max-filesize 73022935 -o awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip)
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
token=$(curl -fsS --connect-timeout 5 --max-time 10 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl -fsS --connect-timeout 5 --max-time 10 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id > instance.txt
grep -Eq '^i-[0-9a-f]+$' instance.txt
(ulimit -f 64; timeout -k 5 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261009/actual1m-q32-admission-smoke-a0001/inputs/source.tar.gz --if-match '"3396cacabce9141883a78541d69c4269"' source.tar.gz)
printf '%s  source.tar.gz\n' 831dd077c0ba7540ba885ae8916431de031df8bca2ab48d439625aaa57de6e1a | sha256sum -c -
tar -xzf source.tar.gz
sha256sum --strict -c assets.sha256
cat > /run/systemd/system/borsuk-smoke.slice <<'UNIT'
[Slice]
CPUQuota=100%
AllowedCPUs=0
MemoryMax=256M
MemorySwapMax=0
TasksMax=128
UNIT
systemctl daemon-reload
systemctl start borsuk-smoke.slice
[[ $(date +%s) -ge $boot_epoch ]]
(( $(date +%s) + 750 <= boot_epoch + 840 ))
systemd-run --unit=borsuk-smoke-collector --no-block -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=720 -p TimeoutStopSec=30 -p KillMode=control-group -p "ExecStopPost=/bin/bash $root/sources/service-stop.sh bootstrap borsuk-bench-453182569524-euc1 research/semantic-router/20261009/actual1m-q32-admission-smoke-a0001/bootstrap-manager.json" /bin/bash "$root/collector.sh"
