#!/bin/bash
# SSM setup template; no Rust toolchain, compiler or scientific execution.
set -euo pipefail
support_sha=PENDING_SUPPORT_ARCHIVE_SHA256
support_bytes=PENDING_SUPPORT_ARCHIVE_BYTES
support_key=PENDING_SUPPORT_ARCHIVE_S3_KEY
[[ $support_sha =~ ^[0-9a-f]{64}$ && $support_bytes =~ ^[1-9][0-9]*$ && $support_key == research/semantic-router/* && $support_key != *PENDING* ]] || exit 125
(( support_bytes <= 1048576 )) || exit 125
root=/mnt/borsuk-v9-panel
[[ $EUID == 0 && ! -e $root && ! -L $root ]] || exit 125
mkdir -m 700 "$root"
mkdir -m 700 "$root/config" "$root/bin" "$root/evidence" "$root/results" "$root/archives" "$root/inputs" "$root/scratch"
mkdir -m 700 "$root/config/templates"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_SHARED_CREDENTIALS_FILE AWS_CONFIG_FILE
read -r up _ < /proc/uptime
stop=$(( $(date +%s)-${up%%.*}+3600 ))
printf '%s\n' "$stop" > "$root/machine-stop.epoch"
systemctl is-active --quiet borsuk-v9-runtime-shutdown.timer
awk '/^MemTotal:/{ok=($2>=7500000 && $2<=9000000)} END{exit !ok}' /proc/meminfo
cat /proc/meminfo > "$root/evidence/setup-meminfo.txt"
df -B1 "$root" > "$root/evidence/setup-disk.txt"
(( $(df --output=avail -B1 "$root" | tail -n 1) >= 16000000000 )) || exit 125
timeout -k 10 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 10 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time tar gzip util-linux
cd "$root"
curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum --strict -c -
timeout -k 10 60 unzip -q awscliv2.zip
timeout -k 10 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
timeout -k 2 60 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$support_key" support.tar > evidence/support.get.json
[[ $(stat -c %s support.tar) == "$support_bytes" ]]
printf '%s  support.tar\n' "$support_sha" | sha256sum --strict -c -
mkdir -m 700 support
tar -tf support.tar > evidence/support-members.txt
# Archive is root-built and hash-pinned, never an arbitrary caller archive.
tar -xf support.tar -C support --no-same-owner --no-same-permissions
(cd support && sha256sum --strict -c support.sha256) > evidence/support.sha-check.log
cp support/*.sh "$root/"
cp support/*.json "$root/config/"
chmod 0500 "$root/"*.sh
chmod 0400 "$root/config/"*.json
cp support/*.jq "$root/"
cp support/runtime-code.sha256 "$root/runtime-code.sha256"
chmod 0400 "$root/runtime-code.sha256"
jq -n --arg sha "$support_sha" --argjson stop "$stop" '{schema:"borsuk-v9-runtime-setup-v1",status:"SETUP_ONLY",support_sha256:$sha,machine_stop_epoch:$stop,compiler_installed:false,native_execution:false}' > evidence/setup.json
sync -f evidence/setup.json
sync -f evidence
