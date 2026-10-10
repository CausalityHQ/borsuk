#!/bin/bash
set -Eeuo pipefail
umask 077
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 DEBIAN_FRONTEND=noninteractive
root=/mnt/borsuk-lifecycle-smoke
bucket=borsuk-bench-453182569524-euc1
prefix=research/native-library-check/20261010/parity-lifecycle-smoke-a0001
mkdir -p "$root"
cd "$root"
systemd-run --unit=borsuk-lifecycle-host-stop --on-active=1200s /usr/sbin/shutdown -h now
finish() {
 original=$?; trap - EXIT; set +e
 tar -czf evidence.tar.gz bootstrap.log collector.sh smoke.sh output stage.exit stage.stdout stage.stderr
 archive_rc=$?
 sha=$(sha256sum evidence.tar.gz | cut -d ' ' -f1); bytes=$(stat -c %s evidence.tar.gz)
 code=$original; (( archive_rc == 0 )) || code=96
 if command -v aws >/dev/null; then
  timeout -k 2 90 aws s3api put-object --bucket "$bucket" --key "$prefix/output/evidence.tar.gz" --body evidence.tar.gz --if-none-match '*' >archive-put.json || code=96
  printf '{"schema":"borsuk-parity-lifecycle-smoke-v1","original_exit":%s,"exit":%s,"archive":{"bytes":%s,"sha256":"%s"},"performance_claim":false}\n' "$original" "$code" "${bytes:-0}" "$sha" >terminal.json
  sync -f terminal.json && sync -f "$root" && timeout -k 2 60 aws s3api put-object --bucket "$bucket" --key "$prefix/output/terminal.json" --body terminal.json --if-none-match '*' >terminal-put.json
 fi
 /usr/sbin/shutdown -h now
 exit "$code"
}
trap finish EXIT
exec >bootstrap.log 2>&1
timeout -k 2 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 2 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip
curl -fsSL --connect-timeout 10 --max-time 120 -o aws.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  aws.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
[[ $(stat -c %s aws.zip) == 73022935 ]]
timeout -k 2 90 unzip -q aws.zip
timeout -k 2 90 ./aws/install
for name in collector.sh smoke.sh; do timeout -k 2 45 aws s3api get-object --bucket "$bucket" --key "$prefix/input/$name" "$name" >"$name.get.json"; done
printf '%s  collector.sh\n' 1b090217e0927eb44c63df6fc0f465205531050cc27d3f80dd98dc4586986e60 | sha256sum -c -
printf '%s  smoke.sh\n' f246204f5d0c8f2a1b641c624deab2fa89f912f0ff8428841546e019c0cc236c | sha256sum -c -
set +e
systemd-run --unit=borsuk-lifecycle-smoke --wait --pipe -p CPUQuota=100% -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=180 -p TimeoutStopSec=15 -p KillMode=control-group -p LimitCORE=0 /bin/bash "$root/smoke.sh" "$root/collector.sh" "$root/output" >stage.stdout 2>stage.stderr
rc=$?
set -e
printf '%s\n' "$rc" >stage.exit
exit "$rc"
