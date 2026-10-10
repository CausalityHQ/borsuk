#!/bin/bash
set -Eeuo pipefail
umask 077
shutdown -h +10
mkdir -p /mnt/borsuk-replay
cd /mnt/borsuk-replay
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261010/actual1m-a0002-replay-a0001
finish() {
 rc=$?; trap - EXIT; set +e
 if command -v aws >/dev/null; then
  for name in replay.stdout replay.stderr replay.exit replay.manager.show replay.launcher.stdout replay.launcher.stderr; do
   if [[ -f $name ]]; then timeout -k 2 10 aws s3api put-object --region eu-central-1 --bucket "$bucket" --key "$prefix/output/$name" --body "$name" --if-none-match '*' || rc=96; fi
  done
  printf '{"bootstrap_exit":%s,"replay_exit":%s,"performance_claim":false}\n' "$rc" "${replay_exit:-null}" > terminal.json
  timeout -k 2 10 aws s3api put-object --region eu-central-1 --bucket "$bucket" --key "$prefix/output/terminal.json" --body terminal.json --if-none-match '*'
 fi
 shutdown -h now
 exit "$rc"
}
trap finish EXIT
export DEBIAN_FRONTEND=noninteractive
timeout -k 2 90 apt-get -qq -o DPkg::Lock::Timeout=30 update
timeout -k 2 120 apt-get -qq -y -o DPkg::Lock::Timeout=30 install curl unzip python3
curl -fsS --connect-timeout 10 --max-time 90 --max-filesize 73022935 -o awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout -k 2 45 unzip -q awscliv2.zip
timeout -k 2 45 ./aws/install
timeout -k 2 30 aws s3api get-object --region eu-central-1 --bucket "$bucket" --key "$prefix/input/inputs.tar.gz" inputs.tar.gz
printf '%s  inputs.tar.gz\n' c33557b66441522f2e7f80416ba892ca9a7492c345952c0ad7af79a6722c1b8d | sha256sum -c -
[[ $(stat -c %s inputs.tar.gz) == 545297 ]]
tar -xzf inputs.tar.gz
set +e
systemd-run --unit=borsuk-closed-replay --wait --pipe -p WorkingDirectory=/mnt/borsuk-replay -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=5 -p KillMode=control-group -p LimitCORE=0 /bin/bash -c 'set +e; python3 verify-closed.py remote-results replay-pins.json >replay.stdout 2>replay.stderr; rc=$?; printf "%s\n" "$rc" >replay.exit; exit "$rc"' >replay.launcher.stdout 2>replay.launcher.stderr
launch_exit=$?
set -e
systemctl show borsuk-closed-replay.service >replay.manager.show
replay_exit=$(cat replay.exit)
[[ $replay_exit =~ ^[0-9]+$ && $launch_exit == "$replay_exit" ]]
exit "$replay_exit"
