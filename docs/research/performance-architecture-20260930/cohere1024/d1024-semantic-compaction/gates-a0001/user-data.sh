#!/bin/bash
set -euo pipefail
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261007/d1024-semantic-compaction-gates-a0001
phase=bootstrap
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  systemctl stop borsuk-http-gates.service
  systemctl show borsuk-http-gates.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/systemd-after.txt
  cp run.log evidence/bootstrap.log
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  status=$original
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  evidence_bytes=$(stat -c %s evidence.tar.gz)
  timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors || status=96
  timeout --kill-after=5 30 aws s3 cp artifacts.sha256 "s3://$bucket/$prefix/artifacts.sha256" --only-show-errors || status=96
  native_exit=null
  if [[ -f evidence/final-exit ]]; then native_exit=$(cat evidence/final-exit); fi
  jq -n --arg schema borsuk-d1024-semantic-compaction-gates-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,source_commit:"68c6243a0c560f875e33fcc31d83e80ffc6c8813",candidate_commit:"68c6243a0c560f875e33fcc31d83e80ffc6c8813",native_source_file_count:414,native_source_identity_sha256:"db32f28f71a5544eb9485c6323ba002a2e62df5df210f78209a9ea2a630e8adc",source_archive_sha256:"5bd1dc71698c2529f8eddb63e7729d975d1c4fc06e18a54f5d208a0782498db1",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},performance_claim:false}' > terminal.json
  timeout --kill-after=5 30 aws s3 cp terminal.json "s3://$bucket/$prefix/terminal.json" --only-show-errors
  { printf 'BORSUK_TERMINAL '; cat terminal.json; } >/dev/ttyS0 2>/dev/null
  shutdown -h now
  exit "$status"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
phase=apt
 timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
 timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake python3.12 python3-dev
phase=awscli
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]
phase=source
aws s3 cp "s3://$bucket/research/native-library-check/sources/5bd1dc71698c2529f8eddb63e7729d975d1c4fc06e18a54f5d208a0782498db1.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 5bd1dc71698c2529f8eddb63e7729d975d1c4fc06e18a54f5d208a0782498db1 | sha256sum -c -
mkdir source
tar -xzf source.tar.gz -C source
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" source-files.sha256 --only-show-errors
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" native-source.json --only-show-errors
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" gates.sh --only-show-errors
printf '%s  %s\n' 62a85eb1825b2bc838bd7dddbeca8252187865282e93d837a09b540af6d6c976 source-files.sha256 55f5983a62989bad2d3c08511181979e927cee721c1537d1815876a6b567dfd8 native-source.json 32122b6025271f4d807125c89e2d0d8f34a90792b1a080cc303dc141963d0011 gates.sh | sha256sum -c -
test "$(jq length native-source.json)" = 414
cp source-files.sha256 native-source.json gates.sh evidence/
phase=rust
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
rustup component add clippy --toolchain 1.98.0
phase=native
systemd-run --unit=borsuk-http-gates --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p AllowedCPUs=0,1 -p TasksMax=512 -p RuntimeMaxSec=7200 -p WorkingDirectory="$root/source" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" /bin/bash "$root/gates.sh"
phase=complete
