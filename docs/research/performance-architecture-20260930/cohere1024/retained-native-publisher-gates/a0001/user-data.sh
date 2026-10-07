#!/bin/bash
set -euo pipefail
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261007/retained-s3-publisher-gates-a0001
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
  jq -n --arg schema borsuk-retained-s3-publisher-gates-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,source_commit:"4c2e535fef1fdc2e61fa003358614287bb8ed488",candidate_commit:"4c2e535fef1fdc2e61fa003358614287bb8ed488",native_source_file_count:414,native_source_identity_sha256:"07fe340f2e449e970cb04891a4509a1d766cd8498e93757ac4e11e8ce6f63fd7",source_archive_sha256:"dc29619b1e252f28dbc7a08236d304886360aa17a0e39ba039eb6d1c0a637515",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},performance_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/native-library-check/sources/dc29619b1e252f28dbc7a08236d304886360aa17a0e39ba039eb6d1c0a637515.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' dc29619b1e252f28dbc7a08236d304886360aa17a0e39ba039eb6d1c0a637515 | sha256sum -c -
mkdir source
tar -xzf source.tar.gz -C source
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" source-files.sha256 --only-show-errors
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" native-source.json --only-show-errors
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" gates.sh --only-show-errors
printf '%s  %s\n' 6e1b690182cc2e79d323fb86c7c8573c46da3eec3f65274817d4e35a07bbefbd source-files.sha256 a2ea3485337f40300eb81048551a9df2c0b680c7032ab87f16102b87f0e55b37 native-source.json f02d570edc0546ee91fd983648e16c78053d49d2c2ef4c7562f546ff34021e76 gates.sh | sha256sum -c -
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
