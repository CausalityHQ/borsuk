#!/usr/bin/env bash
set -euo pipefail
# FROZEN scale/profile maintenance compiler and correctness qualification; no real-data ANN run.
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence" "$root/inputs"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261009/scale-maintenance-qualification-a0002
phase=bootstrap
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  systemctl stop borsuk-http-gates.service
  systemctl show borsuk-http-gates.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/systemd-after.txt
  global_cg=$(systemctl show borsukauth.slice -p ControlGroup --value)
  if [[ -n "$global_cg" ]]; then
    for field in memory.current memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do
      if [[ -f "/sys/fs/cgroup${global_cg}/$field" ]]; then cat "/sys/fs/cgroup${global_cg}/$field" > "evidence/global-$field.after"; fi
    done
  fi
  systemctl stop borsukauth.slice
  systemctl show borsukauth.slice -p ActiveState -p SubState > evidence/global-systemd-after.txt
  cp run.log evidence/bootstrap.log
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  status=$original
  for name in publish_two_bit_generation check_cohere_native_baseline; do
    if [[ -f "retained/$name" ]]; then
      timeout --kill-after=5 180 aws s3 cp "retained/$name" "s3://$bucket/$prefix/supplemental/$name" --only-show-errors
      binary_upload=$?
      printf '%s\n' "$binary_upload" > "evidence/$name.binary-upload-exit"
      [[ "$binary_upload" == 0 ]] || status=96
    fi
  done
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  evidence_bytes=$(stat -c %s evidence.tar.gz)
  timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors || status=96
  timeout --kill-after=5 30 aws s3 cp artifacts.sha256 "s3://$bucket/$prefix/artifacts.sha256" --only-show-errors || status=96
  qualification_exit=null
  [[ ! -f evidence/qualification-exit ]] || qualification_exit=$(cat evidence/qualification-exit)
  jq -n --arg schema borsuk-scale-maintenance-qualification-closed-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson qualification_exit "$qualification_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,qualification_exit:$qualification_exit,source_commit:"481f4518a3faffab604be3fd57941378d7790f5e",native_source_file_count:426,native_source_identity_sha256:"9210ac2b4b60a0536544d1423bae42c8f782af1caca29ad984b16dbd01709aae",source_archive_sha256:"087cddcb3b933191d66937c5e8ddc6ae59328d31812dac711863de75ced551ca",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},scope:"compiler and synthetic correctness qualification only",ann_run:false,performance_claim:false,quality_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/native-library-check/sources/087cddcb3b933191d66937c5e8ddc6ae59328d31812dac711863de75ced551ca.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 087cddcb3b933191d66937c5e8ddc6ae59328d31812dac711863de75ced551ca | sha256sum -c -
test "$(stat -c %s source.tar.gz)" = 16966116
mkdir source
tar -xzf source.tar.gz -C source
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" "source-files.sha256" --only-show-errors
printf '%s  source-files.sha256\n' 9b0d1923affd3045502a79a58bd8b470c826aa44193bdceca9ca77b0bb148dcf | sha256sum -c -
test "$(stat -c %s source-files.sha256)" = 280890
aws s3 cp "s3://$bucket/$prefix/inputs/candidate-contract.json" "candidate-contract.json" --only-show-errors
printf '%s  candidate-contract.json\n' 897b265bb1a0fc9dd647033ff6fb0e6489d957069abb0617d7758e1a317746e6 | sha256sum -c -
test "$(stat -c %s candidate-contract.json)" = 21764
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" "gates.sh" --only-show-errors
printf '%s  gates.sh\n' 3c9b343842ad1d6b9f0d59dcf234d809f15d36dcc6c3e35469633f57efccb812 | sha256sum -c -
test "$(stat -c %s gates.sh)" = 3485
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" "native-source.json" --only-show-errors
printf '%s  native-source.json\n' 771c975029bd50c1ba604a70282c21700d10557f9aa428cfe5158f93d3ad2a33 | sha256sum -c -
test "$(stat -c %s native-source.json)" = 49468
cp source-files.sha256 native-source.json candidate-contract.json gates.sh evidence/
(cd source && sha256sum --check ../source-files.sha256) > evidence/source-staging.log
touch FROZEN_SCALE_MAINTENANCE_AUTHORITY
phase=rust
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
rustup component add clippy --toolchain 1.98.0
phase=native
cat > /etc/systemd/system/borsukauth.slice <<'SLICE'
[Unit]
Description=Bounded Rust scale/profile maintenance compiler and correctness qualification
[Slice]
CPUQuota=200%
AllowedCPUs=0,1
MemoryMax=8G
MemorySwapMax=0
TasksMax=512
SLICE
systemctl daemon-reload
systemctl start borsukauth.slice
systemd-run --unit=borsuk-http-gates --slice=borsukauth.slice --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p AllowedCPUs=0,1 -p TasksMax=512 -p RuntimeMaxSec=7440 -p WorkingDirectory="$root/source" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" /bin/bash "$root/gates.sh"
phase=complete
