#!/usr/bin/env bash
set -euo pipefail
# FROZEN compile-only direct closure qualification; no real-data ANN run.
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence" "$root/inputs"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-direct-closure-qualification-a0002
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
  if [[ -f retained/check_cohere_native_baseline ]]; then
    timeout --kill-after=5 180 aws s3 cp retained/check_cohere_native_baseline "s3://$bucket/$prefix/supplemental/check_cohere_native_baseline" --only-show-errors
    binary_upload=$?
    printf '%s\n' "$binary_upload" > evidence/runner.binary-upload-exit
    [[ "$binary_upload" == 0 ]] || status=96
  fi
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  evidence_bytes=$(stat -c %s evidence.tar.gz)
  timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors || status=96
  timeout --kill-after=5 30 aws s3 cp artifacts.sha256 "s3://$bucket/$prefix/artifacts.sha256" --only-show-errors || status=96
  qualification_exit=null
  [[ ! -f evidence/qualification-exit ]] || qualification_exit=$(cat evidence/qualification-exit)
  jq -n --arg schema borsuk-direct-closure-native-qualification-closed-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson qualification_exit "$qualification_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,qualification_exit:$qualification_exit,source_commit:"2fb771bdb31f9490a3341bdbfa2b4ad510cfb487",native_source_file_count:425,native_source_identity_sha256:"4d04840e2db4487f9e77ef57e5eadef42684ab3d354c1a51c2adc20a8c3474e9",source_archive_sha256:"477466d874c82faf67710f436fdddbea2b603b83387e49898381238bb0585c17",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},scope:"compiler and synthetic correctness qualification only",ann_run:false,performance_claim:false,quality_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/native-library-check/sources/477466d874c82faf67710f436fdddbea2b603b83387e49898381238bb0585c17.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 477466d874c82faf67710f436fdddbea2b603b83387e49898381238bb0585c17 | sha256sum -c -
test "$(stat -c %s source.tar.gz)" = 13538735
mkdir source
tar -xzf source.tar.gz -C source
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" "source-files.sha256" --only-show-errors
printf '%s  source-files.sha256\n' 025fed5f6758ece0adeafbbb114c54f7f5cd695d8b72ac7400fe4d0f23310358 | sha256sum -c -
test "$(stat -c %s source-files.sha256)" = 280779
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" "native-source.json" --only-show-errors
printf '%s  native-source.json\n' 00a1805e3d44343b727cfff8f5c30c17d7e86d3f3ab991bd5678045289dae16b | sha256sum -c -
test "$(stat -c %s native-source.json)" = 49350
aws s3 cp "s3://$bucket/$prefix/inputs/mandatory-tests.json" "mandatory-tests.json" --only-show-errors
printf '%s  mandatory-tests.json\n' fa8325c6d0d5ce1cdbdbe56801e598478cceeef8db356ffc54331838c69fd395 | sha256sum -c -
test "$(stat -c %s mandatory-tests.json)" = 980
aws s3 cp "s3://$bucket/$prefix/inputs/expected-test-inventory.json" "expected-test-inventory.json" --only-show-errors
printf '%s  expected-test-inventory.json\n' c0fc48e30af66bd8593aa2e11ec2992ac2e83d3ba6f8004a0b05af4926c4a0e6 | sha256sum -c -
test "$(stat -c %s expected-test-inventory.json)" = 3873
aws s3 cp "s3://$bucket/$prefix/inputs/candidate-contract.json" "candidate-contract.json" --only-show-errors
printf '%s  candidate-contract.json\n' 7853daa71854544a6df33e86a8d5639996c8551baafc7a5a3aad2c0fbc4cb949 | sha256sum -c -
test "$(stat -c %s candidate-contract.json)" = 35919
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" "gates.sh" --only-show-errors
printf '%s  gates.sh\n' b8040c942e1ff806d77366241132104b79ea8fc29157b88e8c3cf1d5efcdd7bd | sha256sum -c -
test "$(stat -c %s gates.sh)" = 3576
cp source-files.sha256 native-source.json mandatory-tests.json expected-test-inventory.json candidate-contract.json evidence/
(cd source && sha256sum --check ../source-files.sha256) > evidence/source-staging.log
touch FROZEN_DIRECT_CLOSURE_AUTHORITY
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
Description=Bounded Rust direct closure compiler and correctness qualification
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
