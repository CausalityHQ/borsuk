#!/usr/bin/env bash
set -euo pipefail
# NOT FROZEN: placeholders must be replaced from root-authenticated final pins.
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence" "$root/inputs"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-source-utilization-a0001
phase=bootstrap
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  systemctl stop borsuk-source-utilization-replay.service
  systemctl stop borsuk-http-gates.service
  systemctl show borsuk-http-gates.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/systemd-after.txt
  systemctl show borsuk-source-utilization-replay.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/replay-systemd-after.txt
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
  if [[ -f retained/compare_native_replay ]]; then
    timeout --kill-after=5 180 aws s3 cp retained/compare_native_replay "s3://$bucket/$prefix/supplemental/compare_native_replay" --only-show-errors
    binary_upload=$?
    printf '%s\n' "$binary_upload" > evidence/reducer.binary-upload-exit
    [[ "$binary_upload" == 0 ]] || status=96
  fi
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  evidence_bytes=$(stat -c %s evidence.tar.gz)
  timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors || status=96
  timeout --kill-after=5 30 aws s3 cp artifacts.sha256 "s3://$bucket/$prefix/artifacts.sha256" --only-show-errors || status=96
  qualification_exit=null
  replay_exit=null
  [[ ! -f evidence/qualification-exit ]] || qualification_exit=$(cat evidence/qualification-exit)
  [[ ! -f evidence/replay-original-exit ]] || replay_exit=$(cat evidence/replay-original-exit)
  jq -n --arg schema borsuk-source-utilization-closed-execution-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson qualification_exit "$qualification_exit" --argjson replay_exit "$replay_exit" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,qualification_exit:$qualification_exit,replay_exit:$replay_exit,source_commit:"9b98778dec172972c2e57d695e26341a8cfb18be",native_source_file_count:425,native_source_identity_sha256:"5f0688630765e86cc6cc145d20374c8aa1b1631acdaad28c51643df9d77259d5",source_archive_sha256:"180892563e843db1b0d4703d23c6221025ca3bb11168004afdd689977bff68e9",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},scope:"compiler qualification and closed metadata cost analysis only",ann_run:false,performance_claim:false,quality_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/native-library-check/sources/180892563e843db1b0d4703d23c6221025ca3bb11168004afdd689977bff68e9.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 180892563e843db1b0d4703d23c6221025ca3bb11168004afdd689977bff68e9 | sha256sum -c -
mkdir source
tar -xzf source.tar.gz -C source
# This inserted section downloads only root-frozen source/support manifests,
# gate/replay shell scripts, configuration and closed B1 metadata, checking
# the exact byte lengths and SHA256s before creating the admission marker.
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" "source-files.sha256" --only-show-errors
printf '%s  source-files.sha256\n' 3c90555944b323de1ed2656ab55cf10878846e801a52dfb7e95f63efac9ce57c | sha256sum -c -
test "$(stat -c %s "source-files.sha256")" = 280779
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" "native-source.json" --only-show-errors
printf '%s  native-source.json\n' 89186a14227d93648aff0c87b2bb989a6e39cada011e6d1ec17511ddb1e06a51 | sha256sum -c -
test "$(stat -c %s "native-source.json")" = 49350
aws s3 cp "s3://$bucket/$prefix/inputs/mandatory-tests.json" "mandatory-tests.json" --only-show-errors
printf '%s  mandatory-tests.json\n' aa1445da9b73cffa78ceeb275e7bf389514a74d55b306b7600493e95279eaf98 | sha256sum -c -
test "$(stat -c %s "mandatory-tests.json")" = 708
aws s3 cp "s3://$bucket/$prefix/inputs/expected-test-inventory.json" "expected-test-inventory.json" --only-show-errors
printf '%s  expected-test-inventory.json\n' 922ff4060310194bd0f35578c8fd40f34a01df4d3502516beb174d388161f5bb | sha256sum -c -
test "$(stat -c %s "expected-test-inventory.json")" = 2394
aws s3 cp "s3://$bucket/$prefix/inputs/candidate-contract.json" "candidate-contract.json" --only-show-errors
printf '%s  candidate-contract.json\n' 0ee3525115bdb94729ee9c566b5346bbb7f9cbda25f7c2ea7f0eddb46644a30a | sha256sum -c -
test "$(stat -c %s "candidate-contract.json")" = 23323
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" "gates.sh" --only-show-errors
printf '%s  gates.sh\n' 9a6b1fd5e92c1beb3bd750512a2c4b6a685a8cf03095b56da9bcaa5a4e364aa2 | sha256sum -c -
test "$(stat -c %s "gates.sh")" = 3776
aws s3 cp "s3://$bucket/$prefix/inputs/replay.sh" "replay.sh" --only-show-errors
printf '%s  replay.sh\n' c30642e071d02c0e7f1a267b092002b1d4a3e1fae82c01eaa31025d2f366a985 | sha256sum -c -
test "$(stat -c %s "replay.sh")" = 1566
aws s3 cp "s3://$bucket/$prefix/inputs/replay-config.json" "replay-config.json" --only-show-errors
printf '%s  replay-config.json\n' 3c6bbb86904dffae46967bfa558cb79133d7e20e024a61a2e0891fec01395f93 | sha256sum -c -
test "$(stat -c %s "replay-config.json")" = 3606
aws s3 cp "s3://$bucket/$prefix/inputs/B1.jsonl" "inputs/B1.jsonl" --only-show-errors
printf '%s  inputs/B1.jsonl\n' 29da105428503267e73e9e23e1a61bef9967ccdc9fb4f43ef64b23d306ba0cbb | sha256sum -c -
test "$(stat -c %s "inputs/B1.jsonl")" = 10777291
aws s3 cp "s3://$bucket/$prefix/inputs/replay-inputs.sha256" "replay-inputs.sha256" --only-show-errors
printf '%s  replay-inputs.sha256\n' 825a73e2cd2502170f57aeda7c8fd0ea5b22c9677a205181169ec3a65a381459 | sha256sum -c -
test "$(stat -c %s "replay-inputs.sha256")" = 201
cp source-files.sha256 native-source.json mandatory-tests.json expected-test-inventory.json candidate-contract.json replay-config.json replay-inputs.sha256 evidence/
(cd source && sha256sum --check ../source-files.sha256) > evidence/source-staging.log
touch FROZEN_SOURCE_UTILIZATION_AUTHORITY
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
Description=Bounded Rust source-utilization qualification and evidence replay
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
