#!/bin/bash
set -euo pipefail
# DRAFT: final source and support hashes must be frozen before launch.
# Exact source and reconciled review are frozen by the root before launch.
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-membership-native-gates-a0002
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
  for role in compare_native_replay check_cohere_native_baseline two_bit_http; do
  if [[ "$role" == check_cohere_native_baseline ]]; then binary="source/target/release/$role"; else binary="source/target/release/examples/$role"; fi
  if [[ -f "$binary" ]]; then
    bytes=$(stat -c %s "$binary")
    sha=$(sha256sum "$binary" | cut -d' ' -f1)
    jq -n --arg role "$role" --arg sha "$sha" --argjson bytes "$bytes" '{role:$role,bytes:$bytes,sha256:$sha}' > "evidence/${role}.binary.json"
    timeout --kill-after=5 120 aws s3 cp "$binary" "s3://$bucket/$prefix/supplemental/$role" --only-show-errors
    upload_status=$?
    printf '%s\n' "$upload_status" > "evidence/${role}.binary-upload-exit"
    [[ "$upload_status" == 0 ]] || status=96
  else
    printf '%s\n' 'release artifact absent; no rebuild or qualification claimed' > evidence/binary-retention-skipped.txt
  fi
  done
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > artifacts.sha256 || status=96
  tar -czf evidence.tar.gz -C evidence . || status=96
  evidence_sha=$(sha256sum evidence.tar.gz | cut -d' ' -f1)
  evidence_bytes=$(stat -c %s evidence.tar.gz)
  timeout --kill-after=5 90 aws s3 cp evidence.tar.gz "s3://$bucket/$prefix/evidence.tar.gz" --only-show-errors || status=96
  timeout --kill-after=5 30 aws s3 cp artifacts.sha256 "s3://$bucket/$prefix/artifacts.sha256" --only-show-errors || status=96
  resolved_lock_sha=null
  resolved_map_sha=null
  if [[ -f evidence/Cargo.lock.resolved ]]; then resolved_lock_sha=$(sha256sum evidence/Cargo.lock.resolved | cut -d' ' -f1); fi
  if [[ -f evidence/source-resolved.sha256 ]]; then resolved_map_sha=$(sha256sum evidence/source-resolved.sha256 | cut -d' ' -f1); fi
  native_exit=null
  if [[ -f evidence/final-exit ]]; then native_exit=$(cat evidence/final-exit); fi
  jq -n --arg schema borsuk-cold-membership-native-gates-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg resolved_lock_sha "$resolved_lock_sha" --arg resolved_map_sha "$resolved_map_sha" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,source_commit:"34fa1f48246992d1ae1983f938f4cbc67faf5af9",candidate_commit:"2f3c78f0d6076d339a1ded344432a8db0582f22c",native_source_file_count:415,native_source_identity_sha256:"90ec35ef11c8f6e2d69374ef2421f331a6e6a26bead98accc27afc3f7ef39324",source_archive_sha256:"da891254ee38d8936521d5a69e118a051c49151ef37863310bd113a828f50997",resolved_lock_sha256:$resolved_lock_sha,resolved_source_map_sha256:$resolved_map_sha,source_identity_scope:"exact immutable full source; no lock mutation admitted",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},performance_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/native-library-check/sources/da891254ee38d8936521d5a69e118a051c49151ef37863310bd113a828f50997.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' da891254ee38d8936521d5a69e118a051c49151ef37863310bd113a828f50997 | sha256sum -c -
mkdir source
tar -xzf source.tar.gz -C source
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" source-files.sha256 --only-show-errors
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" native-source.json --only-show-errors
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" gates.sh --only-show-errors
printf '%s  %s\n' 46c7a5f27ada30be8a0a83bbfc81dd108114431937c850e4ef52823716f4bfab source-files.sha256 f0aaf5627f7bcdbbea21cfb28dfd7a3b41c9c68261c4d6401ab3a24e101ee17e native-source.json e30ff6176dae64a7a71a1b028313ed80132056d4831e44c77222429ed6635808 gates.sh | sha256sum -c -
test "$(jq length native-source.json)" = 415
cp source-files.sha256 native-source.json gates.sh evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/mandatory-tests.json" mandatory-tests.json --only-show-errors
printf '%s  mandatory-tests.json\n' 8b647f683b541800438cd333c8e0e6efa7e27f9e4ae4f8862d447816cb2fc784 | sha256sum -c -
test "$(jq length mandatory-tests.json)" = 24
cp mandatory-tests.json evidence/
touch FROZEN_SOURCE_AND_ROSTER

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
