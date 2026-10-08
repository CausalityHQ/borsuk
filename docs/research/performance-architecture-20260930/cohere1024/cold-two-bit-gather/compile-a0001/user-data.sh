#!/bin/bash
set -euo pipefail
# Exact page-authentication candidate and gates must be preregistered before launch.
# Exact source and reconciled review are frozen by the root before launch.
systemd-run --unit=borsuk-http-shutdown --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/borsuk-http
mkdir -p "$root/evidence"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261008/cold-two-bit-gather-compile-a0001
phase=bootstrap
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  systemctl stop borsuk-auth-probe.service
  systemctl stop borsuk-http-gates.service
  systemctl show borsuk-http-gates.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/systemd-after.txt
  systemctl show borsuk-auth-probe.service -p LoadState -p MainPID -p ActiveState -p SubState -p ExecMainStatus -p Result > evidence/probe-systemd-final.txt
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
  for role in probe-libtest; do
    case "$role" in
      two_bit_http) binary="source/target/release/examples/$role" ;;
      probe-libtest) binary=$(cat evidence/probe-executable.txt 2>/dev/null) ;;
      *) binary="source/target/release/$role" ;;
    esac
    if [[ -n "$binary" && -f "$binary" ]]; then
      bytes=$(stat -c %s "$binary")
      sha=$(sha256sum "$binary" | cut -d' ' -f1)
      jq -n --arg role "$role" --arg sha "$sha" --argjson bytes "$bytes" '{role:$role,bytes:$bytes,sha256:$sha}' > "evidence/${role}.binary.json"
      timeout --kill-after=5 180 aws s3 cp "$binary" "s3://$bucket/$prefix/supplemental/$role" --only-show-errors
      upload_status=$?
      printf '%s\n' "$upload_status" > "evidence/${role}.binary-upload-exit"
      [[ "$upload_status" == 0 ]] || status=96
    else
      printf '%s\n' 'release artifact absent; no rebuild or qualification claimed' > "evidence/${role}.retention-skipped.txt"
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
  jq -n --arg schema borsuk-two-bit-gather-compile-v1 --arg instance "$instance" --arg phase "$phase" --argjson original "$original" --argjson exit "$status" --argjson native_exit "$native_exit" --arg resolved_lock_sha "$resolved_lock_sha" --arg resolved_map_sha "$resolved_map_sha" --arg evidence_sha "$evidence_sha" --argjson evidence_bytes "$evidence_bytes" \
    '{schema:$schema,instance_id:$instance,phase:$phase,original_exit:$original,exit:$exit,native_exit:$native_exit,source_commit:"751b5327571c7fec17af4bbdd7f1b1594f0aaf9d",candidate_commit:"751b5327571c7fec17af4bbdd7f1b1594f0aaf9d",native_source_file_count:415,native_source_identity_sha256:"3b8a5dc5f9f17bfc4780a1ce293e2c347976ae67b6400796ab6269b7fc877b41",source_archive_sha256:"f7c1f66cd85453fd64b1b3f8fa03e7e9590e5de0a290d8fdf7ab21dba41bd513",resolved_lock_sha256:$resolved_lock_sha,resolved_source_map_sha256:$resolved_map_sha,source_identity_scope:"exact immutable full source; no lock mutation admitted",evidence:{bytes:$evidence_bytes,sha256:$evidence_sha},scope:"compile and bounded synthetic correctness only; primitive NOT_RUN; full qualification pending",performance_claim:false}' > terminal.json
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
aws s3 cp "s3://$bucket/research/native-library-check/sources/f7c1f66cd85453fd64b1b3f8fa03e7e9590e5de0a290d8fdf7ab21dba41bd513.tar.gz" source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' f7c1f66cd85453fd64b1b3f8fa03e7e9590e5de0a290d8fdf7ab21dba41bd513 | sha256sum -c -
mkdir source
tar -xzf source.tar.gz -C source
aws s3 cp "s3://$bucket/$prefix/inputs/source-files.sha256" source-files.sha256 --only-show-errors
printf '%s  source-files.sha256\n' 0fbdb2c4fce010265de861f37e8e2866dd6bf53a83abf40f03ebd596d84d9a07 | sha256sum -c -
cp source-files.sha256 evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/native-source.json" native-source.json --only-show-errors
printf '%s  native-source.json\n' 7d11eb0f86e729ef17730b3a4d7e85ca1243faee66ad1c809f5c114c7a5a5b06 | sha256sum -c -
cp native-source.json evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/mandatory-tests.json" mandatory-tests.json --only-show-errors
printf '%s  mandatory-tests.json\n' fcab437de97085f436cdc5c29aba79e0e59b13bdd77db3efe886ca9049638f52 | sha256sum -c -
cp mandatory-tests.json evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/gates.sh" gates.sh --only-show-errors
printf '%s  gates.sh\n' e53e003502eb82b0d735602965e13d10fa4110eb97b5453d90f7ff79af757957 | sha256sum -c -
cp gates.sh evidence/
aws s3 cp "s3://$bucket/$prefix/inputs/select-probe.py" select-probe.py --only-show-errors
printf '%s  select-probe.py\n' 11e06ab85d1fadb816e4f3eaa315f6e518e114e06532d48189385ce917b238eb | sha256sum -c -
cp select-probe.py evidence/
test "$(jq length native-source.json)" = 415
test "$(jq length mandatory-tests.json)" = 6
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
cat > /etc/systemd/system/borsukauth.slice <<'SLICE'
[Unit]
Description=Bounded page-auth backend qualification aggregate
[Slice]
CPUQuota=200%
AllowedCPUs=0,1
MemoryMax=8G
MemorySwapMax=0
TasksMax=512
SLICE
systemctl daemon-reload
systemctl start borsukauth.slice
systemd-run --unit=borsuk-http-gates --slice=borsukauth.slice --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p AllowedCPUs=0,1 -p TasksMax=512 -p RuntimeMaxSec=7200 -p WorkingDirectory="$root/source" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" /bin/bash "$root/gates.sh"
phase=complete
