#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cargo-pilot-stop --on-active=3540s /usr/sbin/shutdown -h now
root=/mnt/native-cargo-pilot
mkdir -p "$root" && cd "$root"
stamp() { printf '{"phase":"%s","time_ns":%s}\n' "$1" "$(date +%s%N)" >>timings.jsonl; }
phase=bootstrap
stamp "$phase"
stamp userdata-start
printf '{"phase":"boot","time_ns":%s000000000}\n' "$(awk '$1 == "btime" { print $2 }' /proc/stat)" >>timings.jsonl
export ARTIFACT_NAMES='config.json native-source-manifest.json source-qualification.json source-before.json source-after.json pilot-receipt.json test.log test-resources.txt workspace-cgroup.json cpu.txt rustc-version.txt cargo-version.txt cache.json volume.json timings.jsonl resource-samples.jsonl run-closed.log'
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  set +e
  cd "$root"
  { printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\n'; } >/dev/ttyS0 2>/dev/null || true
  if mountpoint -q /mnt/cargo-pilot-cache; then sync -f /mnt/cargo-pilot-cache; umount /mnt/cargo-pilot-cache || code=96; fi
  stamp worker-terminal
  cp run.log run-closed.log || code=96
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
  instance_id=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-cargo-pilot/20261002/pilot-a0001/artifacts/$name" --only-show-errors || code=96
      fi
    done
  else code=96; fi
  write_terminal() {
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" ORIGINAL_EXIT_CODE="$original_code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in os.environ['ARTIFACT_NAMES'].split():
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-cargo-pilot-v1','source_commit':'3666440662c2aa43b524c0949e2c73e3867075b2',
  'source_archive_sha256':'62c2ffe087bec075ab1c6dc00c4a94228bfa9e9ab2a56c6edd639fc1b173583c','config_sha256': '96977ed79bbe99b6ac646213b91c2ce509fc6a3d2118afec4b2a37deeda52edf', 'code_identity_sha256': 'ac2f101211dc40bffe89d408ee567f65b1fa4862943346b7319d52440734877f', 'campaign_schema': 'borsuk-native-cargo-pilot-v1', 'source_identity_sha256': 'addf62bce23ceee034f22e4d1dfc0b318564d924c6e0fcee34b9336d1532813e', 'source_file_count': 399, 'artifact_roster_sha256': 'a30cae9f52111b68f7471d6e55f38fc11acb3c8656ffd18fc3ea026b69bc5e2e', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-cargo-pilot/20261002/pilot-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1
phase=apt-update
stamp "$phase"
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
stamp "$phase"
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 python3-dev time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake
phase=awscli-download
stamp "$phase"
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6' | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
phase=awscli-install
stamp "$phase"
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
cli_version=$(aws --version)
[[ "$cli_version" == aws-cli/2.36.11\ * ]]
phase=source-download
stamp "$phase"
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/62c2ffe087bec075ab1c6dc00c4a94228bfa9e9ab2a56c6edd639fc1b173583c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '62c2ffe087bec075ab1c6dc00c4a94228bfa9e9ab2a56c6edd639fc1b173583c' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
stamp download-end
phase=install
stamp "$phase"
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 --default-host x86_64-unknown-linux-gnu
cat >volume.json <<'VOLUME'
{"auto_delete":false,"availability_zone":"eu-central-1a","created":true,"encrypted":true,"expires_ns":1791070064959738265,"ownedVolume":"borsuk-cargo-pilot-20261002-a0001","size_gib":80,"volume_id":"vol-0b559583b4b92951c"}
VOLUME
stamp setup-end
phase=cache-preparation
stamp "$phase"
PYTHONPATH="$root/repo" timeout --kill-after=5 180 python3.12 -m scripts.launch_native_cargo_pilot --mount "$root/volume.json"
stamp cache-mount-end
phase=execution
stamp "$phase"
systemd-run --unit=native-cargo-pilot --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=2220 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=PATH="$PATH" \
 python3.12 -m scripts.launch_native_cargo_pilot --worker "$root/repo" "$root" docs/research/performance-architecture-20260930/semantic-1m/build-lane-pilot-20261002/config.json 96977ed79bbe99b6ac646213b91c2ce509fc6a3d2118afec4b2a37deeda52edf
phase=cache-close
stamp "$phase"
sync -f /mnt/cargo-pilot-cache
umount /mnt/cargo-pilot-cache
phase=receipt-qualification
stamp "$phase"
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_cargo_pilot --check-receipt "$root"
phase=complete
stamp "$phase"
