#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/native-workspace-execution
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json config.json native-source-manifest.json source-before.json source-after.json workspace-receipt.json test.log test-resources.txt workspace-cgroup.json cpu.txt rustc-version.txt cargo-version.txt run-closed.log binaries/two_bit_http'
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  set +e
  cd "$root"
  { printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\n'; } >/dev/ttyS0 2>/dev/null || true
  cp run.log run-closed.log || code=96
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
  instance_id=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/startup-wave8-implementation-a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-startup-wave8-implementation-gates-spot-v1','source_commit':'cf832b65ce7322cfb5a1f41fc88c5d9b764af57a',
  'source_archive_sha256':'6575f75727149e6a0efbf533e6c11552f7f933b18875206a2ef21693c121028a','config_sha256': 'daa4727e1357591d2ac41e1c91ddbb78019055a16a02e8d388efb282884cd5a1', 'code_identity_sha256': 'fe28992b6b794e5192fbece57aefc57e9ae43fe6407b4659c8c1d9a3ffcb5fa0', 'campaign_schema': 'borsuk-startup-wave8-implementation-gates-spot-v1', 'source_identity_sha256': '7e4fabf96284e4ccd080cf14b8bfbfd0f3ff41271e920c8e39d5d1fa252cd830', 'source_file_count': 399, 'native_source_manifest_sha256': '975638f75cff8c18204e6f43b78b09da7d463a96bc7e35bf30490635d5027c60', 'native_source_commit': '1e4ed13777a59e7acdd34acc9510b9d156aa17bf', 'artifact_roster_sha256': 'ed0f08ed65898aff44ad33b942508963371f06a82e00c5bb2109e3c6fa5bc9aa', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'controller_source_commit': '4e1616eb7ccb9d41d7daed1fa6ff78d16eda2c39', 'candidate_delta_paths': ['crates/borsuk/src/object_native_generation.rs', 'crates/borsuk/src/two_bit_generation.rs'], 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/startup-wave8-implementation-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 python3-dev time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake
phase=awscli-download
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6' | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
phase=awscli-install
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
cli_version=$(aws --version)
[[ "$cli_version" == aws-cli/2.36.11\ * ]]
phase=source-download
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/6575f75727149e6a0efbf533e6c11552f7f933b18875206a2ef21693c121028a.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '6575f75727149e6a0efbf533e6c11552f7f933b18875206a2ef21693c121028a' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
rustup component add clippy --toolchain 1.98.0
phase=source-qualification
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --startup-wave8-implementation --stage "$root/repo" "$root"
phase=execution
systemd-run --unit=native-workspace-execution --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" \
 python3.12 -m scripts.check_native_workspace_execution --startup-wave8-implementation "$CARGO_HOME/bin/cargo" "$root/repo" "$root"
phase=receipt-qualification
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --startup-wave8-implementation --check-receipt "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
