#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/native-workspace-execution
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json config.json native-source-manifest.json source-before.json source-after.json workspace-receipt.json test.log test-resources.txt workspace-cgroup.json cpu.txt rustc-version.txt cargo-version.txt run-closed.log binaries/hierarchical_semantic_cells binaries/two_bit_http binaries/build_two_bit_generation binaries/check_semantic_router_scorer'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261003/hierarchical-cells-implementation-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-hierarchical-cells-implementation-gates-spot-v1','source_commit':'6e04f06a3aa92a7d447d29435d3dfcbec1230342',
  'source_archive_sha256':'f2c36ee7a8d18f17e37a9c41950240c2410eba95dd4f2e554ef467446abbce4e','config_sha256': '1cbb00760839ee87c8083dbc67da3e857fdb74f8d5d1367bb12be07b42f4f4c1', 'code_identity_sha256': 'bb78ef05962f15237ae5d181215864f0f017662d2b7b97552d4d2906c7adfa4a', 'campaign_schema': 'borsuk-hierarchical-cells-implementation-gates-spot-v1', 'source_identity_sha256': 'cff2e3c24e372f380c0a4f42817d8a732aed068f55634df922d8221d90261d51', 'source_file_count': 401, 'native_source_manifest_sha256': '4ae41f41af3183cf6b047662fe61752ef1f732efce1ed221e2136789ef53dfc7', 'native_source_commit': 'da80bb508a421cc7e61cb1ac2ef2caf9a3aa9c62', 'artifact_roster_sha256': '6d5f86dc79eaf5d20bc5477f6eeb3a9be95c1895589f4d2adda15c7ea723a0cf', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'controller_source_commit': '3fda72c61cefdffb369df3a4661a96da36ec451d', 'candidate_delta_paths': ['crates/borsuk/src/bin/hierarchical_semantic_cells.rs', 'crates/borsuk/src/hierarchical_semantic_cells.rs', 'crates/borsuk/src/lib.rs'], 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261003/hierarchical-cells-implementation-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/f2c36ee7a8d18f17e37a9c41950240c2410eba95dd4f2e554ef467446abbce4e.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'f2c36ee7a8d18f17e37a9c41950240c2410eba95dd4f2e554ef467446abbce4e' | sha256sum -c -
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
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --hierarchical-cells-implementation --stage "$root/repo" "$root"
phase=execution
systemd-run --unit=native-workspace-execution --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" \
 python3.12 -m scripts.check_native_workspace_execution --hierarchical-cells-implementation "$CARGO_HOME/bin/cargo" "$root/repo" "$root"
phase=receipt-qualification
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --hierarchical-cells-implementation --check-receipt "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
