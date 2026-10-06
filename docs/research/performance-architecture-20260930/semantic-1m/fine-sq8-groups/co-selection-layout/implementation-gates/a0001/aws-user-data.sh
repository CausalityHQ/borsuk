#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/native-workspace-execution
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json config.json native-source-manifest.json source-before.json source-after.json workspace-receipt.json test.log test-resources.txt workspace-cgroup.json cpu.txt rustc-version.txt cargo-version.txt run-closed.log binaries/hierarchical_semantic_cells'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261006/co-selection-implementation-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-co-selection-implementation-gates-spot-v1','source_commit':'c6688a6e4893b86ed52acdb0372fa317eb4f6201',
  'source_archive_sha256':'0bbc67a736146635d8552e6bda026173f08e807f7504e648aabe8ff85e55383a','config_sha256': 'c41bfb661e5430d2cefe554968f05135c9457f4a225b8f90fd57ee282790def1', 'code_identity_sha256': 'cf42c125fb339ea929ec6f387fc3c5f2004a34d5b661c7272dd57425d87ca285', 'campaign_schema': 'borsuk-co-selection-implementation-gates-spot-v1', 'source_identity_sha256': '64fe7b41484ece6a6379c716727e9407c9a1d48e0a44e71669c8994c3e32289c', 'source_file_count': 407, 'native_source_manifest_sha256': 'e4bb7e85e625e3e3e9abca7de77869babc2b3b39c620681c94e3ea945fea36c8', 'native_source_commit': '510bde198af4f28952db2fc85a42018472b23fdc', 'artifact_roster_sha256': '79aa7df432795fd1bf991c67c7e2b66175f242611fb0a9635c615e3cef9b5aa8', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'controller_source_commit': '45b93cf7434f881c0baaefa0a2d36729ca60ee08', 'candidate_delta_paths': ['crates/borsuk/src/bin/hierarchical_semantic_cells.rs', 'crates/borsuk/src/co_selection_layout.rs', 'crates/borsuk/src/fine_sq8_groups.rs', 'crates/borsuk/src/lib.rs'], 'source_archive_paths_sha256': '1473a4cbdd0cd7184717575f2df0f340b098f94e25a291defcee1217ca604054', 'source_archive_file_count': 2435, 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261006/co-selection-implementation-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/0bbc67a736146635d8552e6bda026173f08e807f7504e648aabe8ff85e55383a.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '0bbc67a736146635d8552e6bda026173f08e807f7504e648aabe8ff85e55383a' | sha256sum -c -
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
BORSUK_CO_SELECTION_SOURCE_ARCHIVE_PATHS_SHA256=1473a4cbdd0cd7184717575f2df0f340b098f94e25a291defcee1217ca604054 BORSUK_CO_SELECTION_SOURCE_ARCHIVE_FILE_COUNT=2435 PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --co-selection-implementation --stage "$root/repo" "$root"
phase=execution
systemd-run --unit=native-workspace-execution --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" \
 python3.12 -m scripts.check_native_workspace_execution --co-selection-implementation "$CARGO_HOME/bin/cargo" "$root/repo" "$root"
phase=receipt-qualification
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --co-selection-implementation --check-receipt "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
