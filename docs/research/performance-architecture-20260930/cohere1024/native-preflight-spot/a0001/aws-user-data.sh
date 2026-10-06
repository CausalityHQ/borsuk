#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=10800s /usr/sbin/shutdown -h now
root=/mnt/borsuk-cohere
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json config.json helper-config.json transport-receipt.json retention-receipt.json cpu.txt helper.log helper-resources.txt run-closed.log preflight/config.json preflight/admission.json preflight/execution-receipt.json preflight/terminal.json preflight/publish-receipt.json preflight/prepared/complete.json preflight/generation/manifest.json preflight/generation/plane/manifest.json preflight/baseline-result.jsonl preflight/configs/prepare.json preflight/configs/normalize.json preflight/configs/fit.json preflight/configs/sq8.json preflight/configs/generation.json preflight/configs/publish.json preflight/configs/baseline.json preflight/prepare.log preflight/prepare-closure.json preflight/prepare-stage.json preflight/prepare-stage-receipt.json preflight/prepare-unit.log preflight/normalize.log preflight/normalize-closure.json preflight/normalize-stage.json preflight/normalize-stage-receipt.json preflight/normalize-unit.log preflight/fit.log preflight/fit-closure.json preflight/fit-stage.json preflight/fit-stage-receipt.json preflight/fit-unit.log preflight/sq8.log preflight/sq8-closure.json preflight/sq8-stage.json preflight/sq8-stage-receipt.json preflight/sq8-unit.log preflight/generation.log preflight/generation-closure.json preflight/generation-stage.json preflight/generation-stage-receipt.json preflight/generation-unit.log preflight/publish.log preflight/publish-closure.json preflight/publish-stage.json preflight/publish-stage-receipt.json preflight/publish-unit.log preflight/baseline.log preflight/baseline-closure.json preflight/baseline-stage.json preflight/baseline-stage-receipt.json preflight/baseline-unit.log'
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  systemctl stop borsuk-global-leaf-a0001.slice 2>/dev/null || true
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261006/cohere-native-preflight-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-cohere-native-preflight-spot-v1','source_commit':'5bdd9f5a4f7eff419b90d3b9b9869db09bf511ba',
  'source_archive_sha256':'a0b0b393e65e24fbb2676056651d7d61094b52aa50ae955d0bdecf85ebefb975','config_sha256': 'd9f9c95efe9068b760506ef8de463db0db128229f8405a5876af591ccbb885a3', 'code_identity_sha256': 'b6de5fd2b286ebdad9bfe160d3a6b69bda9e2e1bf14fe2af2667cc12982ce17e', 'campaign_schema': 'borsuk-cohere-native-preflight-spot-v1', 'source_identity_sha256': 'd0e7029d484db02904505b7c83c57a13a3258ab9db804e54338ff3fca0f39676', 'source_file_count': 412, 'artifact_roster_sha256': '0615964a23a80f5be680adeecaf9f1a6ca5ce22af728c3b2934190669cc1bdf0', 'helper_config_sha256': 'ac155a31a02fd0cfbeb9aae6cfb022a3a40bf46bb693c60ada7599420f1cfd6c', 'admission_sha256': 'a3b280160fefd106def5200ddd506dd44f9571eb50c02b6abc268539a5d48c91', 'source_archive_paths_sha256': '5048fa5251730340c1fbc11df757f343648cb9eb45c162606e635163c366f8ee', 'source_archive_file_count': 447, 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261006/cohere-native-preflight-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 time tar gzip util-linux binutils
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/a0b0b393e65e24fbb2676056651d7d61094b52aa50ae955d0bdecf85ebefb975.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'a0b0b393e65e24fbb2676056651d7d61094b52aa50ae955d0bdecf85ebefb975' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -c 'import boto3'
lscpu >cpu.txt
test "$(uname -m)" = x86_64
phase=source-qualification
BORSUK_COHERE_PREFLIGHT_SOURCE_ARCHIVE_PATHS_SHA256=5048fa5251730340c1fbc11df757f343648cb9eb45c162606e635163c366f8ee BORSUK_COHERE_PREFLIGHT_SOURCE_ARCHIVE_FILE_COUNT=447 PYTHONPATH="$root/repo" python3.12 -m scripts.launch_cohere_native_preflight_spot --stage "$root/repo" "$root"
phase=transport
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_cohere_native_preflight_spot --transport "$root"
phase=preflight
systemctl start borsuk-global-leaf-a0001.slice
systemctl set-property --runtime borsuk-global-leaf-a0001.slice MemoryMax=8448M MemorySwapMax=0 CPUQuota=400% TasksMax=512
set +e
systemd-run --slice=borsuk-global-leaf-a0001.slice --unit=cohere-native-preflight --wait --pipe -p MemoryMax=8448M -p MemorySwapMax=0 -p CPUQuota=400% -p TasksMax=512 -p RuntimeMaxSec=9660 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=BORSUK_GLOBAL_LEAF_SLICE=borsuk-global-leaf-a0001.slice \
 /usr/bin/time -v -o "$root/helper-resources.txt" timeout --signal=TERM --kill-after=30 9630 \
 python3.12 scripts/run_cohere_native_preflight.py /mnt/borsuk-cohere/repo/docs/research/performance-architecture-20260930/cohere1024/native-preflight-spot/helper-config.json ac155a31a02fd0cfbeb9aae6cfb022a3a40bf46bb693c60ada7599420f1cfd6c "$root/preflight" >helper.log 2>&1
helper_code=$?
set -e
phase=check-closed
if ! PYTHONPATH="$root/repo" python3.12 -m scripts.launch_cohere_native_preflight_spot --check-closed "$root" "$helper_code"; then
  if [ "$helper_code" = 0 ]; then helper_code=96; fi
  exit "$helper_code"
fi
phase=retain
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_cohere_native_preflight_spot --retain "$root" research/semantic-router/20261006/cohere-native-preflight-a0001 || { if [ "$helper_code" = 0 ]; then helper_code=96; fi; exit "$helper_code"; }
test "$helper_code" = 0 || exit "$helper_code"
for name in $ARTIFACT_NAMES; do
  case "$name" in
    run-closed.log) test -s "$root/run.log";;
    preflight/publish.log|preflight/*-unit.log) test -f "$root/$name";;
    *) test -s "$root/$name";;
  esac
done
phase=complete
exit "$helper_code"
