#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-metadata-waves-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES=run-closed.log
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/metadata-waves-offered-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-metadata-waves-cold-offered-spot-v1','source_commit':'09c62f23781720939c66b2280f8b2f035ba01a36',
  'source_archive_sha256':'a7f178ad12fda56c8af2331151500297a292c31c35d86962d8129e104ce66909','config_sha256': '8e26545e9c23c738227a478a46403632fb0f3996be6feb093b5c31c101d672da', 'role_bindings': {'control': {'native_role': 'control', 'config_sha256': '8e26545e9c23c738227a478a46403632fb0f3996be6feb093b5c31c101d672da', 'binary_sha256': '82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1', 'binary_bytes': 16189792, 'proof_sha256': '528591dd8e6d88c850b445ef17234a700638e5fae939378318fa9cc7d299abbe', 'source_manifest_sha256': '2cfbead9e036ae09ed4c2cf2e1f9e8bec3e324c8716173f61c1ffbaef75cb004', 'native_source_identity_sha256': 'b095ba7d738a65a31faefa0d705ce34e12cb83aeaa153c25079564c53b129c70', 'checker_sha256': '93db7c38b40aa9480f70076d359f35754850d46c2e853a1d9107621ba9bc07d8', 'checker_authority_sha256': 'c90ecbee9443ea46b75ede73b034040fdfcfcb61e0329e2d060235bb0b61a0b0'}, 'candidate': {'native_role': 'candidate', 'config_sha256': '8e26545e9c23c738227a478a46403632fb0f3996be6feb093b5c31c101d672da', 'binary_sha256': 'e3516453797add1f8e76daddcc97a8fb5e4c1c3467749ae9cb732934b3bc2c0e', 'binary_bytes': 16259024, 'proof_sha256': '2a3104109738822581167161db9d535602fa03666c2bbd4ef5865da9ba3a873d', 'source_manifest_sha256': '38a19170e8974de3259d90e7a1137c66b9eccf93c1978f90f4cba42a43d45894', 'native_source_identity_sha256': '714794a10c2d886f7a53a9926a4bb63e093cec16858676268e31833aeead2681', 'checker_sha256': 'd1fb970c0ab6ec98ec5311204a54277180631116d48a29a564ed3bbd68e5c5c2', 'checker_authority_sha256': 'c90ecbee9443ea46b75ede73b034040fdfcfcb61e0329e2d060235bb0b61a0b0'}}, 'artifact_roster_sha256': '1677e2733fee307ca5783ba0a1cc44e2572aecc619e6e25afc33101e6ade4be5', 'code_identity_sha256': '075fb761e13575f909922a98cb4d692458da89532529a64a0e74d9c0d5657819', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/metadata-waves-offered-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/a7f178ad12fda56c8af2331151500297a292c31c35d86962d8129e104ce66909.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'a7f178ad12fda56c8af2331151500297a292c31c35d86962d8129e104ce66909' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
export ARTIFACT_NAMES=$(PYTHONPATH="$root/repo" python3.12 -c "from scripts.launch_native_semantic_metadata_cold_spot import OFFERED_ARTIFACTS; print(' '.join(OFFERED_ARTIFACTS))")
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir -p binaries/control binaries/candidate
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1/two_bit_http binaries/control/two_bit_http --only-show-errors
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/e3516453797add1f8e76daddcc97a8fb5e4c1c3467749ae9cb732934b3bc2c0e/two_bit_http binaries/candidate/two_bit_http --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_metadata_cold_spot --offered --stage "$root/repo" "$root"
phase=profile
set +e
systemd-run --unit=native-semantic-metadata-waves-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_metadata_cold_spot --offered --run-offered research/semantic-router/20261001/metadata-waves-offered-a0001 docs/research/performance-architecture-20260930/semantic-cold/metadata-waves/offered-config.json 8e26545e9c23c738227a478a46403632fb0f3996be6feb093b5c31c101d672da "$1/binaries/control/two_bit_http" "$1/control-proof.json" "$1/binaries/candidate/two_bit_http" "$1/candidate-proof.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592; resources=$?; if [ "$code" = 0 ] && [ "$resources" != 0 ]; then code=96; fi; exit "$code"' _ "$root" >profile.log 2>&1
profile_code=$?
set -e
if [ "$profile_code" -gt 1 ]; then exit "$profile_code"; fi
if ! PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_metadata_cold_spot --offered --check-closed "$root"; then
  if [ "$profile_code" = 0 ]; then profile_code=96; fi
  exit "$profile_code"
fi
for name in $ARTIFACT_NAMES; do
  if [ "$name" = run-closed.log ]; then test -s "$root/run.log"; else test -s "$root/$name"; fi
done
phase=complete
exit "$profile_code"
