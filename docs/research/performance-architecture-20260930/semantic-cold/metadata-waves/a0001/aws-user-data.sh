#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-metadata-waves-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json runtime-abi.json cpu.txt run-closed.log profile.log profile-resources.txt profile-cgroup.json binaries/control/two_bit_http binaries/candidate/two_bit_http checker-authority.json control-source.json control-checker.py.gz control-proof.json candidate-source.json candidate-checker.py.gz candidate-proof.json panel-authority.json control/native-assurance.json control/native-source.tar.gz control/worker-verification.json control/assurance/affected-final.json control/assurance/affected-final.log control/assurance/release-final.json control/assurance/release-final.log control/assurance/clippy-final.json control/assurance/clippy-final.log control/assurance/test-build-final.json control/assurance/test-build-final.log control/assurance/full-workspace-final.json control/assurance/full-workspace-final.log candidate/native-assurance.json candidate/native-source.tar.gz candidate/implementation-verification.json candidate/assurance/object-native.json candidate/assurance/object-native.log candidate/assurance/generation.json candidate/assurance/generation.log candidate/assurance/http-release.json candidate/assurance/http-release.log candidate/assurance/clippy.json candidate/assurance/clippy.log candidate/assurance/workspace-test-build.json candidate/assurance/workspace-test-build.log candidate/assurance/full-workspace-final.json candidate/assurance/full-workspace-final.log screen/config.json screen/records.jsonl screen/summary.json screen/checker-authority.json screen/control-source.json screen/control-checker.py.gz screen/control-proof.json screen/candidate-source.json screen/candidate-checker.py.gz screen/candidate-proof.json screen/inputs/ReLAION/requests screen/inputs/ReLAION/truth screen/inputs/ReLAION/control/reference-k10 screen/inputs/ReLAION/candidate/reference-k10 screen/inputs/CoHere/requests screen/inputs/CoHere/truth screen/inputs/CoHere/control/reference-k10 screen/inputs/CoHere/candidate/reference-k10'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/metadata-waves-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-metadata-waves-cold-spot-v1','source_commit':'f962d7c5b367739564557e473a05e30be1389093',
  'source_archive_sha256':'8f51f90184b54d30d0bce9222293360f97e5f1eeb1a58ca5f8090cebadfb33ee','config_sha256': '13e9193efdb50161b452755de5587e6b943ab3fa5b54f0a73850bb658b987317', 'role_bindings': {'control': {'native_role': 'control', 'config_sha256': '13e9193efdb50161b452755de5587e6b943ab3fa5b54f0a73850bb658b987317', 'binary_sha256': '82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1', 'binary_bytes': 16189792, 'proof_sha256': '528591dd8e6d88c850b445ef17234a700638e5fae939378318fa9cc7d299abbe', 'source_manifest_sha256': '2cfbead9e036ae09ed4c2cf2e1f9e8bec3e324c8716173f61c1ffbaef75cb004', 'native_source_identity_sha256': 'b095ba7d738a65a31faefa0d705ce34e12cb83aeaa153c25079564c53b129c70', 'checker_sha256': '93db7c38b40aa9480f70076d359f35754850d46c2e853a1d9107621ba9bc07d8', 'checker_authority_sha256': 'c90ecbee9443ea46b75ede73b034040fdfcfcb61e0329e2d060235bb0b61a0b0'}, 'candidate': {'native_role': 'candidate', 'config_sha256': '13e9193efdb50161b452755de5587e6b943ab3fa5b54f0a73850bb658b987317', 'binary_sha256': 'e3516453797add1f8e76daddcc97a8fb5e4c1c3467749ae9cb732934b3bc2c0e', 'binary_bytes': 16259024, 'proof_sha256': '2a3104109738822581167161db9d535602fa03666c2bbd4ef5865da9ba3a873d', 'source_manifest_sha256': '38a19170e8974de3259d90e7a1137c66b9eccf93c1978f90f4cba42a43d45894', 'native_source_identity_sha256': '714794a10c2d886f7a53a9926a4bb63e093cec16858676268e31833aeead2681', 'checker_sha256': 'd1fb970c0ab6ec98ec5311204a54277180631116d48a29a564ed3bbd68e5c5c2', 'checker_authority_sha256': 'c90ecbee9443ea46b75ede73b034040fdfcfcb61e0329e2d060235bb0b61a0b0'}}, 'artifact_roster_sha256': 'b1f84f60d45a6337f9838abae5a76870b85161c9c7588a786f905d7071edc80d', 'code_identity_sha256': 'eb2a0af7b6253e496396657c6f0fa9561e9f3af9fa8bb19a3133f220a1006bfa', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/metadata-waves-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/8f51f90184b54d30d0bce9222293360f97e5f1eeb1a58ca5f8090cebadfb33ee.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '8f51f90184b54d30d0bce9222293360f97e5f1eeb1a58ca5f8090cebadfb33ee' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir -p binaries/control binaries/candidate
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1/two_bit_http binaries/control/two_bit_http --only-show-errors
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/e3516453797add1f8e76daddcc97a8fb5e4c1c3467749ae9cb732934b3bc2c0e/two_bit_http binaries/candidate/two_bit_http --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_metadata_cold_spot --stage "$root/repo" "$root"
phase=profile
set +e
systemd-run --unit=native-semantic-metadata-waves-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_metadata_cold docs/research/performance-architecture-20260930/semantic-cold/metadata-waves/paired-config.json 13e9193efdb50161b452755de5587e6b943ab3fa5b54f0a73850bb658b987317 "$1/binaries/control/two_bit_http" "$1/control-proof.json" "$1/binaries/candidate/two_bit_http" "$1/candidate-proof.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592; resources=$?; if [ "$code" = 0 ] && [ "$resources" != 0 ]; then code=96; fi; exit "$code"' _ "$root" >profile.log 2>&1
profile_code=$?
set -e
if [ "$profile_code" -gt 1 ]; then exit "$profile_code"; fi
if ! PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_metadata_cold_spot --check-closed "$root"; then
  if [ "$profile_code" = 0 ]; then profile_code=96; fi
  exit "$profile_code"
fi
for name in $ARTIFACT_NAMES; do
  if [ "$name" = run-closed.log ]; then test -s "$root/run.log"; else test -s "$root/$name"; fi
done
phase=complete
exit "$profile_code"
