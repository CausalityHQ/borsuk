#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-metadata-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json runtime-abi.json cpu.txt run-closed.log profile.log profile-resources.txt profile-cgroup.json binaries/control/two_bit_http binaries/candidate/two_bit_http checker-authority.json control-source.json control-checker.py.gz control-proof.json candidate-source.json candidate-checker.py.gz candidate-proof.json panel-authority.json control/native-assurance.json control/native-source.tar.gz control/assurance/affected-final.json control/assurance/affected-final.log control/assurance/release-final.json control/assurance/release-final.log control/assurance/clippy-final.json control/assurance/clippy-final.log control/assurance/test-build-final.json control/assurance/test-build-final.log control/assurance/full-workspace-final.json control/assurance/full-workspace-final.log candidate/native-assurance.json candidate/native-source.tar.gz candidate/assurance/affected-final.json candidate/assurance/affected-final.log candidate/assurance/release-final.json candidate/assurance/release-final.log candidate/assurance/clippy-final.json candidate/assurance/clippy-final.log candidate/assurance/test-build-final.json candidate/assurance/test-build-final.log candidate/assurance/full-workspace-final.json candidate/assurance/full-workspace-final.log control/qualified-source.tar.gz candidate/worker-verification.json screen/config.json screen/records.jsonl screen/summary.json screen/checker-authority.json screen/control-source.json screen/control-checker.py.gz screen/control-proof.json screen/candidate-source.json screen/candidate-checker.py.gz screen/candidate-proof.json screen/inputs/ReLAION/requests screen/inputs/ReLAION/truth screen/inputs/ReLAION/control/reference-k10 screen/inputs/ReLAION/candidate/reference-k10 screen/inputs/CoHere/requests screen/inputs/CoHere/truth screen/inputs/CoHere/control/reference-k10 screen/inputs/CoHere/candidate/reference-k10'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/metadata-head-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-metadata-cold-spot-v1','source_commit':'0000000000000000000000000000000000000000',
  'source_archive_sha256':'1111111111111111111111111111111111111111111111111111111111111111','config_sha256': '51b1bfcde9812c2605c81f0a44d25721fafa2310ff0ea0c2a95cfc5d45b8cc43', 'role_bindings': {'control': {'native_role': 'control', 'config_sha256': '51b1bfcde9812c2605c81f0a44d25721fafa2310ff0ea0c2a95cfc5d45b8cc43', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'proof_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'checker_sha256': '3815ee3cf166c659d7e9a31c2109730fb100bb301c3afe50cc3396e3a58399b6', 'checker_authority_sha256': 'a1edd5e6947ca4b1f5b5cb0669a7a60d7db9c2079c9976b1dba9a8206f1b3df3'}, 'candidate': {'native_role': 'candidate', 'config_sha256': '51b1bfcde9812c2605c81f0a44d25721fafa2310ff0ea0c2a95cfc5d45b8cc43', 'binary_sha256': '82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1', 'binary_bytes': 16189792, 'proof_sha256': '528591dd8e6d88c850b445ef17234a700638e5fae939378318fa9cc7d299abbe', 'source_manifest_sha256': '2cfbead9e036ae09ed4c2cf2e1f9e8bec3e324c8716173f61c1ffbaef75cb004', 'native_source_identity_sha256': 'b095ba7d738a65a31faefa0d705ce34e12cb83aeaa153c25079564c53b129c70', 'checker_sha256': '93db7c38b40aa9480f70076d359f35754850d46c2e853a1d9107621ba9bc07d8', 'checker_authority_sha256': 'a1edd5e6947ca4b1f5b5cb0669a7a60d7db9c2079c9976b1dba9a8206f1b3df3'}}, 'artifact_roster_sha256': 'ef4d532187bddb536293f9ba35cf6cf46d871639f57eeb44058d9e88ff4a8364', 'code_identity_sha256': '56a636446f74bee21ee58d00edf18abd7002fe46afbb9973e19101d87f9d3eab', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/metadata-head-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/1111111111111111111111111111111111111111111111111111111111111111.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '1111111111111111111111111111111111111111111111111111111111111111' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir -p binaries/control binaries/candidate
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533/two_bit_http binaries/control/two_bit_http --only-show-errors
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1/two_bit_http binaries/candidate/two_bit_http --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_metadata_cold_spot --stage "$root/repo" "$root"
phase=profile
set +e
systemd-run --unit=native-semantic-metadata-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_metadata_cold docs/research/performance-architecture-20260930/semantic-cold/metadata-head/paired-config.json 51b1bfcde9812c2605c81f0a44d25721fafa2310ff0ea0c2a95cfc5d45b8cc43 "$1/binaries/control/two_bit_http" "$1/control-proof.json" "$1/binaries/candidate/two_bit_http" "$1/candidate-proof.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592; resources=$?; if [ "$code" = 0 ] && [ "$resources" != 0 ]; then code=96; fi; exit "$code"' _ "$root" >profile.log 2>&1
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
