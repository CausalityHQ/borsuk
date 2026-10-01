#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-router-cold
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
  if [ -d "$root/repo/scripts" ]; then
    export PYTHONPATH="$root/repo"
    ARTIFACT_NAMES=$(python3.12 -m scripts.launch_native_semantic_router_cold_spot --closed-artifacts "$root") || ARTIFACT_NAMES="run-closed.log profile.log profile-resources.txt profile-cgroup.json"
    export ARTIFACT_NAMES
  else ARTIFACT_NAMES="run-closed.log"; export ARTIFACT_NAMES; fi
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/offered-a0003/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-offered-spot-v1','source_commit':'c2e74dfab9f7704c45748a41cb28fb1d400afd79',
  'source_archive_sha256':'2fcb1ac172843279f43b0038b222f2e2c5899b48e9e90499581eedea4afd01d0','config_sha256': 'cdfcb3c9b02a2d530a3ca4a8975190a426f01b250a86e762b2a581a1f4044582', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': 'b7d82ec23cf45003bf53f353937020e1fa75368eb608eb74a0bb8c8cab98b955', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1', 'runtime_os': {'ID': 'ubuntu', 'VERSION_ID': '24.04'}, 'runtime_glibc': '2.39', 'required_glibc': {'two_bit_http': '2.38', 'two_bit_plan_demo': '2.38'}, 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),'scientific_qualification':('PASS' if json.loads(Path('screen/summary.json').read_text())['offered_gate_passed'] is True and __import__('scripts.launch_native_semantic_router_cold_spot',fromlist=['_profile_resources'])._profile_resources(json.loads(Path('profile-cgroup.json').read_text())) else 'FAIL') if os.environ['PHASE']=='complete' else None,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/offered-a0003/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/2fcb1ac172843279f43b0038b222f2e2c5899b48e9e90499581eedea4afd01d0.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '2fcb1ac172843279f43b0038b222f2e2c5899b48e9e90499581eedea4afd01d0' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
export ARTIFACT_NAMES=$(PYTHONPATH="$root/repo" python3.12 -c "from scripts.launch_native_semantic_router_cold_spot import OFFERED_ARTIFACTS; print(' '.join(OFFERED_ARTIFACTS))")
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir binaries
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533/two_bit_http binaries/two_bit_http --only-show-errors
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/native/6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813/two_bit_plan_demo binaries/two_bit_plan_demo --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --stage "$root/repo" "$root"
phase=publication
systemd-run --unit=native-semantic-publication --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3600 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_router_cold_spot --publish "$1/repo" "$1"' _ "$root"
phase=profile
set +e
systemd-run --unit=native-semantic-router-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_router_cold_spot --run-offered docs/research/performance-architecture-20260930/semantic-cold/offered-config.json cdfcb3c9b02a2d530a3ca4a8975190a426f01b250a86e762b2a581a1f4044582 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen" research/semantic-router/20261001/offered-a0003; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
profile_code=$?
set -e
test "$profile_code" -le 1
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --check-offered-closure "$root"
for name in $ARTIFACT_NAMES; do
  case "$name" in
    publication/*/stdout.log|publication/*/stderr.log) test -f "$root/$name";;
    run-closed.log) test -s "$root/run.log";;
    *) test -s "$root/$name";;
  esac
done
phase=complete
exit "$profile_code"
