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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/offered-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-offered-spot-v1','source_commit':'e1c9b4456baf0cb80c94808b30d23aa2af9ac0f6',
  'source_archive_sha256':'e05cc0e2fe9b7e5469345ada3c146127487623807662447b08ea913c4570a46b','config_sha256': '3e987b9256913cc738b954213f1a6c7701ff131f6206ee4e6e828acf11ebf92d', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': 'b7d82ec23cf45003bf53f353937020e1fa75368eb608eb74a0bb8c8cab98b955', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1', 'runtime_os': {'ID': 'ubuntu', 'VERSION_ID': '24.04'}, 'runtime_glibc': '2.39', 'required_glibc': {'two_bit_http': '2.38', 'two_bit_plan_demo': '2.38'}, 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/offered-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/e05cc0e2fe9b7e5469345ada3c146127487623807662447b08ea913c4570a46b.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'e05cc0e2fe9b7e5469345ada3c146127487623807662447b08ea913c4570a46b' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_router_cold_spot --run-offered docs/research/performance-architecture-20260930/semantic-cold/offered-config.json 3e987b9256913cc738b954213f1a6c7701ff131f6206ee4e6e828acf11ebf92d "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen" research/semantic-router/20261001/offered-a0001; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
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
