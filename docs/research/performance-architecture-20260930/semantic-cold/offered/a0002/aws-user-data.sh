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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/offered-a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-offered-spot-v1','source_commit':'ace63b7dcba2dd4e6bf1e7d6ac4d13d0099bb91b',
  'source_archive_sha256':'b8338396bbe5ffd96e9b846e42e1063d6e49cd47889871d20909b37ca503a64a','config_sha256': '4bd2583af8637979983e620ac605c7e71f9bd45e17452276966be0af49aff412', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': 'b7d82ec23cf45003bf53f353937020e1fa75368eb608eb74a0bb8c8cab98b955', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1', 'runtime_os': {'ID': 'ubuntu', 'VERSION_ID': '24.04'}, 'runtime_glibc': '2.39', 'required_glibc': {'two_bit_http': '2.38', 'two_bit_plan_demo': '2.38'}, 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/offered-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/b8338396bbe5ffd96e9b846e42e1063d6e49cd47889871d20909b37ca503a64a.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'b8338396bbe5ffd96e9b846e42e1063d6e49cd47889871d20909b37ca503a64a' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_router_cold_spot --run-offered docs/research/performance-architecture-20260930/semantic-cold/offered-config.json 4bd2583af8637979983e620ac605c7e71f9bd45e17452276966be0af49aff412 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen" research/semantic-router/20261001/offered-a0002; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
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
