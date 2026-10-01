#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-router-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json runtime-abi.json binaries/two_bit_http binaries/two_bit_plan_demo cpu.txt run-closed.log profile.log profile-resources.txt profile-cgroup.json screen/records.jsonl screen/summary.json screen/config.json screen/qualification.json native-assurance.json boundary-check.json native-source-manifest.json native-source.tar.gz qualified-source.tar.gz publisher-proof.json asset-manifest.json assurance/affected-final.json assurance/affected-final.log assurance/release-final.json assurance/release-final.log assurance/clippy-final.json assurance/clippy-final.log assurance/test-build-final.json assurance/test-build-final.log assurance/full-workspace-final.json assurance/full-workspace-final.log publication/config.json publication/asset-manifest.json publication/publication-receipt.json publication/ReLAION/control/native.jsonl publication/ReLAION/control/stdout.log publication/ReLAION/control/stderr.log publication/ReLAION/control/resources.txt publication/ReLAION/control/head.json publication/ReLAION/candidate/native.jsonl publication/ReLAION/candidate/stdout.log publication/ReLAION/candidate/stderr.log publication/ReLAION/candidate/resources.txt publication/ReLAION/candidate/head.json publication/CoHere/control/native.jsonl publication/CoHere/control/stdout.log publication/CoHere/control/stderr.log publication/CoHere/control/resources.txt publication/CoHere/control/head.json publication/CoHere/candidate/native.jsonl publication/CoHere/candidate/stdout.log publication/CoHere/candidate/stderr.log publication/CoHere/candidate/resources.txt publication/CoHere/candidate/head.json'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0003/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-spot-v1','source_commit':'ee704e2b6305edc7285dd9fad8ec313194bbb8e9',
  'source_archive_sha256':'e186b577d101b6687e428b233579a5ff4da1213ab90d6fff528af8fd7d80a863','config_sha256': '6fb475bec06314e1c6cb86bb77ae19a188278eeb6182fb69a4ce095b61144312', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': '194fe86ffa2a6b07b5f0e2c098bc5a678ba56f09a27c9ea7f45da7b6c9b31f8c', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1', 'runtime_os': {'ID': 'ubuntu', 'VERSION_ID': '24.04'}, 'runtime_glibc': '2.39', 'required_glibc': {'two_bit_http': '2.38', 'two_bit_plan_demo': '2.38'}, 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0003/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/e186b577d101b6687e428b233579a5ff4da1213ab90d6fff528af8fd7d80a863.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'e186b577d101b6687e428b233579a5ff4da1213ab90d6fff528af8fd7d80a863' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
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
systemd-run --unit=native-semantic-router-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_router_cold docs/research/performance-architecture-20260930/semantic-cold/config.json 6fb475bec06314e1c6cb86bb77ae19a188278eeb6182fb69a4ce095b61144312 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
  case "$name" in
    publication/*/stdout.log|publication/*/stderr.log) test -f "$root/$name";;
    run-closed.log) test -s "$root/run.log";;
    *) test -s "$root/$name";;
  esac
done
phase=complete
