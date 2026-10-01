#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-router-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in source-qualification.json runtime-abi.json binaries/two_bit_http binaries/two_bit_plan_demo cpu.txt run-closed.log profile.log profile-resources.txt profile-cgroup.json screen/records.jsonl screen/summary.json screen/config.json screen/qualification.json native-assurance.json boundary-check.json native-source-manifest.json native-source.tar.gz qualified-source.tar.gz publisher-proof.json asset-manifest.json assurance/affected-final.json assurance/affected-final.log assurance/release-final.json assurance/release-final.log assurance/clippy-final.json assurance/clippy-final.log assurance/test-build-final.json assurance/test-build-final.log assurance/full-workspace-final.json assurance/full-workspace-final.log publication/config.json publication/asset-manifest.json publication/publication-receipt.json publication/ReLAION/control/native.jsonl publication/ReLAION/control/stdout.log publication/ReLAION/control/stderr.log publication/ReLAION/control/resources.txt publication/ReLAION/control/head.json publication/ReLAION/candidate/native.jsonl publication/ReLAION/candidate/stdout.log publication/ReLAION/candidate/stderr.log publication/ReLAION/candidate/resources.txt publication/ReLAION/candidate/head.json publication/CoHere/control/native.jsonl publication/CoHere/control/stdout.log publication/CoHere/control/stderr.log publication/CoHere/control/resources.txt publication/CoHere/control/head.json publication/CoHere/candidate/native.jsonl publication/CoHere/candidate/stdout.log publication/CoHere/candidate/stderr.log publication/CoHere/candidate/resources.txt publication/CoHere/candidate/head.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('source-qualification.json', 'runtime-abi.json', 'binaries/two_bit_http', 'binaries/two_bit_plan_demo', 'cpu.txt', 'run-closed.log', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/records.jsonl', 'screen/summary.json', 'screen/config.json', 'screen/qualification.json', 'native-assurance.json', 'boundary-check.json', 'native-source-manifest.json', 'native-source.tar.gz', 'qualified-source.tar.gz', 'publisher-proof.json', 'asset-manifest.json', 'assurance/affected-final.json', 'assurance/affected-final.log', 'assurance/release-final.json', 'assurance/release-final.log', 'assurance/clippy-final.json', 'assurance/clippy-final.log', 'assurance/test-build-final.json', 'assurance/test-build-final.log', 'assurance/full-workspace-final.json', 'assurance/full-workspace-final.log', 'publication/config.json', 'publication/asset-manifest.json', 'publication/publication-receipt.json', 'publication/ReLAION/control/native.jsonl', 'publication/ReLAION/control/stdout.log', 'publication/ReLAION/control/stderr.log', 'publication/ReLAION/control/resources.txt', 'publication/ReLAION/control/head.json', 'publication/ReLAION/candidate/native.jsonl', 'publication/ReLAION/candidate/stdout.log', 'publication/ReLAION/candidate/stderr.log', 'publication/ReLAION/candidate/resources.txt', 'publication/ReLAION/candidate/head.json', 'publication/CoHere/control/native.jsonl', 'publication/CoHere/control/stdout.log', 'publication/CoHere/control/stderr.log', 'publication/CoHere/control/resources.txt', 'publication/CoHere/control/head.json', 'publication/CoHere/candidate/native.jsonl', 'publication/CoHere/candidate/stdout.log', 'publication/CoHere/candidate/stderr.log', 'publication/CoHere/candidate/resources.txt', 'publication/CoHere/candidate/head.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-spot-v1','source_commit':'6facef790036392b2f4f2c58b9c5e764545de779',
  'source_archive_sha256':'4f0ae3503509544a594a3c94c6e10e912807df46e54fe468344dd88c5aac8f3c','config_sha256': '3c7fe631dd22f0ef017a5b96007ce1e50ccc2723c3783e0e776ab68a15917abd', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': '194fe86ffa2a6b07b5f0e2c098bc5a678ba56f09a27c9ea7f45da7b6c9b31f8c', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1', 'runtime_os': {'ID': 'ubuntu', 'VERSION_ID': '24.04'}, 'runtime_glibc': '2.39', 'required_glibc': {'two_bit_http': '2.38', 'two_bit_plan_demo': '2.38'}, 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
export DEBIAN_FRONTEND=noninteractive
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install awscli python3-boto3 python3.12 time tar gzip util-linux binutils
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/4f0ae3503509544a594a3c94c6e10e912807df46e54fe468344dd88c5aac8f3c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '4f0ae3503509544a594a3c94c6e10e912807df46e54fe468344dd88c5aac8f3c' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_router_cold docs/research/performance-architecture-20260930/semantic-cold/config.json 3c7fe631dd22f0ef017a5b96007ce1e50ccc2723c3783e0e776ab68a15917abd "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/source-qualification.json"
test -s "$root/runtime-abi.json"
test -s "$root/binaries/two_bit_http"
test -s "$root/binaries/two_bit_plan_demo"
test -s "$root/cpu.txt"
test -s "$root/run-closed.log"
test -s "$root/profile.log"
test -s "$root/profile-resources.txt"
test -s "$root/profile-cgroup.json"
test -s "$root/screen/records.jsonl"
test -s "$root/screen/summary.json"
test -s "$root/screen/config.json"
test -s "$root/screen/qualification.json"
test -s "$root/native-assurance.json"
test -s "$root/boundary-check.json"
test -s "$root/native-source-manifest.json"
test -s "$root/native-source.tar.gz"
test -s "$root/qualified-source.tar.gz"
test -s "$root/publisher-proof.json"
test -s "$root/asset-manifest.json"
test -s "$root/assurance/affected-final.json"
test -s "$root/assurance/affected-final.log"
test -s "$root/assurance/release-final.json"
test -s "$root/assurance/release-final.log"
test -s "$root/assurance/clippy-final.json"
test -s "$root/assurance/clippy-final.log"
test -s "$root/assurance/test-build-final.json"
test -s "$root/assurance/test-build-final.log"
test -s "$root/assurance/full-workspace-final.json"
test -s "$root/assurance/full-workspace-final.log"
test -s "$root/publication/config.json"
test -s "$root/publication/asset-manifest.json"
test -s "$root/publication/publication-receipt.json"
test -s "$root/publication/ReLAION/control/native.jsonl"
test -f "$root/publication/ReLAION/control/stdout.log"
test -f "$root/publication/ReLAION/control/stderr.log"
test -s "$root/publication/ReLAION/control/resources.txt"
test -s "$root/publication/ReLAION/control/head.json"
test -s "$root/publication/ReLAION/candidate/native.jsonl"
test -f "$root/publication/ReLAION/candidate/stdout.log"
test -f "$root/publication/ReLAION/candidate/stderr.log"
test -s "$root/publication/ReLAION/candidate/resources.txt"
test -s "$root/publication/ReLAION/candidate/head.json"
test -s "$root/publication/CoHere/control/native.jsonl"
test -f "$root/publication/CoHere/control/stdout.log"
test -f "$root/publication/CoHere/control/stderr.log"
test -s "$root/publication/CoHere/control/resources.txt"
test -s "$root/publication/CoHere/control/head.json"
test -s "$root/publication/CoHere/candidate/native.jsonl"
test -f "$root/publication/CoHere/candidate/stdout.log"
test -f "$root/publication/CoHere/candidate/stderr.log"
test -s "$root/publication/CoHere/candidate/resources.txt"
test -s "$root/publication/CoHere/candidate/head.json"
phase=complete
