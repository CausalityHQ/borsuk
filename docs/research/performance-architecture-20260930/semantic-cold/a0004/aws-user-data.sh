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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0004/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-semantic-router-cold-spot-v1','source_commit':'4c5890d64d8fb8ee79669d24e70d3af49d67a338',
  'source_archive_sha256':'c05708ce66f4deab1194b48406e06b870a6c5e751e064b4f656dd700f768d240','config_sha256': '361f82d84e3a08a1efb5f31b516f20aa0d8735a264052ebfef2e921620564304', 'qualification_sha256': '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'binary_sha256': 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533', 'binary_bytes': 16191384, 'native_source_commit': 'eeaafae3cd5d374b982b22914e1634dfce117dd7', 'source_identity_sha256': '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', 'source_file_count': 399, 'artifact_roster_sha256': '194fe86ffa2a6b07b5f0e2c098bc5a678ba56f09a27c9ea7f45da7b6c9b31f8c', 'native_source_archive_sha256': '033d196e982bca44c4adfe39b00a9353698f7e2d93494fbe94130e236430287a', 'native_source_manifest_sha256': '21b769e5d54a85251f128aee6f1653673457227724d33c2801cde93795de4ccf', 'native_assurance_sha256': '98b1bf6f091842470391635af1307380c8e4870176b0b8fa38006985303b4e35', 'publisher_sha256': '6647839e08bcd5d50beb7badc313274028e0e5a36d2c354454fcfc0e325d1813', 'publisher_bytes': 15578880, 'publisher_qualification_sha256': 'd6cc84a5f87d087d2dc4c2826effa3d9d9fbecf77cabd054e4d9c57215fbcdcf', 'asset_manifest_sha256': 'e87e21b333d360005f325634588e351196503f2fb10290b54d7f0768a73be338', 'publication_assets': {'key': 'research/semantic-router/20261001/publication-assets/1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973.tar.gz', 'bytes': 105632412, 'sha256': '1d30e5a5dbfaa8c7ce054a121b55ebda8de751ba37bd27274ec33bb7cd5ff973'}, 'asset_preparation_sha256': 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1', 'runtime_os': {'ID': 'ubuntu', 'VERSION_ID': '24.04'}, 'runtime_glibc': '2.39', 'required_glibc': {'two_bit_http': '2.38', 'two_bit_plan_demo': '2.38'}, 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/cold-a0004/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c05708ce66f4deab1194b48406e06b870a6c5e751e064b4f656dd700f768d240.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c05708ce66f4deab1194b48406e06b870a6c5e751e064b4f656dd700f768d240' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_router_cold docs/research/performance-architecture-20260930/semantic-cold/config.json 361f82d84e3a08a1efb5f31b516f20aa0d8735a264052ebfef2e921620564304 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
  case "$name" in
    publication/*/stdout.log|publication/*/stderr.log) test -f "$root/$name";;
    run-closed.log) test -s "$root/run.log";;
    *) test -s "$root/$name";;
  esac
done
phase=complete
