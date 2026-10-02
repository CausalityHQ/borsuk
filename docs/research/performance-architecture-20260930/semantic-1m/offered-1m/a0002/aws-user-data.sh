#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-1m-offered
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='test-resources.txt run-closed.log screen/source-qualification.json screen/config.json screen/tool-versions.json screen/input-hashes.json screen/records.jsonl screen/failures.jsonl screen/summary.json screen/resources.json screen/offered-cgroup.json screen/cleanup.json screen/rate0-records.jsonl screen/rate0-summary.json screen/rate1-records.jsonl screen/rate1-summary.json screen/rate2-records.jsonl screen/rate2-summary.json screen/rate3-records.jsonl screen/rate3-summary.json screen/rate4-records.jsonl screen/rate4-summary.json screen/rate5-records.jsonl screen/rate5-summary.json'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fresh1m-offered-a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-semantic-1m-offered-spot-v1','source_commit':'9a778f3461468447cbdf32a992b86cf4ca893b77',
  'source_archive_sha256':'3901c6a3b0a6a0150f334ebd58361585499cf9980877741731e72a7e55fd988e','config_sha256': '4b6ab555053d64f7df09b7e86ab2ef607eb164fe423b3b1fbfb40b20df9394d6', 'code_identity_sha256': '2f53d303f14a285bc70794d51f72f20a537b4e22dd700f9e7bf0d6a6d244d7f4', 'refs_identity_sha256': 'cda69e545646687818c72d5829221a4b72098656f2eeb93b53b3f58dde757f12', 'panel_identity_sha256': '7aa89386314c25e1b09183732304c29a9f1912a21abb841eecaa03e63f19cd3e', 'inputs_identity_sha256': '5471e3951f196ae7f1fdb5ce46795ec645760ddc3fa30f86d1f1f40f6b5b56d5', 'artifact_roster_sha256': 'de21b15871b980d0f6751d513fee185a427867b607737e87ec5f258c7322ffa2', 'source_identity_sha256': '295a79de9a499cc388db14b4b78ac9fcd4f1eb8673ceb5c1222dfc7f119e9ae4', 'original_source_identity_sha256': 'c9ecb3ac7138978b45efde523c8e96a88811d559e50906c178fd4cb602fb9ff5', 'campaign_schema': 'borsuk-semantic-1m-offered-spot-v1', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'quality_reference_sha256': '6e87a3eb7c255c50ab09546e5a88858a71f91da396cd4cfc88fddd82164aed7e', 'namespace_prefix': 'research/semantic-router/20261001/fresh1m-cold-a0005/native', 'measurement_prefix': 'research/semantic-router/20261002/fresh1m-offered-a0002', 'cold_terminal_sha256': '25de5c930a066bdae71d48078e23901d79c6bc150770987cfaeeb5c105d714b6',
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fresh1m-offered-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3.12 python3.12-venv time tar gzip util-linux binutils
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/3901c6a3b0a6a0150f334ebd58361585499cf9980877741731e72a7e55fd988e.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '3901c6a3b0a6a0150f334ebd58361585499cf9980877741731e72a7e55fd988e' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0 boto3==1.40.72 botocore==1.40.72 s3transfer==0.14.0 jmespath==1.0.1 python-dateutil==2.9.0 six==1.17.0 urllib3==2.6.3
PYTHONPATH="$root/repo" "$root/venv/bin/python" -c 'import numpy, pyarrow; from scripts import launch_native_semantic_1m_offered_spot as launch; launch.ids.lifecycle()'
phase=quality
systemd-run --unit=semantic-1m-offered --wait --pipe -p MemoryMax=8G -p IOAccounting=yes -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=3000 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C \
 --setenv=BORSUK_OFFERED_SOURCE_COMMIT=9a778f3461468447cbdf32a992b86cf4ca893b77 --setenv=BORSUK_OFFERED_ARCHIVE_SHA256=3901c6a3b0a6a0150f334ebd58361585499cf9980877741731e72a7e55fd988e \
 --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=90 3000 \
 taskset -c 4-5 "$root/venv/bin/python" -m scripts.launch_native_semantic_1m_offered_spot --worker "$root/repo/docs/research/performance-architecture-20260930/semantic-1m/offered-1m/config.json" 4b6ab555053d64f7df09b7e86ab2ef607eb164fe423b3b1fbfb40b20df9394d6 "$root/repo" "$root/screen" research/semantic-router/20261002/fresh1m-offered-a0002
for name in $ARTIFACT_NAMES; do
 if [ "$name" = screen/failures.jsonl ]; then test -f "$name"; elif [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
