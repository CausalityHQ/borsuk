#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/native-cohere-semantic-1m-quality
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='test-resources.txt run-closed.log screen/source-qualification.json screen/config.json screen/tool-versions.json screen/input-hashes.json screen/sq8-ordinal-check.json screen/local-sq8-head.json screen/builder-config.json screen/scorer-config.json screen/build.log screen/build-resources.txt screen/score.log screen/score-resources.txt screen/records.jsonl screen/summary.json screen/resources.json screen/quality-cgroup.json screen/cleanup.json screen/preparation.log screen/generation-manifest.json'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/cohere-fresh1m-quality-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-cohere-semantic-1m-quality-spot-v1','source_commit':'f86a19a00191325e6da918d2946789c84ab6d249',
  'source_archive_sha256':'5a857f0e4c55dbb2a00c83aec925124304c33a496a935321702e8a477c41be5f','config_sha256': '435bb52600646820b47713ebb6a652c894407016e0a9c86cb15143ba4470814c', 'code_identity_sha256': '9368ce8850aad475b41c15ae540f4c80ad7978a234f189eb3180cdeb6a78e314', 'refs_identity_sha256': 'd00388c8499719844456123a9f3aae82d89c14db67fa5e45c69108ba2d3e66a3', 'panel_identity_sha256': '9f30bcd3aae9c22705ddec4ec387ac4ed852c9af059b4aa5a17644d99899a322', 'inputs_identity_sha256': 'afbc39ffc377352d36c0d836b79039c2906ea0f4ebc57d477691f33efa809b1e', 'artifact_roster_sha256': '8c312f34e10a0468511e946687733d404906cf9aac8147c1bd4e40919db9fad4', 'source_identity_sha256': '295a79de9a499cc388db14b4b78ac9fcd4f1eb8673ceb5c1222dfc7f119e9ae4', 'original_source_identity_sha256': 'c9ecb3ac7138978b45efde523c8e96a88811d559e50906c178fd4cb602fb9ff5', 'campaign_schema': 'borsuk-cohere-semantic-1m-quality-spot-v1', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'dataset': 'CoHere', 'original_preparation_campaign_status': 'FAIL', 'binary_assurance_only_reused': True,
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/cohere-fresh1m-quality-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/5a857f0e4c55dbb2a00c83aec925124304c33a496a935321702e8a477c41be5f.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '5a857f0e4c55dbb2a00c83aec925124304c33a496a935321702e8a477c41be5f' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
phase=quality
systemd-run --unit=cohere-semantic-1m-quality --wait --pipe -p MemoryMax=4G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7200 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C \
 --setenv=BORSUK_QUALITY_SOURCE_COMMIT=f86a19a00191325e6da918d2946789c84ab6d249 --setenv=BORSUK_QUALITY_ARCHIVE_SHA256=5a857f0e4c55dbb2a00c83aec925124304c33a496a935321702e8a477c41be5f \
 --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=5 7200 \
 "$root/venv/bin/python" -m scripts.run_native_cohere_semantic_1m_quality "$root/repo/docs/research/performance-architecture-20260930/semantic-1m/cohere-quality/config.json" 435bb52600646820b47713ebb6a652c894407016e0a9c86cb15143ba4470814c "$root/repo" "$root/screen"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
