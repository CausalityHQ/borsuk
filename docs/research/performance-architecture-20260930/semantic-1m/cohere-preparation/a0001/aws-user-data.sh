#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=5400s /usr/sbin/shutdown -h now
root=/mnt/cohere-fresh64-preparation
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='config.json helper-config.json source-qualification.json cpu.txt tool-versions.json run-closed.log profile.log profile-resources.txt profile-cgroup.json preparation-closure.json failure.json screen/queries.raw screen/requests.jsonl screen/truth.u32 screen/truth.i64 screen/panel.json screen/duplicate-audit.json screen/oracle.json screen/resources.json screen/decision.json screen/final-resources.json screen/seal-readback.json screen/failure.json'
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  set +e
  cd "$root"
  { printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\n'; } >/dev/ttyS0 2>/dev/null || true
  if [ "$phase" != complete ] || [ "$original_code" != 0 ]; then
    FAILURE_CODE="$original_code" FAILURE_PHASE="$phase" python3.12 - <<'FAILURE' || code=96
import json,os
from pathlib import Path
p=Path('failure.json'); value=json.loads(p.read_text()) if p.exists() else {}
value.update(schema='borsuk-cohere-preparation-failure-v1',status='failed',replacement_allowed=False,bootstrap_phase=os.environ['FAILURE_PHASE'],bootstrap_exit_code=int(os.environ['FAILURE_CODE']))
p.write_text(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n')
FAILURE
  fi
  cp run.log run-closed.log || code=96
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
  instance_id=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/cohere-fresh64-preparation-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-cohere-semantic-1m-preparation-spot-v1','source_commit':'96b9a4a249df1d9940a160b0e44398abb19bde2e',
  'source_archive_sha256':'d373a252883cdf7f8f3e368993a0492f9d82393e397d5ad191a20a7e91f81d79','config_sha256': '914ccbeb9b6a09e52040003ce86e9075af9a46a2500553c4e2d991f50a163208', 'code_identity_sha256': 'f733ca09a5059ebff0bf349292cfd3454c1d96f0105112657f1803041fd8e96b', 'helper_config_sha256': '21804f730d1044e5798e391949d62abd588efab2a09b5883d258adf4cc175dff', 'refs_identity_sha256': 'f355f0e8a2794c8b5c9122f5936332ecb69e5dfb7e2e701ff2b0a132f9685b27', 'artifact_roster_sha256': 'c8932393787cc8993d20f8dcdcac3fc65031b56662fe9d569808aefc9b585c1e', 'campaign_schema': 'borsuk-cohere-semantic-1m-preparation-spot-v1', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'ann_quality_measured': False, 'native_qualification_claim': False, 'complete_historical_coverage': False,
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/cohere-fresh64-preparation-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
printf '%s\n' '{"schema":"borsuk-cohere-preparation-failure-v1","status":"pending","replacement_allowed":false}' >failure.json
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/d373a252883cdf7f8f3e368993a0492f9d82393e397d5ad191a20a7e91f81d79.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'd373a252883cdf7f8f3e368993a0492f9d82393e397d5ad191a20a7e91f81d79' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
cp "$root/repo/docs/research/performance-architecture-20260930/semantic-1m/cohere-panel-tools/preparation-spot-config.json" config.json
cp "$root/repo/docs/research/performance-architecture-20260930/semantic-1m/cohere-panel-tools/preparation-config.json" helper-config.json
lscpu >cpu.txt
phase=preparation
systemd-run --unit=cohere-fresh64-preparation --wait --pipe -p MemoryMax=2G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=4860 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=VECLIB_MAXIMUM_THREADS=2 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 4800 \
 "$root/venv/bin/python" -m scripts.launch_cohere_semantic_1m_preparation_spot --stage "$root/repo" "$root" research/semantic-router/20261002/cohere-fresh64-preparation-a0001 >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
