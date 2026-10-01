#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/native-semantic-fresh-panel
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json config.json construction-config.json input-hashes.json remote-replay.json construction-cgroup.json tool-versions.json test.log test-resources.txt run-closed.log screen/queries.raw screen/requests.jsonl screen/truth.u32 screen/truth.i64 screen/panel.json screen/duplicate-audit.json screen/oracle.json screen/resources.json screen/decision.json screen/seal-readback.json'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/fresh-panel-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-semantic-fresh-panel-spot-v1','source_commit':'88c25ab72026722e401eab4a1fde91125377b2a8',
  'source_archive_sha256':'f928ac4a27a8c5dace84dc41782ae8aa44629cad7615c36c904e6b41a40fcd50','config_sha256': '453fadf2ee6aec8a834d0c95c54f9cd85d2ca4e12a7bdc62d66ce760de75737f', 'code_identity_sha256': '58f8a9200b8f17c715f15cfac570f7ec6013a1bc3bc4e347577852f866bfb41c', 'construction_config_sha256': '6d0d4e0bd350ecea929db7c478116703f6f52c0eb30a9cc48f1056e7dec0f199', 'construction_code_identity_sha256': '82848543280aa97f2e8f95a9ee1760e3177108b2c03086ea9056799918c79d7f', 'refs_identity_sha256': 'eb2aab2a001b57643f492bfe350ee7516425cf7ba06f148c1bc290a7c62ab6f7', 'artifact_roster_sha256': '4780fa18162c2db0e16b13186fbacd626b87021c919e8d322952826eace89291', 'campaign_schema': 'borsuk-semantic-fresh-panel-spot-v1', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),'remote_replay_sha256':artifacts.get('remote-replay.json',{}).get('sha256'),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261001/fresh-panel-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/f928ac4a27a8c5dace84dc41782ae8aa44629cad7615c36c904e6b41a40fcd50.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'f928ac4a27a8c5dace84dc41782ae8aa44629cad7615c36c904e6b41a40fcd50' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
phase=construction
systemd-run --unit=semantic-fresh-panel --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_PANEL_CONFIG_SHA256=453fadf2ee6aec8a834d0c95c54f9cd85d2ca4e12a7bdc62d66ce760de75737f \
 --setenv=BORSUK_PANEL_SOURCE_COMMIT=88c25ab72026722e401eab4a1fde91125377b2a8 --setenv=BORSUK_PANEL_ARCHIVE_SHA256=f928ac4a27a8c5dace84dc41782ae8aa44629cad7615c36c904e6b41a40fcd50 \
 --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 7200 \
 "$root/venv/bin/python" -m scripts.launch_native_semantic_fresh_panel_spot --stage "$root/repo" "$root" research/semantic-router/20261001/fresh-panel-a0001
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
