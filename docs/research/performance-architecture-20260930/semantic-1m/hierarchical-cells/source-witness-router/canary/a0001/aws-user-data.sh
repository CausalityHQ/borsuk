#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=480s /usr/sbin/shutdown -h now
root=/mnt/source-witness
mkdir -p "$root" && cd "$root"
phase=bootstrap
export BORSUK_HIERARCHICAL_DEADLINE_EPOCH=$(($(date +%s)+480))
export BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=$(df -B1 --output=used "$root" | tail -1 | tr -d ' ')
export ARTIFACT_NAMES='test-resources.txt run-closed.log screen/config.json screen/source-qualification.json screen/tool-versions.json screen/resources.json screen/worker-cgroup.json screen/native-drain.json screen/cleanup.json screen/summary.json screen/canary.json screen/gate.log'
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  systemctl stop borsuk-global-leaf-canary-a0001.slice 2>/dev/null || true
  if [ -n "${scratch_watch_pid:-}" ]; then
    kill "$scratch_watch_pid" 2>/dev/null || true
    wait "$scratch_watch_pid" 2>/dev/null || true
  fi
  set +e
  cd "$root"
  { printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\n'; } >/dev/ttyS0 2>/dev/null || true
  cp run.log run-closed.log || code=96
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
  instance_id=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
  sync -f "$root" || code=96
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/hierarchical-cells/20261004/source-witness-canary-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-source-witness-fixed-canary-v1','source_commit':'d92099ecc06dbcd378959039da17713ca8c1bab4',
  'source_archive_sha256':'e02c1ec57050e2f7ac7eb1ac8f9e0b80947ca9d71500869118ebca4c0f2c7eb9','config_sha256': 'fc66d5fd2a4401b6bb1fd4b7438ad28bb006feb2b25f69737e9b066c767962a2', 'code_identity_sha256': '295651b699734c4e34717ba827c6959c4a1e9739de3d0c8ff49c780933ad8f7d', 'refs_identity_sha256': '5cc7cc1f09da6efbc6cd344bfd9f50165a69f9642d253dbca074e975b25a8152', 'native_identity_sha256': '2e8145e3639b3c4bf93f363dca110ff16f374bee4057e8426738fc02dc51941f', 'source_file_count': 402, 'source_archive_paths_sha256': '1191ad81e0ab4cd8c39b056b0fe44c108ec49b103d2be0a0a5efe5683045632c', 'artifact_roster_sha256': '0fc5269b98d1815efb0845e16add4407a5ea500fa5e167cc7772b4c594f9db68', 'campaign_schema': 'borsuk-source-witness-fixed-canary-v1', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && sync -f terminal.json && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/hierarchical-cells/20261004/source-witness-canary-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
scratch_owner=$$
(while kill -0 "$scratch_owner" 2>/dev/null; do
 rooted=$(du -sb "$root" | cut -f1)
 used=$(df -B1 --output=used "$root" | tail -1 | tr -d ' ')
 growth=$((used-BORSUK_HIERARCHICAL_SCRATCH_BASE_USED))
 if [ "$growth" -lt 0 ]; then growth=0; fi
 if [ "$((rooted+growth))" -gt 4294967296 ]; then kill -TERM "$scratch_owner"; exit; fi
 sleep 1
done) &
scratch_watch_pid=$!
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
timeout --kill-after=30 120 ./aws/install --install-dir "$root/aws-cli" --bin-dir "$root/bin"
export PATH="$root/bin:$PATH"
cli_version=$(aws --version)
[[ "$cli_version" == aws-cli/2.36.11\ * ]]
rm -rf -- "$root/aws" "$root/awscliv2.zip"
phase=source-download
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/e02c1ec57050e2f7ac7eb1ac8f9e0b80947ca9d71500869118ebca4c0f2c7eb9.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'e02c1ec57050e2f7ac7eb1ac8f9e0b80947ca9d71500869118ebca4c0f2c7eb9' | sha256sum -c -
mkdir probe-repo && tar -xzf source.tar.gz -C probe-repo
rm -f -- "$root/source.tar.gz"
sync -f "$root"
phase=install
test "$(uname -m)" = x86_64
python3.12 -m venv "$root/venv"
export PIP_CACHE_DIR="$root/pip-cache" TMPDIR="$root"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0 boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
rm -rf -- "$root/pip-cache"
sync -f "$root"
phase=infrastructure-canary
remaining=$((BORSUK_HIERARCHICAL_DEADLINE_EPOCH-$(date +%s)))
test "$remaining" -gt 0
systemctl start borsuk-global-leaf-canary-a0001.slice
systemctl set-property --runtime borsuk-global-leaf-canary-a0001.slice MemoryMax=256M MemorySwapMax=0 CPUQuota=100% TasksMax=512
systemd-run --slice=borsuk-global-leaf-canary-a0001.slice --unit=global-leaf-canary-a0001 --wait --pipe -p MemoryMax=256M -p MemorySwapMax=0 -p CPUQuota=100% -p TasksMax=512 -p RuntimeMaxSec="$remaining" -p WorkingDirectory="$root" \
 --setenv=BORSUK_GLOBAL_LEAF_SLICE=borsuk-global-leaf-canary-a0001.slice --setenv=PYTHONPATH="$root/probe-repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=TMPDIR="$root" \
 --setenv=BORSUK_HIERARCHICAL_CONFIG_SHA256=fc66d5fd2a4401b6bb1fd4b7438ad28bb006feb2b25f69737e9b066c767962a2 \
 --setenv=BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256=1191ad81e0ab4cd8c39b056b0fe44c108ec49b103d2be0a0a5efe5683045632c \
 --setenv=BORSUK_HIERARCHICAL_DEADLINE_EPOCH="$BORSUK_HIERARCHICAL_DEADLINE_EPOCH" \
 --setenv=BORSUK_HIERARCHICAL_SCRATCH_BASE_USED="$BORSUK_HIERARCHICAL_SCRATCH_BASE_USED" \
 --setenv=OPENBLAS_NUM_THREADS=1 --setenv=OMP_NUM_THREADS=1 --setenv=MKL_NUM_THREADS=1 --setenv=RAYON_NUM_THREADS=1 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=5 "$remaining" \
 taskset -c 0 "$root/venv/bin/python" "$root/probe-repo/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/campaign.py" --stage-canary "$root/probe-repo" "$root/screen" "$root"
for name in $ARTIFACT_NAMES; do
 case "$name" in run-closed.log) test -s run.log;; screen/runner.stdout|screen/runner.stderr) test -f "$name";; screen/bootstrap-closeout.json) :;; *) test -s "$name";; esac
done
phase=complete
