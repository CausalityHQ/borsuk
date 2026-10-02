#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3000s /usr/sbin/shutdown -h now
root=/mnt/cohere-top32-coverage
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='config.json helper-config.json source-qualification.json archived-builder-assurance.json cpu.txt tool-versions.json run-closed.log profile.log profile-resources.txt profile-cgroup.json coverage-closure.json failure.json screen/queries.raw screen/requests.jsonl screen/panel.json screen/duplicate-audit.json screen/source-qualification.json screen/sq8-ordinal-check.json screen/builder-config.json screen/build.log screen/build-resources.txt screen/build-resources.json screen/generation-manifest.json screen/nominate-config.json screen/nomination.json screen/nomination-seal.json screen/truth.u32 screen/truth.i64 screen/oracle.json screen/reduce-config.json screen/coverage.json screen/resources.json screen/source-order.u64 screen/source-root.json screen/prospective-protocol.json screen/decision.json screen/final-resources.json screen/seal-readback.json'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/cohere-top32-coverage-a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-cohere-top32-coverage-spot-v1','source_commit':'2d178890da6aec550cbbc5f81ff2737c5bd68fc4',
  'source_archive_sha256':'f35b08806829df80c249dc52858154b3e67da4724cc40a1023601698f5510fd0','config_sha256': '1519dd8b662470231d67cbd28f189ffbce5b261dc4d81d1df2df7bc6634111e0', 'code_identity_sha256': '23f4fc1da751b79f2c508986aa0f86a19596e38c388b976e2966b7f7538d15e5', 'helper_config_sha256': '7e4dc4aec01e127fb9a25d2095b870678b7e61589c08931b3d6b0f91d7011874', 'refs_identity_sha256': 'a0dc76b0123ac5b66df2aea90aeea46035ab965bebdc2299c0e882f4a20d5dbb', 'artifact_roster_sha256': 'c7b439dfce99bdf33ca5fd7ad77f183eee243f8dec509469fe8fa18c91664ddb', 'builder_assurance_sha256': '1ff352e0d77895143708de6a505ef1e4dc315786aa6ff01cff5d5bc18bb1aae6', 'original_source_identity_sha256': 'c9ecb3ac7138978b45efde523c8e96a88811d559e50906c178fd4cb602fb9ff5', 'archived_quality_source_identity_sha256': '295a79de9a499cc388db14b4b78ac9fcd4f1eb8673ceb5c1222dfc7f119e9ae4', 'builder_binary_sha256': '54071f8e7daf70589a416a2d9b8b4eefe42456a566bad767a77872b55c6e6c3f', 'campaign_schema': 'borsuk-cohere-top32-coverage-spot-v1', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'coverage_only': True, 'old_fail_preserved': True, 'returned_recall_measured': False, 'cold_http_measured': False, 'physical_s3_query_gets_measured': False, 'native_qualification_claim': False, 'current_whole_tree_full_execution': False, 'complete_historical_coverage': False,
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/cohere-top32-coverage-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/f35b08806829df80c249dc52858154b3e67da4724cc40a1023601698f5510fd0.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'f35b08806829df80c249dc52858154b3e67da4724cc40a1023601698f5510fd0' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
phase=coverage
systemd-run --unit=cohere-top32-coverage --wait --pipe -p MemoryMax=8589934592 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=1860 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=VECLIB_MAXIMUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 --setenv=BORSUK_COVERAGE_SOURCE_COMMIT=2d178890da6aec550cbbc5f81ff2737c5bd68fc4 --setenv=BORSUK_COVERAGE_ARCHIVE_SHA256=f35b08806829df80c249dc52858154b3e67da4724cc40a1023601698f5510fd0 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1800 \
 "$root/venv/bin/python" -m scripts.launch_cohere_top32_coverage_spot --stage "$root/repo" "$root" research/semantic-router/20261002/cohere-top32-coverage-a0002 >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
