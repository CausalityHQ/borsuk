#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/cohere-artifact-reproduction
mkdir -p "$root" && cd "$root"
phase=bootstrap
BORSUK_OUTPUT=s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-artifact-reproduction-a0001
export ARTIFACT_NAMES="$(printf '%s ' config.json source-qualification.json archived-builder-assurance.json runtime-abi.json cpu.txt tool-versions.json run-closed.log profile.log profile-resources.txt helper-process.log profile-cgroup.json reproduction-closure.json failure.json screen/{source.raw,source-order.u64,source-root.json,source-sq8.bin,builder,generation/{manifest.json,canonical.bin,centroids.bin,router/root.bin,router/membership.bin,router/leaves.bin,plane/manifest.json,plane/mean.bin,plane/records.bin,plane/page_digests.bin,page_manifest.json,page_digests.bin},config.json,source-qualification.json,input-hashes.json,local-sq8-head.json,sq8-ordinal-check.json,builder-config.json,build.log,build-resources.txt,build-resources.json,payload-verification.json,provenance.json,cleanup.json,resources.json,failure.json,failure-resources.json,COMPLETE.json})"
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  set +e
  cd "$root"
  { printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\n'; } >/dev/ttyS0 2>/dev/null || true
  if [ "$phase" != complete ] || [ "$original_code" != 0 ]; then
    FAILURE_CODE="$original_code" FAILURE_PHASE="$phase" python3 - <<'FAILURE' || code=96
import json,os
from pathlib import Path
p=Path('failure.json'); value=json.loads(p.read_text()) if p.exists() else {}
value.update(status='failed',replacement_allowed=False,bootstrap_phase=os.environ['FAILURE_PHASE'],bootstrap_exit_code=int(os.environ['FAILURE_CODE']))
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
      if [ -f "$name" ] && { [ "$name" != screen/COMPLETE.json ] || [ "$code" = 0 ]; }; then
        systemd-run --quiet --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=135 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 120 aws s3 cp "$name" "$BORSUK_OUTPUT/artifacts/$name" --only-show-errors || code=96
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
    if path.is_file() and (name != "screen/COMPLETE.json" or os.environ["EXIT_CODE"] == "0"):
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-cohere-semantic-artifact-reproduction-spot-v1','source_commit':'98ec24a45f4509a13b9d294464b77b9fd972cdb7',
  **json.loads(Path('source-qualification.json').read_text()),
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
    timeout --kill-after=5 60 aws s3 cp terminal.json "$BORSUK_OUTPUT/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
python3 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/22T2XLbMAxF/8XPdQyS4Naf8YBYYrW15ZHktJ1O/71wOo0z0zzoQVwOcC8uf+1o2SYj3o7LvG66HNcTxVx2n3eNgYdUyexfLBGKFWBsWVlraJiwlsII2MaovjAMBVgacastS9192tH3lb9ND2QG87MUDZFqUxpIMQC0BgjaDAbj6D1VdgZg4pgyxtIltxJQrDyQL7qs03xxZnxK5SkE3xq36Zu4AlrX20IX1kfhYJZyVJBaW88BU4UmWihDVguKwink2gpRMYPAZlnyYBc1ApGWd/QxXWj5+U4TQg3WtApZhdw6YSgUpY82UNUUI2avVMogqaWSt1DjyJmLFk7maKbzlabny3Hlk57JoWNe1tvXPc8nXXS/+uJlm3j/b1b7Ra/LLDfe3IP9ep23/cvdAZ5Fj5OoH97etQghC1NSa8UodW+0eFMMYBCVCTUE6gkgp16LkKsfo0MfOqhGEXolX2x6Pl5pOzlQZl4Pi65KC58OV11sXs53x/f3hWlT3m7ed4RYwMGHNwHhfLDphwq2w4daDn/rPH1ZfbZvVd+ECHf2sVhXgRhogBZJZlJi7KTsCfHwpFCjthCimQtomRIU6sgB0ZGL2vqBRdbD8Lt9dIkVYs3gQ4MGpeSYnFvDIERzO/LdwU4V1TB4qIZxyurkdb4tnrlXB170yPP5PG2O7k05ImE2zNAppHuNjljQn83oJr1GllH/RzwyFrJfG24XQOjiWe3dCgGzEnn+MtyTG9CGdX8/Ykb37mPw/1FG2/3+A6N+/E3oAwAA",validate=True)))'
printf '%s\n' '{"status":"pending","replacement_allowed":false}' >failure.json
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1
phase=apt-update
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 python3.12-venv time tar gzip util-linux binutils
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/51513bbfac0019d31599f6a0cceaa407506aa614fbf9993dffa560821bf9b6b8.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '51513bbfac0019d31599f6a0cceaa407506aa614fbf9993dffa560821bf9b6b8' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 - <<'CONFIG'
import base64,gzip,hashlib,json
from pathlib import Path
body=gzip.decompress(base64.b64decode('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',validate=True))
assert hashlib.sha256(body).hexdigest()=='dc9c5bcf9ed021ab0e6d3ffd6229aecdf69373172e8112ffeba85a306a94c144'
Path('config.json').write_bytes(body)
CONFIG
python3.12 -m venv --system-site-packages "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
aws configure set default.s3.max_concurrent_requests 2
aws configure set default.s3.multipart_chunksize 64MB
lscpu >cpu.txt
phase=reproduction
systemd-run --unit=cohere-artifact-reproduction --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=1860 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=VECLIB_MAXIMUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1800 \
 "$root/venv/bin/python" -m scripts.launch_cohere_semantic_artifact_reproduction_spot --stage "$root/repo" "$root" research/semantic-router/20261002/fixed48-artifact-reproduction-a0001 >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 case "$name" in screen/failure.json|screen/failure-resources.json) continue;; esac
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
