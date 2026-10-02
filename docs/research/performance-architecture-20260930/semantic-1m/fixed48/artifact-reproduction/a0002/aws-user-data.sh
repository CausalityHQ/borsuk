#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/cohere-artifact-reproduction
mkdir -p "$root" && cd "$root"
phase=bootstrap
BORSUK_OUTPUT=s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-artifact-reproduction-a0002
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
        systemd-run --quiet --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=135 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 120 aws s3 cp "$root/$name" "$BORSUK_OUTPUT/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-cohere-semantic-artifact-reproduction-spot-v1','source_commit':'7b8b3a3d4930aba6195af23ac69929545631d30a',
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
python3 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/22T3W7bMAyF3yXXS6MfipL2MgFJkY23JQ5sp9sw7N3HdFhaYL3whUXhI8/h0a8dLdtkJNtxmddNl+N6olRw93nXJAiPOor4lzAFNAwCrahojQ0yVESBAI25+gEbjCCjkbTayqi7Tzv6vsq36Q1ZgvldSgZAtSkxUIohtBYgaLPAAtx7ruKMAFlSLpCwj9IwwjB8Q77osk7zxZnpKeNTjF7i2/RtuAJa19tCF9G3xtEsl6Rh1Np6iZBraEORSihqUWFIjqU2JEKzEMWsjMLiojgSKb6j83Sh5ec7TRBqtKZ1kNVQWieISGl0bgyqppCgeCdEplGxko9QE5ciqCjZHC10vtL0fDmuctIzOZTnZb193ct80kX3qx9etkn2/3a1X/S6zOMmm3uwX6/ztn+5OyDz0OM01C9v70ZMUWpLEgfmbhC7FWTMo3vVtatLTp1Mc1MxSAUYowFyT8OsidEr+WLT8/FK28mBY5b1sOiqtMjpcNXF5uV8d3x/P5g2le3mc6eQMPQcDg8B8Xyw6YcOaIcPtRz+9nn6svpuH10fQghT9eQEH3qUwk1ykyiFfHWdzTyw6KsdXKATc2qxdBUK0a8WSlUcuaitH1hkPbIHr3MfqYZUS/ClhRYQS8qevBqZAKymUSj30amCupUeKjbJRZ28zrfFM/fqwIseZT6fp83R1YOQKQ9wJ4gJYy9kKZNg76kX75Pj8NL/iMd0yjYYhe6PESvSkF7uDxJsNLfEY8vCQhoZmBnAx0VuucWk/iNl9/sPfwazLegDAAA=",validate=True)))'
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/ebfdb6ca7d5c676adc9562064fd86276aabcbcae1b4bbb446716b83812e446c5.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'ebfdb6ca7d5c676adc9562064fd86276aabcbcae1b4bbb446716b83812e446c5' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 - <<'CONFIG'
import base64,gzip,hashlib,json
from pathlib import Path
body=gzip.decompress(base64.b64decode('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',validate=True))
assert hashlib.sha256(body).hexdigest()=='a6274a70e4ed55b8c38c1c5a5789bffc0c6437db549abb28159eca015b85a27c'
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
 "$root/venv/bin/python" -m scripts.launch_cohere_semantic_artifact_reproduction_spot --stage "$root/repo" "$root" research/semantic-router/20261002/fixed48-artifact-reproduction-a0002 >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 case "$name" in screen/failure.json|screen/failure-resources.json) continue;; esac
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
