#!/bin/bash
set -euo pipefail
systemd-run --unit=fresh-cohere-confirm936-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/fresh-cohere-confirm936
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
  for name in test.log test-resources.txt run-closed.log cpu.txt environment.txt screen/decision.json screen/cgroup.json screen/native-quality.json screen/requests936.jsonl screen/reference-k10.jsonl screen/reference-k100.jsonl screen/reference-k10.log screen/reference-k10.time screen/reference-k100.log screen/reference-k100.time screen/run0-k10/http.jsonl screen/run0-k10/result.json screen/run0-k10/server.log screen/run0-k10/server.time screen/run0-k10/server-closeout.json screen/run1-k100/http.jsonl screen/run1-k100/result.json screen/run1-k100/server.log screen/run1-k100/server.time screen/run1-k100/server-closeout.json screen/run2-k100/http.jsonl screen/run2-k100/result.json screen/run2-k100/server.log screen/run2-k100/server.time screen/run2-k100/server-closeout.json screen/run3-k10/http.jsonl screen/run3-k10/result.json screen/run3-k10/server.log screen/run3-k10/server.time screen/run3-k10/server-closeout.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/fresh-cohere-confirm936-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'environment.txt', 'screen/decision.json', 'screen/cgroup.json', 'screen/native-quality.json', 'screen/requests936.jsonl', 'screen/reference-k10.jsonl', 'screen/reference-k100.jsonl', 'screen/reference-k10.log', 'screen/reference-k10.time', 'screen/reference-k100.log', 'screen/reference-k100.time', 'screen/run0-k10/http.jsonl', 'screen/run0-k10/result.json', 'screen/run0-k10/server.log', 'screen/run0-k10/server.time', 'screen/run0-k10/server-closeout.json', 'screen/run1-k100/http.jsonl', 'screen/run1-k100/result.json', 'screen/run1-k100/server.log', 'screen/run1-k100/server.time', 'screen/run1-k100/server-closeout.json', 'screen/run2-k100/http.jsonl', 'screen/run2-k100/result.json', 'screen/run2-k100/server.log', 'screen/run2-k100/server.time', 'screen/run2-k100/server-closeout.json', 'screen/run3-k10/http.jsonl', 'screen/run3-k10/result.json', 'screen/run3-k10/server.log', 'screen/run3-k10/server.time', 'screen/run3-k10/server-closeout.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-fresh-cohere-1m-confirm936-spot-v1','source_base_commit':'44b04832873d4ef409cbd057b37fa6397b82287f',
  'source_archive_sha256':'410c3e09b97756a166c059b13dafa7623bc1b42b2ea4465180e1edd3180cae61',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/fresh-cohere-confirm936-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/410c3e09b97756a166c059b13dafa7623bc1b42b2ea4465180e1edd3180cae61.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '410c3e09b97756a166c059b13dafa7623bc1b42b2ea4465180e1edd3180cae61' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,platform; print(numpy.__version__,platform.platform())' >environment.txt
mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/native-reference-panel-a0002/artifacts/binaries/two_bit_plan_demo' binaries/two_bit_plan_demo --only-show-errors
echo 'a2a9ca63d3bb2f6df8ce80d72020361bbba210d593d5c8db7f2401394340c04c  binaries/two_bit_plan_demo' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_plan_demo)" = '11460464' ]
chmod 755 binaries/two_bit_plan_demo
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/four-slot-1m-offered-http-a0001/artifacts/binaries/two_bit_http' binaries/two_bit_http --only-show-errors
echo 'fe7084ce75e56788f156639f534d41fe9fb576ce771bc6f9f5094a1f20264f82  binaries/two_bit_http' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_http)" = '12546280' ]
chmod 755 binaries/two_bit_http
phase=measure
systemd-run --unit=fresh-cohere-confirm936 --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_fresh_rank16_dev64.py" "$2/repo/docs/research/native-union-20260928/fresh-cohere-confirm936-config.json" "$3" "$2/screen" "$2/binaries" "$4"' _ \
 "$root/.venv/bin/python" "$root" 'ccd1af074ee18c1edd3a77edbb5929cea9677c14fd7b669dc0251efa2d773795' 'research/native-union/20260929/fresh-cohere-confirm936-a0001' >test.log 2>&1
test -s "$root/screen/decision.json"
test -s "$root/screen/native-quality.json"
test -s "$root/screen/cgroup.json"
phase=complete
