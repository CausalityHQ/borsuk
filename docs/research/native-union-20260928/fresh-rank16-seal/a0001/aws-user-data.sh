#!/bin/bash
set -euo pipefail
systemd-run --unit=rank16-fresh-1m-seal-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/rank16-fresh-1m-seal
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
  for name in test.log test-resources.txt run-closed.log cpu.txt environment.txt screen/decision.json screen/cgroup.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/fresh-rank16-seal-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'environment.txt', 'screen/decision.json', 'screen/cgroup.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-v36-rank16-fresh-1m-seal-spot-v1','source_base_commit':'165aa6d0d795d77bc5363e1752ed538390ba094a',
  'source_archive_sha256':'2500a6c97a219c4e91576f32f77079268184946eb901ef38f8da2b81a5cb5afc',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/fresh-rank16-seal-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/2500a6c97a219c4e91576f32f77079268184946eb901ef38f8da2b81a5cb5afc.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '2500a6c97a219c4e91576f32f77079268184946eb901ef38f8da2b81a5cb5afc' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
phase=construct
systemd-run --unit=rank16-seal --wait --pipe -p MemoryMax=24G -p MemorySwapMax=0 -p RuntimeMaxSec=3300 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=OPENBLAS_NUM_THREADS=8 --setenv=OMP_NUM_THREADS=8 --setenv=MKL_NUM_THREADS=8 --setenv=AWS_MAX_ATTEMPTS=1 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3000 \
 bash -c 'set -e; ulimit -v 29360128; exec taskset -c 0-7 "$1" "$2/repo/scripts/seal_v36_rank16_fresh_1m.py" "$2/repo/docs/research/native-union-20260928/fresh-rank16-seal-config.json" "$3" "$2/repo" "$2/screen" "$4"' _ \
 "$root/.venv/bin/python" "$root" '7885240547ca7791c2c7aca4d6905cafd8af8e651ee3c1c5925290c4bbb0ff16' 'research/native-union/20260928/fresh-rank16-seal-a0001' >test.log 2>&1
test -s "$root/screen/decision.json"
test -s "$root/screen/cgroup.json"
phase=complete
