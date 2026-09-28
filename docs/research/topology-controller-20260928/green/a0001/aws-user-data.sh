#!/bin/bash
set -euo pipefail
systemd-run --unit=topology-controller-green-stop --on-active=300s /usr/sbin/shutdown -h now
root=/mnt/topology-controller-green
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
  for name in test.log test-resources.txt run-closed.log cpu.txt; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-library-check/topology-controller/20260928/green-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-topology-controller-green-check-v1','source_base_commit':'346aa524e69001b61751671fcee826af20fb10af',
  'source_archive_sha256':'87d1747663886e3999ed5e0cd9a227d58f73ac30e30e1c68f1adb732a2a4aa01',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-library-check/topology-controller/20260928/green-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/87d1747663886e3999ed5e0cd9a227d58f73ac30e30e1c68f1adb732a2a4aa01.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '87d1747663886e3999ed5e0cd9a227d58f73ac30e30e1c68f1adb732a2a4aa01' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=test
lscpu >cpu.txt
export PYTHONPATH="$root/repo"
set +e
timeout --signal=TERM --kill-after=10 90 python3 - <<'PYTEST'
import json,resource,subprocess,time
from pathlib import Path
started=time.monotonic()
with open('test.log','w') as log:
    result=subprocess.run(['python3','repo/scripts/test_native_two_bit_topology.py'],stdout=log,stderr=subprocess.STDOUT)
usage=resource.getrusage(resource.RUSAGE_CHILDREN)
Path('test-resources.txt').write_text(json.dumps(dict(wall_seconds=time.monotonic()-started,user_seconds=usage.ru_utime,system_seconds=usage.ru_stime,peak_rss_kib=usage.ru_maxrss,exit_code=result.returncode)))
raise SystemExit(result.returncode)
PYTEST
red_code=$?
set -e
[ "$red_code" = 0 ] || exit 91
grep -q 'six topology controller checks passed' test.log || exit 92
phase=complete
