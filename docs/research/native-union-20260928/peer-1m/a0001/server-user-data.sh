#!/bin/bash
set -euo pipefail
systemd-run --unit=native-peer-1m-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-peer-1m
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
  for name in test.log test-resources.txt run-closed.log screen/summary.json cpu.txt environment.txt binaries/two_bit_http screen/ready0.json screen/cell0-server.log screen/cell0-server.time screen/close0.json screen/closed0.json screen/ready1.json screen/cell1-server.log screen/cell1-server.time screen/close1.json screen/closed1.json screen/ready2.json screen/cell2-server.log screen/cell2-server.time screen/close2.json screen/closed2.json screen/ready3.json screen/cell3-server.log screen/cell3-server.time screen/close3.json screen/closed3.json screen/ready4.json screen/cell4-server.log screen/cell4-server.time screen/close4.json screen/closed4.json screen/ready5.json screen/cell5-server.log screen/cell5-server.time screen/close5.json screen/closed5.json screen/ready6.json screen/cell6-server.log screen/cell6-server.time screen/close6.json screen/closed6.json screen/ready7.json screen/cell7-server.log screen/cell7-server.time screen/close7.json screen/closed7.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/peer-1m-a0001/server/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'screen/summary.json', 'cpu.txt', 'environment.txt', 'binaries/two_bit_http', 'screen/ready0.json', 'screen/cell0-server.log', 'screen/cell0-server.time', 'screen/close0.json', 'screen/closed0.json', 'screen/ready1.json', 'screen/cell1-server.log', 'screen/cell1-server.time', 'screen/close1.json', 'screen/closed1.json', 'screen/ready2.json', 'screen/cell2-server.log', 'screen/cell2-server.time', 'screen/close2.json', 'screen/closed2.json', 'screen/ready3.json', 'screen/cell3-server.log', 'screen/cell3-server.time', 'screen/close3.json', 'screen/closed3.json', 'screen/ready4.json', 'screen/cell4-server.log', 'screen/cell4-server.time', 'screen/close4.json', 'screen/closed4.json', 'screen/ready5.json', 'screen/cell5-server.log', 'screen/cell5-server.time', 'screen/close5.json', 'screen/closed5.json', 'screen/ready6.json', 'screen/cell6-server.log', 'screen/cell6-server.time', 'screen/close6.json', 'screen/closed6.json', 'screen/ready7.json', 'screen/cell7-server.log', 'screen/cell7-server.time', 'screen/close7.json', 'screen/closed7.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-peer-1m-role-v1','source_commit':'1e735a0e9e2fa5b58b3949a830b0b102984a21f4',
  'source_archive_sha256':'59f503f648be4bd3bd3c4bb13c570927fc764f465bb29e62125dbfd3ba8eb025',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/peer-1m-a0001/server/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/59f503f648be4bd3bd3c4bb13c570927fc764f465bb29e62125dbfd3ba8eb025.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '59f503f648be4bd3bd3c4bb13c570927fc764f465bb29e62125dbfd3ba8eb025' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 util-linux time
lscpu >cpu.txt
python3.12 -c 'import platform; print(platform.platform())' >environment.txt
mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260929/peer-http-build-a0001/artifacts/binaries/two_bit_http' binaries/two_bit_http --only-show-errors
echo '75cbcff863e60625ca2a728eebdc48361f03153ec016a22042f6e22bcd6f7925  binaries/two_bit_http' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_http)" = '12475240' ]
chmod 755 binaries/two_bit_http
phase=measure
systemd-run --unit=native-peer-server --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1430 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1400 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 python3.12 scripts/run_native_peer_1m_worker.py server docs/research/native-union-20260928/peer-1m-config.json cd32e35343deff1b5006b83aa1be558a1821873c095856cce85fcde221d691aa research/native-union/20260929/peer-1m-a0001 "$1/screen" "$1/binaries/two_bit_http"' _ "$root" >test.log 2>&1
test -s "$root/screen/summary.json"
phase=complete
