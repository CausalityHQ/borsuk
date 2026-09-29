#!/bin/bash
set -euo pipefail
systemd-run --unit=native-http-offered-protocol-stop --on-active=600s /usr/sbin/shutdown -h now
root=/mnt/native-http-offered-protocol
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
  for name in test.log test-resources.txt run-closed.log protocol-check.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/http-offered-protocol-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'protocol-check.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-http-offered-protocol-v1','source_base_commit':'83c72d1f4d1ca9be0fb80b81be79c6279e188355',
  'source_archive_sha256':'aaa91c3abad65ce423c9441c9f9d37e4640364baf81d4a70b0e18fe9bcb14c8e',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/http-offered-protocol-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/aaa91c3abad65ce423c9441c9f9d37e4640364baf81d4a70b0e18fe9bcb14c8e.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'aaa91c3abad65ce423c9441c9f9d37e4640364baf81d4a70b0e18fe9bcb14c8e' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q time
phase=protocol-check
systemd-run --unit=native-http-offered-protocol --wait --pipe -p MemoryMax=512M -p MemorySwapMax=0 -p RuntimeMaxSec=210 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=10 180 \
 taskset -c 0-1 python3 -m scripts.check_native_union_offered_http "$root/protocol-check.json" >test.log 2>&1
phase=check-artifacts
test -s "$root/protocol-check.json"
phase=complete
