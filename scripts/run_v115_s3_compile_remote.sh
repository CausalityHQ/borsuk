#!/bin/bash
# One immutable Spot compile/test cell for a working-tree source snapshot.
set -euo pipefail
root=/mnt/v115-s3-compile
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" \
    http://169.254.169.254/latest/meta-data/instance-id || true)
  upload_failed=0
  for path in install.log test.log test-resources.txt worker.log; do
    if [ -f "$path" ] && ! aws s3 cp "$path" \
        "$V115_BUILD_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
files={}
for name in ('install.log','test.log','test-resources.txt','worker.log'):
    path=Path(name)
    if path.is_file():
        files[name]={'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v115-s3-compile-spot-v1',
    'source_sha256':os.environ['V115_BUILD_SOURCE_SHA256'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':files},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V115_BUILD_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
phase=test
timeout --signal=TERM --kill-after=30 3300 /usr/bin/time -v \
  "$CARGO_HOME/bin/cargo" test --manifest-path repo/Cargo.toml \
  -p borsuk --lib sq8_s3_range::tests \
  --jobs 4 -- --nocapture >test.log 2>test-resources.txt
phase=complete
exit 0
