#!/bin/bash
set -euo pipefail
systemd-run --unit=native-peer-http-build-stop --on-active=2700s /usr/sbin/shutdown -h now
root=/mnt/native-peer-http-build
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
  for name in test.log test-resources.txt run-closed.log cpu.txt green.log release.log http.fixed.rs boundary-check.json boundary-cgroup.json listener.log binaries/two_bit_http; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/peer-http-build-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'green.log', 'release.log', 'http.fixed.rs', 'boundary-check.json', 'boundary-cgroup.json', 'listener.log', 'binaries/two_bit_http'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-peer-http-build-v1','source_commit':'6c092772f3e7c68cf7c5b7896b12fa8f92f46ab7',
  'source_archive_sha256':'89233c974c82614ec456b64a06c120c0a3ecfdd567bf0ff851ceb2f3249b087c',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260929/peer-http-build-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/89233c974c82614ec456b64a06c120c0a3ecfdd567bf0ff851ceb2f3249b087c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '89233c974c82614ec456b64a06c120c0a3ecfdd567bf0ff851ceb2f3249b087c' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=example-qualification
lscpu >cpu.txt
systemd-run --unit=peer-http-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 taskset -c 0-3 python3 "$root/repo/scripts/check_http_peer_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
cd repo
PATH="$CARGO_HOME/bin:$PATH" python3 scripts/check_http_peer_listener.py >"$root/listener.log" 2>&1
cd "$root"
test -s boundary-check.json && test -s binaries/two_bit_http
phase=complete
