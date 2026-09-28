#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cold-red-stop --on-active=1200s /usr/sbin/shutdown -h now
root=/mnt/native-cold-red
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/red-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-live-split-red-check-v1','source_base_commit':'73569e89b3d53f75ae022714cf80aecd99a7a058',
  'source_archive_sha256':'471c7b32ed8dcc9cbc6fda1247b420d98efd14c4ca1ca7710091b7303fb6b833',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/red-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/471c7b32ed8dcc9cbc6fda1247b420d98efd14c4ca1ca7710091b7303fb6b833.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '471c7b32ed8dcc9cbc6fda1247b420d98efd14c4ca1ca7710091b7303fb6b833' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=test
lscpu >cpu.txt
set +e
systemd-run --unit=native-cold-red-check --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=930 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 900 \
 "$CARGO_HOME/bin/cargo" test --release --locked --manifest-path "$root/repo/Cargo.toml" --target-dir "$root/target" -p borsuk --bin two_bit_plan_demo live_scope_is_development_only --jobs 4 >test.log 2>&1
red_code=$?
set -e
[ "$red_code" = 101 ] || exit 91
grep -q 'live split not implemented' test.log || exit 92
phase=complete
