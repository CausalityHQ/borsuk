#!/bin/bash
set -euo pipefail
systemd-run --unit=union-nomination-red-stop --on-active=1500s /usr/sbin/shutdown -h now
root=/mnt/union-nomination-red
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/red-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-union-red-v1','source_base_commit':'897cb2587c744b6aa348fcc46841907c796e13db',
  'source_archive_sha256':'165030ab68c92d870ce9444e0135cf4cfe2587d82e64c0d7afa8510e9f2c8b7a',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/red-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/165030ab68c92d870ce9444e0135cf4cfe2587d82e64c0d7afa8510e9f2c8b7a.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '165030ab68c92d870ce9444e0135cf4cfe2587d82e64c0d7afa8510e9f2c8b7a' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=test
lscpu >cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
set +e
systemd-run --unit=native-union-red --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=1230 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1200 \
 "$CARGO_HOME/bin/cargo" test --locked --manifest-path "$root/repo/Cargo.toml" --target-dir "$root/target" -p borsuk --lib --jobs 4 \
 two_bit_build::tests::union_generation_has_two_authenticated_graphs -- --exact --nocapture >test.log 2>&1
code=$?
set -e
[ "$code" = 101 ]
grep -q 'two-graph root format missing' test.log
grep -q 'borsuk-two-bit-generation-v3' test.log
phase=complete
