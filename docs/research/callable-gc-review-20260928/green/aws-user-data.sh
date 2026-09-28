#!/bin/bash
set -euo pipefail
systemd-run --unit=native-callable-gc-check-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-callable-gc-check
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
  for name in test.log test-resources.txt run-closed.log red-test.log red-exit.txt; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-library-check/gc-review-green/04d9692f0e1bca64547048dc3c3ca3e80949a228e6c23216596078c9a7add768/a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'red-test.log', 'red-exit.txt'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-library-spot-check-v1','source_base_commit':'29e578badbb9d62314b71c7b92aa241e5babcc77',
  'source_archive_sha256':'04d9692f0e1bca64547048dc3c3ca3e80949a228e6c23216596078c9a7add768',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-library-check/gc-review-green/04d9692f0e1bca64547048dc3c3ca3e80949a228e6c23216596078c9a7add768/a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/04d9692f0e1bca64547048dc3c3ca3e80949a228e6c23216596078c9a7add768.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '04d9692f0e1bca64547048dc3c3ca3e80949a228e6c23216596078c9a7add768' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=claim-red-test
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/dd5d8b0bff6c94d3f71828652a40baa5e052197aae4c04ac8af3bc91b8710d6a.tar.gz' source.red.tar.gz --only-show-errors
printf '%s  source.red.tar.gz\n' 'dd5d8b0bff6c94d3f71828652a40baa5e052197aae4c04ac8af3bc91b8710d6a' | sha256sum -c -
rm -rf repo
mkdir repo && tar -xzf source.red.tar.gz -C repo
set +e
timeout --signal=TERM --kill-after=30 300 "$CARGO_HOME/bin/cargo" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --jobs 4 >red-test.log 2>&1
red_code=$?
set -e
printf '%s\n' "$red_code" >red-exit.txt
[ "$red_code" -eq 101 ] || exit 91
grep -q 'missing claim must rebuild ready state' red-test.log || exit 92
rm -rf repo
mkdir repo && tar -xzf source.tar.gz -C repo
"$CARGO_HOME/bin/cargo" clean --manifest-path repo/Cargo.toml -p borsuk
phase=test
export BORSUK_NATIVE_TEST_BUCKET='borsuk-bench-453182569524-euc1' BORSUK_NATIVE_TEST_REGION='eu-central-1' BORSUK_NATIVE_TEST_PREFIX='research/native-library-check/gc-review-green/04d9692f0e1bca64547048dc3c3ca3e80949a228e6c23216596078c9a7add768/a0001/smoke'
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1200 \
  bash -c 'set -e; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --test two_bit_generation --jobs 4; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --lib sq8_s3_range::tests --jobs 4; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids native_s3_conditional_fence_and_gc_smoke --jobs 4 -- --ignored --exact --nocapture; "$1" test --locked --manifest-path repo/Cargo.toml --workspace --all-targets --jobs 4' _ "$CARGO_HOME/bin/cargo" >test.log 2>&1
phase=complete
