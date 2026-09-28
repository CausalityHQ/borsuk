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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-library-check/v26-platform-assurance/cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685/a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-library-spot-check-v1','source_base_commit':'1643c471f43a192a62481a671263105561442fac',
  'source_archive_sha256':'cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-library-check/v26-platform-assurance/cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685/a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=test
export BORSUK_NATIVE_TEST_BUCKET='borsuk-bench-453182569524-euc1' BORSUK_NATIVE_TEST_REGION='eu-central-1' BORSUK_NATIVE_TEST_PREFIX='research/native-library-check/gc/cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685/review-a0003/smoke'
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1500 \
  bash -c 'set -e; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk-v26 --lib --jobs 4; "$1" test --locked --manifest-path repo/Cargo.toml --workspace --all-targets --jobs 4' _ "$CARGO_HOME/bin/cargo" >test.log 2>&1
phase=complete
