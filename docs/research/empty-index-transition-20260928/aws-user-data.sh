#!/bin/bash
set -euo pipefail
systemd-run --unit=native-empty-index-check-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-empty-index-check
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-library-check/empty-index/b8a0c51f7e80d2894330b8721fa44b111fedd2ac6c15a5ec251434436fefc367/a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-library-spot-check-v1','source_base_commit':'3bc23eda9cd6425835bec28458154c98ef9c9e11',
  'source_archive_sha256':'b8a0c51f7e80d2894330b8721fa44b111fedd2ac6c15a5ec251434436fefc367',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-library-check/empty-index/b8a0c51f7e80d2894330b8721fa44b111fedd2ac6c15a5ec251434436fefc367/a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/b8a0c51f7e80d2894330b8721fa44b111fedd2ac6c15a5ec251434436fefc367.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'b8a0c51f7e80d2894330b8721fa44b111fedd2ac6c15a5ec251434436fefc367' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=red-test
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/e822ea13108264ced144b1fb75ca8858211004ea0bc1b8539d6b8a41a1b04da3.tar.gz' source.red.tar.gz --only-show-errors
printf '%s  source.red.tar.gz\n' 'e822ea13108264ced144b1fb75ca8858211004ea0bc1b8539d6b8a41a1b04da3' | sha256sum -c -
rm -rf repo
mkdir repo && tar -xzf source.red.tar.gz -C repo
set +e
timeout --signal=TERM --kill-after=30 300 "$CARGO_HOME/bin/cargo" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --jobs 4 >red-test.log 2>&1
red_code=$?
set -e
printf '%s\n' "$red_code" >red-exit.txt
[ "$red_code" -eq 101 ] || exit 91
grep -Eq 'two_bit_index|publish_empty_two_bit_generation' red-test.log || exit 92
rm -rf repo
mkdir repo && tar -xzf source.tar.gz -C repo
"$CARGO_HOME/bin/cargo" clean --manifest-path repo/Cargo.toml -p borsuk
phase=test
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1200 \
  bash -c 'set -e; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --test two_bit_generation --jobs 4; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --lib sq8_s3_range::tests --jobs 4' _ "$CARGO_HOME/bin/cargo" >test.log 2>&1
phase=complete
