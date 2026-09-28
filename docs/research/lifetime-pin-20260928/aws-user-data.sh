#!/bin/bash
set -euo pipefail
systemd-run --unit=native-lifetime-pin-check-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-lifetime-pin-check
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-library-check/lifetime-pin/5211a5dad1888171b758ac615bbe7d9d1f7f84bf1f9c7d2ac0db10cfaf8e7b2f/a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-library-spot-check-v1','source_base_commit':'29fa70cd45f45df2d2175df8838285a0cf9e3bae',
  'source_archive_sha256':'5211a5dad1888171b758ac615bbe7d9d1f7f84bf1f9c7d2ac0db10cfaf8e7b2f',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-library-check/lifetime-pin/5211a5dad1888171b758ac615bbe7d9d1f7f84bf1f9c7d2ac0db10cfaf8e7b2f/a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/5211a5dad1888171b758ac615bbe7d9d1f7f84bf1f9c7d2ac0db10cfaf8e7b2f.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '5211a5dad1888171b758ac615bbe7d9d1f7f84bf1f9c7d2ac0db10cfaf8e7b2f' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=red-test
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/9a8ddd3aa631156ecadc854d79bb6722c2c94394efcca8e3a89ef251d875c385.tar.gz' source.red.tar.gz --only-show-errors
printf '%s  source.red.tar.gz\n' '9a8ddd3aa631156ecadc854d79bb6722c2c94394efcca8e3a89ef251d875c385' | sha256sum -c -
rm -rf repo
mkdir repo && tar -xzf source.red.tar.gz -C repo
set +e
timeout --signal=TERM --kill-after=30 300 "$CARGO_HOME/bin/cargo" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --jobs 4 >red-test.log 2>&1
red_code=$?
set -e
printf '%s\n' "$red_code" >red-exit.txt
[ "$red_code" -eq 101 ] || exit 91
grep -Eq 'open_coordinated' red-test.log || exit 92
rm -rf repo
mkdir repo && tar -xzf source.tar.gz -C repo
"$CARGO_HOME/bin/cargo" clean --manifest-path repo/Cargo.toml -p borsuk
phase=test
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1200 \
  bash -c 'set -e; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --test two_bit_generation --jobs 4; "$1" test --locked --manifest-path repo/Cargo.toml -p borsuk --lib sq8_s3_range::tests --jobs 4' _ "$CARGO_HOME/bin/cargo" >test.log 2>&1
phase=complete
