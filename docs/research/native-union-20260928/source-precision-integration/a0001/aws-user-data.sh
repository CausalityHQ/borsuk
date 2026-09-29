#!/bin/bash
set -euo pipefail
systemd-run --unit=union-nomination-red-stop --on-active=4200s /usr/sbin/shutdown -h now
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
  for name in test.log test-resources.txt run-closed.log cpu.txt green.log full.log release.log boundary-check.json boundary-cgroup.json binaries/two_bit_plan_demo binaries/two_bit_http binaries/build_two_bit_generation; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-precision-integration-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'green.log', 'full.log', 'release.log', 'boundary-check.json', 'boundary-cgroup.json', 'binaries/two_bit_plan_demo', 'binaries/two_bit_http', 'binaries/build_two_bit_generation'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-source-precision-integration-v1','source_base_commit':'1cb200314172e93ecf40419c59017c2948874ca8',
  'source_archive_sha256':'468190d64f855134bffddee3991e672fde58b477a8e2e7ae4dc119995849cf0a',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-precision-integration-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/468190d64f855134bffddee3991e672fde58b477a8e2e7ae4dc119995849cf0a.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '468190d64f855134bffddee3991e672fde58b477a8e2e7ae4dc119995849cf0a' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=boundary-check
"$CARGO_HOME/bin/rustup" component add rustfmt
lscpu >cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
systemd-run --unit=native-source-precision-integration --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=3930 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 --setenv=TOKIO_WORKER_THREADS=4 --setenv=MALLOC_ARENA_MAX=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3900 \
 taskset -c 0-3 python3 "$root/repo/scripts/check_source_precision_integration.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1

phase=check-artifacts
for name in boundary-check.json boundary-cgroup.json release.log green.log full.log binaries/two_bit_plan_demo binaries/two_bit_http binaries/build_two_bit_generation; do test -s "$root/$name"; done
phase=complete
