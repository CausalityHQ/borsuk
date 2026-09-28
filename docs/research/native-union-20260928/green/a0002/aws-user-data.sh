#!/bin/bash
set -euo pipefail
systemd-run --unit=union-nomination-red-stop --on-active=3600s /usr/sbin/shutdown -h now
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
  for name in test.log test-resources.txt run-closed.log cpu.txt focused.log full.log release.log binaries/build_two_bit_generation binaries/two_bit_plan_demo; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/green-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'focused.log', 'full.log', 'release.log', 'binaries/build_two_bit_generation', 'binaries/two_bit_plan_demo'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-union-green-v1','source_base_commit':'897cb2587c744b6aa348fcc46841907c796e13db',
  'source_archive_sha256':'2b5732b3e420bb846282097a7e5d853b20b4c7c636ad22dd205c5458008dbbad',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/green-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/2b5732b3e420bb846282097a7e5d853b20b4c7c636ad22dd205c5458008dbbad.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '2b5732b3e420bb846282097a7e5d853b20b4c7c636ad22dd205c5458008dbbad' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=focused
lscpu >cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
export TOKIO_WORKER_THREADS=4 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MALLOC_ARENA_MAX=2
systemd-run --unit=native-union-assurance --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=3330 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 --setenv=TOKIO_WORKER_THREADS=4 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MALLOC_ARENA_MAX=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3300 \
 bash -c 'set -e
 "$1" test --locked --manifest-path "$2/repo/Cargo.toml" --target-dir "$2/target" -p borsuk --jobs 4 --test two_bit_generation --test two_bit_application_ids --test two_bit_gc_delayed_delete >>"$2/focused.log" 2>&1
 "$1" test --locked --manifest-path "$2/repo/Cargo.toml" --target-dir "$2/target" --workspace --all-targets --jobs 4 >"$2/full.log" 2>&1
 "$1" build --release --locked --manifest-path "$2/repo/Cargo.toml" --target-dir "$2/target" -p borsuk --jobs 4 --bin build_two_bit_generation --bin two_bit_plan_demo >"$2/release.log" 2>&1
 mkdir "$2/binaries";cp "$2/target/release/build_two_bit_generation" "$2/target/release/two_bit_plan_demo" "$2/binaries/"
 ' _ "$CARGO_HOME/bin/cargo" "$root" >test.log 2>&1
phase=complete
