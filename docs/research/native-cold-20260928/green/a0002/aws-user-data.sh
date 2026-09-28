#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cold-baseline-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-cold-baseline
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
  for name in test.log test-resources.txt run-closed.log cpu.txt compile.log compile.time environment.txt unit-check.log live/binding.json live/control/manifest.json live/normal-plans.jsonl live/preflight.log live/live.log live/live.time live/live.jsonl live/result.json live/cgroup.json binaries/two_bit_plan_demo; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/green-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'compile.log', 'compile.time', 'environment.txt', 'unit-check.log', 'live/binding.json', 'live/control/manifest.json', 'live/normal-plans.jsonl', 'live/preflight.log', 'live/live.log', 'live/live.time', 'live/live.jsonl', 'live/result.json', 'live/cgroup.json', 'binaries/two_bit_plan_demo'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-cold-baseline-aws-v1','source_base_commit':'73569e89b3d53f75ae022714cf80aecd99a7a058',
  'source_archive_sha256':'333bf1f9c62e7bbbb9ec24e7035aec32d7b17c44c22e623d3ab32c04c596f2a0',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/green-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/333bf1f9c62e7bbbb9ec24e7035aec32d7b17c44c22e623d3ab32c04c596f2a0.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '333bf1f9c62e7bbbb9ec24e7035aec32d7b17c44c22e623d3ab32c04c596f2a0' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3.12 python3.12-pip util-linux
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=compile
lscpu >cpu.txt
"$CARGO_HOME/bin/cargo" --version >>cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3
export PYTHONPATH="$root/repo" MALLOC_ARENA_MAX=2 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ARROW_DEFAULT_MEMORY_POOL=system TOKIO_WORKER_THREADS=4
.venv/bin/python -c 'import numpy,platform; print(numpy.__version__,platform.platform())' >environment.txt
systemd-run --unit=topology-screen-compile --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=1230 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/compile.time" timeout --signal=TERM --kill-after=30 1200 \
 "$CARGO_HOME/bin/cargo" build --release --locked --manifest-path "$root/repo/Cargo.toml" --target-dir "$root/target" -p borsuk --jobs 4 \
 --bin two_bit_plan_demo >compile.log 2>&1
phase=unit-check
systemd-run --unit=native-cold-unit-check --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=630 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 "$CARGO_HOME/bin/cargo" test --release --locked --manifest-path "$root/repo/Cargo.toml" --target-dir "$root/target" -p borsuk --jobs 4 --bin two_bit_plan_demo live_scope_is_development_only >unit-check.log 2>&1
mkdir binaries
cp "$root/target/release/two_bit_plan_demo" binaries/two_bit_plan_demo
phase=live-s3
systemd-run --unit=native-cold-measure --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1830 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1800 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_cold.py" "$2/repo/docs/research/native-cold-20260928/config.json" "$3" "$2/repo" "$2/live" "$2/target/release/two_bit_plan_demo" "$4"' _ \
 "$root/.venv/bin/python" "$root" '5e472a236bc42e7b327ec2b8f4d0cd8a113b51bd457df2c901881aea6e7b1264' 'research/native-cold/20260928/green-a0002/index' >test.log 2>&1
phase=complete
