#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cold-baseline-stop --on-active=2400s /usr/sbin/shutdown -h now
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
  for name in test.log test-resources.txt run-closed.log cpu.txt compile.log compile.time environment.txt unit-check.log test-resources.txt test.log screen/decision.json screen/cgroup.json screen/relaion/nomination.jsonl screen/relaion/nomination.log screen/relaion/nomination.time screen/relaion/result.json screen/relaion/decomposition.json screen/cohere/nomination.jsonl screen/cohere/nomination.log screen/cohere/nomination.time screen/cohere/result.json screen/cohere/decomposition.json binaries/two_bit_union_nomination; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/union-nomination/20260928/green-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'compile.log', 'compile.time', 'environment.txt', 'unit-check.log', 'test-resources.txt', 'test.log', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/nomination.jsonl', 'screen/relaion/nomination.log', 'screen/relaion/nomination.time', 'screen/relaion/result.json', 'screen/relaion/decomposition.json', 'screen/cohere/nomination.jsonl', 'screen/cohere/nomination.log', 'screen/cohere/nomination.time', 'screen/cohere/result.json', 'screen/cohere/decomposition.json', 'binaries/two_bit_union_nomination'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-union-nomination-aws-v1','source_base_commit':'9813e37bcf66751109c40cf4dc1f3fafcc3a2186',
  'source_archive_sha256':'18741a7b22c8e49e15fbf3f6e7804c703edd403942e59769546cf179b7354527',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/union-nomination/20260928/green-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/18741a7b22c8e49e15fbf3f6e7804c703edd403942e59769546cf179b7354527.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '18741a7b22c8e49e15fbf3f6e7804c703edd403942e59769546cf179b7354527' | sha256sum -c -
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
 --bin two_bit_union_nomination >compile.log 2>&1
phase=unit-check
"$CARGO_HOME/bin/rustc" --edition=2021 --test "$root/repo/crates/borsuk/src/bin/two_bit_union_nomination.rs" -o "$root/union-check"
timeout --signal=TERM --kill-after=10 60 "$root/union-check" >unit-check.log 2>&1
mkdir binaries
cp "$root/target/release/two_bit_union_nomination" binaries/two_bit_union_nomination
phase=nomination
systemd-run --unit=union-nomination-measure --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=630 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 600 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_union_nomination.py" "$2/repo/docs/research/union-nomination-20260928/config.json" "$3" "$2/repo" "$2/screen" "$2/target/release/two_bit_union_nomination"' _ \
 "$root/.venv/bin/python" "$root" 'bfcdc670abc82e0c726c4914f1795ce9443f933b51b8c302f4b446d8d3e371f9' >test.log 2>&1
phase=complete
