#!/bin/bash
set -euo pipefail
systemd-run --unit=topology-mechanism-screen-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/topology-mechanism-screen
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
  for name in test.log test-resources.txt run-closed.log cpu.txt compile.log compile.time environment.txt metadata-check.log screen/decision.json screen/cgroup.json screen/relaion/normalize.stdout screen/relaion/normalize.stderr screen/relaion/normalize.time screen/relaion/fit.stdout screen/relaion/fit.stderr screen/relaion/fit.time screen/relaion/encode.stdout screen/relaion/encode.stderr screen/relaion/encode.time screen/relaion/build.stdout screen/relaion/build.stderr screen/relaion/build.time screen/relaion/preflight.stdout screen/relaion/preflight.stderr screen/relaion/preflight.time screen/relaion/graph.stdout screen/relaion/graph.stderr screen/relaion/graph.time screen/relaion/paired.stdout screen/relaion/paired.stderr screen/relaion/paired.time screen/relaion/builder.json screen/relaion/authority.json screen/relaion/scoring.json screen/relaion/preflight-plans.jsonl screen/relaion/paired-plans.jsonl screen/relaion/decomposition.json screen/relaion/gained-lost.json screen/relaion/result.json screen/relaion/candidate/build.json screen/relaion/control/manifest.json screen/relaion/control/graph.bin screen/relaion/control/centroids.bin screen/relaion/control/page_manifest.json screen/relaion/control/page_digests.bin screen/relaion/control/plane/manifest.json screen/relaion/control/plane/mean.bin screen/relaion/control/plane/records.bin screen/relaion/candidate/manifest.json screen/relaion/candidate/graph.bin screen/relaion/candidate/centroids.bin screen/relaion/candidate/page_manifest.json screen/relaion/candidate/page_digests.bin screen/relaion/candidate/plane/manifest.json screen/relaion/candidate/plane/mean.bin screen/relaion/candidate/plane/records.bin screen/cohere/normalize.stdout screen/cohere/normalize.stderr screen/cohere/normalize.time screen/cohere/fit.stdout screen/cohere/fit.stderr screen/cohere/fit.time screen/cohere/encode.stdout screen/cohere/encode.stderr screen/cohere/encode.time screen/cohere/build.stdout screen/cohere/build.stderr screen/cohere/build.time screen/cohere/preflight.stdout screen/cohere/preflight.stderr screen/cohere/preflight.time screen/cohere/graph.stdout screen/cohere/graph.stderr screen/cohere/graph.time screen/cohere/paired.stdout screen/cohere/paired.stderr screen/cohere/paired.time screen/cohere/builder.json screen/cohere/authority.json screen/cohere/scoring.json screen/cohere/preflight-plans.jsonl screen/cohere/paired-plans.jsonl screen/cohere/decomposition.json screen/cohere/gained-lost.json screen/cohere/result.json screen/cohere/candidate/build.json screen/cohere/control/manifest.json screen/cohere/control/graph.bin screen/cohere/control/centroids.bin screen/cohere/control/page_manifest.json screen/cohere/control/page_digests.bin screen/cohere/control/plane/manifest.json screen/cohere/control/plane/mean.bin screen/cohere/control/plane/records.bin screen/cohere/candidate/manifest.json screen/cohere/candidate/graph.bin screen/cohere/candidate/centroids.bin screen/cohere/candidate/page_manifest.json screen/cohere/candidate/page_digests.bin screen/cohere/candidate/plane/manifest.json screen/cohere/candidate/plane/mean.bin screen/cohere/candidate/plane/records.bin binaries/build_sq8_source binaries/build_two_bit_generation binaries/build_two_bit_graph_variant binaries/two_bit_plan_demo screen/relaion/order.u64 screen/relaion/sq8.bin screen/relaion/normalized.f32 screen/relaion/scores.npy screen/relaion/control/canonical.bin screen/cohere/order.u64 screen/cohere/sq8.bin screen/cohere/normalized.f32 screen/cohere/scores.npy screen/cohere/control/canonical.bin; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/topology-mechanism-screen/20260928/a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'compile.log', 'compile.time', 'environment.txt', 'metadata-check.log', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/normalize.stdout', 'screen/relaion/normalize.stderr', 'screen/relaion/normalize.time', 'screen/relaion/fit.stdout', 'screen/relaion/fit.stderr', 'screen/relaion/fit.time', 'screen/relaion/encode.stdout', 'screen/relaion/encode.stderr', 'screen/relaion/encode.time', 'screen/relaion/build.stdout', 'screen/relaion/build.stderr', 'screen/relaion/build.time', 'screen/relaion/preflight.stdout', 'screen/relaion/preflight.stderr', 'screen/relaion/preflight.time', 'screen/relaion/graph.stdout', 'screen/relaion/graph.stderr', 'screen/relaion/graph.time', 'screen/relaion/paired.stdout', 'screen/relaion/paired.stderr', 'screen/relaion/paired.time', 'screen/relaion/builder.json', 'screen/relaion/authority.json', 'screen/relaion/scoring.json', 'screen/relaion/preflight-plans.jsonl', 'screen/relaion/paired-plans.jsonl', 'screen/relaion/decomposition.json', 'screen/relaion/gained-lost.json', 'screen/relaion/result.json', 'screen/relaion/candidate/build.json', 'screen/relaion/control/manifest.json', 'screen/relaion/control/graph.bin', 'screen/relaion/control/centroids.bin', 'screen/relaion/control/page_manifest.json', 'screen/relaion/control/page_digests.bin', 'screen/relaion/control/plane/manifest.json', 'screen/relaion/control/plane/mean.bin', 'screen/relaion/control/plane/records.bin', 'screen/relaion/candidate/manifest.json', 'screen/relaion/candidate/graph.bin', 'screen/relaion/candidate/centroids.bin', 'screen/relaion/candidate/page_manifest.json', 'screen/relaion/candidate/page_digests.bin', 'screen/relaion/candidate/plane/manifest.json', 'screen/relaion/candidate/plane/mean.bin', 'screen/relaion/candidate/plane/records.bin', 'screen/cohere/normalize.stdout', 'screen/cohere/normalize.stderr', 'screen/cohere/normalize.time', 'screen/cohere/fit.stdout', 'screen/cohere/fit.stderr', 'screen/cohere/fit.time', 'screen/cohere/encode.stdout', 'screen/cohere/encode.stderr', 'screen/cohere/encode.time', 'screen/cohere/build.stdout', 'screen/cohere/build.stderr', 'screen/cohere/build.time', 'screen/cohere/preflight.stdout', 'screen/cohere/preflight.stderr', 'screen/cohere/preflight.time', 'screen/cohere/graph.stdout', 'screen/cohere/graph.stderr', 'screen/cohere/graph.time', 'screen/cohere/paired.stdout', 'screen/cohere/paired.stderr', 'screen/cohere/paired.time', 'screen/cohere/builder.json', 'screen/cohere/authority.json', 'screen/cohere/scoring.json', 'screen/cohere/preflight-plans.jsonl', 'screen/cohere/paired-plans.jsonl', 'screen/cohere/decomposition.json', 'screen/cohere/gained-lost.json', 'screen/cohere/result.json', 'screen/cohere/candidate/build.json', 'screen/cohere/control/manifest.json', 'screen/cohere/control/graph.bin', 'screen/cohere/control/centroids.bin', 'screen/cohere/control/page_manifest.json', 'screen/cohere/control/page_digests.bin', 'screen/cohere/control/plane/manifest.json', 'screen/cohere/control/plane/mean.bin', 'screen/cohere/control/plane/records.bin', 'screen/cohere/candidate/manifest.json', 'screen/cohere/candidate/graph.bin', 'screen/cohere/candidate/centroids.bin', 'screen/cohere/candidate/page_manifest.json', 'screen/cohere/candidate/page_digests.bin', 'screen/cohere/candidate/plane/manifest.json', 'screen/cohere/candidate/plane/mean.bin', 'screen/cohere/candidate/plane/records.bin', 'binaries/build_sq8_source', 'binaries/build_two_bit_generation', 'binaries/build_two_bit_graph_variant', 'binaries/two_bit_plan_demo', 'screen/relaion/order.u64', 'screen/relaion/sq8.bin', 'screen/relaion/normalized.f32', 'screen/relaion/scores.npy', 'screen/relaion/control/canonical.bin', 'screen/cohere/order.u64', 'screen/cohere/sq8.bin', 'screen/cohere/normalized.f32', 'screen/cohere/scores.npy', 'screen/cohere/control/canonical.bin'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-topology-mechanism-screen-v1','source_base_commit':'3cf3b515bc9cfffa73e61911d1bf4e7f83f6fefb',
  'source_archive_sha256':'c11940854d11f2ebe12a0f5761293f776a84ee51549d152e601001672b6fedf5',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/topology-mechanism-screen/20260928/a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c11940854d11f2ebe12a0f5761293f776a84ee51549d152e601001672b6fedf5.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c11940854d11f2ebe12a0f5761293f776a84ee51549d152e601001672b6fedf5' | sha256sum -c -
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
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
export PYTHONPATH="$root/repo" MALLOC_ARENA_MAX=2 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ARROW_DEFAULT_MEMORY_POOL=system TOKIO_WORKER_THREADS=4
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
systemd-run --unit=topology-screen-compile --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=1230 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/compile.time" timeout --signal=TERM --kill-after=30 1200 \
 "$CARGO_HOME/bin/cargo" build --release --locked --manifest-path "$root/repo/Cargo.toml" --target-dir "$root/target" -p borsuk --jobs 4 \
 --example build_sq8_source --bin build_two_bit_generation --bin build_two_bit_graph_variant --bin two_bit_plan_demo >compile.log 2>&1
mkdir binaries
cp "$root/target/release/examples/build_sq8_source" binaries/build_sq8_source
for name in build_two_bit_generation build_two_bit_graph_variant two_bit_plan_demo; do cp "$root/target/release/$name" "binaries/$name"; done
phase=mechanism-screen
systemd-run --unit=topology-screen-measure --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1830 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=ARROW_DEFAULT_MEMORY_POOL=system --setenv=TOKIO_WORKER_THREADS=4 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1800 \
 bash -c 'set -e; ulimit -v 4194304; "$1" -m scripts.test_native_two_bit_topology >"$2/metadata-check.log" 2>&1; "$1" -m scripts.test_native_two_bit_topology_runner >>"$2/metadata-check.log" 2>&1; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_two_bit_topology.py" "$2/repo/docs/research/topology-screen-20260928/config.json" "$3" "$2/repo" "$2/screen" "$2/target/release"' _ \
 "$root/.venv/bin/python" "$root" '2a6f38ef86e8e2c1a41f2016741a0749e3e74abb5da3f086d3eca3a14ef1b45b' >test.log 2>&1
phase=complete
