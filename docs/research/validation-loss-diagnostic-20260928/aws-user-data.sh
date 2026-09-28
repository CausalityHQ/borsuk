#!/bin/bash
set -euo pipefail
systemd-run --unit=historical-validation-loss-stop --on-active=2700s /usr/sbin/shutdown -h now
root=/mnt/historical-validation-loss
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
  for name in test.log test-resources.txt run-closed.log red-test.log red-exit.txt compile.log compile.time cpu.txt diagnostic/source-admitted.json diagnostic/result.json diagnostic/builder.json diagnostic/development-traces.jsonl diagnostic/validation-traces.jsonl diagnostic/normalize.time diagnostic/normalize.stdout diagnostic/fit.time diagnostic/fit.stdout diagnostic/encode.time diagnostic/encode.stdout diagnostic/build.time diagnostic/build.stdout diagnostic/truth-source.time diagnostic/truth-source.stdout diagnostic/truth.time diagnostic/truth.stdout diagnostic/development-plan.time diagnostic/development-plan.stdout diagnostic/validation-plan.time diagnostic/validation-plan.stdout diagnostic/order.u64 diagnostic/sq8.bin diagnostic/truth.u32 diagnostic/generation/manifest.json diagnostic/generation/graph.bin diagnostic/generation/centroids.bin diagnostic/generation/page_manifest.json diagnostic/generation/page_digests.bin diagnostic/generation/plane/manifest.json diagnostic/generation/plane/mean.bin diagnostic/generation/plane/records.bin; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/validation-loss-diagnostic/20260928/a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'red-test.log', 'red-exit.txt', 'compile.log', 'compile.time', 'cpu.txt', 'diagnostic/source-admitted.json', 'diagnostic/result.json', 'diagnostic/builder.json', 'diagnostic/development-traces.jsonl', 'diagnostic/validation-traces.jsonl', 'diagnostic/normalize.time', 'diagnostic/normalize.stdout', 'diagnostic/fit.time', 'diagnostic/fit.stdout', 'diagnostic/encode.time', 'diagnostic/encode.stdout', 'diagnostic/build.time', 'diagnostic/build.stdout', 'diagnostic/truth-source.time', 'diagnostic/truth-source.stdout', 'diagnostic/truth.time', 'diagnostic/truth.stdout', 'diagnostic/development-plan.time', 'diagnostic/development-plan.stdout', 'diagnostic/validation-plan.time', 'diagnostic/validation-plan.stdout', 'diagnostic/order.u64', 'diagnostic/sq8.bin', 'diagnostic/truth.u32', 'diagnostic/generation/manifest.json', 'diagnostic/generation/graph.bin', 'diagnostic/generation/centroids.bin', 'diagnostic/generation/page_manifest.json', 'diagnostic/generation/page_digests.bin', 'diagnostic/generation/plane/manifest.json', 'diagnostic/generation/plane/mean.bin', 'diagnostic/generation/plane/records.bin'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-historical-validation-loss-spot-v1','source_base_commit':'5ca0caf46395650e7c4d8f0c8dd49ba19dd20f28',
  'source_archive_sha256':'c3c5854a53b0b00c23b4d818a433be86a4263b6ff99ce116824bcb633e7cda3f',
  'historical_source_archive_sha256':'b9ac3d6e22c5e210bafe96b56c4ffa520d84a3a85469ed38e782ea81d3084372', 'historical_code_commit':'2db5b8ff192cd310bb67edca47d4915c15087c17', 'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/validation-loss-diagnostic/20260928/a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c3c5854a53b0b00c23b4d818a433be86a4263b6ff99ce116824bcb633e7cda3f.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c3c5854a53b0b00c23b4d818a433be86a4263b6ff99ce116824bcb633e7cda3f' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3.12 python3.12-pip
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=historical-compile
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/b9ac3d6e22c5e210bafe96b56c4ffa520d84a3a85469ed38e782ea81d3084372.tar.gz' historical.tar.gz --only-show-errors
printf '%s  historical.tar.gz\n' 'b9ac3d6e22c5e210bafe96b56c4ffa520d84a3a85469ed38e782ea81d3084372' | sha256sum -c -
mkdir historical && tar -xzf historical.tar.gz -C historical
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
export OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ARROW_DEFAULT_MEMORY_POOL=system
lscpu >cpu.txt
set +e
.venv/bin/python repo/docs/research/validation-loss-diagnostic-20260928/red-check.py --self-check >red-test.log 2>&1
red_code=$?
set -e
printf '%s\n' "$red_code" >red-exit.txt
[ "$red_code" -ne 0 ] || exit 91
grep -q 'NotImplementedError: coverage not implemented' red-test.log || exit 92
.venv/bin/python repo/docs/research/validation-loss-diagnostic-20260928/probe.py --self-check
/usr/bin/time -v -o compile.time timeout --signal=TERM --kill-after=30 1200 \
 "$CARGO_HOME/bin/cargo" build --release --locked --manifest-path historical/Cargo.toml -p borsuk --jobs 4 \
 --example build_sq8_source --bin build_two_bit_generation --bin two_bit_plan_demo >compile.log 2>&1
phase=historical-reproduction
export PYTHONPATH="$root/historical"
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 900 \
 bash -c 'ulimit -v 4194304; exec "$1" "$2" "$3" "$4" "$5" "$6" "$7"' _ \
 "$root/.venv/bin/python" "$root/repo/docs/research/validation-loss-diagnostic-20260928/probe.py" \
 "$root/repo/docs/research/validation-loss-diagnostic-20260928/config.json" "$root/historical" \
 "$root/diagnostic" "$root/repo" "$CARGO_TARGET_DIR/release" >test.log 2>&1
phase=complete
