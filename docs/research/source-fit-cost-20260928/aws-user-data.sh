#!/bin/bash
set -euo pipefail
systemd-run --unit=source-fit-cost-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/source-fit-cost
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
  for name in test.log test-resources.txt run-closed.log cpu.txt compile.log compile.time fit-cost/result.json fit-cost/100000/normalize.time fit-cost/100000/normalize.stdout fit-cost/100000/normalize.stderr fit-cost/100000/fit.time fit-cost/100000/fit.stdout fit-cost/100000/fit.stderr fit-cost/100000/hier-fit.time fit-cost/100000/hier-fit.stdout fit-cost/100000/hier-fit.stderr fit-cost/100000/fit.u64 fit-cost/100000/hier-fit.u64 fit-cost/1000000/normalize.time fit-cost/1000000/normalize.stdout fit-cost/1000000/normalize.stderr fit-cost/1000000/fit.time fit-cost/1000000/fit.stdout fit-cost/1000000/fit.stderr fit-cost/1000000/hier-fit.time fit-cost/1000000/hier-fit.stdout fit-cost/1000000/hier-fit.stderr fit-cost/1000000/fit.u64 fit-cost/1000000/hier-fit.u64; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-fit-cost/20260928/a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'compile.log', 'compile.time', 'fit-cost/result.json', 'fit-cost/100000/normalize.time', 'fit-cost/100000/normalize.stdout', 'fit-cost/100000/normalize.stderr', 'fit-cost/100000/fit.time', 'fit-cost/100000/fit.stdout', 'fit-cost/100000/fit.stderr', 'fit-cost/100000/hier-fit.time', 'fit-cost/100000/hier-fit.stdout', 'fit-cost/100000/hier-fit.stderr', 'fit-cost/100000/fit.u64', 'fit-cost/100000/hier-fit.u64', 'fit-cost/1000000/normalize.time', 'fit-cost/1000000/normalize.stdout', 'fit-cost/1000000/normalize.stderr', 'fit-cost/1000000/fit.time', 'fit-cost/1000000/fit.stdout', 'fit-cost/1000000/fit.stderr', 'fit-cost/1000000/hier-fit.time', 'fit-cost/1000000/hier-fit.stdout', 'fit-cost/1000000/hier-fit.stderr', 'fit-cost/1000000/fit.u64', 'fit-cost/1000000/hier-fit.u64'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-source-fit-cost-spot-v1','source_base_commit':'bc5c87ad3fa8636fc15ec303c466632fcf521f92',
  'source_archive_sha256':'9075006cab1b455a4b491069ee4f9aa3d4187f34876da5098d74a411843b2de8',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-fit-cost/20260928/a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/9075006cab1b455a4b491069ee4f9aa3d4187f34876da5098d74a411843b2de8.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '9075006cab1b455a4b491069ee4f9aa3d4187f34876da5098d74a411843b2de8' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3.12 python3.12-pip util-linux
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=source-compile
lscpu >cpu.txt
"$CARGO_HOME/bin/cargo" --version >>cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
export MALLOC_ARENA_MAX=2 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 BORSUK_CPU_THREADS=4 ARROW_DEFAULT_MEMORY_POOL=system
/usr/bin/time -v -o compile.time timeout --signal=TERM --kill-after=30 1200   "$CARGO_HOME/bin/cargo" build --release --locked --manifest-path repo/Cargo.toml -p borsuk --jobs 4 --example build_sq8_source >compile.log 2>&1
phase=source-cost
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1800   bash -c 'ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2" "$3" "$4" "$5"' _   "$root/.venv/bin/python" "$root/repo/docs/research/source-fit-cost-20260928/probe.py"   "$root/repo/docs/research/source-fit-cost-20260928/config.json" "$root/fit-cost"   "$CARGO_TARGET_DIR/release/examples/build_sq8_source" >test.log 2>&1
phase=complete
