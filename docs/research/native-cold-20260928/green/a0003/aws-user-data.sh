#!/bin/bash
set -euo pipefail
systemd-run --unit=native-cold-baseline-stop --on-active=900s /usr/sbin/shutdown -h now
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/green-a0003/artifacts/$name" --only-show-errors || code=96
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
  'source_archive_sha256':'b3f6be984a863622b6ab0b24de74d0c71068759be544895880fe80bc9cef165a',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/green-a0003/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/b3f6be984a863622b6ab0b24de74d0c71068759be544895880fe80bc9cef165a.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'b3f6be984a863622b6ab0b24de74d0c71068759be544895880fe80bc9cef165a' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=reuse-native
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3
export PYTHONPATH=\"$root/repo\" MALLOC_ARENA_MAX=2 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 TOKIO_WORKER_THREADS=4
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,platform; print(numpy.__version__,platform.platform())' >environment.txt
mkdir -p binaries target/release
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/green-a0002/artifacts/binaries/two_bit_plan_demo' binaries/two_bit_plan_demo --only-show-errors
printf '%s  binaries/two_bit_plan_demo\\n' '5a78c8d19fd374616c7649c08f17f55deec603ae0c521f0e17719530b2cd0a2f' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_plan_demo)" = '11390680' ]
chmod 755 binaries/two_bit_plan_demo
cp binaries/two_bit_plan_demo target/release/two_bit_plan_demo
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-cold/20260928/green-a0002/artifacts/unit-check.log' unit-check.log --only-show-errors
printf '%s  unit-check.log\\n' '806f854a3bcadbd8e67d088ee31b38121bc36a05ac5e48870a5260956badcfae' | sha256sum -c -
printf '%s\\n' 'Reused exact driver source 333bf1f9c62e7bbbb9ec24e7035aec32d7b17c44c22e623d3ab32c04c596f2a0; 391 native files matched; no compile/test rerun' >compile.log
cp compile.log compile.time
phase=live-s3
systemd-run --unit=native-cold-measure --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=630 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 600 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_cold.py" "$2/repo/docs/research/native-cold-20260928/config.json" "$3" "$2/repo" "$2/live" "$2/target/release/two_bit_plan_demo" "$4"' _ \
 "$root/.venv/bin/python" "$root" '5e472a236bc42e7b327ec2b8f4d0cd8a113b51bd457df2c901881aea6e7b1264' 'research/native-cold/20260928/green-a0003/index' >test.log 2>&1
phase=complete
