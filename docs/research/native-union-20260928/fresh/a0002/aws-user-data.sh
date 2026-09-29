#!/bin/bash
set -euo pipefail
systemd-run --unit=union-nomination-red-stop --on-active=900s /usr/sbin/shutdown -h now
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
  for name in test.log test-resources.txt run-closed.log cpu.txt environment.txt reuse.json test.log test-resources.txt screen/decision.json screen/cgroup.json screen/relaion/source-parity.json screen/relaion/cohort.json screen/cohere/source-parity.json screen/cohere/cohort.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/fresh-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'environment.txt', 'reuse.json', 'test.log', 'test-resources.txt', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/source-parity.json', 'screen/relaion/cohort.json', 'screen/cohere/source-parity.json', 'screen/cohere/cohort.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-union-fresh-v1','source_base_commit':'5bd6a5bd88f82a7e7e569cd1c7d9c68d239b086f',
  'source_archive_sha256':'4b9b78f3f74c863930f7d03b9b0bc4d64377f89324985c5abb708cad94c6b319',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/fresh-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/4b9b78f3f74c863930f7d03b9b0bc4d64377f89324985c5abb708cad94c6b319.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '4b9b78f3f74c863930f7d03b9b0bc4d64377f89324985c5abb708cad94c6b319' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=reuse-native
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
mkdir binaries
echo '{"native_files_matched":392,"source_archive_sha256":"975b050eaa36f17de7b9502e8849f8349f366f1a46a56ce4386ddbaaca39bc97","terminal_sha256":"e9049db8260e186f2fd218dc79a3d60d7b2521fe7281cfa0afc5a7d3da312746","no_compile_or_test_rerun":true}' >reuse.json
phase=fresh
systemd-run --unit=native-union-fresh --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=810 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 780 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/seal_native_union_fresh.py" "$2/repo/docs/research/native-union-20260928/fresh-config.json" "$3" "$2/repo" "$2/screen" "$2/binaries" "$4"' _ \
 "$root/.venv/bin/python" "$root" 'f396143a5ff3e7af7eab96db0cf027c23c8f7206520cad3e803b62c83d45d169' 'research/native-union/20260928/fresh-a0002' >test.log 2>&1
phase=complete
