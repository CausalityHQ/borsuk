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
  for name in test.log test-resources.txt run-closed.log cpu.txt environment.txt reuse.json test.log test-resources.txt screen/decision.json screen/cgroup.json screen/relaion/binding.json screen/relaion/dev-control.log screen/relaion/dev-control.time screen/relaion/dev-control.jsonl screen/relaion/dev-candidate.log screen/relaion/dev-candidate.time screen/relaion/dev-candidate.jsonl screen/relaion/validation-control.log screen/relaion/validation-control.time screen/relaion/validation-control.jsonl screen/relaion/validation-candidate.log screen/relaion/validation-candidate.time screen/relaion/validation-candidate.jsonl screen/relaion/decomposition.json screen/relaion/result.json screen/cohere/binding.json screen/cohere/dev-control.log screen/cohere/dev-control.time screen/cohere/dev-control.jsonl screen/cohere/dev-candidate.log screen/cohere/dev-candidate.time screen/cohere/dev-candidate.jsonl screen/cohere/validation-control.log screen/cohere/validation-control.time screen/cohere/validation-control.jsonl screen/cohere/validation-candidate.log screen/cohere/validation-candidate.time screen/cohere/validation-candidate.jsonl screen/cohere/decomposition.json screen/cohere/result.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/validation-a0002/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'environment.txt', 'reuse.json', 'test.log', 'test-resources.txt', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/binding.json', 'screen/relaion/dev-control.log', 'screen/relaion/dev-control.time', 'screen/relaion/dev-control.jsonl', 'screen/relaion/dev-candidate.log', 'screen/relaion/dev-candidate.time', 'screen/relaion/dev-candidate.jsonl', 'screen/relaion/validation-control.log', 'screen/relaion/validation-control.time', 'screen/relaion/validation-control.jsonl', 'screen/relaion/validation-candidate.log', 'screen/relaion/validation-candidate.time', 'screen/relaion/validation-candidate.jsonl', 'screen/relaion/decomposition.json', 'screen/relaion/result.json', 'screen/cohere/binding.json', 'screen/cohere/dev-control.log', 'screen/cohere/dev-control.time', 'screen/cohere/dev-control.jsonl', 'screen/cohere/dev-candidate.log', 'screen/cohere/dev-candidate.time', 'screen/cohere/dev-candidate.jsonl', 'screen/cohere/validation-control.log', 'screen/cohere/validation-control.time', 'screen/cohere/validation-control.jsonl', 'screen/cohere/validation-candidate.log', 'screen/cohere/validation-candidate.time', 'screen/cohere/validation-candidate.jsonl', 'screen/cohere/decomposition.json', 'screen/cohere/result.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-union-validation-v1','source_base_commit':'0286df020589fd07aec07c1f59a8b3dc68b4abea',
  'source_archive_sha256':'15b853f3dcd0b7995676e94a94e8e14d96cbd26abc9ff6d46e377d4d4381eb34',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/validation-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/15b853f3dcd0b7995676e94a94e8e14d96cbd26abc9ff6d46e377d4d4381eb34.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '15b853f3dcd0b7995676e94a94e8e14d96cbd26abc9ff6d46e377d4d4381eb34' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=reuse-native
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/green-a0003/artifacts/binaries/build_two_bit_generation' binaries/build_two_bit_generation --only-show-errors
echo '42ccc8088754dd52b0bd4eddee5460eb0fa89e65fcced5b056ea7b14a22f0a9d  binaries/build_two_bit_generation' | sha256sum -c -
[ "$(stat -c %s binaries/build_two_bit_generation)" = '1248520' ]
chmod 755 binaries/build_two_bit_generation
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/green-a0003/artifacts/binaries/two_bit_plan_demo' binaries/two_bit_plan_demo --only-show-errors
echo 'dfc3690943e938a4c46a3e1a3be226a55321baeae2291da9542cdf3206c24a55  binaries/two_bit_plan_demo' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_plan_demo)" = '11392232' ]
chmod 755 binaries/two_bit_plan_demo
echo '{"native_files_matched":392,"source_archive_sha256":"975b050eaa36f17de7b9502e8849f8349f366f1a46a56ce4386ddbaaca39bc97","terminal_sha256":"e9049db8260e186f2fd218dc79a3d60d7b2521fe7281cfa0afc5a7d3da312746","no_compile_or_test_rerun":true}' >reuse.json
phase=validation
systemd-run --unit=native-union-validation --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3330 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3300 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_union_validation.py" "$2/repo/docs/research/native-union-20260928/validation-config.json" "$3" "$2/repo" "$2/screen" "$2/binaries" "$4"' _ \
 "$root/.venv/bin/python" "$root" 'bdc21ed32a5cf667470596bba62e35d349d1f3cb048b558f90649a13d843081d' 'research/native-union/20260928/validation-a0002' >test.log 2>&1
phase=complete
