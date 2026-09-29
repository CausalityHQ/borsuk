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
  for name in test.log test-resources.txt run-closed.log helper.json binaries/build_sq8_source cpu.txt environment.txt reuse.json test.log test-resources.txt screen/decision.json screen/cgroup.json screen/relaion/normalize.log screen/relaion/normalize.time screen/relaion/hier-fit.log screen/relaion/hier-fit.time screen/relaion/order.u64 screen/relaion/sq8.log screen/relaion/sq8.time screen/relaion/builder.json screen/relaion/binding.json screen/relaion/build.log screen/relaion/build.time screen/relaion/paired.log screen/relaion/paired.time screen/relaion/paired.jsonl screen/relaion/quality.json screen/relaion/result.json screen/relaion/candidate/manifest.json screen/cohere/normalize.log screen/cohere/normalize.time screen/cohere/hier-fit.log screen/cohere/hier-fit.time screen/cohere/order.u64 screen/cohere/sq8.log screen/cohere/sq8.time screen/cohere/builder.json screen/cohere/binding.json screen/cohere/build.log screen/cohere/build.time screen/cohere/paired.log screen/cohere/paired.time screen/cohere/paired.jsonl screen/cohere/quality.json screen/cohere/result.json screen/cohere/candidate/manifest.json screen/relaion/run0-control/live.jsonl screen/relaion/run0-control/live.log screen/relaion/run0-control/live.time screen/relaion/run0-control/result.json screen/relaion/run1-candidate/live.jsonl screen/relaion/run1-candidate/live.log screen/relaion/run1-candidate/live.time screen/relaion/run1-candidate/result.json screen/relaion/run2-candidate/live.jsonl screen/relaion/run2-candidate/live.log screen/relaion/run2-candidate/live.time screen/relaion/run2-candidate/result.json screen/relaion/run3-control/live.jsonl screen/relaion/run3-control/live.log screen/relaion/run3-control/live.time screen/relaion/run3-control/result.json screen/cohere/run0-control/live.jsonl screen/cohere/run0-control/live.log screen/cohere/run0-control/live.time screen/cohere/run0-control/result.json screen/cohere/run1-candidate/live.jsonl screen/cohere/run1-candidate/live.log screen/cohere/run1-candidate/live.time screen/cohere/run1-candidate/result.json screen/cohere/run2-candidate/live.jsonl screen/cohere/run2-candidate/live.log screen/cohere/run2-candidate/live.time screen/cohere/run2-candidate/result.json screen/cohere/run3-control/live.jsonl screen/cohere/run3-control/live.log screen/cohere/run3-control/live.time screen/cohere/run3-control/result.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/locality-source-layout-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'helper.json', 'binaries/build_sq8_source', 'cpu.txt', 'environment.txt', 'reuse.json', 'test.log', 'test-resources.txt', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/normalize.log', 'screen/relaion/normalize.time', 'screen/relaion/hier-fit.log', 'screen/relaion/hier-fit.time', 'screen/relaion/order.u64', 'screen/relaion/sq8.log', 'screen/relaion/sq8.time', 'screen/relaion/builder.json', 'screen/relaion/binding.json', 'screen/relaion/build.log', 'screen/relaion/build.time', 'screen/relaion/paired.log', 'screen/relaion/paired.time', 'screen/relaion/paired.jsonl', 'screen/relaion/quality.json', 'screen/relaion/result.json', 'screen/relaion/candidate/manifest.json', 'screen/cohere/normalize.log', 'screen/cohere/normalize.time', 'screen/cohere/hier-fit.log', 'screen/cohere/hier-fit.time', 'screen/cohere/order.u64', 'screen/cohere/sq8.log', 'screen/cohere/sq8.time', 'screen/cohere/builder.json', 'screen/cohere/binding.json', 'screen/cohere/build.log', 'screen/cohere/build.time', 'screen/cohere/paired.log', 'screen/cohere/paired.time', 'screen/cohere/paired.jsonl', 'screen/cohere/quality.json', 'screen/cohere/result.json', 'screen/cohere/candidate/manifest.json', 'screen/relaion/run0-control/live.jsonl', 'screen/relaion/run0-control/live.log', 'screen/relaion/run0-control/live.time', 'screen/relaion/run0-control/result.json', 'screen/relaion/run1-candidate/live.jsonl', 'screen/relaion/run1-candidate/live.log', 'screen/relaion/run1-candidate/live.time', 'screen/relaion/run1-candidate/result.json', 'screen/relaion/run2-candidate/live.jsonl', 'screen/relaion/run2-candidate/live.log', 'screen/relaion/run2-candidate/live.time', 'screen/relaion/run2-candidate/result.json', 'screen/relaion/run3-control/live.jsonl', 'screen/relaion/run3-control/live.log', 'screen/relaion/run3-control/live.time', 'screen/relaion/run3-control/result.json', 'screen/cohere/run0-control/live.jsonl', 'screen/cohere/run0-control/live.log', 'screen/cohere/run0-control/live.time', 'screen/cohere/run0-control/result.json', 'screen/cohere/run1-candidate/live.jsonl', 'screen/cohere/run1-candidate/live.log', 'screen/cohere/run1-candidate/live.time', 'screen/cohere/run1-candidate/result.json', 'screen/cohere/run2-candidate/live.jsonl', 'screen/cohere/run2-candidate/live.log', 'screen/cohere/run2-candidate/live.time', 'screen/cohere/run2-candidate/result.json', 'screen/cohere/run3-control/live.jsonl', 'screen/cohere/run3-control/live.log', 'screen/cohere/run3-control/live.time', 'screen/cohere/run3-control/result.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-locality-source-layout-v1','source_base_commit':'a9118ad2cd90015dc430f48d094ab609a0f3f6b7',
  'source_archive_sha256':'dc534ae0c3a648f84b54fedee55c910a7ebdec59da3763475f8d06bcb0ec2a41',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/locality-source-layout-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/dc534ae0c3a648f84b54fedee55c910a7ebdec59da3763475f8d06bcb0ec2a41.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'dc534ae0c3a648f84b54fedee55c910a7ebdec59da3763475f8d06bcb0ec2a41' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=reuse-native
dnf install -y -q gcc gcc-c++ cmake perl tar gzip python3.12 python3.12-pip util-linux time pkgconf-pkg-config
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/locality-source-check-a0001/artifacts/binaries/build_two_bit_generation' binaries/build_two_bit_generation --only-show-errors
echo '67bc20387f3f09b6421329d0f9b0766f3ff6437ea089b627cf22cf2ad84fbb7a  binaries/build_two_bit_generation' | sha256sum -c -
[ "$(stat -c %s binaries/build_two_bit_generation)" = '1267176' ]
chmod 755 binaries/build_two_bit_generation
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/locality-source-check-a0001/artifacts/binaries/two_bit_plan_demo' binaries/two_bit_plan_demo --only-show-errors
echo '5b7811480937e7a5f74c289e4e0323f7f7df5fecea621c24b522bda13dd25375  binaries/two_bit_plan_demo' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_plan_demo)" = '11618544' ]
chmod 755 binaries/two_bit_plan_demo
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/locality-source-check-a0001/artifacts/binaries/build_sq8_source' binaries/build_sq8_source --only-show-errors
echo 'be0a1ae28817b99ae925e9f7db2f5b68c5741a5a58b518d7cf5f5f9bc8963abe  binaries/build_sq8_source' | sha256sum -c -
[ "$(stat -c %s binaries/build_sq8_source)" = '1266704' ]
chmod 755 binaries/build_sq8_source
python3 - <<'PYHELPER' >helper.json
import hashlib,json
from pathlib import Path
p=Path('binaries/build_sq8_source')
print(json.dumps(dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),source_archive_sha256='06c40c0959719bd7738430312b770beedee53ab65e2fb1bd713329b922f3caeb',existing_target='borsuk example build_sq8_source',native_source_matches_compiled_transformation=True,no_full_assurance_rerun=True,reused_binary=True)))
PYHELPER
echo '{"native_files_matched":393,"source_archive_sha256":"06c40c0959719bd7738430312b770beedee53ab65e2fb1bd713329b922f3caeb","terminal_sha256":"967c922507ec0bc81302ef42712cc7ebb2bebf790d6466fd32ca0c6714037452","native_serving_binary_and_full_assurance_reused":true}' >reuse.json
phase=layout
systemd-run --unit=native-union-layout --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=630 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 600 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_union_layout_transfer.py" "$2/repo/docs/research/native-union-20260928/locality-source-config.json" "$3" "$2/repo" "$2/screen" "$2/binaries" "$4"' _ \
 "$root/.venv/bin/python" "$root" '668b84b01647bfae0e236215c7792de27ff21885c67760c113a1fe2a3c223213' 'research/native-union/20260928/locality-source-layout-a0001' >test.log 2>&1
phase=measurement-artifacts
for name in screen/decision.json screen/cgroup.json screen/relaion/result.json test-resources.txt; do
 test -s "$root/$name"
done
phase=complete
