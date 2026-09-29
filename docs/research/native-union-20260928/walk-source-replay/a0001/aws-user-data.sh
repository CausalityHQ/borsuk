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
  for name in test.log test-resources.txt run-closed.log helper.json binaries/build_sq8_source cpu.txt environment.txt reuse.json test.log test-resources.txt screen/decision.json screen/cgroup.json screen/relaion/normalize.log screen/relaion/normalize.time screen/relaion/order.u64 screen/relaion/sq8.log screen/relaion/sq8.time screen/relaion/builder.json screen/relaion/binding.json screen/relaion/build.log screen/relaion/build.time screen/relaion/paired.log screen/relaion/paired.time screen/relaion/paired.jsonl screen/relaion/preflight.json screen/relaion/replay.jsonl screen/relaion/replay.log screen/relaion/replay.time screen/relaion/quality.json screen/relaion/result.json screen/relaion/candidate/manifest.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/walk-source-replay-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'helper.json', 'binaries/build_sq8_source', 'cpu.txt', 'environment.txt', 'reuse.json', 'test.log', 'test-resources.txt', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/normalize.log', 'screen/relaion/normalize.time', 'screen/relaion/order.u64', 'screen/relaion/sq8.log', 'screen/relaion/sq8.time', 'screen/relaion/builder.json', 'screen/relaion/binding.json', 'screen/relaion/build.log', 'screen/relaion/build.time', 'screen/relaion/paired.log', 'screen/relaion/paired.time', 'screen/relaion/paired.jsonl', 'screen/relaion/preflight.json', 'screen/relaion/replay.jsonl', 'screen/relaion/replay.log', 'screen/relaion/replay.time', 'screen/relaion/quality.json', 'screen/relaion/result.json', 'screen/relaion/candidate/manifest.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-walk-source-replay-v1','source_base_commit':'26522bab4287293778b6078243185c791a6f2d4d',
  'source_archive_sha256':'eaaa5bac4b3499a859fe95ebb237b7f777602ed778eab852bf1ae92788e3b1b6',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/walk-source-replay-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/eaaa5bac4b3499a859fe95ebb237b7f777602ed778eab852bf1ae92788e3b1b6.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'eaaa5bac4b3499a859fe95ebb237b7f777602ed778eab852bf1ae92788e3b1b6' | sha256sum -c -
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/walk-source-check-a0001/artifacts/binaries/two_bit_walk_nomination' binaries/two_bit_walk_nomination --only-show-errors
echo '84b85f54f66fbdbaaa3d06b92733d0ccb44e068abb3de04153b13203b42fb4c4  binaries/two_bit_walk_nomination' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_walk_nomination)" = '1295040' ]
chmod 755 binaries/two_bit_walk_nomination
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
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_walk_source_replay.py" "$2/repo/docs/research/native-union-20260928/walk-source-config.json" "$3" "$2/repo" "$2/screen" "$2/binaries" "$4"' _ \
 "$root/.venv/bin/python" "$root" 'abd7073456f2ab172f00cbd50bc361e9cd9fbd7f278e96a76cfb15a3590c1cff' 'research/native-union/20260928/walk-source-replay-a0001' >test.log 2>&1
phase=measurement-artifacts
for name in screen/decision.json screen/cgroup.json screen/relaion/result.json test-resources.txt; do
 test -s "$root/$name"
done
phase=complete
