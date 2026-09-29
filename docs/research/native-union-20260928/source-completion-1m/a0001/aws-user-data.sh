#!/bin/bash
set -euo pipefail
systemd-run --unit=union-nomination-red-stop --on-active=2700s /usr/sbin/shutdown -h now
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
  for name in test.log test-resources.txt run-closed.log helper.json binaries/build_sq8_source cpu.txt environment.txt reuse.json test.log test-resources.txt screen/decision.json screen/cgroup.json screen/self-check.json screen/relaion/normalize.log screen/relaion/normalize.time screen/relaion/hier-fit.log screen/relaion/hier-fit.time screen/relaion/order.u64 screen/relaion/sq8.log screen/relaion/sq8.time screen/relaion/builder.json screen/relaion/binding.json screen/relaion/build.log screen/relaion/build.time screen/relaion/plan-control.log screen/relaion/plan-control.time screen/relaion/plan-control.jsonl screen/relaion/plan-candidate.log screen/relaion/plan-candidate.time screen/relaion/plan-candidate.jsonl screen/relaion/quality.json screen/relaion/result.json screen/relaion/oracle.json screen/relaion/truth.u32 screen/relaion/generation/manifest.json screen/relaion/generation/page_manifest.json screen/relaion/generation/page_digests.bin screen/relaion/generation/centroids.bin screen/relaion/generation/graph.bin screen/relaion/generation/diverse_graph.bin screen/relaion/generation/plane/manifest.json screen/relaion/generation/plane/mean.bin screen/relaion/generation/plane/records.bin screen/relaion/generation/canonical.bin screen/relaion/native-control/live.log screen/relaion/native-control/live.time screen/relaion/native-control/live.jsonl screen/relaion/native-candidate/live.log screen/relaion/native-candidate/live.time screen/relaion/native-candidate/live.jsonl screen/relaion/run0-control/http.jsonl screen/relaion/run0-control/result.json screen/relaion/run0-control/boundary.json screen/relaion/run0-control/server.log screen/relaion/run0-control/server.time screen/relaion/run0-control/server-closeout.json screen/relaion/run1-candidate/http.jsonl screen/relaion/run1-candidate/result.json screen/relaion/run1-candidate/boundary.json screen/relaion/run1-candidate/server.log screen/relaion/run1-candidate/server.time screen/relaion/run1-candidate/server-closeout.json screen/relaion/run2-candidate/http.jsonl screen/relaion/run2-candidate/result.json screen/relaion/run2-candidate/boundary.json screen/relaion/run2-candidate/server.log screen/relaion/run2-candidate/server.time screen/relaion/run2-candidate/server-closeout.json screen/relaion/run3-control/http.jsonl screen/relaion/run3-control/result.json screen/relaion/run3-control/boundary.json screen/relaion/run3-control/server.log screen/relaion/run3-control/server.time screen/relaion/run3-control/server-closeout.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-completion-1m-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'helper.json', 'binaries/build_sq8_source', 'cpu.txt', 'environment.txt', 'reuse.json', 'test.log', 'test-resources.txt', 'screen/decision.json', 'screen/cgroup.json', 'screen/self-check.json', 'screen/relaion/normalize.log', 'screen/relaion/normalize.time', 'screen/relaion/hier-fit.log', 'screen/relaion/hier-fit.time', 'screen/relaion/order.u64', 'screen/relaion/sq8.log', 'screen/relaion/sq8.time', 'screen/relaion/builder.json', 'screen/relaion/binding.json', 'screen/relaion/build.log', 'screen/relaion/build.time', 'screen/relaion/plan-control.log', 'screen/relaion/plan-control.time', 'screen/relaion/plan-control.jsonl', 'screen/relaion/plan-candidate.log', 'screen/relaion/plan-candidate.time', 'screen/relaion/plan-candidate.jsonl', 'screen/relaion/quality.json', 'screen/relaion/result.json', 'screen/relaion/oracle.json', 'screen/relaion/truth.u32', 'screen/relaion/generation/manifest.json', 'screen/relaion/generation/page_manifest.json', 'screen/relaion/generation/page_digests.bin', 'screen/relaion/generation/centroids.bin', 'screen/relaion/generation/graph.bin', 'screen/relaion/generation/diverse_graph.bin', 'screen/relaion/generation/plane/manifest.json', 'screen/relaion/generation/plane/mean.bin', 'screen/relaion/generation/plane/records.bin', 'screen/relaion/generation/canonical.bin', 'screen/relaion/native-control/live.log', 'screen/relaion/native-control/live.time', 'screen/relaion/native-control/live.jsonl', 'screen/relaion/native-candidate/live.log', 'screen/relaion/native-candidate/live.time', 'screen/relaion/native-candidate/live.jsonl', 'screen/relaion/run0-control/http.jsonl', 'screen/relaion/run0-control/result.json', 'screen/relaion/run0-control/boundary.json', 'screen/relaion/run0-control/server.log', 'screen/relaion/run0-control/server.time', 'screen/relaion/run0-control/server-closeout.json', 'screen/relaion/run1-candidate/http.jsonl', 'screen/relaion/run1-candidate/result.json', 'screen/relaion/run1-candidate/boundary.json', 'screen/relaion/run1-candidate/server.log', 'screen/relaion/run1-candidate/server.time', 'screen/relaion/run1-candidate/server-closeout.json', 'screen/relaion/run2-candidate/http.jsonl', 'screen/relaion/run2-candidate/result.json', 'screen/relaion/run2-candidate/boundary.json', 'screen/relaion/run2-candidate/server.log', 'screen/relaion/run2-candidate/server.time', 'screen/relaion/run2-candidate/server-closeout.json', 'screen/relaion/run3-control/http.jsonl', 'screen/relaion/run3-control/result.json', 'screen/relaion/run3-control/boundary.json', 'screen/relaion/run3-control/server.log', 'screen/relaion/run3-control/server.time', 'screen/relaion/run3-control/server-closeout.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-source-completion-1m-dev-v1','source_base_commit':'86719351f8edb0377e7c3572eb77cb894bf5c6a8',
  'source_archive_sha256':'1546dd67b81a2f09f5100d10cab959a38c8c13b02af9b8867d877b6a08ffb38c',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-completion-1m-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/1546dd67b81a2f09f5100d10cab959a38c8c13b02af9b8867d877b6a08ffb38c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '1546dd67b81a2f09f5100d10cab959a38c8c13b02af9b8867d877b6a08ffb38c' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=reuse-native
dnf install -y -q gcc gcc-c++ cmake perl tar gzip python3.12 python3.12-pip util-linux time pkgconf-pkg-config
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/locality-source-check-a0001/artifacts/binaries/build_sq8_source' binaries/build_sq8_source --only-show-errors
echo 'be0a1ae28817b99ae925e9f7db2f5b68c5741a5a58b518d7cf5f5f9bc8963abe  binaries/build_sq8_source' | sha256sum -c -
[ "$(stat -c %s binaries/build_sq8_source)" = '1266704' ]
chmod 755 binaries/build_sq8_source
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/walk-source-integration-a0001/artifacts/binaries/two_bit_plan_demo' binaries/old_two_bit_plan_demo --only-show-errors
echo '2248338210e833a569eef9129a6238bdd72fcd40ae428f1bbc22430600b660ea  binaries/old_two_bit_plan_demo' | sha256sum -c -
[ "$(stat -c %s binaries/old_two_bit_plan_demo)" = '11630088' ]
chmod 755 binaries/old_two_bit_plan_demo
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/walk-source-integration-a0001/artifacts/binaries/two_bit_http' binaries/old_two_bit_http --only-show-errors
echo '01e00da5376903b3043073769472b4906adedc9e6a2544e7f2caf7ed7737abfb  binaries/old_two_bit_http' | sha256sum -c -
[ "$(stat -c %s binaries/old_two_bit_http)" = '12527608' ]
chmod 755 binaries/old_two_bit_http
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-completion-integration-a0002/artifacts/binaries/build_two_bit_generation' binaries/build_two_bit_generation --only-show-errors
echo '06d3eed0009757cb31c41b6da964ec911cf74a500cae368f1dff20163c0d61ce  binaries/build_two_bit_generation' | sha256sum -c -
[ "$(stat -c %s binaries/build_two_bit_generation)" = '1262520' ]
chmod 755 binaries/build_two_bit_generation
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-completion-integration-a0002/artifacts/binaries/two_bit_plan_demo' binaries/two_bit_plan_demo --only-show-errors
echo 'a14112ca68fee0b3318d33c8e3c57db85a387e08b283cb560c729f0889cf832b  binaries/two_bit_plan_demo' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_plan_demo)" = '11653464' ]
chmod 755 binaries/two_bit_plan_demo
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/source-completion-integration-a0002/artifacts/binaries/two_bit_http' binaries/two_bit_http --only-show-errors
echo '16d251c900e7571ed0fa62deef4eeea314b5b6ddd1c704aab81083eb1613abc7  binaries/two_bit_http' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_http)" = '12480992' ]
chmod 755 binaries/two_bit_http
python3 - <<'PYHELPER' >helper.json
import hashlib,json
from pathlib import Path
p=Path('binaries/build_sq8_source')
print(json.dumps(dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),source_archive_sha256='06c40c0959719bd7738430312b770beedee53ab65e2fb1bd713329b922f3caeb',existing_target='borsuk example build_sq8_source',native_source_matches_compiled_transformation=True,no_full_assurance_rerun=True,reused_binary=True)))
PYHELPER
echo '{"native_files_matched":393,"source_archive_sha256":"06c40c0959719bd7738430312b770beedee53ab65e2fb1bd713329b922f3caeb","terminal_sha256":"967c922507ec0bc81302ef42712cc7ebb2bebf790d6466fd32ca0c6714037452","native_serving_binary_and_full_assurance_reused":true}' >reuse.json
phase=layout
systemd-run --unit=native-union-layout --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_source_completion_1m.py" "$2/repo/docs/research/native-union-20260928/source-completion-1m-config.json" "$3" "$2/repo" "$2/screen" "$2/binaries" "$4"' _ \
 "$root/.venv/bin/python" "$root" '164e8e64dfc6058698bee8b6e0d511eb89c8afa6b748040734e4f1fd7ef7a8f2' 'research/native-union/20260928/source-completion-1m-a0001' >test.log 2>&1
phase=measurement-artifacts
for name in screen/decision.json screen/cgroup.json screen/relaion/result.json test-resources.txt; do
 test -s "$root/$name"
done
phase=complete
