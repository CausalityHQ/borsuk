#!/bin/bash
set -euo pipefail
systemd-run --unit=union-nomination-red-stop --on-active=1200s /usr/sbin/shutdown -h now
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
  for name in test.log test-resources.txt run-closed.log cpu.txt reuse.json screen/decision.json screen/cgroup.json screen/relaion/result.json screen/relaion/bad-head.log screen/relaion/bad-head.time screen/relaion/bad-head.json screen/cohere/result.json screen/cohere/bad-head.log screen/cohere/bad-head.time screen/cohere/bad-head.json screen/relaion/run0-control/http.jsonl screen/relaion/run0-control/server.log screen/relaion/run0-control/server.time screen/relaion/run0-control/boundary.json screen/relaion/run0-control/server-closeout.json screen/relaion/run0-control/result.json screen/relaion/run1-candidate/http.jsonl screen/relaion/run1-candidate/server.log screen/relaion/run1-candidate/server.time screen/relaion/run1-candidate/boundary.json screen/relaion/run1-candidate/server-closeout.json screen/relaion/run1-candidate/result.json screen/relaion/run2-candidate/http.jsonl screen/relaion/run2-candidate/server.log screen/relaion/run2-candidate/server.time screen/relaion/run2-candidate/boundary.json screen/relaion/run2-candidate/server-closeout.json screen/relaion/run2-candidate/result.json screen/relaion/run3-control/http.jsonl screen/relaion/run3-control/server.log screen/relaion/run3-control/server.time screen/relaion/run3-control/boundary.json screen/relaion/run3-control/server-closeout.json screen/relaion/run3-control/result.json screen/cohere/run0-control/http.jsonl screen/cohere/run0-control/server.log screen/cohere/run0-control/server.time screen/cohere/run0-control/boundary.json screen/cohere/run0-control/server-closeout.json screen/cohere/run0-control/result.json screen/cohere/run1-candidate/http.jsonl screen/cohere/run1-candidate/server.log screen/cohere/run1-candidate/server.time screen/cohere/run1-candidate/boundary.json screen/cohere/run1-candidate/server-closeout.json screen/cohere/run1-candidate/result.json screen/cohere/run2-candidate/http.jsonl screen/cohere/run2-candidate/server.log screen/cohere/run2-candidate/server.time screen/cohere/run2-candidate/boundary.json screen/cohere/run2-candidate/server-closeout.json screen/cohere/run2-candidate/result.json screen/cohere/run3-control/http.jsonl screen/cohere/run3-control/server.log screen/cohere/run3-control/server.time screen/cohere/run3-control/boundary.json screen/cohere/run3-control/server-closeout.json screen/cohere/run3-control/result.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/http-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt', 'reuse.json', 'screen/decision.json', 'screen/cgroup.json', 'screen/relaion/result.json', 'screen/relaion/bad-head.log', 'screen/relaion/bad-head.time', 'screen/relaion/bad-head.json', 'screen/cohere/result.json', 'screen/cohere/bad-head.log', 'screen/cohere/bad-head.time', 'screen/cohere/bad-head.json', 'screen/relaion/run0-control/http.jsonl', 'screen/relaion/run0-control/server.log', 'screen/relaion/run0-control/server.time', 'screen/relaion/run0-control/boundary.json', 'screen/relaion/run0-control/server-closeout.json', 'screen/relaion/run0-control/result.json', 'screen/relaion/run1-candidate/http.jsonl', 'screen/relaion/run1-candidate/server.log', 'screen/relaion/run1-candidate/server.time', 'screen/relaion/run1-candidate/boundary.json', 'screen/relaion/run1-candidate/server-closeout.json', 'screen/relaion/run1-candidate/result.json', 'screen/relaion/run2-candidate/http.jsonl', 'screen/relaion/run2-candidate/server.log', 'screen/relaion/run2-candidate/server.time', 'screen/relaion/run2-candidate/boundary.json', 'screen/relaion/run2-candidate/server-closeout.json', 'screen/relaion/run2-candidate/result.json', 'screen/relaion/run3-control/http.jsonl', 'screen/relaion/run3-control/server.log', 'screen/relaion/run3-control/server.time', 'screen/relaion/run3-control/boundary.json', 'screen/relaion/run3-control/server-closeout.json', 'screen/relaion/run3-control/result.json', 'screen/cohere/run0-control/http.jsonl', 'screen/cohere/run0-control/server.log', 'screen/cohere/run0-control/server.time', 'screen/cohere/run0-control/boundary.json', 'screen/cohere/run0-control/server-closeout.json', 'screen/cohere/run0-control/result.json', 'screen/cohere/run1-candidate/http.jsonl', 'screen/cohere/run1-candidate/server.log', 'screen/cohere/run1-candidate/server.time', 'screen/cohere/run1-candidate/boundary.json', 'screen/cohere/run1-candidate/server-closeout.json', 'screen/cohere/run1-candidate/result.json', 'screen/cohere/run2-candidate/http.jsonl', 'screen/cohere/run2-candidate/server.log', 'screen/cohere/run2-candidate/server.time', 'screen/cohere/run2-candidate/boundary.json', 'screen/cohere/run2-candidate/server-closeout.json', 'screen/cohere/run2-candidate/result.json', 'screen/cohere/run3-control/http.jsonl', 'screen/cohere/run3-control/server.log', 'screen/cohere/run3-control/server.time', 'screen/cohere/run3-control/boundary.json', 'screen/cohere/run3-control/server-closeout.json', 'screen/cohere/run3-control/result.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-http-v1','source_base_commit':'0e96983c91fc10e5b9df64587739e715b858ec7d',
  'source_archive_sha256':'400ccf28a4aae978942dcd15f10391323f519bcb4db4450288283e350fe2b34f',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/http-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/400ccf28a4aae978942dcd15f10391323f519bcb4db4450288283e350fe2b34f.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '400ccf28a4aae978942dcd15f10391323f519bcb4db4450288283e350fe2b34f' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=reuse-native
dnf install -y -q python3 util-linux time
lscpu >cpu.txt

mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/http-check-a0002/artifacts/binaries/two_bit_http' binaries/two_bit_http --only-show-errors
echo '9202a5358e080454404cef15e2ef1f0605c29b6a5c6f73d3f015b7ed257707ab  binaries/two_bit_http' | sha256sum -c -
[ "$(stat -c %s binaries/two_bit_http)" = '12523896' ]
chmod 755 binaries/two_bit_http
echo '{"native_files_matched":393,"source_archive_sha256":"67acdf5698cbd5f58f59f5a5bc0c8e330d3be19a8911b773090bc723c225a977","terminal_sha256":"acf69a64ec5fd7e2c48fe3a0f69a573b05747d67efc1985e5d54149b2e0e5fba","no_compile_or_test_rerun":true}' >reuse.json
phase=http
systemd-run --unit=native-http --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=930 \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 900 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 "$1" "$2/repo/scripts/run_native_union_http.py" "$2/repo/docs/research/native-union-20260928/http-config.json" "$3" "$2/screen" "$2/binaries/two_bit_http" "$4"' _ \
 python3 "$root" '0857d27aa0c4e8662cb3a4642ff48ac1634975fed09dadc29f41ff34fdb4cc9e' 'research/native-union/20260928/http-a0001' >test.log 2>&1
phase=complete
