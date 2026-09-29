#!/bin/bash
set -euo pipefail
systemd-run --unit=native-current-1m-offered-http-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-current-1m-offered-http
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
  for name in test.log test-resources.txt run-closed.log reuse.json cpu.txt screen/native-quality.json screen/decision.json screen/cgroup.json screen/requests64.jsonl screen/reference-k10.jsonl screen/reference-k10.log screen/reference-k10.time screen/reference-k100.jsonl screen/reference-k100.log screen/reference-k100.time screen/run0-k10/http.jsonl screen/run0-k10/result.json screen/run0-k10/server.log screen/run0-k10/server.time screen/run0-k10/server-closeout.json screen/run1-k100/http.jsonl screen/run1-k100/result.json screen/run1-k100/server.log screen/run1-k100/server.time screen/run1-k100/server-closeout.json screen/run2-k100/http.jsonl screen/run2-k100/result.json screen/run2-k100/server.log screen/run2-k100/server.time screen/run2-k100/server-closeout.json screen/run3-k10/http.jsonl screen/run3-k10/result.json screen/run3-k10/server.log screen/run3-k10/server.time screen/run3-k10/server-closeout.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/current-1m-offered-http-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'reuse.json', 'cpu.txt', 'screen/native-quality.json', 'screen/decision.json', 'screen/cgroup.json', 'screen/requests64.jsonl', 'screen/reference-k10.jsonl', 'screen/reference-k10.log', 'screen/reference-k10.time', 'screen/reference-k100.jsonl', 'screen/reference-k100.log', 'screen/reference-k100.time', 'screen/run0-k10/http.jsonl', 'screen/run0-k10/result.json', 'screen/run0-k10/server.log', 'screen/run0-k10/server.time', 'screen/run0-k10/server-closeout.json', 'screen/run1-k100/http.jsonl', 'screen/run1-k100/result.json', 'screen/run1-k100/server.log', 'screen/run1-k100/server.time', 'screen/run1-k100/server-closeout.json', 'screen/run2-k100/http.jsonl', 'screen/run2-k100/result.json', 'screen/run2-k100/server.log', 'screen/run2-k100/server.time', 'screen/run2-k100/server-closeout.json', 'screen/run3-k10/http.jsonl', 'screen/run3-k10/result.json', 'screen/run3-k10/server.log', 'screen/run3-k10/server.time', 'screen/run3-k10/server-closeout.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-current-1m-offered-http-dev-v1','source_base_commit':'77122d25208a507666c4e116c837130508702b5f',
  'source_archive_sha256':'1697b39b5fa31a41dd133314ac3bbb2de2ab2f148355177fd59502913f53569f',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/current-1m-offered-http-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/1697b39b5fa31a41dd133314ac3bbb2de2ab2f148355177fd59502913f53569f.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '1697b39b5fa31a41dd133314ac3bbb2de2ab2f148355177fd59502913f53569f' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q time
lscpu >cpu.txt
mkdir binaries
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/native-reference-panel-a0002/artifacts/binaries/two_bit_plan_demo' 'binaries/two_bit_plan_demo' --only-show-errors
echo 'a2a9ca63d3bb2f6df8ce80d72020361bbba210d593d5c8db7f2401394340c04c  binaries/two_bit_plan_demo' | sha256sum -c -
test $(stat -c %s binaries/two_bit_plan_demo) -eq 11460464
chmod +x binaries/two_bit_plan_demo
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-union/20260928/http-topk-authority-a0002/artifacts/binaries/two_bit_http' 'binaries/two_bit_http' --only-show-errors
echo '012b3ff5e13b5bc7f59a4cb8af3d890706648702d7545636d3b8a1b61e31e571  binaries/two_bit_http' | sha256sum -c -
test $(stat -c %s binaries/two_bit_http) -eq 12480928
chmod +x binaries/two_bit_http
cat >reuse.json <<'REUSE'
{"authorities": {"http-offered-protocol/a0003": {"aws-terminal.json": "d17b23371cd0d909055c7fa4b5369e32a9c7198c14a6a08a6de5a306c2166080", "verification.json": "c88377716a5cf3bb4bcf90e02ea16e8223bcd440babc15dd39be87bbb21a860e"}, "http-topk-authority/a0002": {"aws-terminal.json": "3767c83a8552e08db72bcd575c45a79cecac60f6b74eea866fe078fb5d536644", "verification.json": "2dd5a5c408fef5ec83093c4196fdbdd2262c58a91b3ab20257bda7b84cd5c3db"}, "native-reference-panel/a0002": {"aws-terminal.json": "daec7b4fec08bfd178d45655520de42ae41306ed8bef9e346a77945f7f348263", "verification.json": "a6e80b23262c6367c09bbc4dfbf1641fa8e24d576f694946c23b875591a269bc"}, "source-completion-1m/a0001": {"aws-terminal.json": "2e3306d61c5ecc83a7bd95578e9a9f0dd1c163b500b76761796febb1b3e3e365", "verification.json": "aefcc55b9ad0e90a1812879c43b57ef00fb3a6eeaacbbeb1a24507a0c31cc13f"}, "source-completion-integration/a0002": {"aws-terminal.json": "d9a7f53bf0f2f1bbf60a3e295e7ed145916afb8cae411ca4ce05fa689456d80b", "verification.json": "5322570dd1d26a8c1f3cb43cafa74bc965e11cc163260105a694426b26584f6f"}}, "binaries": {"two_bit_http": {"bytes": 12480928, "key": "research/native-union/20260928/http-topk-authority-a0002/artifacts/binaries/two_bit_http", "sha256": "012b3ff5e13b5bc7f59a4cb8af3d890706648702d7545636d3b8a1b61e31e571"}, "two_bit_plan_demo": {"bytes": 11460464, "key": "research/native-union/20260928/native-reference-panel-a0002/artifacts/binaries/two_bit_plan_demo", "sha256": "a2a9ca63d3bb2f6df8ce80d72020361bbba210d593d5c8db7f2401394340c04c"}}, "full_library_tests_reused": 2696, "offered_protocol_checks_reused": 5}
REUSE
phase=measurement
systemd-run --unit=native-current-1m-offered-http --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 python3 -m scripts.run_native_current_1m_offered_http "$1/repo/docs/research/native-union-20260928/current-1m-offered-http-config.json" "$2" "$1/screen" "$1/binaries" "$3"' _ "$root" 'd78116754e2374f5dc59a3a318e61354a6e3423faa840edc2466e23c6e55e722' 'research/native-union/20260928/current-1m-offered-http-a0001' >test.log 2>&1
phase=check-artifacts
for name in screen/decision.json screen/native-quality.json screen/cgroup.json test-resources.txt; do test -s "$root/$name"; done
phase=complete
