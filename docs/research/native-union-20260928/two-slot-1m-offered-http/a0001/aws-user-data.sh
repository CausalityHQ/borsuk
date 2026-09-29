#!/bin/bash
set -euo pipefail
systemd-run --unit=native-two-slot-1m-offered-http-stop --on-active=2700s /usr/sbin/shutdown -h now
root=/mnt/native-two-slot-1m-offered-http
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
  for name in test.log test-resources.txt run-closed.log compile-resources.txt reuse.json cpu.txt red.log green.log release.log http.fixed.rs boundary-check.json boundary-cgroup.json binaries/two_bit_http screen/native-quality.json screen/decision.json screen/cgroup.json screen/requests64.jsonl screen/reference-k10.jsonl screen/reference-k100.jsonl screen/run0-k10/http.jsonl screen/run0-k10/result.json screen/run0-k10/server.log screen/run0-k10/server.time screen/run0-k10/server-closeout.json screen/run1-k100/http.jsonl screen/run1-k100/result.json screen/run1-k100/server.log screen/run1-k100/server.time screen/run1-k100/server-closeout.json screen/run2-k100/http.jsonl screen/run2-k100/result.json screen/run2-k100/server.log screen/run2-k100/server.time screen/run2-k100/server-closeout.json screen/run3-k10/http.jsonl screen/run3-k10/result.json screen/run3-k10/server.log screen/run3-k10/server.time screen/run3-k10/server-closeout.json; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/two-slot-1m-offered-http-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in ('test.log', 'test-resources.txt', 'run-closed.log', 'compile-resources.txt', 'reuse.json', 'cpu.txt', 'red.log', 'green.log', 'release.log', 'http.fixed.rs', 'boundary-check.json', 'boundary-cgroup.json', 'binaries/two_bit_http', 'screen/native-quality.json', 'screen/decision.json', 'screen/cgroup.json', 'screen/requests64.jsonl', 'screen/reference-k10.jsonl', 'screen/reference-k100.jsonl', 'screen/run0-k10/http.jsonl', 'screen/run0-k10/result.json', 'screen/run0-k10/server.log', 'screen/run0-k10/server.time', 'screen/run0-k10/server-closeout.json', 'screen/run1-k100/http.jsonl', 'screen/run1-k100/result.json', 'screen/run1-k100/server.log', 'screen/run1-k100/server.time', 'screen/run1-k100/server-closeout.json', 'screen/run2-k100/http.jsonl', 'screen/run2-k100/result.json', 'screen/run2-k100/server.log', 'screen/run2-k100/server.time', 'screen/run2-k100/server-closeout.json', 'screen/run3-k10/http.jsonl', 'screen/run3-k10/result.json', 'screen/run3-k10/server.log', 'screen/run3-k10/server.time', 'screen/run3-k10/server-closeout.json'):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-two-slot-1m-offered-http-dev-v1','source_base_commit':'efaeb752de4ab0b8757a70a99edcef8babdca415',
  'source_archive_sha256':'747f60f859c6a1551312c28d3a96dce8bd83698756bb57ccc5b8195a33cd76ce',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/native-union/20260928/two-slot-1m-offered-http-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/747f60f859c6a1551312c28d3a96dce8bd83698756bb57ccc5b8195a33cd76ce.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '747f60f859c6a1551312c28d3a96dce8bd83698756bb57ccc5b8195a33cd76ce' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
phase=boundary-check
lscpu >cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
systemd-run --unit=native-http-two-slot-authority --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=1830 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" --setenv=TOKIO_WORKER_THREADS=4 --setenv=MALLOC_ARENA_MAX=2 \
 /usr/bin/time -v -o "$root/compile-resources.txt" timeout --signal=TERM --kill-after=30 1800 \
 taskset -c 0-3 python3 "$root/repo/scripts/check_http_two_slot_authority.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
chmod +x binaries/two_bit_http
cat >reuse.json <<'REUSE'
{"actual_native_references_reused": true, "authorities": {"current-1m-offered-http/a0001": {"aws-terminal.json": "40bea9a0de1ece2a5e5cf46920b22ada614165af8df2d431abbfee98e107b856", "verification.json": "ae5d4fbb18bf24ef4d09b8264b880a3ee993ef95fd88fd129232b5a8ba1f420f"}, "http-offered-protocol/a0003": {"aws-terminal.json": "d17b23371cd0d909055c7fa4b5369e32a9c7198c14a6a08a6de5a306c2166080", "verification.json": "c88377716a5cf3bb4bcf90e02ea16e8223bcd440babc15dd39be87bbb21a860e"}, "http-topk-authority/a0002": {"aws-terminal.json": "3767c83a8552e08db72bcd575c45a79cecac60f6b74eea866fe078fb5d536644", "verification.json": "2dd5a5c408fef5ec83093c4196fdbdd2262c58a91b3ab20257bda7b84cd5c3db"}, "native-reference-panel/a0002": {"aws-terminal.json": "daec7b4fec08bfd178d45655520de42ae41306ed8bef9e346a77945f7f348263", "verification.json": "a6e80b23262c6367c09bbc4dfbf1641fa8e24d576f694946c23b875591a269bc"}, "source-completion-1m/a0001": {"aws-terminal.json": "2e3306d61c5ecc83a7bd95578e9a9f0dd1c163b500b76761796febb1b3e3e365", "verification.json": "aefcc55b9ad0e90a1812879c43b57ef00fb3a6eeaacbbeb1a24507a0c31cc13f"}, "source-completion-integration/a0002": {"aws-terminal.json": "d9a7f53bf0f2f1bbf60a3e295e7ed145916afb8cae411ca4ce05fa689456d80b", "verification.json": "5322570dd1d26a8c1f3cb43cafa74bc965e11cc163260105a694426b26584f6f"}}, "full_library_tests_reused": 2696, "offered_protocol_checks_reused": 5}
REUSE
phase=measurement
systemd-run --unit=native-two-slot-1m-offered-http --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=630 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 600 \
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 python3 -m scripts.run_native_two_slot_1m_offered_http "$1/repo/docs/research/native-union-20260928/two-slot-1m-offered-http-config.json" "$2" "$1/screen" "$1/binaries" "$3"' _ "$root" '6227235aaceab16fd340878dcc18b47c8fbab8101c8e45fb00ca483df47a247d' 'research/native-union/20260928/two-slot-1m-offered-http-a0001' >>test.log 2>&1
phase=check-artifacts
for name in screen/decision.json screen/native-quality.json screen/cgroup.json test-resources.txt boundary-check.json binaries/two_bit_http; do test -s "$root/$name"; done
phase=complete
