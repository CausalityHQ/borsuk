#!/bin/bash
set -euo pipefail
systemd-run --unit=native-centroid-bulk-cold-stop --on-active=5400s /usr/sbin/shutdown -h now
root=/mnt/native-centroid-bulk-cold
mkdir -p "$root" && cd "$root"
python3 -c 'import base64,gzip; from pathlib import Path; Path("artifact-roster.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/52X247bIBRF/yXPJW47UpX3fkZVIYyJwwQD4ZJJ/r54AI/tggN+Cj6Hvbmfpfw5aGEVJuBmEaNnipGhgh/fteCHbwcs7dE8jGsZos2RiT40gSJep0NeWQ4wE5p0oVcrLO+QegLcK2FldJRKnCkjoVP4+s8txpfalnKkKNGN+RCwpQZejJGLoS4EX6e5i0E6jw5460y4wQq5BTWtUNpeG/JAg2SrIY5KvxRqhRvRvhNsIHdbeCewJ5wov5uF+jhmnXI8Dl2s/Y1UL45GDCybZAJfX4666bM1wXAahVuibyeo36BCvK/SSNQTiKy5CEXNs/YA6uYYVa2lrKsVUd6RR6nIcqfAhBslaAd7heSlVOreTvM5QTjdlFEO7+5FIW5qbKKBZIjDjgxij9gtRXDIxUB51RuZe3wgdq20WN5FJCULFQ/STu94bdhtAENP0o2/xJB9Jzne1jB6uHq+PIbSPK4zRm4nYNxr0FKoWI992QG+7ISYvxuh/VUSfGC2bDAuO3TDofFZ8nwzzjB8KrdGpOMYymqDwZ0oPVr7wo3HsrCOSUv5WYQvpAZwJshYRdxKCAnhx+lXKozFOAHW5Er/lM8gwGezKEima5BQYlCChlKffJkvcXiFirTHqtRvdZrQUTKbIt9ylJRuYQopNdo0WmoPcN/ck6ipFS+QUyreQE+pRSGCauyyKNpjkkPSHq8MmurveAZRO157DlX7bsAMWdFiga5lcI6wKZNAWcylkBZzc7RNsTXiYiKNukmGV4E5+uLGLBEYw0sUTtEUEqNRCo0xt0DkNPk0KmM6g8yzZQxoS02c3FcAaOO668g/jZ2KN9oOg0PnKtqOtfy7+2eGheq8hK2yPzazPzezb6vs3383fPjOiQ4AAA==")))'
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  for name in $(python3 -c 'import json; print(" ".join(json.load(open("artifact-roster.json"))))'); do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/centroid-bulk-a0001/artifacts/$name" --only-show-errors || code=96
    fi
  done
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in json.loads(Path("artifact-roster.json").read_text()):
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-native-centroid-bulk-spot-v1','source_commit':'4c2986f5332d0098df7a214dae6049fe29c2568c',
  'source_archive_sha256':'c36678b4b603692a42c279cf39231bfdd299bb1f95927759d0ed19f03a44a163',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'5b789c61423c44ec8091e03c74f02f56ab508f8979338728c0d8bb9c8120fc9a',
  'manifest_sha256':'0dcb1b0342bc257edc772c6106b813648537ef7930f6d5a2e53b2dbc7b2505a2',
  'artifact_roster_sha256':'5fabd9a4466f5a58cabb3aec22f042210494224253deb84e4484af312286769f',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/centroid-bulk-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c36678b4b603692a42c279cf39231bfdd299bb1f95927759d0ed19f03a44a163.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c36678b4b603692a42c279cf39231bfdd299bb1f95927759d0ed19f03a44a163' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
python3 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
phase=binary-qualification
lscpu >cpu.txt
systemd-run --unit=centroid-bulk-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=3630 \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3600 \
 taskset -c 0-3 python3.12 -m scripts.check_native_centroid_bulk_build "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-centroid-bulk-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_centroid_bulk_cold docs/research/source-paging-20260930/centroid-bulk/config.json 5b789c61423c44ec8091e03c74f02f56ab508f8979338728c0d8bb9c8120fc9a "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
