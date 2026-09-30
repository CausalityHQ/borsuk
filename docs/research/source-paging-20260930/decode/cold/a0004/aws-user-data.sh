#!/bin/bash
set -euo pipefail
systemd-run --unit=native-graph-decode-cold-stop --on-active=5400s /usr/sbin/shutdown -h now
root=/mnt/native-graph-decode-cold
mkdir -p "$root" && cd "$root"
python3 -c 'import base64,gzip; from pathlib import Path; Path("artifact-roster.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/51X247bIBT8lzyXuO1KVd77GVWFMCYOGwzkANnk74sX8MYudiBPwWeY8TGXGeXPzigHlKGLI4IfOSWWK7l/N0ruvu2odnt7s35kmbF7ofo4RMACz0QcnERUKMO6OKtVTnYE7oj2oJxOihrUkQsWJ8Wn/9RSfc5tuSTAmWnsh8Itt/hkrZ696sToeepdDdprdChIr5QbCsR/UNMqMO7csBsZtFi8Yg/mKdEAbVT7zqjF0i/hleGeSQZhNQv56Z11zHE7TDH3N4Fe7a0axCooFD0/feumzlaDcTcKl8RcDti8YSCyr+Jo0jNMnD0p4PZeuwF1PSZW67joaklcduxWSnLSMyiTFhTvcA9En0qp/u40nw3i6aSMdHz1N4pIWyOTBLQgEndsUK+Q/acoiaUauKy6I48aH0ScKyXmZ5FoLaLjYd6ZF24b9QsgyJ114y+z8czE8xM8Lvrr2GyqXA7I+iNttIJkqsE7UPCOWAsbHMdf9zoUHnpHY+9xGo2DT98KQ/CdEZNEwRlL0ZWBGbWC3dLxMi9r2nF5VPGJwICOjFgHzLfOWCzfDr9yZarGIyqaNcOe8BXjDuiqgWfhGiMvESgx9FKddXMuUXhm8HmNhUFvTZoMv6SbIt3yAChdwlwQ1HDzgVC7ga/1ng2IWvIsKErJG4FRKlEYHDVyqwHyishakLyitRIo9Wd8JVheuO35gElSs6CZFx8DZ0IywZOwXAAl7DGIptoykBKQD6aJRheFh6BKpXlgTdVccKUlzQVYwmZBNnWZD7QErwTb0QmBjOM2NfdVQMb66SallKGeJRvjhsEH3KLajo773f/roQq6QBEL9Mcm+nMTfVugf/8B4VKo/OUNAAA=")))'
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0004/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-graph-decode-spot-v1','source_commit':'94fb80f05c761b14ce894aa3db9af58edb8406c9',
  'source_archive_sha256':'c1441b635e3d0db1cbea74086eeaf5b0f7f304d4b41ac553ad518cd4d1ac5ff4',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'b7dfcdf159b9c19f97279776ea97d06719d567a0c3d12787a91fb530d782dbef',
  'manifest_sha256':'5e7ae4ed3234910e3c31f662f157e6e58c7f01f1774190e250573dba16bd5ff8',
  'artifact_roster_sha256':'872de0ead982a5fa85b7f39c4c3fc9a094f8a45d87a68096686775139411e382',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0004/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c1441b635e3d0db1cbea74086eeaf5b0f7f304d4b41ac553ad518cd4d1ac5ff4.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c1441b635e3d0db1cbea74086eeaf5b0f7f304d4b41ac553ad518cd4d1ac5ff4' | sha256sum -c -
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
systemd-run --unit=graph-decode-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=3630 \
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3600 \
 taskset -c 0-3 python3.12 -m scripts.check_native_graph_decode_build "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http
phase=profile
systemd-run --unit=native-graph-decode-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_graph_decode_cold docs/research/source-paging-20260930/decode/config.json b7dfcdf159b9c19f97279776ea97d06719d567a0c3d12787a91fb530d782dbef "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
