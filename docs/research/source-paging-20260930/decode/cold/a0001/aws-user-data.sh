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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-graph-decode-spot-v1','source_commit':'cf13c741c7f33a0cb839a7685ba9ae5670adfad6',
  'source_archive_sha256':'9f10b27fda2a1552ed69bc3730d4f742c57b96b0a389daf9319803118d9b3f7c',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'0d1734b626f88dc80f491f5e424c3db28865630e943a8589bf444081e823a513',
  'manifest_sha256':'dccf895e92da7b11eaa1afaf15e7915946e2ef7bd764a032cfc967d01f64627b',
  'artifact_roster_sha256':'872de0ead982a5fa85b7f39c4c3fc9a094f8a45d87a68096686775139411e382',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/9f10b27fda2a1552ed69bc3730d4f742c57b96b0a389daf9319803118d9b3f7c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '9f10b27fda2a1552ed69bc3730d4f742c57b96b0a389daf9319803118d9b3f7c' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_graph_decode_cold docs/research/source-paging-20260930/decode/config.json 0d1734b626f88dc80f491f5e424c3db28865630e943a8589bf444081e823a513 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
