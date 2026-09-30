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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0003/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-graph-decode-spot-v1','source_commit':'751decdb0ba7e188469eca5389168e3aed623fe8',
  'source_archive_sha256':'c9184d5ee9b6492593ca276a932cfd726659f8aac5a798fe5ba0c04af1cef676',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'9cd9cc8eb956d947002cfa5d14b2d0cb7ed7e89bdb455fd779bad7cdf6bfe6bb',
  'manifest_sha256':'e1a283e57bf82a3606dc7ce36a4221f8f2c9054057799f6ce816cc3262e95f16',
  'artifact_roster_sha256':'872de0ead982a5fa85b7f39c4c3fc9a094f8a45d87a68096686775139411e382',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0003/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/c9184d5ee9b6492593ca276a932cfd726659f8aac5a798fe5ba0c04af1cef676.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'c9184d5ee9b6492593ca276a932cfd726659f8aac5a798fe5ba0c04af1cef676' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_graph_decode_cold docs/research/source-paging-20260930/decode/config.json 9cd9cc8eb956d947002cfa5d14b2d0cb7ed7e89bdb455fd779bad7cdf6bfe6bb "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
