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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0002/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-graph-decode-spot-v1','source_commit':'169e610decfd5001c6dfb95771532ee64ffe7fc9',
  'source_archive_sha256':'763bbaab1e988660a4a4086c783edeaf2e9b54080e4e4a3769d74bdb65ae4210',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'2874c47ca751842eee38a1de7d322276aadeb2bc5b7270cc8101e974bef679d9',
  'manifest_sha256':'0e97343766bd46e1c4a13c617837c1faf46a2a28ec65a42a8dfb5009ff32baeb',
  'artifact_roster_sha256':'872de0ead982a5fa85b7f39c4c3fc9a094f8a45d87a68096686775139411e382',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/graph-decode-a0002/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/763bbaab1e988660a4a4086c783edeaf2e9b54080e4e4a3769d74bdb65ae4210.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '763bbaab1e988660a4a4086c783edeaf2e9b54080e4e4a3769d74bdb65ae4210' | sha256sum -c -
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
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_graph_decode_cold docs/research/source-paging-20260930/decode/config.json 2874c47ca751842eee38a1de7d322276aadeb2bc5b7270cc8101e974bef679d9 "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/control/binaries/two_bit_http" "$1/control/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/block0-records.jsonl"
test -s "$root/screen/block1-records.jsonl"
test -s "$root/screen/block2-records.jsonl"
test -s "$root/screen/block3-records.jsonl"
phase=complete
