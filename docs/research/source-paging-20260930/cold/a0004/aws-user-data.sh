#!/bin/bash
set -euo pipefail
systemd-run --unit=native-paged-source-cold-stop --on-active=4200s /usr/sbin/shutdown -h now
root=/mnt/native-paged-source-cold
mkdir -p "$root" && cd "$root"
cat >artifact-roster.json <<'ROSTER'
["test.log","test-resources.txt","run-closed.log","cpu.txt","source-qualification.json","boundary-check.json","boundary-cgroup.json","compiled-source.json","binaries/two_bit_http","resolved-config.json","source.log","source-walk.log","sq8-transport.log","object-native.log","graph.log","generation.log","application-ids.log","gc.log","http.log","release.log","rustc-version.txt","cargo-version.txt","cpuinfo.txt","arm-feature-tree.txt","x86-feature-tree.txt","profile.log","profile-resources.txt","profile-cgroup.json","screen/summary.json","screen/relaion-records.jsonl","screen/cohere-records.jsonl","compiled-source/crates/borsuk/examples/two_bit_http.rs","compiled-source/crates/borsuk/src/object_native_generation.rs","compiled-source/crates/borsuk/src/two_bit_generation.rs","compiled-source/crates/borsuk/tests/two_bit_generation.rs","compiled-source/Cargo.toml","compiled-source/Cargo.lock","compiled-source/crates/borsuk/Cargo.toml","compiled-source/crates/borsuk/tests/two_bit_source.rs","compiled-source/crates/borsuk/src/sq8_s3_range.rs","compiled-source/crates/borsuk/src/sq8_page_authority.rs","compiled-source/crates/borsuk/src/two_bit_source.rs","compiled-source/crates/borsuk/src/two_bit_build.rs","compiled-source/crates/borsuk/src/two_bit_index.rs","compiled-source/crates/borsuk/src/unit_centroid_graph.rs","compiled-source/crates/borsuk/src/bin/build_two_bit_graph_variant.rs","compiled-source/crates/borsuk/src/bin/two_bit_plan_demo.rs","compiled-source/crates/borsuk/src/bin/two_bit_union_nomination.rs","compiled-source/crates/borsuk/src/bin/two_bit_walk_nomination.rs","compiled-source/crates/borsuk/tests/two_bit_application_ids.rs","compiled-source/crates/borsuk/tests/two_bit_gc_delayed_delete.rs"]
ROSTER
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/cold-a0004/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-paged-source-cold-spot-v1','source_commit':'24342b9e5a740049cdb7f02e859af04b8eb9fa32',
  'source_archive_sha256':'3ea1f9903194747d7f741753fc66f48a21c89f22985e6bc9a4660b1f347abca9',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9',
  'original_config_sha256':'92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9',
  'manifest_sha256':'e23787927669acf07cbf0d5e687e99ef5a09b24f27e42e76de4c2070991a744c',
  'resolved_config_sha256':artifacts.get('resolved-config.json',{}).get('sha256'),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/cold-a0004/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/3ea1f9903194747d7f741753fc66f48a21c89f22985e6bc9a4660b1f347abca9.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '3ea1f9903194747d7f741753fc66f48a21c89f22985e6bc9a4660b1f347abca9' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=4
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal   --default-toolchain 1.98.0
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
phase=binary-qualification
lscpu >cpu.txt
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_paged_source_cold_spot --restore-qualified "$root/repo" "$root"
test -s boundary-check.json && test -s binaries/two_bit_http && test -s resolved-config.json
resolved_sha=$(python3.12 -c 'import json; print(json.load(open("boundary-check.json"))["resolved_config_sha256"])')
phase=profile
systemd-run --unit=native-paged-source-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_paged_cold_first_query "$1/resolved-config.json" "$2" "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" "$resolved_sha" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/relaion-records.jsonl"
test -s "$root/screen/cohere-records.jsonl"
phase=complete
