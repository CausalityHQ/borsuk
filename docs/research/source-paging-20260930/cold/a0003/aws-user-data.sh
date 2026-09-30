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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/cold-a0003/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-paged-source-cold-spot-v1','source_commit':'b6dbffbfb1fb242097f0b31614cf91a654c8136c',
  'source_archive_sha256':'68d058ae7fff39e0707cba3f6f2f25fc997d17a9397651291a2f5e999781e232',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'config_sha256':'92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9',
  'original_config_sha256':'92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9',
  'manifest_sha256':'e23787927669acf07cbf0d5e687e99ef5a09b24f27e42e76de4c2070991a744c',
  'resolved_config_sha256':artifacts.get('resolved-config.json',{}).get('sha256'),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/cold-a0003/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/68d058ae7fff39e0707cba3f6f2f25fc997d17a9397651291a2f5e999781e232.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '68d058ae7fff39e0707cba3f6f2f25fc997d17a9397651291a2f5e999781e232' | sha256sum -c -
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
python3.12 "$root/repo/scripts/launch_native_paged_source_cold_spot.py" --restore-qualified "$root/repo" "$root"
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
