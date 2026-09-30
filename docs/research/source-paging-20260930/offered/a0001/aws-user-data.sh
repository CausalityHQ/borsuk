#!/bin/bash
set -euo pipefail
systemd-run --unit=native-paged-cold-offered-stop --on-active=2700s /usr/sbin/shutdown -h now
root=/mnt/native-paged-cold-offered
mkdir -p "$root" && cd "$root"
cat >artifact-roster.json <<'ROSTER'
["source-qualification.json","boundary-check.json","compiled-source.json","binaries/two_bit_http","cpu.txt","test.log","run-closed.log","profile.log","profile-resources.txt","profile-cgroup.json","screen/summary.json","screen/rate0-relaion-records.jsonl","screen/rate0-cohere-records.jsonl","screen/rate1-relaion-records.jsonl","screen/rate1-cohere-records.jsonl","screen/rate2-relaion-records.jsonl","screen/rate2-cohere-records.jsonl","screen/rate3-relaion-records.jsonl","screen/rate3-cohere-records.jsonl","screen/rate4-relaion-records.jsonl","screen/rate4-cohere-records.jsonl","screen/rate5-relaion-records.jsonl","screen/rate5-cohere-records.jsonl","frozen/binaries/two_bit_http","frozen/boundary-check.json","frozen/compiled-source.json","frozen/source-qualification.json","frozen/run-closed.log","frozen/rustc-version.txt","frozen/cargo-version.txt","frozen/cpuinfo.txt","frozen/arm-feature-tree.txt","frozen/x86-feature-tree.txt","frozen/compiled-source/crates/borsuk/examples/two_bit_http.rs","frozen/compiled-source/crates/borsuk/src/object_native_generation.rs","frozen/compiled-source/crates/borsuk/src/two_bit_generation.rs","frozen/compiled-source/crates/borsuk/tests/two_bit_generation.rs","frozen/compiled-source/Cargo.toml","frozen/compiled-source/Cargo.lock","frozen/compiled-source/crates/borsuk/Cargo.toml","frozen/compiled-source/crates/borsuk/tests/two_bit_source.rs","frozen/compiled-source/crates/borsuk/src/sq8_s3_range.rs","frozen/compiled-source/crates/borsuk/src/sq8_page_authority.rs","frozen/compiled-source/crates/borsuk/src/two_bit_source.rs","frozen/compiled-source/crates/borsuk/src/two_bit_build.rs","frozen/compiled-source/crates/borsuk/src/two_bit_index.rs","frozen/compiled-source/crates/borsuk/src/unit_centroid_graph.rs","frozen/compiled-source/crates/borsuk/src/bin/build_two_bit_graph_variant.rs","frozen/compiled-source/crates/borsuk/src/bin/two_bit_plan_demo.rs","frozen/compiled-source/crates/borsuk/src/bin/two_bit_union_nomination.rs","frozen/compiled-source/crates/borsuk/src/bin/two_bit_walk_nomination.rs","frozen/compiled-source/crates/borsuk/tests/two_bit_application_ids.rs","frozen/compiled-source/crates/borsuk/tests/two_bit_gc_delayed_delete.rs"]
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
      aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/offered-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-native-paged-cold-offered-spot-v1','source_commit':'cf1b22f20027229bd6f0305cf485718e45dbac98',
  'source_archive_sha256':'9745a6a2c4c24973cf480b12a7f5ae65409f5f0826eef5c80073f50c82c1255c',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/source-paging/20260930/offered-a0001/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/9745a6a2c4c24973cf480b12a7f5ae65409f5f0826eef5c80073f50c82c1255c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '9745a6a2c4c24973cf480b12a7f5ae65409f5f0826eef5c80073f50c82c1255c' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("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")))'
phase=binary-qualification
lscpu >cpu.txt
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_paged_cold_offered_spot --extract-frozen "$root/repo" "$root" >test.log 2>&1
phase=profile
systemd-run --unit=native-paged-cold-offered --wait --pipe -p MemoryMax=7G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 -p WorkingDirectory="$root/repo" \
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2400 \
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_paged_cold_offered docs/research/source-paging-20260930/offered-config.json 422465367366c909b061e8d211a30386e10ad2f56c70032fe38aa954ca05534d "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" 7516192768 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
test -s "$root/screen/summary.json"
test -s "$root/screen/rate0-relaion-records.jsonl"
test -s "$root/screen/rate0-cohere-records.jsonl"
test -s "$root/screen/rate1-relaion-records.jsonl"
test -s "$root/screen/rate1-cohere-records.jsonl"
test -s "$root/screen/rate2-relaion-records.jsonl"
test -s "$root/screen/rate2-cohere-records.jsonl"
test -s "$root/screen/rate3-relaion-records.jsonl"
test -s "$root/screen/rate3-cohere-records.jsonl"
test -s "$root/screen/rate4-relaion-records.jsonl"
test -s "$root/screen/rate4-cohere-records.jsonl"
test -s "$root/screen/rate5-relaion-records.jsonl"
test -s "$root/screen/rate5-cohere-records.jsonl"
phase=complete
