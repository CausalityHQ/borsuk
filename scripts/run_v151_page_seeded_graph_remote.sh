#!/bin/bash
# One frozen V151 fresh returned-quality screen on Causality Spot.
set -euo pipefail
root=/mnt/v151-page-seeded-graph
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V151_WALL_SECONDS - 900))
monitor_pid=
imds_token=
imds() {
  if [ -z "$imds_token" ]; then
    imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
      http://169.254.169.254/latest/api/token) || return
  fi
  curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" \
    "http://169.254.169.254/latest/meta-data/$1"
}
finish() {
  code=$?
  trap - EXIT
  trap '' TERM
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(imds instance-id || true)
  if [ -d /sys/fs/cgroup/v151-science ]; then
    { cat /sys/fs/cgroup/v151-science/memory.current;
      cat /sys/fs/cgroup/v151-science/memory.peak;
      cat /sys/fs/cgroup/v151-science/memory.stat; } >science.cgroup-memory.txt
  fi
  upload_failed=0
  for path in install.log core-check.log core-check-resources.txt \
      core-test.log core-test-resources.txt \
      gate-build.log gate-build-resources.txt download.log \
      prepare.log prepare-resources.txt fresh/queries.jsonl \
      fresh/routing.jsonl fresh/truth.npy fresh/manifest.json \
      science.log science-resources.txt science.jsonl \
      science.summary.json science.graph.bin science.cgroup-memory.txt \
      replay.log replay-resources.txt replay.jsonl replay.seal.json \
      audit.log audit-resources.txt returned.audit.json \
      reduce.log reduce-resources.txt evidence.jsonl returned.summary.json \
      decision.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 300 aws s3 cp "$path" \
        "$V151_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" \
    STARTED_EPOCH="$started_epoch" ENDED_EPOCH="$ended_epoch" \
    python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
names=('install.log','core-check.log','core-check-resources.txt',
       'core-test.log','core-test-resources.txt',
       'gate-build.log','gate-build-resources.txt','download.log',
       'prepare.log','prepare-resources.txt','fresh/queries.jsonl',
       'fresh/routing.jsonl','fresh/truth.npy','fresh/manifest.json',
       'science.log','science-resources.txt','science.jsonl',
       'science.summary.json','science.graph.bin','science.cgroup-memory.txt',
       'replay.log','replay-resources.txt','replay.jsonl','replay.seal.json',
       'audit.log','audit-resources.txt','returned.audit.json',
       'reduce.log','reduce-resources.txt','evidence.jsonl','returned.summary.json',
       'decision.json','worker.log','interrupt-stop.txt')
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda:source.read(4*1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'borsuk-v151-page-seeded-graph-spot-v1',
  'source_commit':os.environ['V151_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V151_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'interrupted':os.path.isfile('interrupt-stop.txt'),
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V151_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
monitor() {
  while true; do
    if imds spot/instance-action >/dev/null 2>&1; then
      : >interrupt-stop.txt
      if [ -f science-pgid ]; then
        kill -TERM -- "-$(cat science-pgid)" 2>/dev/null || true
      fi
      kill -TERM "$main_pid" 2>/dev/null || true
      return
    fi
    sleep 5
  done
}
run_science() {
  label=$1; shift
  remaining=$((deadline_epoch - $(date +%s)))
  [ "$remaining" -gt 0 ] || exit 124
  setsid timeout --signal=TERM --kill-after=30 "$remaining" \
    /usr/bin/time -v "$@" >"$label.log" 2>"$label-resources.txt" &
  science_pid=$!
  printf '%s\n' "$science_pid" >science-pgid
  status=0
  wait "$science_pid" || status=$?
  rm -f science-pgid
  [ ! -f interrupt-stop.txt ] || exit 97
  [ "$status" -eq 0 ] || return "$status"
}
download_checked() {
  name=$1; key=$2; bytes=$3; sha=$4
  mkdir -p "$(dirname "$name")"
  aws s3 cp "s3://borsuk-bench-453182569524-euc1/$key" "$name" \
    --only-show-errors >>download.log 2>&1
  [ "$(stat -c%s "$name")" = "$bytes" ] || exit 93
  printf '%s  %s\n' "$sha" "$name" | sha256sum -c - >>download.log
}
main_pid=$BASHPID
monitor & monitor_pid=$!
phase=install
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time python3.12 python3.12-pip >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q --only-binary=:all: numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
phase=core-check
run_science core-check "$CARGO_HOME/bin/cargo" check --lib --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml
phase=core-test
run_science core-test "$CARGO_HOME/bin/cargo" test --lib --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml unit_centroid_graph::tests
phase=gate-build
run_science gate-build "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml --bin v151_page_seeded_graph
phase=download
: >download.log
v146=research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001
download_checked v146-terminal.json "$v146/terminal.json" 2844 45d98502b274be939c9e9fd4ba4fca2033cdb3a0323cf28a6148ffd88a095086
download_checked deep/centroids.bin "$v146/artifacts/deep.centroids.bin" 600032 9f8a924ebeacd3c512365934c49c9fc42705a1bde1098ec99c981fb1a7fe3f12
v122=research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts
download_checked deep/source.parquet "$v122/subset/source.parquet" 36528755 da3ad1295d6031818b7ccb817529c6102e9f0ec93bb4ad0792e2b7ab3cd21e69
download_checked deep/built/layout.npy "$v122/built/layout.npy" 800128 8b23fb6d2f76704394733f5540f25e36db961386b568495b7bfd73fc52d60aea
download_checked deep/built/manifest.json "$v122/built/manifest.json" 548 c221b8cfe3fce36911ed9fe16a1c179ae89f400a252fd08b746709c2eab933ef
download_checked deep/built/sq8.bin "$v122/built/sq8.bin" 10800000 c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df
download_checked deep/router/manifest.json "$v122/router/manifest.json" 954 cd1113cf26fd2114324a65a7dca9adfc2453d630b8b4eae5d519c0d4d88a011a
download_checked deep/router/summaries.bin "$v122/router/summaries.bin" 300288 712eb13e612efe7c6a07dddb1de9457c861040c3a13aaa007de782a63056fc47
download_checked deep/router/books.bin "$v122/router/books.bin" 131072 533dccd98e1c60c2fcb95427d3f2d5e09184ad4e76f919a5f9dd3c40059f8c01
download_checked deep/router/codes.bin "$v122/router/codes.bin" 6400000 12013176c849dede7cfe0a8dc4a5ce1276f309382381ef12aa35d8cba5225c38
download_checked deep/router/low.bin "$v122/router/low.bin" 384 3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e
download_checked deep/router/step.bin "$v122/router/step.bin" 384 bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038
download_checked deep/test.parquet publication/v3/20260812/datasets/deep-image-96/attempts/0001/materialized/test.parquet 3843448 296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4
python3 - <<'PYINNER'
import json
v146=json.load(open('v146-terminal.json'))
if v146.get('status')!='complete' or v146.get('source_commit')!='ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0':
    raise ValueError('V146 terminal identity differs')
PYINNER
mkdir /sys/fs/cgroup/v151-science
phase=prepare
run_science prepare .venv/bin/python -m scripts.v151_prepare_fresh \
  --source deep/source.parquet --test deep/test.parquet \
  --built deep/built --router deep/router --output fresh
phase=science
sync
echo 3 >/proc/sys/vm/drop_caches
run_science science bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v151-science/cgroup.procs; exec "$@"' \
  v151 "$CARGO_TARGET_DIR/release/v151_page_seeded_graph" \
  fresh/queries.jsonl fresh/routing.jsonl deep/centroids.bin \
  science.graph.bin science.summary.json
cp science.log science.jsonl
{ cat /sys/fs/cgroup/v151-science/memory.current;
  cat /sys/fs/cgroup/v151-science/memory.peak;
  cat /sys/fs/cgroup/v151-science/memory.stat; } >science.cgroup-memory.txt
phase=replay
run_science replay .venv/bin/python -m scripts.v151_returned_quality replay \
  --source deep/source.parquet --sq8 deep/built/sq8.bin \
  --low deep/router/low.bin --step deep/router/step.bin \
  --queries fresh/queries.jsonl --routing fresh/routing.jsonl \
  --seal fresh/manifest.json --plans science.jsonl \
  --replay replay.jsonl --replay-seal replay.seal.json
phase=audit
run_science audit .venv/bin/python -m scripts.v151_returned_audit \
  --source deep/source.parquet --sq8 deep/built/sq8.bin \
  --low deep/router/low.bin --step deep/router/step.bin \
  --queries fresh/queries.jsonl --routing fresh/routing.jsonl \
  --replay replay.jsonl --output returned.audit.json
phase=reduce
run_science reduce .venv/bin/python -m scripts.v151_returned_quality reduce \
  --sq8 deep/built/sq8.bin --queries fresh/queries.jsonl \
  --routing fresh/routing.jsonl --seal fresh/manifest.json \
  --plans science.jsonl --plan-summary science.summary.json \
  --replay replay.jsonl --replay-seal replay.seal.json \
  --truth fresh/truth.npy --evidence evidence.jsonl \
  --summary returned.summary.json
python3 - <<'PYINNER'
import json
value=json.load(open('returned.summary.json'))
if value.get('schema')!='borsuk-v151-returned-quality-v1':
    raise ValueError('V151 returned summary differs')
with open('decision.json','w') as output:
    json.dump({'schema':'borsuk-v151-decision-v1',
               'verdict':'pass' if value['passes_frozen_gate'] else 'reject'},output,sort_keys=True)
    output.write('\n')
PYINNER
phase=complete
exit 0
