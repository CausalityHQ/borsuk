#!/bin/bash
# One frozen GT-free V150 cost/correctness screen on Causality Spot.
set -euo pipefail
root=/mnt/v150-unit-centroid-graph
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V150_WALL_SECONDS - 900))
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
  if [ -d /sys/fs/cgroup/v150-science ]; then
    { cat /sys/fs/cgroup/v150-science/memory.current;
      cat /sys/fs/cgroup/v150-science/memory.peak;
      cat /sys/fs/cgroup/v150-science/memory.stat; } >science.cgroup-memory.txt
  fi
  upload_failed=0
  for path in install.log core-check.log core-check-resources.txt \
      core-test.log core-test-resources.txt \
      gate-build.log gate-build-resources.txt download.log \
      science.log science-resources.txt science.jsonl \
      science.summary.json science.graph.bin science.cgroup-memory.txt \
      decision.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 300 aws s3 cp "$path" \
        "$V150_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
       'science.log','science-resources.txt','science.jsonl',
       'science.summary.json','science.graph.bin','science.cgroup-memory.txt',
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
print(json.dumps({'schema':'borsuk-v150-unit-centroid-graph-spot-v1',
  'source_commit':os.environ['V150_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V150_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'interrupted':os.path.isfile('interrupt-stop.txt'),
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V150_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
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
  --manifest-path repo/crates/borsuk/Cargo.toml --bin v150_unit_centroid_graph
phase=download
: >download.log
v146=research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001
download_checked v146-terminal.json "$v146/terminal.json" 2844 45d98502b274be939c9e9fd4ba4fca2033cdb3a0323cf28a6148ffd88a095086
download_checked deep/centroids.bin "$v146/artifacts/deep.centroids.bin" 600032 9f8a924ebeacd3c512365934c49c9fc42705a1bde1098ec99c981fb1a7fe3f12
download_checked deep/flat-scores.bin "$v146/artifacts/deep.rust-scores.bin" 1564000 341436ca66c24b0e247cf2a8e40b606d7d983cbd7b170ba4e2740f390cf0807c
v140=research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001
download_checked v140-terminal.json "$v140/terminal.json" 1738 d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4
download_checked deep/v140.raw.jsonl "$v140/artifacts/deep.raw.jsonl" 3757457 e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5
v122=research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts
download_checked deep/queries.jsonl "$v122/queries.jsonl" 2041773 dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99
cp repo/docs/research/inputs/v138-deep-primary.jsonl deep/primary.jsonl
printf '%s  %s\n' 04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3 deep/primary.jsonl | sha256sum -c - >>download.log
python3 - <<'PYINNER'
import json
v146=json.load(open('v146-terminal.json'))
v140=json.load(open('v140-terminal.json'))
if v146.get('status')!='complete' or v146.get('source_commit')!='ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0':
    raise ValueError('V146 terminal identity differs')
if v140.get('status')!='complete' or v140.get('source_commit')!='3213ca34cf6a8c32824dec6967b39eb06dd5fba6':
    raise ValueError('V140 terminal identity differs')
PYINNER
phase=science
mkdir /sys/fs/cgroup/v150-science
sync
echo 3 >/proc/sys/vm/drop_caches
run_science science bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v150-science/cgroup.procs; exec "$@"' \
  v150 "$CARGO_TARGET_DIR/release/v150_unit_centroid_graph" \
  deep/queries.jsonl deep/primary.jsonl deep/centroids.bin \
  deep/flat-scores.bin deep/v140.raw.jsonl science.graph.bin \
  science.summary.json 100000
cp science.log science.jsonl
python3 - <<'PYINNER'
import json
value=json.load(open('science.summary.json'))
if value.get('schema')!='borsuk-v150-unit-centroid-graph-v1':
    raise ValueError('V150 scientific summary differs')
with open('decision.json','w') as output:
    json.dump({'schema':'borsuk-v150-decision-v1','verdict':value['verdict']},output,sort_keys=True)
    output.write('\n')
PYINNER
phase=complete
exit 0
