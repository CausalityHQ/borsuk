#!/bin/bash
# One frozen, fail-fast, source-only page-parity feasibility screen on Spot.
set -euo pipefail
root=/mnt/v145-page-parity
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V145_WALL_SECONDS))
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
  trap - EXIT TERM
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(imds instance-id || true)
  upload_failed=0
  for path in install.log core-check.log core-check-resources.txt \
      parity-build.log parity-build-resources.txt download.log \
      deep.log deep-resources.txt deep-parity.log deep-parity-resources.txt \
      relaion.log relaion-resources.txt relaion-parity.log relaion-parity-resources.txt \
      deep.scores.bin deep.raw.jsonl deep.summary.json \
      deep.parity.jsonl deep.parity.summary.json \
      relaion.scores.bin relaion.raw.jsonl relaion.summary.json \
      relaion.parity.jsonl relaion.parity.summary.json \
      decision.json cgroup-memory.txt worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V145_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
       'parity-build.log','parity-build-resources.txt','download.log',
       'deep.log','deep-resources.txt','deep-parity.log','deep-parity-resources.txt',
       'relaion.log','relaion-resources.txt','relaion-parity.log','relaion-parity-resources.txt',
       'deep.scores.bin','deep.raw.jsonl','deep.summary.json',
       'deep.parity.jsonl','deep.parity.summary.json',
       'relaion.scores.bin','relaion.raw.jsonl','relaion.summary.json',
       'relaion.parity.jsonl','relaion.parity.summary.json',
       'decision.json','cgroup-memory.txt','worker.log','interrupt-stop.txt')
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
print(json.dumps({'schema':'borsuk-v145-page-parity-feasibility-spot-v1',
  'source_commit':os.environ['V145_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V145_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V145_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q --only-binary=:all: numpy==1.26.4 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=core-check
run_science core-check "$CARGO_HOME/bin/cargo" check --lib --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml
phase=parity-build
run_science parity-build "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml --bin v145_page_parity
phase=download-deep
: >download.log
v140=research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001
download_checked v140-terminal.json "$v140/terminal.json" 1738 d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4
download_checked v140-deep.raw.jsonl "$v140/artifacts/deep.raw.jsonl" 3757457 e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5
download_checked v140-relaion.raw.jsonl "$v140/artifacts/relaion.raw.jsonl" 3430072 3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a
python3 - <<'PY'
import json
with open('v140-terminal.json') as source: terminal=json.load(source)
if terminal.get('status')!='complete' or terminal.get('source_commit')!='3213ca34cf6a8c32824dec6967b39eb06dd5fba6':
    raise ValueError('V140 terminal identity differs')
PY
v122=research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts
download_checked deep/queries.jsonl "$v122/queries.jsonl" 2041773 dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99
cp repo/docs/research/inputs/v138-deep-primary.jsonl deep/primary.jsonl
printf '%s  %s\n' 04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3 deep/primary.jsonl | sha256sum -c - >>download.log
download_checked deep/sq8.bin "$v122/built/sq8.bin" 10800000 c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df
download_checked deep/low.bin "$v122/router/low.bin" 384 3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e
download_checked deep/step.bin "$v122/router/step.bin" 384 bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038
phase=evaluate-deep
mkdir /sys/fs/cgroup/v145-evaluate
sync
echo 3 >/proc/sys/vm/drop_caches
run_science deep bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v145-evaluate/cgroup.procs; exec "$@"' \
  v145 .venv/bin/python repo/scripts/v140_budgeted_page_rank.py \
  --cohort deep-image-96-angular-random100k --rows 100000 --dimensions 96 \
  --input deep --sq8 deep/sq8.bin --low deep/low.bin --step deep/step.bin \
  --raw deep.raw.jsonl --summary deep.summary.json \
  --scores-output deep.scores.bin
printf '%s  %s\n' e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5 deep.raw.jsonl | sha256sum -c - >>download.log
phase=parity-deep
run_science deep-parity "$CARGO_TARGET_DIR/release/v145_page_parity" \
  deep-image-96-angular-random100k 100000 96 deep.scores.bin \
  deep/primary.jsonl v140-deep.raw.jsonl deep.parity.summary.json
cp deep-parity.log deep.parity.jsonl
.venv/bin/python - <<'PY'
import json
value=json.load(open('deep.parity.summary.json'))
if not value['matches_v140'] or value['planner_p95_ms']>5:
    raise ValueError('D96 planner parity or p95 gate differs')
PY
phase=download-relaion
v116=research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts
v115=research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/runs/v115-router-20260923T235000Z/a0001/artifacts/router
download_checked relaion/requests.jsonl "$v116/requests.jsonl" 18726909 c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9
cp repo/docs/research/inputs/v139-relaion-primary.jsonl relaion/rust-replay.jsonl
printf '%s  %s\n' 71bfdf71f293ca1d23f58694866b3ba52ee8ee95ed9b02e676a2a8bf031f3162 relaion/rust-replay.jsonl | sha256sum -c - >>download.log
download_checked relaion/sq8.bin research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin 780000000 2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b
download_checked relaion/low.bin "$v115/low.bin" 3072 ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891
download_checked relaion/step.bin "$v115/step.bin" 3072 64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c
phase=evaluate-relaion
sync
echo 3 >/proc/sys/vm/drop_caches
run_science relaion bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v145-evaluate/cgroup.procs; exec "$@"' \
  v145 .venv/bin/python repo/scripts/v140_budgeted_page_rank.py \
  --cohort ReLAION-1M --rows 1000000 --dimensions 768 \
  --input relaion --sq8 relaion/sq8.bin --low relaion/low.bin \
  --step relaion/step.bin --raw relaion.raw.jsonl \
  --summary relaion.summary.json --scores-output relaion.scores.bin
printf '%s  %s\n' 3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a relaion.raw.jsonl | sha256sum -c - >>download.log
phase=parity-relaion
run_science relaion-parity "$CARGO_TARGET_DIR/release/v145_page_parity" \
  ReLAION-1M 1000000 768 relaion.scores.bin \
  relaion/rust-replay.jsonl v140-relaion.raw.jsonl relaion.parity.summary.json
cp relaion-parity.log relaion.parity.jsonl
.venv/bin/python - <<'PY'
import json
value=json.load(open('relaion.parity.summary.json'))
if not value['matches_v140'] or value['planner_p95_ms']>5:
    raise ValueError('ReLAION planner parity or p95 gate differs')
PY
printf '%s\n' '{"schema":"borsuk-v145-decision-v1","deep_completed":true,"relaion_completed":true}' >decision.json
{
  cat /sys/fs/cgroup/v145-evaluate/memory.current
  cat /sys/fs/cgroup/v145-evaluate/memory.peak
  cat /sys/fs/cgroup/v145-evaluate/memory.stat
} >cgroup-memory.txt
phase=complete
exit 0
