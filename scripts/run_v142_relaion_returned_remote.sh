#!/bin/bash
# One frozen D768 returned-quality replay on Spot, with GT after replay.
set -euo pipefail
root=/mnt/v142-relaion-returned
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V142_WALL_SECONDS))
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
  for path in install.log compile.log compile-resources.txt download.log \
      score.log score-resources.txt replay.log reduce.log \
      replay-resources.txt reduce-resources.txt \
      scored.jsonl replay.jsonl evidence.jsonl summary.json \
      decision.json cgroup-memory.txt worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V142_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
names=('install.log','compile.log','compile-resources.txt','download.log',
       'score.log','score-resources.txt','replay.log','reduce.log',
       'replay-resources.txt','reduce-resources.txt',
       'scored.jsonl','replay.jsonl','evidence.jsonl','summary.json',
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
print(json.dumps({'schema':'borsuk-v142-relaion-returned-feasibility-spot-v1',
  'source_commit':os.environ['V142_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V142_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V142_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
.venv/bin/pip install -q --only-binary=:all: numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
export PYTHONPATH="$root/repo"
phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml
phase=download-relaion
: >download.log
v36=research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
v116=research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts
v114=research/v114-1m-paired/87881c71e048d557c5c1285ffa0ac1d8c381f764/runs/v114-1m-dev-20260923T222600Z/a0001/artifacts
v140=research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001
download_checked source.parquet "$v36/source.parquet" 1458450077 2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86
download_checked layout.npy research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy 4000128 32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b
download_checked requests.jsonl "$v116/requests.jsonl" 18726909 c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9
download_checked sealed.jsonl "$v116/rust-replay.jsonl" 13455525 3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960
download_checked sq8.bin research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin 780000000 2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b
download_checked mirror-manifest.json "$v114/mirror/manifest.json" 29780 48e01d4488d0c84b991baef521a02bca75ce096c0f9a1154f779c046732707f8
download_checked blocks.sha256 "$v114/mirror/blocks.sha256" 6093760 ec32ad7926aaf8dc1e80186d226d1e9dc5e97fdbd8aeccd14cdeefc1173938ca
download_checked v140-terminal.json "$v140/terminal.json" 1738 d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4
python3 - <<'PY'
import json
with open('v140-terminal.json') as source: terminal=json.load(source)
if (terminal.get('status')!='complete' or terminal.get('exit_code')!=0
    or terminal.get('source_commit')!='3213ca34cf6a8c32824dec6967b39eb06dd5fba6'):
    raise ValueError('V140 terminal identity differs')
PY
download_checked v140-raw.jsonl "$v140/artifacts/relaion.raw.jsonl" 3430072 3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a
phase=score
run_science score "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  replay-fixed-ranges mirror-manifest.json sq8.bin blocks.sha256 \
  requests.jsonl sealed.jsonl v140-raw.jsonl 512
cp score.log scored.jsonl
phase=replay
mkdir /sys/fs/cgroup/v142-evaluate
sync
echo 3 >/proc/sys/vm/drop_caches
run_science replay bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v142-evaluate/cgroup.procs; exec "$@"' \
  v142 .venv/bin/python -m scripts.v142_relaion_returned_quality replay \
  --source source.parquet --layout layout.npy --sq8 sq8.bin \
  --requests requests.jsonl --sealed sealed.jsonl \
  --v140-raw v140-raw.jsonl --scored scored.jsonl \
  --replay replay.jsonl
phase=truth-download
download_checked truth.parquet "$v36/validation-gt100.parquet" 2045045 bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871
phase=reduce
run_science reduce bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v142-evaluate/cgroup.procs; exec "$@"' \
  v142 .venv/bin/python -m scripts.v142_relaion_returned_quality reduce \
  --replay replay.jsonl --truth truth.parquet --sq8 sq8.bin \
  --evidence evidence.jsonl --summary summary.json
printf '%s\n' '{"schema":"borsuk-v142-decision-v1","relaion_completed":true}' >decision.json
{
  cat /sys/fs/cgroup/v142-evaluate/memory.current
  cat /sys/fs/cgroup/v142-evaluate/memory.peak
  cat /sys/fs/cgroup/v142-evaluate/memory.stat
} >cgroup-memory.txt
phase=complete
exit 0
