#!/bin/bash
# One frozen D96 returned-quality replay on Spot, with GT after replay.
set -euo pipefail
root=/mnt/v141-deep-returned
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V141_WALL_SECONDS))
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
  if [ "$code" -ne 0 ] || [ "$phase" != complete ]; then
    rm -f replay.jsonl evidence.jsonl summary.json
  fi
  upload_failed=0
  for path in install.log download.log replay.log reduce.log \
      replay-resources.txt reduce-resources.txt \
      replay.jsonl evidence.jsonl summary.json \
      decision.json cgroup-memory.txt worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V141_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
names=('install.log','download.log','replay.log','reduce.log',
       'replay-resources.txt','reduce-resources.txt',
       'replay.jsonl','evidence.jsonl','summary.json',
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
print(json.dumps({'schema':'borsuk-v141-deep-returned-feasibility-spot-v1',
  'source_commit':os.environ['V141_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V141_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V141_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q python3.12 python3.12-pip tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q --only-binary=:all: numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
export PYTHONPATH="$root/repo"
phase=download-deep
: >download.log
v122=research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts
v140=research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001
download_checked source.parquet "$v122/subset/source.parquet" 36528755 da3ad1295d6031818b7ccb817529c6102e9f0ec93bb4ad0792e2b7ab3cd21e69
download_checked queries.jsonl "$v122/queries.jsonl" 2041773 dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99
download_checked sq8.bin "$v122/built/sq8.bin" 10800000 c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df
download_checked low.bin "$v122/router/low.bin" 384 3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e
download_checked step.bin "$v122/router/step.bin" 384 bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038
cp repo/docs/research/inputs/v141-deep-route-only.jsonl routes.jsonl
printf '%s  %s\n' 388fe94cfd6c6ac59e7153c9947af5cdb07119e8fe5acabef4d5c1b3ffe422d4 routes.jsonl | sha256sum -c - >>download.log
download_checked v140-terminal.json "$v140/terminal.json" 1738 d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4
python3 - <<'PY'
import json
with open('v140-terminal.json') as source: terminal=json.load(source)
if (terminal.get('status')!='complete' or terminal.get('exit_code')!=0
    or terminal.get('source_commit')!='3213ca34cf6a8c32824dec6967b39eb06dd5fba6'):
    raise ValueError('V140 terminal identity differs')
PY
download_checked v140-raw.jsonl "$v140/artifacts/deep.raw.jsonl" 3757457 e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5
phase=replay
mkdir /sys/fs/cgroup/v141-evaluate
sync
echo 3 >/proc/sys/vm/drop_caches
run_science replay bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v141-evaluate/cgroup.procs; exec "$@"' \
  v141 .venv/bin/python -m scripts.v141_deep_returned_quality replay \
  --source source.parquet --sq8 sq8.bin --low low.bin --step step.bin \
  --queries queries.jsonl --routes routes.jsonl --v140-raw v140-raw.jsonl \
  --replay replay.jsonl
phase=truth-download
download_checked truth.npy "$v122/truth.npy" 800128 9e29a3e07ee2fe199fb17d8ec19b62f158d0cebe86ed23db14edecfa877c1a7f
phase=reduce
run_science reduce bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v141-evaluate/cgroup.procs; exec "$@"' \
  v141 .venv/bin/python -m scripts.v141_deep_returned_quality reduce \
  --replay replay.jsonl --truth truth.npy --sq8 sq8.bin \
  --evidence evidence.jsonl --summary summary.json
printf '%s\n' '{"schema":"borsuk-v141-decision-v1","deep_completed":true}' >decision.json
{
  cat /sys/fs/cgroup/v141-evaluate/memory.current
  cat /sys/fs/cgroup/v141-evaluate/memory.peak
  cat /sys/fs/cgroup/v141-evaluate/memory.stat
} >cgroup-memory.txt
phase=complete
exit 0
