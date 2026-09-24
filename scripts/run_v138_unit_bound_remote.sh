#!/bin/bash
# One frozen, fail-fast, source-only unit-bound feasibility screen on Spot.
set -euo pipefail
root=/mnt/v138-unit-bound
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V138_WALL_SECONDS))
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
    rm -f deep.raw.jsonl deep.summary.json relaion.raw.jsonl relaion.summary.json
  fi
  upload_failed=0
  for path in install.log download.log deep-resources.txt relaion-resources.txt \
      deep.raw.jsonl deep.summary.json relaion.raw.jsonl relaion.summary.json \
      decision.json cgroup-memory.txt worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V138_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
names=('install.log','download.log','deep-resources.txt','relaion-resources.txt',
       'deep.raw.jsonl','deep.summary.json','relaion.raw.jsonl','relaion.summary.json',
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
print(json.dumps({'schema':'borsuk-v138-unit-bound-feasibility-spot-v1',
  'source_commit':os.environ['V138_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V138_ARCHIVE_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V138_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
.venv/bin/pip install -q numpy==1.26.4 >>install.log 2>&1
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=download-deep
: >download.log
v122=research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001/artifacts
download_checked deep/queries.jsonl "$v122/queries.jsonl" 2041773 dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99
cp repo/docs/research/inputs/v138-deep-primary.jsonl deep/primary.jsonl
printf '%s  %s\n' 04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3 deep/primary.jsonl | sha256sum -c - >>download.log
download_checked deep/sq8.bin "$v122/built/sq8.bin" 10800000 c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df
download_checked deep/low.bin "$v122/router/low.bin" 384 3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e
download_checked deep/step.bin "$v122/router/step.bin" 384 bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038
phase=evaluate-deep
mkdir /sys/fs/cgroup/v138-evaluate
sync
echo 3 >/proc/sys/vm/drop_caches
run_science deep bash -c \
  'echo "$BASHPID" >/sys/fs/cgroup/v138-evaluate/cgroup.procs; exec "$@"' \
  v138 .venv/bin/python repo/scripts/v138_unit_bound_feasibility.py \
  --cohort deep-image-96-angular-random100k --rows 100000 --dimensions 96 \
  --input deep --sq8 deep/sq8.bin --low deep/low.bin --step deep/step.bin \
  --raw deep.raw.jsonl --summary deep.summary.json
if .venv/bin/python - <<'PY'
import json
with open('deep.summary.json') as source: summary=json.load(source)
raise SystemExit(0 if summary['stop_primary_exact_policy'] else 1)
PY
then
  printf '%s\n' '{"schema":"borsuk-v138-decision-v1","deep_stop":true,"relaion_skipped":true}' >decision.json
else
  phase=download-relaion
  v116=research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001/artifacts
  v115=research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/runs/v115-router-20260923T235000Z/a0001/artifacts/router
  download_checked relaion/requests.jsonl "$v116/requests.jsonl" 18726909 c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9
  download_checked relaion/rust-replay.jsonl "$v116/rust-replay.jsonl" 13455525 3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960
  download_checked relaion/sq8.bin research/v70-algorithm-first/single-stage-a4a695d66f508edf/index/sq8.bin 780000000 2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b
  download_checked relaion/low.bin "$v115/low.bin" 3072 ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891
  download_checked relaion/step.bin "$v115/step.bin" 3072 64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c
  phase=evaluate-relaion
  sync
  echo 3 >/proc/sys/vm/drop_caches
  run_science relaion bash -c \
    'echo "$BASHPID" >/sys/fs/cgroup/v138-evaluate/cgroup.procs; exec "$@"' \
    v138 .venv/bin/python repo/scripts/v138_unit_bound_feasibility.py \
    --cohort ReLAION-1M --rows 1000000 --dimensions 768 \
    --input relaion --sq8 relaion/sq8.bin --low relaion/low.bin \
    --step relaion/step.bin --raw relaion.raw.jsonl \
    --summary relaion.summary.json
  .venv/bin/python - <<'PY' >decision.json
import json
with open('deep.summary.json') as source: deep=json.load(source)
with open('relaion.summary.json') as source: relaion=json.load(source)
print(json.dumps({'schema':'borsuk-v138-decision-v1','deep_stop':False,
  'relaion_skipped':False,'relaion_stop':relaion['stop_primary_exact_policy'],
  'primary_exact_policy_feasible':not relaion['stop_primary_exact_policy']},
  sort_keys=True,separators=(',',':')))
PY
fi
{
  cat /sys/fs/cgroup/v138-evaluate/memory.current
  cat /sys/fs/cgroup/v138-evaluate/memory.peak
  cat /sys/fs/cgroup/v138-evaluate/memory.stat
} >cgroup-memory.txt
phase=complete
exit 0
