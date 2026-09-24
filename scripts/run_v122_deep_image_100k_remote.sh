#!/bin/bash
# One source-only D96 100k development screen on Causality Spot.
set -euo pipefail
root=/mnt/v122-screen
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V122_WALL_SECONDS))
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
  for path in hashes.log install.log download.log download-resources.txt \
      subset.log subset-resources.txt build.log build-resources.txt \
      router.log router-resources.txt prepare.log prepare-resources.txt \
      evaluate.log evaluate-resources.txt subset/manifest.json \
      subset/source.json subset/source.parquet subset/original_ids.npy \
      built/manifest.json built/layout.npy built/sq8.bin \
      router/manifest.json router/summaries.bin router/books.bin \
      router/codes.bin router/low.bin router/step.bin \
      queries.jsonl truth.npy evidence.jsonl summary.json \
      worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V122_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('hashes.log','install.log','download.log','download-resources.txt',
       'subset.log','subset-resources.txt','build.log','build-resources.txt',
       'router.log','router-resources.txt','prepare.log','prepare-resources.txt',
       'evaluate.log','evaluate-resources.txt','subset/manifest.json',
       'subset/source.json','subset/source.parquet','subset/original_ids.npy',
       'built/manifest.json','built/layout.npy','built/sq8.bin',
       'router/manifest.json','router/summaries.bin','router/books.bin',
       'router/codes.bin','router/low.bin','router/step.bin',
       'queries.jsonl','truth.npy','evidence.jsonl','summary.json',
       'worker.log','interrupt-stop.txt')
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v122-deep-image-100k-spot-v1',
    'source_commit':os.environ['V122_SOURCE_COMMIT'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V122_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
main_pid=$BASHPID
monitor & monitor_pid=$!
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=4 OPENBLAS_NUM_THREADS=4 MKL_NUM_THREADS=4
phase=source-download
run_science download aws s3 cp "$V122_SOURCE_URI" source.parquet --only-show-errors
[ "$(stat -c%s source.parquet)" = "$V122_SOURCE_BYTES" ] || exit 93
printf '%s  source.parquet\n' "$V122_SOURCE_SHA256" >hashes.log
sha256sum -c hashes.log
phase=subset
run_science subset .venv/bin/python -m scripts.v122_subset_screen subset \
  --source source.parquet --source-sha256 "$V122_SOURCE_SHA256" --output subset
read -r subset_sha provenance_sha < <(.venv/bin/python - <<'PY'
import json
value=json.load(open('subset/manifest.json'))
print(value['subset_source_sha256'],value['subset_provenance_sha256'])
PY
)
phase=build
run_science build .venv/bin/python -m scripts.v120_source_layout \
  --source subset/source.parquet --provenance subset/source.json \
  --output built --source-sha256 "$subset_sha" \
  --provenance-sha256 "$provenance_sha"
read -r layout_sha sq8_sha < <(.venv/bin/python - <<'PY'
import json
value=json.load(open('built/manifest.json'))
print(value['layout_sha256'],value['sq8_sha256'])
PY
)
phase=router
run_science router .venv/bin/python -m scripts.v115_source_router \
  --source subset/source.parquet --layout built/layout.npy --output router \
  --source-sha256 "$subset_sha" --layout-sha256 "$layout_sha" \
  --sq8-sha256 "$sq8_sha" --rows 100000 --dimensions 96 \
  --page-rows 256 --blocks-per-page 2 --generation 1
# The index is sealed before any test query object is downloaded.
phase=query-download
aws s3 cp "$V122_QUERY_URI" test.parquet --only-show-errors
[ "$(stat -c%s test.parquet)" = "$V122_QUERY_BYTES" ] || exit 93
printf '%s  test.parquet\n' "$V122_QUERY_SHA256" >>hashes.log
sha256sum -c hashes.log
phase=prepare
run_science prepare .venv/bin/python -m scripts.v122_subset_screen prepare \
  --subset-source subset/source.parquet --subset-source-sha256 "$subset_sha" \
  --queries test.parquet --output queries.jsonl --truth truth.npy
phase=evaluate
run_science evaluate .venv/bin/python -m scripts.v122_subset_screen evaluate \
  --built built --router router --queries queries.jsonl --truth truth.npy \
  --evidence evidence.jsonl --summary summary.json
phase=complete
exit 0
