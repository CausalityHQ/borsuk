#!/bin/bash
# One immutable cross-corpus paired replay; GT enters after returned IDs seal.
set -euo pipefail
root=/mnt/v121-paired
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V121_WALL_SECONDS))
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
  for path in hashes.log install.log index-download.log index-download-resources.txt \
      compile.log compile-resources.txt prepare.log prepare-resources.txt \
      screen.log screen-resources.txt screen-decision.json \
      nominate.log nominate-resources.txt compose.log compose-resources.txt \
      replay.log replay-resources.txt reduce.log reduce-resources.txt \
      queries.jsonl screen-queries.jsonl rosters.jsonl requests.jsonl \
      rust-replay.jsonl evidence.jsonl summary.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V121_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('hashes.log','install.log','index-download.log','index-download-resources.txt',
       'compile.log','compile-resources.txt','prepare.log','prepare-resources.txt',
       'screen.log','screen-resources.txt','screen-decision.json',
       'nominate.log','nominate-resources.txt','compose.log','compose-resources.txt',
       'replay.log','replay-resources.txt','reduce.log','reduce-resources.txt',
       'queries.jsonl','screen-queries.jsonl','rosters.jsonl','requests.jsonl',
       'rust-replay.jsonl','evidence.jsonl','summary.json','worker.log',
       'interrupt-stop.txt')
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
complete=code==0 and phase in ('complete','runtime-screen-rejected')
print(json.dumps({'schema':'borsuk-v121-deep-image-paired-spot-v1',
    'source_commit':os.environ['V121_SOURCE_COMMIT'],
    'index_terminal_sha256':os.environ['V121_INDEX_TERMINAL_SHA256'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if complete else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V121_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q python3.12 python3.12-pip tar gzip time gcc >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=8
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=index-download
run_science index-download .venv/bin/python repo/scripts/v121_download_index.py \
  --index-prefix "$V121_INDEX_PREFIX" \
  --terminal-sha256 "$V121_INDEX_TERMINAL_SHA256" \
  --output-root .
phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml
phase=query-download
aws s3 cp "$V121_QUERY_URI" test.parquet --only-show-errors
[ "$(stat -c%s test.parquet)" = "$V121_QUERY_BYTES" ] || exit 93
printf '%s  test.parquet\n' "$V121_QUERY_SHA256" >hashes.log
sha256sum -c hashes.log
phase=prepare
run_science prepare .venv/bin/python -m scripts.v121_deep_image_paired prepare \
  --queries test.parquet --output queries.jsonl
head -n 16 queries.jsonl >screen-queries.jsonl
router_sha=$(.venv/bin/python - <<'PY'
import hashlib
from pathlib import Path
print(hashlib.sha256(Path('router/manifest.json').read_bytes()).hexdigest())
PY
)
phase=runtime-screen
screen_started=$(date +%s)
run_science screen "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  nominate router "$router_sha" screen-queries.jsonl 10228 512
screen_elapsed=$(( $(date +%s) - screen_started ))
SCREEN_ELAPSED="$screen_elapsed" .venv/bin/python - <<'PY' >screen-decision.json
import json,os
elapsed=int(os.environ['SCREEN_ELAPSED'])
print(json.dumps({'schema':'borsuk-v121-runtime-screen-v1',
  'query_count':16,'elapsed_seconds':elapsed,'maximum_seconds':32,
  'passed':elapsed<=32},sort_keys=True,separators=(',',':')))
PY
if [ "$screen_elapsed" -gt 32 ]; then
  phase=runtime-screen-rejected
  exit 0
fi
phase=nominate
run_science nominate "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  nominate router "$router_sha" queries.jsonl 10228 512
cp nominate.log rosters.jsonl
phase=compose
run_science compose .venv/bin/python -m scripts.v121_deep_image_paired compose \
  --router router --queries queries.jsonl --rosters rosters.jsonl \
  --output requests.jsonl
phase=replay
run_science replay "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  replay-returned mirror/manifest.json sq8.bin mirror/blocks.sha256 requests.jsonl
cp replay.log rust-replay.jsonl
# GT enters only after plans and returned IDs from both arms are sealed.
phase=truth-download
aws s3 cp "$V121_TRUTH_URI" neighbors.parquet --only-show-errors
[ "$(stat -c%s neighbors.parquet)" = "$V121_TRUTH_BYTES" ] || exit 93
printf '%s  neighbors.parquet\n' "$V121_TRUTH_SHA256" >>hashes.log
sha256sum -c hashes.log
phase=reduce
run_science reduce .venv/bin/python -m scripts.v121_deep_image_paired reduce \
  --requests requests.jsonl --replay rust-replay.jsonl \
  --truth neighbors.parquet --evidence evidence.jsonl --summary summary.json
phase=complete
exit 0
