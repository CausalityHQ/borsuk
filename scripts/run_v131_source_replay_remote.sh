#!/bin/bash
# One immutable, development-only source replay on Causality Spot.
set -euo pipefail
root=/mnt/v131-source-replay
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V131_WALL_SECONDS))
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
  for path in hashes.log install.log compile.log compile-resources.txt \
      preflight.log preflight-resources.txt prepare.log prepare-resources.txt \
      evaluate.log evaluate-resources.txt source.raw \
      built/source-tier.bin built/source-id-map.bin built/replay.jsonl \
      summary.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V131_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
names=('hashes.log','install.log','compile.log','compile-resources.txt',
       'preflight.log','preflight-resources.txt','prepare.log',
       'prepare-resources.txt','evaluate.log','evaluate-resources.txt',
       'source.raw','built/source-tier.bin','built/source-id-map.bin',
       'built/replay.jsonl','summary.json','worker.log','interrupt-stop.txt')
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
print(json.dumps({'schema':'borsuk-v131-source-replay-spot-v1',
  'source_commit':os.environ['V131_SOURCE_COMMIT'],
  'source_archive_sha256':os.environ['V131_ARCHIVE_SHA256'],
  'v122_terminal_sha256':os.environ['V131_V122_TERMINAL_SHA256'],
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'phase':phase,
  'status':'complete' if code==0 and phase=='complete' else 'failed',
  'elapsed_seconds':int(os.environ['ENDED_EPOCH'])-int(os.environ['STARTED_EPOCH']),
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V131_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
  role=$1; file=$2
  uri_var=V131_${role}_URI
  sha_var=V131_${role}_SHA256
  bytes_var=V131_${role}_BYTES
  uri=${!uri_var}; digest=${!sha_var}; count=${!bytes_var}
  aws s3 cp "$uri" "$file" --only-show-errors
  [ "$(stat -c%s "$file")" = "$count" ] || exit 93
  printf '%s  %s\n' "$digest" "$file" | sha256sum -c - >/dev/null
  printf '%s  %s\n' "$digest" "$file" >>hashes.log
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
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=input-download
download_checked SOURCE source.parquet
download_checked SQ8 sq8.bin
download_checked LOW low.bin
download_checked STEP step.bin
download_checked QUERIES queries.jsonl
download_checked TRUTH truth.npy
download_checked EVIDENCE evidence.jsonl
download_checked BASELINE summary-v122.json
phase=preflight
run_science preflight .venv/bin/python -m scripts.v131_source_replay_preflight \
  queries.jsonl truth.npy evidence.jsonl summary-v122.json
phase=prepare
run_science prepare .venv/bin/python -m scripts.v131_source_replay_prepare \
  source.parquet source.raw
phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/crates/borsuk/Cargo.toml --example v131_source_replay
phase=evaluate
run_science evaluate "$CARGO_TARGET_DIR/release/examples/v131_source_replay" \
  source.raw sq8.bin low.bin step.bin queries.jsonl evidence.jsonl \
  built summary.json
phase=complete
exit 0
