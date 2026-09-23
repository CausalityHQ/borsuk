#!/bin/bash
# One frozen ReLAION-1M development replay on Causality Spot.
set -euo pipefail
root=/mnt/v115-returned-replay
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V115_WALL_SECONDS))
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
      compose.log compose-resources.txt replay.log replay-resources.txt \
      validate.log validate-resources.txt composed-requests.jsonl \
      rust-replay.jsonl summary.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! aws s3 cp "$path" \
        "$V115_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('hashes.log','install.log','compile.log','compile-resources.txt',
       'compose.log','compose-resources.txt','replay.log','replay-resources.txt',
       'validate.log','validate-resources.txt','composed-requests.jsonl',
       'rust-replay.jsonl','summary.json','worker.log','interrupt-stop.txt')
files={}
for name in names:
    path=Path(name)
    if path.is_file():
        files[name]={'bytes':path.stat().st_size,
                     'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v115-returned-replay-spot-v1',
    'source_commit':os.environ['V115_SOURCE_COMMIT'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':files},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V115_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
  role=$1; name=$2
  uri_var="V115_${role}_URI"
  sha_var="V115_${role}_SHA256"
  bytes_var="V115_${role}_BYTES"
  aws s3 cp "${!uri_var}" "$name" --only-show-errors
  [ "$(stat -c%s "$name")" = "${!bytes_var}" ] || exit 93
  printf '%s  %s\n' "${!sha_var}" "$name" | sha256sum -c - >>hashes.log
}
main_pid=$BASHPID
monitor & monitor_pid=$!
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time gcc >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target"
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml
phase=source-download
mkdir mirror
download_checked SQ8 sq8.bin
download_checked MIRROR_MANIFEST mirror/manifest.json
download_checked SIDECAR mirror/blocks.sha256
download_checked REQUESTS requests-1000.jsonl
download_checked ROSTERS rust-nominees.jsonl
download_checked REFERENCE reference-1000.jsonl
download_checked EVIDENCE evidence-1000.jsonl
phase=compose
run_science compose .venv/bin/python -m scripts.v115_compose_replay_requests \
  --requests requests-1000.jsonl --rosters rust-nominees.jsonl \
  --output composed-requests.jsonl
phase=replay
run_science replay "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  replay-returned mirror/manifest.json sq8.bin mirror/blocks.sha256 \
  composed-requests.jsonl
cp replay.log rust-replay.jsonl
phase=truth-download
download_checked TRUTH truth.parquet
phase=validate
run_science validate .venv/bin/python -m scripts.validate_v115_returned_replay \
  --requests requests-1000.jsonl --rosters rust-nominees.jsonl \
  --reference reference-1000.jsonl --evidence evidence-1000.jsonl \
  --replay rust-replay.jsonl --truth truth.parquet --output summary.json
phase=complete
exit 0
