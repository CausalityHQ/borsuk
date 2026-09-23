#!/bin/bash
# Frozen source-only router parity cell. One Spot attempt, one terminal.
set -euo pipefail
root=/mnt/v115-router-parity
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
      router.log router-resources.txt manifest-export.log manifest-export-resources.txt \
      nominate.log nominate-resources.txt validate.log validate-resources.txt \
      router/manifest.json router/summaries.bin router/books.bin router/codes.bin \
      router/low.bin router/step.bin rust-nominees.jsonl parity.json \
      worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! aws s3 cp "$path" \
        "$V115_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then
    code=96
    phase=evidence-upload
  fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib, json, os, sys
from pathlib import Path
names=("hashes.log", "install.log", "compile.log", "compile-resources.txt",
       "router.log", "router-resources.txt", "manifest-export.log",
       "manifest-export-resources.txt", "nominate.log", "nominate-resources.txt",
       "validate.log", "validate-resources.txt", "router/manifest.json",
       "router/summaries.bin", "router/books.bin", "router/codes.bin",
       "router/low.bin", "router/step.bin", "rust-nominees.jsonl", "parity.json",
       "worker.log", "interrupt-stop.txt")
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        artifacts[name]={"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                         "bytes":path.stat().st_size}
code=int(sys.argv[1]); phase=sys.argv[2]
print(json.dumps({
    "schema":"borsuk-v115-source-router-parity-spot-v1", "attempt":1,
    "source_commit":os.environ["V115_SOURCE_COMMIT"],
    "instance_id":sys.argv[3], "exit_code":code, "phase":phase,
    "status":"complete" if code==0 and phase=="complete" else "failed",
    "elapsed_seconds":int(sys.argv[5])-int(sys.argv[4]),
    "artifacts":artifacts,
},sort_keys=True,separators=(',',':')))
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
  label=$1
  shift
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
  role=$1
  name=$2
  uri_var="V115_${role}_URI"
  bytes_var="V115_${role}_BYTES"
  sha_var="V115_${role}_SHA256"
  aws s3 cp "${!uri_var}" "$name" --only-show-errors
  [ "$(stat -c%s "$name")" = "${!bytes_var}" ] || exit 93
  printf '%s  %s\n' "${!sha_var}" "$name" | sha256sum -c - >>hashes.log
}
main_pid=$BASHPID
monitor &
monitor_pid=$!
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

# Source and layout are the only data available when this router is sealed.
phase=source-download
download_checked SOURCE source.parquet
download_checked LAYOUT layout.npy
phase=router
run_science router .venv/bin/python -m scripts.v115_source_router \
  --source source.parquet --layout layout.npy --output router \
  --source-sha256 "$V115_SOURCE_SHA256" \
  --layout-sha256 "$V115_LAYOUT_SHA256" \
  --sq8-sha256 "$V115_SQ8_SHA256" --rows 1000000 --dimensions 768 \
  --page-rows 256 --blocks-per-page 2 --generation 1
router_manifest_sha256=$(sha256sum router/manifest.json | cut -d' ' -f1)

phase=query-download
download_checked QUERIES queries.parquet
download_checked TRUTH truth.parquet
download_checked REQUESTS requests-1000.jsonl
phase=manifest-export
run_science manifest-export .venv/bin/python -m scripts.v77_export_manifest \
  --source source.parquet --development-query queries.parquet \
  --ground-truth truth.parquet --layout-order layout.npy --output v77-manifest.bin
printf '%s  v77-manifest.bin\n' \
  '131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874' \
  | sha256sum -c - >>hashes.log
phase=nominate
run_science nominate "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  nominate router "$router_manifest_sha256" requests-1000.jsonl 1024 512
cp nominate.log rust-nominees.jsonl
phase=validate
run_science validate .venv/bin/python -m scripts.validate_v115_router_parity \
  --router router --manifest v77-manifest.bin --requests requests-1000.jsonl \
  --rust rust-nominees.jsonl --output parity.json \
  --router-manifest-sha256 "$router_manifest_sha256"
phase=complete
exit 0
