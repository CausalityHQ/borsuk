#!/bin/bash
# One frozen ReLAION-1M development gate on a Causality Spot instance.
set -euo pipefail
root=/mnt/v114-1m-paired
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V114_WALL_SECONDS))
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
      mirror.log mirror-resources.txt manifest-export.log manifest-export-resources.txt \
      prepare-200.log prepare-200-resources.txt score-200.log score-200-resources.txt \
      reduce-200.log reduce-200-resources.txt validate-200.log validate-200-resources.txt \
      prepare-1000.log prepare-1000-resources.txt score-1000.log score-1000-resources.txt \
      reduce-1000.log reduce-1000-resources.txt validate-1000.log validate-1000-resources.txt \
      requests-200.jsonl reference-200.jsonl rust-200.jsonl evidence-200.jsonl \
      summary-200.json validation-200.json requests-1000.jsonl reference-1000.jsonl \
      rust-1000.jsonl evidence-1000.jsonl summary-1000.json validation-1000.json \
      mirror/manifest.json mirror/source.json mirror/blocks.sha256 \
      worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! aws s3 cp "$path" \
        "$V114_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
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
       "mirror.log", "mirror-resources.txt", "manifest-export.log", "manifest-export-resources.txt",
       "prepare-200.log", "prepare-200-resources.txt", "score-200.log", "score-200-resources.txt",
       "reduce-200.log", "reduce-200-resources.txt", "validate-200.log", "validate-200-resources.txt",
       "prepare-1000.log", "prepare-1000-resources.txt", "score-1000.log", "score-1000-resources.txt",
       "reduce-1000.log", "reduce-1000-resources.txt", "validate-1000.log", "validate-1000-resources.txt",
       "requests-200.jsonl", "reference-200.jsonl", "rust-200.jsonl", "evidence-200.jsonl",
       "summary-200.json", "validation-200.json", "requests-1000.jsonl", "reference-1000.jsonl",
       "rust-1000.jsonl", "evidence-1000.jsonl", "summary-1000.json", "validation-1000.json",
       "mirror/manifest.json", "mirror/source.json", "mirror/blocks.sha256",
       "worker.log", "interrupt-stop.txt")
files={}
for name in names:
    path=Path(name)
    if path.is_file():
        files[name]={"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                     "bytes":path.stat().st_size}
code=int(sys.argv[1]); phase=sys.argv[2]
print(json.dumps({
    "schema":"borsuk-v114-1m-paired-spot-v1", "attempt":1,
    "source_commit":os.environ["V114_SOURCE_COMMIT"],
    "instance_id":sys.argv[3], "exit_code":code, "phase":phase,
    "status":"complete" if code==0 and phase=="complete" else "failed",
    "elapsed_seconds":int(sys.argv[5])-int(sys.argv[4]),
    "artifacts":files,
},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V114_OUTPUT_PREFIX/terminal.json" --only-show-errors
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

main_pid=$BASHPID
monitor &
monitor_pid=$!
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time gcc >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup"
export CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target"
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8

phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml

phase=source-download
for role in SOURCE LAYOUT SQ8; do
  case "$role" in
    SOURCE) name=source.parquet ;;
    LAYOUT) name=layout.npy ;;
    SQ8) name=sq8.bin ;;
  esac
  uri_var="V114_${role}_URI"
  bytes_var="V114_${role}_BYTES"
  sha_var="V114_${role}_SHA256"
  aws s3 cp "${!uri_var}" "$name" --only-show-errors
  [ "$(stat -c%s "$name")" = "${!bytes_var}" ] || exit 93
  printf '%s  %s\n' "${!sha_var}" "$name" | sha256sum -c - >>hashes.log
done

# Neither development query nor ground-truth bytes exist during mirror sealing.
phase=mirror
run_science mirror .venv/bin/python -m scripts.v114_1m_mirror \
  --source source.parquet --sq8 sq8.bin --mirror mirror \
  --source-sha256 "$V114_SOURCE_SHA256" --sq8-sha256 "$V114_SQ8_SHA256" \
  --rows 1000000 --dimensions 768 --max-nominees 512
[ -f mirror/source.json ] || exit 92

phase=query-download
for role in QUERIES TRUTH; do
  case "$role" in
    QUERIES) name=queries.parquet ;;
    TRUTH) name=truth.parquet ;;
  esac
  uri_var="V114_${role}_URI"
  bytes_var="V114_${role}_BYTES"
  sha_var="V114_${role}_SHA256"
  aws s3 cp "${!uri_var}" "$name" --only-show-errors
  [ "$(stat -c%s "$name")" = "${!bytes_var}" ] || exit 93
  printf '%s  %s\n' "${!sha_var}" "$name" | sha256sum -c - >>hashes.log
done

phase=manifest-export
run_science manifest-export .venv/bin/python -m scripts.v77_export_manifest \
  --source source.parquet --development-query queries.parquet \
  --ground-truth truth.parquet --layout-order layout.npy --output v77-manifest.bin
printf '%s  v77-manifest.bin\n' "$V114_MANIFEST_SHA256" | sha256sum -c - >>hashes.log

for count in 200 1000; do
  phase="prepare-$count"
  run_science "$phase" .venv/bin/python -m scripts.v114_1m_paired prepare \
    --manifest v77-manifest.bin --mirror mirror --sq8 sq8.bin \
    --queries "$count" --requests "requests-$count.jsonl" \
    --reference "reference-$count.jsonl"
  phase="score-$count"
  run_science "$phase" "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
    mirror/manifest.json sq8.bin mirror/blocks.sha256 "requests-$count.jsonl"
  cp "$phase.log" "rust-$count.jsonl"
  phase="reduce-$count"
  run_science "$phase" .venv/bin/python -m scripts.v114_1m_paired reduce \
    --manifest v77-manifest.bin --mirror mirror --sq8 sq8.bin \
    --queries "$count" --requests "requests-$count.jsonl" \
    --reference "reference-$count.jsonl" \
    --rust "rust-$count.jsonl" --evidence "evidence-$count.jsonl" \
    --summary "summary-$count.json"
  phase="validate-$count"
  run_science "$phase" .venv/bin/python -m scripts.validate_v114_1m_paired \
    --source source.parquet --queries queries.parquet --truth truth.parquet \
    --layout layout.npy --sq8 sq8.bin --manifest v77-manifest.bin \
    --mirror mirror --requests "requests-$count.jsonl" \
    --reference "reference-$count.jsonl" --rust "rust-$count.jsonl" \
    --evidence "evidence-$count.jsonl" --summary "summary-$count.json" \
    --output "validation-$count.json" --query-count "$count"
  phase="gate-$count"
  .venv/bin/python -m scripts.v114_1m_paired gate \
    --queries "$count" --summary "summary-$count.json"
done

phase=complete
exit 0
