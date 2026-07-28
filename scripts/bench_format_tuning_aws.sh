#!/usr/bin/env bash
# Targeted physical-layout sweep. This is exploratory format qualification and
# deliberately cannot launch the full BORSUK publication matrix.
set -euo pipefail

: "${BORSUK_S3_BUCKET:?set BORSUK_S3_BUCKET to the bucket name, without s3://}"
: "${BORSUK_FORMAT_RUN_ID:?set BORSUK_FORMAT_RUN_ID}"

REGION="${AWS_REGION:-eu-central-1}"
TUNING_SCOPE="${BORSUK_FORMAT_TUNING_SCOPE:-base}"
case "$TUNING_SCOPE" in
  base) TUNING_NAMESPACE="format-tuning" ;;
  range-cap) TUNING_NAMESPACE="format-range-cap" ;;
  *)
    echo "BORSUK_FORMAT_TUNING_SCOPE must be base or range-cap" >&2
    exit 2
    ;;
esac
ROOT="${BORSUK_FORMAT_ROOT:-/home/ec2-user/$TUNING_NAMESPACE-results/$BORSUK_FORMAT_RUN_ID}"
S3_RESULT_PREFIX="${BORSUK_FORMAT_RESULT_PREFIX:-$TUNING_NAMESPACE/results/$BORSUK_FORMAT_RUN_ID}"
S3_DATA_PREFIX="${BORSUK_FORMAT_DATA_PREFIX:-$TUNING_NAMESPACE/data/$BORSUK_FORMAT_RUN_ID}"
PYTHON="${BORSUK_FORMAT_PYTHON:-.venv-format/bin/python}"
REPETITIONS="${BORSUK_FORMAT_REPETITIONS:-30}"
WARMUPS="${BORSUK_FORMAT_WARMUPS:-3}"

cd "$(dirname "$0")/.."
mkdir -p "$ROOT"
exec > >(tee -a "$ROOT/campaign.log") 2>&1
{
  printf '%s\n' \
    "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "source_sha256=${BORSUK_SOURCE_SHA256:-unknown}" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-unknown}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-unknown}" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN)" \
    "memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINTS,FSTYPE
} > "$ROOT/environment.txt"
aws --region "$REGION" s3 cp \
  "$ROOT/environment.txt" \
  "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/environment.txt" \
  --only-show-errors

printf '%s\n' "run_id=$BORSUK_FORMAT_RUN_ID" "region=$REGION" "root=$ROOT"
printf '%s\n' "source_sha256=${BORSUK_SOURCE_SHA256:-unknown}"

if [[ ! -x "$PYTHON" ]]; then
  command -v uv >/dev/null 2>&1 || {
    echo "uv is required to install the pinned Python format environment" >&2
    exit 2
  }
  uv venv .venv-format --python 3.13
fi
uv pip install --python "$PYTHON" -r scripts/requirements-format-bench.txt

echo "running tuning dependency preflight"
"$PYTHON" - <<'PY'
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, "scripts")
from benchmark_table_formats import create_table, load_dependencies, write_format

_, _, _, parquet = load_dependencies()
source = create_table(10_000, 64, 0xB05, "variable")
with tempfile.TemporaryDirectory(prefix="borsuk-tuning-preflight-") as root:
    path = Path(root) / "table.parquet"
    write_format("parquet", source, path, 8_192)
    if not parquet.read_table(path).equals(source):
        raise SystemExit("tuning dependency preflight corrupted Parquet")
print("tuning dependency preflight passed")
PY

run_case() {
  local family="$1"
  local profile="$2"
  local backend="$3"
  local case_label="$4"
  local format_name="$5"
  shift 5
  local case_root="$ROOT/$family/$profile/$backend/$case_label"
  local result_root="$case_root/results"
  local marker="$case_root/validated.ok"
  if [[ -f "$marker" ]]; then
    echo "skip validated case $family/$profile/$backend/$case_label"
    return
  fi
  if [[ -e "$case_root" ]]; then
    echo "partial tuning case exists; refusing to mix repetitions: $case_root" >&2
    exit 3
  fi
  mkdir -p "$result_root" "$case_root/scratch"
  local backend_args=(--backend "$backend")
  if [[ "$backend" == "s3" ]]; then
    backend_args+=(
      --s3-bucket "$BORSUK_S3_BUCKET"
      --s3-prefix "$S3_DATA_PREFIX/$family/$profile/$case_label"
      --aws-region "$REGION"
      --cache-profile uncached
    )
  else
    backend_args+=(--cache-profile disk_cached)
  fi
  "$PYTHON" scripts/benchmark_with_resources.py \
    --output "$result_root/resources.csv" \
    --scratch-dir "$case_root/scratch" \
    --interval-ms 100 \
    -- "$PYTHON" "$@" \
      --output-dir "$result_root" \
      --formats "$format_name" \
      --repetitions "$REPETITIONS" \
      --warmups "$WARMUPS" \
      "${backend_args[@]}"
  "$PYTHON" scripts/validate_format_qualification.py \
    --directory "$result_root" \
    --expected-samples "$REPETITIONS"
  "$PYTHON" scripts/render_resource_charts.py \
    --experiment-root "$result_root" \
    --output-dir "$case_root/charts" \
    --prefix "$family-$profile-$backend-$case_label"
  printf '%s\n' "validated $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$marker"
  aws --region "$REGION" s3 sync \
    "$case_root" \
    "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/$family/$profile/$backend/$case_label" \
    --only-show-errors
}

if [[ "$TUNING_SCOPE" == "base" ]]; then
  for backend in local_disk s3; do
    for row_group_rows in 8192 32768 131072 524288; do
      run_case table table-1m "$backend" "parquet-rg-$row_group_rows" parquet \
        scripts/benchmark_table_formats.py \
        --rows 1000000 \
        --code-width 64 \
        --code-type variable \
        --row-group-rows "$row_group_rows"
    done

    for arrow_max_gap_bytes in 65536 262144 1048576 4194304; do
      run_case ann vector-250k-128 "$backend" \
        "arrow-gap-$arrow_max_gap_bytes" arrow-ipc \
        scripts/benchmark_vector_formats.py \
        --rows 250000 \
        --dimensions 128 \
        --element-type float32 \
        --batch-rows 32768 \
        --selected-rows 10,100,1000 \
        --patterns clustered,scattered \
        --arrow-max-gap-bytes "$arrow_max_gap_bytes" \
        --arrow-max-parallel 10
      run_case ann vector-25k-960 "$backend" \
        "arrow-gap-$arrow_max_gap_bytes" arrow-ipc \
        scripts/benchmark_vector_formats.py \
        --rows 25000 \
        --dimensions 960 \
        --element-type float32 \
        --batch-rows 4369 \
        --selected-rows 10,100,1000 \
        --patterns clustered,scattered \
        --arrow-max-gap-bytes "$arrow_max_gap_bytes" \
        --arrow-max-parallel 10
    done
  done
else
  for backend in local_disk s3; do
    for range_config in \
      262144:8388608 \
      1048576:4194304 \
      1048576:8388608 \
      1048576:16777216 \
      4194304:8388608
    do
      IFS=: read -r arrow_max_gap_bytes arrow_max_range_bytes <<< "$range_config"
      case_label="arrow-gap-$arrow_max_gap_bytes-cap-$arrow_max_range_bytes"
      run_case ann vector-250k-128 "$backend" "$case_label" arrow-ipc \
        scripts/benchmark_vector_formats.py \
        --rows 250000 \
        --dimensions 128 \
        --element-type float32 \
        --batch-rows 32768 \
        --selected-rows 10,100,1000 \
        --patterns clustered,scattered \
        --arrow-max-gap-bytes "$arrow_max_gap_bytes" \
        --arrow-max-range-bytes "$arrow_max_range_bytes" \
        --arrow-max-parallel 10
      run_case ann vector-25k-960 "$backend" "$case_label" arrow-ipc \
        scripts/benchmark_vector_formats.py \
        --rows 25000 \
        --dimensions 960 \
        --element-type float32 \
        --batch-rows 4369 \
        --selected-rows 10,100,1000 \
        --patterns clustered,scattered \
        --arrow-max-gap-bytes "$arrow_max_gap_bytes" \
        --arrow-max-range-bytes "$arrow_max_range_bytes" \
        --arrow-max-parallel 10
    done
  done
fi

printf '%s\n' \
  "Format tuning completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)." \
  "No full BORSUK publication benchmark was started." \
  > "$ROOT/FORMAT_TUNING_COMPLETE"
aws --region "$REGION" s3 cp \
  "$ROOT/FORMAT_TUNING_COMPLETE" \
  "s3://$BORSUK_S3_BUCKET/$S3_RESULT_PREFIX/FORMAT_TUNING_COMPLETE" \
  --only-show-errors
echo "FORMAT_TUNING_COMPLETE"
