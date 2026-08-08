#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${BORSUK_GLOBAL_RANGE_HEDGE_MANIFEST:-$ROOT_DIR/docs/research/global-range-hedge-qualification.json}"

[[ "${BORSUK_RUN_GLOBAL_RANGE_HEDGE:-0}" == "1" ]] || {
  echo "set BORSUK_RUN_GLOBAL_RANGE_HEDGE=1 for production execution" >&2
  exit 2
}

OUTPUT="${BORSUK_GLOBAL_RANGE_HEDGE_OUTPUT_ROOT:?set BORSUK_GLOBAL_RANGE_HEDGE_OUTPUT_ROOT}"
RESULT_URI="${BORSUK_GLOBAL_RANGE_HEDGE_RESULT_URI:?set BORSUK_GLOBAL_RANGE_HEDGE_RESULT_URI}"
DATASET_DIR="${BORSUK_GROUP_COMMIT_DATASET:?set validated Cohere dataset directory}"
BASE_INDEX_URI="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_index_uri"])' "$MANIFEST")"
BASE_SAMPLES_URI="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_samples_uri"])' "$MANIFEST")"
BASE_SOURCE_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_source_sha256"])' "$MANIFEST")"
BASE_MANIFEST_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_manifest_sha256"])' "$MANIFEST")"
BASE_SAMPLES_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_samples_sha256"])' "$MANIFEST")"
BASE_CELL="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_cell"])' "$MANIFEST")"
DATASET_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dataset_sha256"])' "$MANIFEST")"
COMPLETE_MARKER="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["root_complete_marker"])' "$MANIFEST")"
FAILED_MARKER="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["root_failure_marker"])' "$MANIFEST")"
MANIFEST_SHA="$(sha256sum "$MANIFEST" | awk '{print $1}')"
CURRENT_ARM=""

[[ "$BASE_INDEX_URI" == s3://* && "$BASE_SAMPLES_URI" == s3://* ]] || {
  echo "production base artifacts must use s3://" >&2
  exit 3
}
[[ "$RESULT_URI" == s3://* ]] || { echo "production result root must use s3://" >&2; exit 3; }
[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }

prefix_is_empty() {
  local uri="$1" location bucket prefix count
  location="${uri#s3://}"
  bucket="${location%%/*}"
  prefix="${location#*/}"
  count="$(aws s3api list-objects-v2 --bucket "$bucket" --prefix "${prefix%/}/" --max-keys 1 --query KeyCount --output text)"
  [[ "$count" == "0" ]]
}

s3_marker_exists() {
  local uri="$1" location bucket key
  location="${uri#s3://}"
  bucket="${location%%/*}"
  key="${location#*/}"
  aws s3api head-object --bucket "$bucket" --key "$key" >/dev/null
}

sync_results() {
  aws s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
}

failed() {
  local status=$?
  if (( status != 0 )); then
    if [[ -n "$CURRENT_ARM" ]]; then
      mkdir -p "$CURRENT_ARM"
      rm -f "$CURRENT_ARM/CELL_COMPLETE"
      printf 'failed\n' > "$CURRENT_ARM/CELL_FAILED"
    fi
    if [[ -d "$OUTPUT" ]]; then
      rm -f "$OUTPUT/$COMPLETE_MARKER"
      printf 'failed\n' > "$OUTPUT/$FAILED_MARKER"
      sync_results || true
    fi
  fi
  exit "$status"
}
trap failed EXIT

prefix_is_empty "$RESULT_URI" || { echo "refusing to reuse non-empty result prefix" >&2; exit 3; }
base_result_root="${BASE_SAMPLES_URI%/samples.csv}"
for marker in READ_QUALIFICATION_COMPLETE DRAIN_COMPLETE POINT_VISIBILITY_COMPLETE CELL_FAILED; do
  s3_marker_exists "$base_result_root/$marker" || {
    echo "base v67 cell lacks terminal marker $marker" >&2
    exit 3
  }
done

SOURCE_FROM_GIT=0
if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  SOURCE_FROM_GIT=1
  SOURCE_SHA="$(git -C "$ROOT_DIR" archive --format=tar HEAD | sha256sum | awk '{print $1}')"
  [[ -z "$(git -C "$ROOT_DIR" status --porcelain)" ]] || {
    echo "production execution requires a clean tracked worktree" >&2
    exit 3
  }
else
  SOURCE_ARCHIVE="${BORSUK_SOURCE_ARCHIVE:?set preserved source archive for extracted source}"
  [[ -f "$SOURCE_ARCHIVE" ]] || { echo "missing preserved source archive" >&2; exit 3; }
  SOURCE_SHA="$(sha256sum "$SOURCE_ARCHIVE" | awk '{print $1}')"
  source_check="$(mktemp -d)"
  tar -xf "$SOURCE_ARCHIVE" -C "$source_check"
  diff -qr "$ROOT_DIR" "$source_check" >/dev/null || {
    echo "extracted source differs from its preserved source archive" >&2
    exit 3
  }
  rm -rf "$source_check"
fi

[[ -f "$DATASET_DIR/dataset.json" && -f "$DATASET_DIR/train.parquet" ]] || {
  echo "missing pinned Cohere dataset" >&2
  exit 3
}
[[ "$(sha256sum "$DATASET_DIR/dataset.json" | awk '{print $1}')" == "$DATASET_SHA" ]] || {
  echo "dataset descriptor SHA-256 mismatch" >&2
  exit 3
}

mkdir -p "$OUTPUT/repetitions"
cp "$MANIFEST" "$OUTPUT/manifest.json"
aws s3 cp "$BASE_SAMPLES_URI" "$OUTPUT/base-samples.csv" --only-show-errors
[[ "$(sha256sum "$OUTPUT/base-samples.csv" | awk '{print $1}')" == "$BASE_SAMPLES_SHA" ]] || {
  echo "base samples SHA-256 mismatch" >&2
  exit 3
}
printf '%s\n' \
  "source_sha256=$SOURCE_SHA" \
  "manifest_sha256=$MANIFEST_SHA" \
  "base_source_sha256=$BASE_SOURCE_SHA" \
  "base_manifest_sha256=$BASE_MANIFEST_SHA" \
  "base_samples_sha256=$BASE_SAMPLES_SHA" \
  "dataset_sha256=$DATASET_SHA" \
  "base_cell=$BASE_CELL" \
  "base_index_uri=$BASE_INDEX_URI" \
  "disk_cache_enabled=false" \
  > "$OUTPUT/environment.txt"

RESOURCE_INTERVAL_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["resource_sample_interval_ms"])' "$MANIFEST")"
ARM_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["arm_timeout_seconds"])' "$MANIFEST")"
WRITERS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["writers"])' "$MANIFEST")"
OPERATIONS_PER_WRITER="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operations_per_writer"])' "$MANIFEST")"
RECORDS_PER_OPERATION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["records_per_operation"])' "$MANIFEST")"
DIMENSIONS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dimensions"])' "$MANIFEST")"
READ_WRITER="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["read_writer"])' "$MANIFEST")"
READ_QUERIES="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["queries_per_arm"])' "$MANIFEST")"
MAX_READ_SEGMENTS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_read_segments"])' "$MANIFEST")"
STRIPE_BYTES="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["stripe_bytes"])' "$MANIFEST")"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
GROUP_BINARY="$TARGET_DIR/release/examples/group_commit_bench"
cargo build --locked --release -p borsuk --example group_commit_bench

while IFS=$'\t' read -r repetition order_position arm_name hedge_after; do
  arm_output="$OUTPUT/repetitions/r$(printf '%02d' "$repetition")/$arm_name"
  [[ ! -e "$arm_output" ]] || { echo "refusing reused arm output $arm_output" >&2; exit 3; }
  mkdir -p "$(dirname "$arm_output")"
  CURRENT_ARM="$arm_output"
  resource_output="${arm_output}.resources.csv"
  storage_trace_output="${arm_output}.storage-access.csv"
  benchmark_stdout="${arm_output}.benchmark.stdout.log"
  benchmark_stderr="${arm_output}.benchmark.stderr.log"
  set +e
  env -u BORSUK_GROUP_COMMIT_CACHE_DIR \
    BORSUK_GROUP_COMMIT_PROTOCOL=read-hedge-qualification \
    BORSUK_GROUP_COMMIT_INDEX_URI="$BASE_INDEX_URI" \
    BORSUK_GROUP_COMMIT_OUTPUT="$arm_output" \
    BORSUK_SOURCE_SHA256="$SOURCE_SHA" \
    BORSUK_GROUP_COMMIT_MANIFEST_SHA256="$MANIFEST_SHA" \
    BORSUK_GROUP_COMMIT_BASE_SOURCE_SHA256="$BASE_SOURCE_SHA" \
    BORSUK_GROUP_COMMIT_BASE_MANIFEST_SHA256="$BASE_MANIFEST_SHA" \
    BORSUK_GROUP_COMMIT_BASE_SAMPLES_SHA256="$BASE_SAMPLES_SHA" \
    BORSUK_GROUP_COMMIT_BASE_SAMPLES="$OUTPUT/base-samples.csv" \
    BORSUK_GROUP_COMMIT_BASE_CELL="$BASE_CELL" \
    BORSUK_GROUP_COMMIT_DATASET="$DATASET_DIR" \
    BORSUK_GROUP_COMMIT_DATASET_SHA256="$DATASET_SHA" \
    BORSUK_GROUP_COMMIT_WRITERS="$WRITERS" \
    BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER="$OPERATIONS_PER_WRITER" \
    BORSUK_GROUP_COMMIT_RECORDS_PER_OPERATION="$RECORDS_PER_OPERATION" \
    BORSUK_GROUP_COMMIT_DIMENSIONS="$DIMENSIONS" \
    BORSUK_GROUP_COMMIT_READ_WRITER="$READ_WRITER" \
    BORSUK_GROUP_COMMIT_READ_QUERIES="$READ_QUERIES" \
    BORSUK_GROUP_COMMIT_MAX_READ_SEGMENTS="$MAX_READ_SEGMENTS" \
    BORSUK_GROUP_COMMIT_PREFETCH_STRIPE_BYTES="$STRIPE_BYTES" \
    BORSUK_GROUP_COMMIT_HEDGE_AFTER_MS="$hedge_after" \
    BORSUK_GROUP_COMMIT_READ_REPETITION="$repetition" \
    BORSUK_GROUP_COMMIT_READ_ORDER_POSITION="$order_position" \
    BORSUK_STORAGE_TRACE="$storage_trace_output" \
    python3 "$ROOT_DIR/scripts/benchmark_with_resources.py" \
      --output "$resource_output" \
      --interval-ms "$RESOURCE_INTERVAL_MS" \
      -- timeout --signal=TERM --kill-after=30s "$ARM_TIMEOUT_SECONDS" "$GROUP_BINARY" \
      >"$benchmark_stdout" 2>"$benchmark_stderr"
  status=$?
  set -e
  mkdir -p "$arm_output"
  [[ -f "$benchmark_stdout" ]] && mv "$benchmark_stdout" "$arm_output/benchmark.stdout.log"
  [[ -f "$benchmark_stderr" ]] && mv "$benchmark_stderr" "$arm_output/benchmark.stderr.log"
  [[ -f "$resource_output" ]] && mv "$resource_output" "$arm_output/resources.csv"
  [[ -f "$storage_trace_output" ]] && mv "$storage_trace_output" "$arm_output/storage-access.csv"
  printf '%s\n' "$status" > "$arm_output/process_exit.txt"
  (( status == 0 )) || exit "$status"
  [[ -f "$arm_output/READ_HEDGE_QUALIFICATION_COMPLETE" ]] || {
    echo "arm exited without read hedge completion marker" >&2
    exit 1
  }
  [[ -s "$arm_output/resources.csv" && -s "$arm_output/storage-access.csv" ]] || {
    echo "arm lacks resource or storage telemetry" >&2
    exit 1
  }
  printf 'complete\n' > "$arm_output/CELL_COMPLETE"
  sync_results
  CURRENT_ARM=""
done < <(python3 - "$MANIFEST" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1]))
for repetition, order in enumerate(manifest["arm_orders"], 1):
    for position, arm_name in enumerate(order):
        print(repetition, position, arm_name, manifest["hedge_after_ms"][arm_name], sep="\t")
PY
)

printf 'complete\n' > "$OUTPUT/$COMPLETE_MARKER"
python3 "$ROOT_DIR/scripts/validate_global_range_hedge_qualification.py" \
  --manifest "$MANIFEST" "$OUTPUT" > "$OUTPUT/selection.json.tmp"
mv "$OUTPUT/selection.json.tmp" "$OUTPUT/selection.json"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
