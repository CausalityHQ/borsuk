#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT_DIR/docs/research/global-cell-stripe-qualification.json"
OUTPUT="${BORSUK_GLOBAL_CELL_STRIPE_OUTPUT_ROOT:?set BORSUK_GLOBAL_CELL_STRIPE_OUTPUT_ROOT}"
RESULT_URI="${BORSUK_GLOBAL_CELL_STRIPE_RESULT_URI:?set BORSUK_GLOBAL_CELL_STRIPE_RESULT_URI}"
DATASET_DIR="${BORSUK_GROUP_COMMIT_DATASET:?set validated Cohere dataset directory}"
CACHE_ROOT="${BORSUK_GLOBAL_CELL_STRIPE_CACHE_ROOT:-${OUTPUT}.caches}"
BASE_INDEX_URI="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_index_uri"])' "$MANIFEST")"
BASE_SAMPLES_URI="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_samples_uri"])' "$MANIFEST")"
BASE_SOURCE_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_source_sha256"])' "$MANIFEST")"
BASE_MANIFEST_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_manifest_sha256"])' "$MANIFEST")"
BASE_SAMPLES_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_samples_sha256"])' "$MANIFEST")"
BASE_CELL="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_cell"])' "$MANIFEST")"
DATASET_SHA="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dataset_sha256"])' "$MANIFEST")"
MANIFEST_SHA="$(sha256sum "$MANIFEST" | awk '{print $1}')"
CURRENT_CELL=""

[[ "${BORSUK_RUN_GLOBAL_CELL_STRIPES:-0}" == "1" ]] || {
  echo "set BORSUK_RUN_GLOBAL_CELL_STRIPES=1 for production execution" >&2
  exit 2
}
[[ "$BASE_INDEX_URI" == s3://* && "$BASE_SAMPLES_URI" == s3://* ]] || {
  echo "production base artifacts must use s3://" >&2
  exit 3
}
[[ "$RESULT_URI" == s3://* ]] || { echo "production result root must use s3://" >&2; exit 3; }
[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
[[ ! -e "$CACHE_ROOT" ]] || { echo "refusing to reuse cache root $CACHE_ROOT" >&2; exit 3; }

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
    if [[ -n "$CURRENT_CELL" ]]; then
      mkdir -p "$CURRENT_CELL"
      rm -f "$CURRENT_CELL/CELL_COMPLETE"
      printf 'failed\n' > "$CURRENT_CELL/CELL_FAILED"
    fi
    if [[ -d "$OUTPUT" ]]; then
      rm -f "$OUTPUT/GLOBAL_CELL_STRIPE_QUALIFICATION_COMPLETE"
      printf 'failed\n' > "$OUTPUT/GLOBAL_CELL_STRIPE_QUALIFICATION_FAILED"
      sync_results || true
    fi
  fi
  exit "$status"
}
trap failed EXIT

prefix_is_empty "$RESULT_URI" || { echo "refusing to reuse non-empty result prefix" >&2; exit 3; }
s3_marker_exists "${BASE_SAMPLES_URI%/samples.csv}/READ_QUALIFICATION_COMPLETE" || {
  echo "base cell lacks terminal read marker" >&2
  exit 3
}
s3_marker_exists "${BASE_SAMPLES_URI%/samples.csv}/DRAIN_COMPLETE" || {
  echo "base cell lacks terminal drain marker" >&2
  exit 3
}
s3_marker_exists "${BASE_SAMPLES_URI%/samples.csv}/POINT_VISIBILITY_COMPLETE" || {
  echo "base cell lacks terminal visibility marker" >&2
  exit 3
}
s3_marker_exists "${BASE_SAMPLES_URI%/samples.csv}/CELL_FAILED" || {
  echo "base v67 cell lacks its expected terminal failure marker" >&2
  exit 3
}

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

mkdir -p "$OUTPUT/repetitions" "$CACHE_ROOT"
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
  > "$OUTPUT/environment.txt"

RESOURCE_INTERVAL_MS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["resource_sample_interval_ms"])' "$MANIFEST")"
ARM_TIMEOUT_SECONDS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["arm_timeout_seconds"])' "$MANIFEST")"
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
GROUP_BINARY="$TARGET_DIR/release/examples/group_commit_bench"
cargo build --locked --release -p borsuk --example group_commit_bench

while IFS=$'\t' read -r repetition order_position stripe_bytes; do
  stripe_mib="$((stripe_bytes / 1024 / 1024))"
  cell_output="$OUTPUT/repetitions/r$(printf '%02d' "$repetition")/s${stripe_mib}m"
  cache_dir="$CACHE_ROOT/r$(printf '%02d' "$repetition")-s${stripe_mib}m"
  [[ ! -e "$cell_output" && ! -e "$cache_dir" ]] || {
    echo "refusing reused arm output or cache: $cell_output $cache_dir" >&2
    exit 3
  }
  mkdir -p "$(dirname "$cell_output")"
  CURRENT_CELL="$cell_output"
  resource_output="${cell_output}.resources.csv"
  storage_trace_output="${cell_output}.storage-access.csv"
  benchmark_stdout="${cell_output}.benchmark.stdout.log"
  benchmark_stderr="${cell_output}.benchmark.stderr.log"
  set +e
  env \
    BORSUK_GROUP_COMMIT_PROTOCOL=read-qualification \
    BORSUK_GROUP_COMMIT_INDEX_URI="$BASE_INDEX_URI" \
    BORSUK_GROUP_COMMIT_OUTPUT="$cell_output" \
    BORSUK_GROUP_COMMIT_CACHE_DIR="$cache_dir" \
    BORSUK_SOURCE_SHA256="$SOURCE_SHA" \
    BORSUK_GROUP_COMMIT_MANIFEST_SHA256="$MANIFEST_SHA" \
    BORSUK_GROUP_COMMIT_BASE_SOURCE_SHA256="$BASE_SOURCE_SHA" \
    BORSUK_GROUP_COMMIT_BASE_MANIFEST_SHA256="$BASE_MANIFEST_SHA" \
    BORSUK_GROUP_COMMIT_BASE_SAMPLES_SHA256="$BASE_SAMPLES_SHA" \
    BORSUK_GROUP_COMMIT_BASE_SAMPLES="$OUTPUT/base-samples.csv" \
    BORSUK_GROUP_COMMIT_BASE_CELL="$BASE_CELL" \
    BORSUK_GROUP_COMMIT_DATASET="$DATASET_DIR" \
    BORSUK_GROUP_COMMIT_DATASET_SHA256="$DATASET_SHA" \
    BORSUK_GROUP_COMMIT_WRITERS=8 \
    BORSUK_GROUP_COMMIT_OPERATIONS_PER_WRITER=1000 \
    BORSUK_GROUP_COMMIT_RECORDS_PER_OPERATION=16 \
    BORSUK_GROUP_COMMIT_DIMENSIONS=768 \
    BORSUK_GROUP_COMMIT_READ_QUERIES=100 \
    BORSUK_GROUP_COMMIT_MAX_READ_SEGMENTS=4 \
    BORSUK_GROUP_COMMIT_PREFETCH_STRIPE_BYTES="$stripe_bytes" \
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
  mkdir -p "$cell_output"
  [[ -f "$benchmark_stdout" ]] && mv "$benchmark_stdout" "$cell_output/benchmark.stdout.log"
  [[ -f "$benchmark_stderr" ]] && mv "$benchmark_stderr" "$cell_output/benchmark.stderr.log"
  [[ -f "$resource_output" ]] && mv "$resource_output" "$cell_output/resources.csv"
  [[ -f "$storage_trace_output" ]] && mv "$storage_trace_output" "$cell_output/storage-access.csv"
  printf '%s\n' "$status" > "$cell_output/process_exit.txt"
  if (( status != 0 )); then
    exit "$status"
  fi
  [[ -f "$cell_output/READ_QUALIFICATION_COMPLETE" ]] || {
    echo "arm exited without read completion marker" >&2
    exit 1
  }
  [[ -f "$cell_output/resources.csv" && -f "$cell_output/storage-access.csv" ]] || {
    echo "arm lacks resource or storage telemetry" >&2
    exit 1
  }
  printf 'complete\n' > "$cell_output/CELL_COMPLETE"
  sync_results
  CURRENT_CELL=""
done < <(python3 - "$MANIFEST" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1]))
for repetition, order in enumerate(manifest["arm_orders"], 1):
    for position, stripe_bytes in enumerate(order):
        print(repetition, position, stripe_bytes, sep="\t")
PY
)

printf 'complete\n' > "$OUTPUT/GLOBAL_CELL_STRIPE_QUALIFICATION_COMPLETE"
python3 "$ROOT_DIR/scripts/validate_global_cell_stripes.py" \
  --manifest "$MANIFEST" "$OUTPUT" > "$OUTPUT/selection.json.tmp"
mv "$OUTPUT/selection.json.tmp" "$OUTPUT/selection.json"
sync_results
trap - EXIT

