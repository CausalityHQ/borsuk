#!/usr/bin/env bash
# Fail-closed durable insert qualification on a realistic immutable base.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
SMOKE="${BORSUK_REALISTIC_DURABLE_WRITE_SMOKE:-0}"
PYTHON="${BORSUK_BENCH_PYTHON:-python3}"

sha256_file() { sha256sum "$1" | awk '{print $1}'; }
json_value() {
  "$PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}
json_array() {
  "$PYTHON" -c 'import json,sys; print(*json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

if [[ "$SMOKE" == "1" ]]; then
  MANIFEST="$ROOT_DIR/docs/research/realistic-durable-write-smoke.json"
  WORK_ROOT="${BORSUK_REALISTIC_DURABLE_WRITE_WORK_ROOT:-$(mktemp -d)}"
  OUTPUT="${BORSUK_REALISTIC_DURABLE_WRITE_OUTPUT_ROOT:-$WORK_ROOT/results}"
  INDEX_ROOT="${BORSUK_REALISTIC_DURABLE_WRITE_INDEX_ROOT:-$WORK_ROOT/indexes}"
  DATASET_DIR="${BORSUK_REALISTIC_DURABLE_WRITE_DATASET:-$WORK_ROOT/smoke-64x8}"
  RESULT_URI=""
  ARCHITECTURE=local
  INSTANCE_TYPE=local
  SOURCE_SHA256="$(git archive --format=tar HEAD | sha256sum | awk '{print $1}')"
  SOURCE_ARCHIVE=""
  mkdir -p "$DATASET_DIR"
  "$PYTHON" - "$DATASET_DIR" <<'PY'
import json, math, sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path(sys.argv[1])
rows, dimensions, queries, neighbors = 64, 8, 10, 10
train = [[float((row + column) % 17) / 17.0 for column in range(dimensions)] for row in range(rows)]
test = train[:queries]
vector_type = pa.list_(pa.float32())
pq.write_table(pa.table({"emb": pa.array(train, type=vector_type)}), root / "train.parquet")
pq.write_table(pa.table({"emb": pa.array(test, type=vector_type)}), root / "test.parquet")
ground_truth = [
    sorted(range(rows), key=lambda row: (math.dist(test[query], train[row]), row))[:neighbors]
    for query in range(queries)
]
pq.write_table(
    pa.table({"neighbors_id": pa.array(ground_truth, type=pa.list_(pa.int64()))}),
    root / "neighbors.parquet",
)
(root / "meta.json").write_text(json.dumps({
    "name": "smoke-64x8", "metric": "euclidean", "dim": dimensions,
    "n_train": rows, "n_test": queries, "k": neighbors,
}, sort_keys=True) + "\n")
(root / "dataset.json").write_text(json.dumps({
    "dataset": "smoke-64x8", "dimensions": dimensions, "scale": rows,
    "source": "deterministic structural smoke; not benchmark evidence",
}, sort_keys=True) + "\n")
PY
else
  [[ "${BORSUK_RUN_REALISTIC_DURABLE_WRITE:-0}" == "1" ]] || {
    echo "set BORSUK_RUN_REALISTIC_DURABLE_WRITE=1 for paid execution" >&2
    exit 2
  }
  MANIFEST="$ROOT_DIR/docs/research/realistic-durable-write-campaign.json"
  OUTPUT="${BORSUK_REALISTIC_DURABLE_WRITE_OUTPUT_ROOT:?set output root}"
  INDEX_ROOT="${BORSUK_REALISTIC_DURABLE_WRITE_INDEX_ROOT:?set fresh index s3 prefix}"
  DATASET_DIR="${BORSUK_REALISTIC_DURABLE_WRITE_DATASET:?set validated dataset directory}"
  RESULT_URI="${BORSUK_REALISTIC_DURABLE_WRITE_RESULT_URI:?set fresh result s3 prefix}"
  ARCHITECTURE="${BORSUK_ARCHITECTURE:?set architecture}"
  INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set instance type}"
  SOURCE_SHA256="${BORSUK_SOURCE_SHA256:?set frozen source SHA-256}"
  SOURCE_ARCHIVE="${BORSUK_SOURCE_ARCHIVE:?set frozen source archive}"
fi

[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
[[ -f "$DATASET_DIR/dataset.json" ]] || { echo "missing dataset descriptor" >&2; exit 3; }
if [[ "$SMOKE" != "1" ]]; then
  [[ "$SOURCE_SHA256" =~ ^[0-9a-f]{64}$ ]] || { echo "invalid frozen source SHA-256" >&2; exit 3; }
  [[ -f "$SOURCE_ARCHIVE" ]] || { echo "missing frozen source archive" >&2; exit 3; }
  [[ "$(sha256_file "$SOURCE_ARCHIVE")" == "$SOURCE_SHA256" ]] || {
    echo "frozen source archive digest mismatch" >&2
    exit 3
  }
  "$PYTHON" scripts/fetch_vdbbench_dataset.py \
    --dataset "$(basename "$DATASET_DIR")" \
    --output-root "$(dirname "$DATASET_DIR")" \
    --check-existing >/dev/null
fi
DATASET_SHA256="$(sha256_file "$DATASET_DIR/dataset.json")"
if [[ "$SMOKE" != "1" && "$DATASET_SHA256" != "$(json_value "$MANIFEST" dataset_sha256)" ]]; then
  echo "dataset descriptor SHA-256 mismatch" >&2
  exit 3
fi

prefix_is_empty() {
  local uri="$1" location bucket prefix count
  location="${uri#s3://}"; bucket="${location%%/*}"; prefix="${location#*/}"
  count="$(aws s3api list-objects-v2 --bucket "$bucket" --prefix "${prefix%/}/" --max-keys 1 --query KeyCount --output text)"
  [[ "$count" == "0" ]]
}
if [[ "$SMOKE" != "1" ]]; then
  [[ "$INDEX_ROOT" == s3://* ]] || { echo "production index root must be s3://" >&2; exit 3; }
  [[ "$RESULT_URI" == s3://* ]] || { echo "production result root must be s3://" >&2; exit 3; }
  index_prefix="${INDEX_ROOT%/}/"
  result_prefix="${RESULT_URI%/}/"
  if [[ "$index_prefix" == "$result_prefix"* || "$result_prefix" == "$index_prefix"* ]]; then
    echo "index and result prefixes must be disjoint" >&2
    exit 3
  fi
  prefix_is_empty "$INDEX_ROOT" || { echo "refusing to reuse non-empty index prefix" >&2; exit 3; }
  prefix_is_empty "$RESULT_URI" || { echo "refusing to reuse non-empty result prefix" >&2; exit 3; }
fi

mkdir -p "$OUTPUT/cells" "$OUTPUT/bases"
cp "$MANIFEST" "$OUTPUT/manifest.json"
cp "$DATASET_DIR/dataset.json" "$OUTPUT/dataset.json"
MANIFEST_SHA256="$(sha256_file "$MANIFEST")"
printf '%s\n' \
  "source_sha256=$SOURCE_SHA256" \
  "dataset_sha256=$DATASET_SHA256" \
  "manifest_sha256=$MANIFEST_SHA256" \
  "architecture=$ARCHITECTURE" \
  "instance_type=$INSTANCE_TYPE" > "$OUTPUT/environment.txt"

sync_results() {
  if [[ -n "$RESULT_URI" ]]; then
    aws s3 sync --only-show-errors \
      --exclude '*/cache/*' \
      --exclude '*/scratch/*' \
      "$OUTPUT" "$RESULT_URI"
  fi
}
fail_campaign() {
  local status=$?
  trap - EXIT
  if (( status != 0 )); then
    rm -f "$OUTPUT/REALISTIC_DURABLE_WRITE_COMPLETE"
    printf 'failed\n' > "$OUTPUT/REALISTIC_DURABLE_WRITE_FAILED"
    sync_results || true
  fi
  exit "$status"
}
trap fail_campaign EXIT

clone_index() {
  local source="$1" destination="$2"
  if [[ "$source" == s3://* ]]; then
    aws s3 sync --only-show-errors "$source" "$destination"
  else
    mkdir -p "$destination"
    cp -a "$source/." "$destination/"
  fi
}
fail_cell() {
  local cell="$1"
  printf 'failed\n' > "$cell/CELL_FAILED"
  echo "cell validation failed: $cell" >&2
  sync_results
  return 1
}

cargo build --locked --release -p borsuk --example production_bench
BINARY="$(cargo metadata --no-deps --format-version 1 | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/examples/production_bench"
read -r -a DATASETS <<<"$(json_array "$MANIFEST" datasets)"
read -r -a BATCHES <<<"$(json_array "$MANIFEST" batch_records)"
REPETITIONS="$(json_value "$MANIFEST" repetitions)"
MINIMUM_BATCHES="$(json_value "$MANIFEST" minimum_raw_batches)"
TIMEOUT_SECONDS="$(json_value "$MANIFEST" cell_timeout_seconds)"
SAMPLE_INTERVAL="$(json_value "$MANIFEST" resource_sample_interval_ms)"

for dataset in "${DATASETS[@]}"; do
  [[ "$DATASET_DIR" == */"$dataset" ]] || { echo "dataset path/name mismatch" >&2; exit 3; }
  for repetition in $(seq 1 "$REPETITIONS"); do
    base_uri="${INDEX_ROOT%/}/$dataset/r$(printf '%02d' "$repetition")/base"
    base_output="$OUTPUT/bases/$dataset/r$(printf '%02d' "$repetition")"
    mkdir -p "$base_output/cache" "$base_output/scratch"
    env \
      BORSUK_BENCH_DATASET="$DATASET_DIR" \
      BORSUK_BENCH_URI="$base_uri" \
      BORSUK_BENCH_CACHE="$base_output/cache" \
      BORSUK_BENCH_OUTPUT_DIR="$base_output" \
      BORSUK_BENCH_BUILD_INDEX=1 \
      BORSUK_BENCH_BUILD_ONLY=1 \
      BORSUK_BENCH_QUERIES=1 \
      "$PYTHON" scripts/benchmark_with_resources.py \
        --output "$base_output/resources.csv" \
        --cache-dir "$base_output/cache" \
        --scratch-dir "$base_output/scratch" \
        --interval-ms "$SAMPLE_INTERVAL" \
        -- "$BINARY"
    if (( repetition % 2 == 1 )); then ORDER=("${BATCHES[@]}"); else
      ORDER=(); for ((index=${#BATCHES[@]}-1; index>=0; index--)); do ORDER+=("${BATCHES[index]}"); done
    fi
    for batch in "${ORDER[@]}"; do
      write_ops="$((batch * MINIMUM_BATCHES))"
      cell_uri="${INDEX_ROOT%/}/$dataset/r$(printf '%02d' "$repetition")/b$batch"
      cell="$OUTPUT/cells/$dataset/r$(printf '%02d' "$repetition")/b$batch"
      mkdir -p "$cell/cache" "$cell/scratch"
      clone_index "$base_uri" "$cell_uri"
      set +e
      env \
        BORSUK_BENCH_DATASET="$DATASET_DIR" \
        BORSUK_BENCH_URI="$cell_uri" \
        BORSUK_BENCH_CACHE="$cell/cache" \
        BORSUK_BENCH_OUTPUT_DIR="$cell" \
        BORSUK_BENCH_BUILD_INDEX=0 \
        BORSUK_BENCH_INSERT_ONLY=1 \
        BORSUK_BENCH_WRITE_BATCH_SIZE="$batch" \
        BORSUK_BENCH_WRITE_OPS="$write_ops" \
        BORSUK_BENCH_QUERIES=1 \
        "$PYTHON" scripts/benchmark_with_resources.py \
          --output "$cell/resources.csv" \
          --cache-dir "$cell/cache" \
          --scratch-dir "$cell/scratch" \
          --interval-ms "$SAMPLE_INTERVAL" \
          -- timeout --signal=TERM --kill-after=30s "$TIMEOUT_SECONDS" "$BINARY"
      status=$?
      set -e
      printf '%s\n' "$status" > "$cell/process_exit.txt"
      if (( status != 0 )); then
        printf 'failed\n' > "$cell/CELL_FAILED"
        sync_results
        exit "$status"
      fi
      if ! "$PYTHON" scripts/validate_benchmark_artifacts.py \
        --directory "$cell" \
        --required bench_write_costs.csv,bench_write_samples.csv,resources.csv; then
        fail_cell "$cell"
        exit 1
      fi
      printf 'complete\n' > "$cell/CELL_COMPLETE"
      if ! "$PYTHON" scripts/validate_realistic_durable_write.py \
        --manifest "$MANIFEST" \
        --expected-dataset-sha "$DATASET_SHA256" \
        --terminal-cell "$dataset/r$repetition/b$batch" \
        "$OUTPUT"; then
        fail_cell "$cell"
        exit 1
      fi
      sync_results
    done
  done
done

printf 'complete\n' > "$OUTPUT/REALISTIC_DURABLE_WRITE_COMPLETE"
"$PYTHON" scripts/validate_realistic_durable_write.py \
  --manifest "$MANIFEST" \
  --expected-dataset-sha "$DATASET_SHA256" \
  "$OUTPUT"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
