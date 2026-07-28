#!/usr/bin/env bash
# Fresh SIFT-1M write-to-serve lifecycle qualification on the production layout.
set -euo pipefail

SCRIPT_PATH="$(
  cd "$(dirname "${BASH_SOURCE[0]}")"
  printf '%s/%s' "$PWD" "$(basename "${BASH_SOURCE[0]}")"
)"
cd "$(dirname "$0")/.."

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

EXECUTE="${BORSUK_LIFECYCLE_EXECUTE:-0}"
RUN_ID="${BORSUK_LIFECYCLE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
DATASET="${BORSUK_LIFECYCLE_DATASET:-/home/ec2-user/borsuk-datasets/sift-128}"
S3_BUCKET="${BORSUK_S3_BUCKET:-}"
RESULT_URI="${BORSUK_LIFECYCLE_RESULT_URI:-}"
OUT="${BORSUK_LIFECYCLE_OUT:-docs/web/assets/benchmarks/production-lifecycle/$RUN_ID}"
QUERIES="${BORSUK_LIFECYCLE_QUERIES:-100}"
QUERY_SEED="${BORSUK_LIFECYCLE_QUERY_SEED:-20260729}"
NPROBES="${BORSUK_LIFECYCLE_NPROBES:-8}"
CANDIDATES="${BORSUK_LIFECYCLE_CANDIDATES:-320}"
SEGMENT_TABLE_FORMAT=parquet
WAL_TABLE_FORMAT=parquet
REGION="${AWS_REGION:-eu-central-1}"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:-not-provided}"
SOURCE_ARCHIVE="${BORSUK_SOURCE_ARCHIVE:-}"
RUNNER_SHA256="$(sha256_file "$SCRIPT_PATH")"

printf '%s\n' \
  "run_id=$RUN_ID" \
  "dataset=$DATASET" \
  "queries=$QUERIES" \
  "query_seed=$QUERY_SEED" \
  "nprobes=$NPROBES" \
  "candidates=$CANDIDATES" \
  "segment_table_format=$SEGMENT_TABLE_FORMAT" \
  "wal_table_format=$WAL_TABLE_FORMAT" \
  "output=$OUT"

if [[ "$EXECUTE" != "1" ]]; then
  echo "dry run only; set BORSUK_LIFECYCLE_EXECUTE=1 and BORSUK_RUN_PRODUCTION_LIFECYCLE=1"
  exit 0
fi
if [[ "${BORSUK_RUN_PRODUCTION_LIFECYCLE:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_PRODUCTION_LIFECYCLE=1" >&2
  exit 2
fi
if [[ ! "$SOURCE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "BORSUK_SOURCE_SHA256 must identify the frozen source archive" >&2
  exit 2
fi
if [[ ! -f "$SOURCE_ARCHIVE" ]]; then
  echo "BORSUK_SOURCE_ARCHIVE must point to the frozen source archive" >&2
  exit 2
fi
if [[ "$(sha256_file "$SOURCE_ARCHIVE")" != "$SOURCE_SHA256" ]]; then
  echo "frozen source archive digest mismatch" >&2
  exit 2
fi
if [[ -z "$S3_BUCKET" || "$S3_BUCKET" != s3://* ]]; then
  echo "BORSUK_S3_BUCKET must be a writable s3:// prefix" >&2
  exit 2
fi
if [[ -z "$RESULT_URI" || "$RESULT_URI" != s3://* ]]; then
  echo "BORSUK_LIFECYCLE_RESULT_URI must be a fresh s3:// prefix" >&2
  exit 2
fi
if [[ ! -d "$DATASET" ]]; then
  echo "missing SIFT-1M dataset directory: $DATASET" >&2
  exit 3
fi
if [[ -e "$OUT" ]]; then
  echo "refusing to overwrite an existing lifecycle run: $OUT" >&2
  exit 3
fi

INDEX_URI="${S3_BUCKET%/}/production-lifecycle/$RUN_ID"
index_root="${INDEX_URI%/}"
result_root="${RESULT_URI%/}"
if [[ "$result_root/" == "$index_root/"* || "$index_root/" == "$result_root/"* ]]; then
  echo "lifecycle result and index prefixes must be disjoint" >&2
  exit 3
fi
s3_location="${INDEX_URI#s3://}"
s3_bucket="${s3_location%%/*}"
s3_prefix="${s3_location#*/}"
if [[ "$s3_bucket" == "$s3_prefix" ]]; then
  s3_prefix=""
fi
existing="$(
  aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$s3_bucket" \
    --prefix "${s3_prefix%/}/" \
    --max-keys 1 \
    --query KeyCount \
    --output text
)"
if [[ "$existing" != "0" ]]; then
  echo "refusing to reuse non-empty lifecycle index prefix: $INDEX_URI" >&2
  exit 3
fi
result_location="${RESULT_URI#s3://}"
result_bucket="${result_location%%/*}"
result_prefix="${result_location#*/}"
if [[ "$result_bucket" == "$result_prefix" ]]; then
  result_prefix=""
fi
result_existing="$(
  aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$result_bucket" \
    --prefix "${result_prefix%/}/" \
    --max-keys 1 \
    --query KeyCount \
    --output text
)"
if [[ "$result_existing" != "0" ]]; then
  echo "refusing to reuse non-empty lifecycle result prefix: $RESULT_URI" >&2
  exit 3
fi

mkdir -p "$OUT/cache" "$OUT/scratch" "$OUT/charts/recall-latency" \
  "$OUT/charts/resources"
{
  printf '%s\n' \
    "run_id=$RUN_ID" \
    "source_sha256=$SOURCE_SHA256" \
    "runner_sha256=$RUNNER_SHA256" \
    "dataset=sift-128" \
    "index_uri=$INDEX_URI" \
    "queries=$QUERIES" \
    "query_seed=$QUERY_SEED" \
    "nprobes=$NPROBES" \
    "candidates=$CANDIDATES" \
    "segment_table_format=$SEGMENT_TABLE_FORMAT" \
    "wal_table_format=$WAL_TABLE_FORMAT" \
    "read_only=0" \
    "skip_exact_recall=1"
} > "$OUT/protocol.txt"

sync_results() {
  aws --region "$REGION" s3 sync "$OUT" "$RESULT_URI" \
    --exclude 'cache/*' \
    --exclude 'scratch/*' \
    --only-show-errors
}

finalize() {
  local status=$?
  trap - EXIT
  if ((status != 0)); then
    rm -f "$OUT/PRODUCTION_LIFECYCLE_COMPLETE"
    printf 'failed\n' > "$OUT/PRODUCTION_LIFECYCLE_FAILED"
  fi
  if ! sync_results; then
    rm -f "$OUT/PRODUCTION_LIFECYCLE_COMPLETE"
    printf 'failed\n' > "$OUT/PRODUCTION_LIFECYCLE_FAILED"
    sync_results || true
    ((status != 0)) || status=1
  fi
  exit "$status"
}
trap finalize EXIT

cargo build --locked --release -p borsuk --example production_bench

env \
  AWS_REGION="$REGION" \
  AWS_DEFAULT_REGION="$REGION" \
  BORSUK_BENCH_DATASET="$DATASET" \
  BORSUK_BENCH_URI="$INDEX_URI" \
  BORSUK_BENCH_CACHE="$OUT/cache" \
  BORSUK_BENCH_OUTPUT_DIR="$OUT" \
  BORSUK_BENCH_QUERY_SEED="$QUERY_SEED" \
  BORSUK_BENCH_REPETITION_ID=r01 \
  BORSUK_BENCH_QUERIES="$QUERIES" \
  BORSUK_BENCH_UNCACHED_QUERIES="$QUERIES" \
  BORSUK_BENCH_NPROBES="$NPROBES" \
  BORSUK_BENCH_CANDIDATES="$CANDIDATES" \
  BORSUK_BENCH_VECTOR_ELEMENT_TYPE=float32 \
  BORSUK_SEGMENT_TABLE_FORMAT="$SEGMENT_TABLE_FORMAT" \
  BORSUK_WAL_TABLE_FORMAT="$WAL_TABLE_FORMAT" \
  BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only \
  BORSUK_BENCH_GLOBAL_SCAN_CODEC=srht-pq-scan \
  BORSUK_BENCH_RECALL_LEAF_MODE=srht-pq-scan \
  BORSUK_BENCH_SERVING_LEAF_MODE=srht-pq-scan \
  BORSUK_BENCH_SERVING_NPROBE="$NPROBES" \
  BORSUK_BENCH_SERVING_CANDIDATES="$CANDIDATES" \
  BORSUK_BENCH_CACHE_EXECUTION=scan \
  BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
  BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
  BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
  BORSUK_BENCH_READ_ONLY=0 \
  BORSUK_BENCH_RECALL_ONLY=0 \
  BORSUK_BENCH_SKIP_EXACT_RECALL=1 \
  python3 scripts/benchmark_with_resources.py \
    --output "$OUT/resources.csv" \
    --cache-dir "$OUT/cache" \
    --scratch-dir "$OUT/scratch" \
    -- target/release/examples/production_bench

python3 scripts/validate_benchmark_artifacts.py \
  --directory "$OUT" \
  --expected-codec srht-pq-scan \
  --required bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,bench_cache_coverage.csv,bench_write_costs.csv,bench_write_samples.csv,bench_lifecycle.csv,bench_mutation_queries.csv,bench_mutation_query_samples.csv,resources.csv

python3 scripts/render_recall_latency_charts.py \
  --input "$OUT/bench_recall_latency.csv" \
  --dataset sift-128 \
  --output-dir "$OUT/charts/recall-latency" \
  --subtitle 'AWS eu-central-1 · fresh v16 lifecycle · Parquet production layout'
python3 scripts/render_resource_charts.py \
  --experiment-root "$OUT" \
  --output-dir "$OUT/charts/resources" \
  --prefix resources

printf 'complete\n' > "$OUT/PRODUCTION_LIFECYCLE_COMPLETE"
echo "wrote $OUT"
