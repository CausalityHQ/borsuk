#!/usr/bin/env bash
# Fresh-build 100M lower-probe qualification for the packed-code reader.
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

EXECUTE="${BORSUK_100M_EXECUTE:-0}"
DATASET="${BORSUK_100M_DATASET:-/home/ec2-user/borsuk-datasets/synthetic-clustered-100m-96}"
RUN_ID="${BORSUK_100M_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
S3_BUCKET="${BORSUK_S3_BUCKET:-}"
RESULT_URI="${BORSUK_100M_RESULT_URI:-}"
OUT="${BORSUK_100M_OUT:-docs/web/assets/benchmarks/100m-code-ranges/$RUN_ID}"
PROBES="${BORSUK_100M_PROBES:-4,8,12,16,24,32,48,64}"
CANDIDATES="${BORSUK_100M_CANDIDATES:-100,200}"
QUERIES="${BORSUK_100M_QUERIES:-100}"
REGION="${AWS_REGION:-eu-central-1}"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:-not-provided}"
SOURCE_ARCHIVE="${BORSUK_SOURCE_ARCHIVE:-}"
RUNNER_SHA256="$(sha256_file "$SCRIPT_PATH")"

if [[ "$EXECUTE" != "1" ]]; then
  echo "planned dataset=$DATASET probes=$PROBES candidates=$CANDIDATES queries=$QUERIES out=$OUT"
  echo "dry run only; set BORSUK_100M_EXECUTE=1 and BORSUK_RUN_100M_QUALIFICATION=1"
  exit 0
fi
if [[ "${BORSUK_RUN_100M_QUALIFICATION:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_100M_QUALIFICATION=1" >&2
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
  echo "BORSUK_100M_RESULT_URI must be a fresh s3:// prefix" >&2
  exit 2
fi
if [[ ! -d "$DATASET" ]]; then
  echo "missing dataset directory: $DATASET" >&2
  exit 3
fi
if [[ -e "$OUT" ]]; then
  echo "refusing to overwrite an existing run: $OUT" >&2
  exit 4
fi

INDEX_URI="${S3_BUCKET%/}/100m-code-ranges/$RUN_ID"
index_root="${INDEX_URI%/}"
result_root="${RESULT_URI%/}"
if [[ "$result_root/" == "$index_root/"* || "$index_root/" == "$result_root/"* ]]; then
  echo "100M result and index prefixes must be disjoint" >&2
  exit 4
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
  echo "refusing to reuse non-empty 100M index prefix: $INDEX_URI" >&2
  exit 4
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
  echo "refusing to reuse non-empty 100M result prefix: $RESULT_URI" >&2
  exit 4
fi

mkdir -p "$OUT/cache" "$OUT/scratch" "$OUT/charts"
{
  printf '%s\n' \
    "run_id=$RUN_ID" \
    "source_sha256=$SOURCE_SHA256" \
    "runner_sha256=$RUNNER_SHA256" \
    "dataset=synthetic-clustered-100m-96" \
    "index_uri=$INDEX_URI" \
    "probes=$PROBES" \
    "candidates=$CANDIDATES" \
    "queries=$QUERIES" \
    "segment_table_format=parquet" \
    "wal_table_format=parquet" \
    "read_only=1" \
    "recall_only=1"
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
    rm -f "$OUT/QUALIFICATION_100M_COMPLETE"
    printf 'failed\n' > "$OUT/QUALIFICATION_100M_FAILED"
  fi
  if ! sync_results; then
    rm -f "$OUT/QUALIFICATION_100M_COMPLETE"
    printf 'failed\n' > "$OUT/QUALIFICATION_100M_FAILED"
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
  BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only \
  BORSUK_BENCH_GLOBAL_SCAN_CODEC=srht-pq-scan \
  BORSUK_BENCH_RECALL_LEAF_MODE=srht-pq-scan \
  BORSUK_BENCH_SERVING_LEAF_MODE=srht-pq-scan \
  BORSUK_BENCH_NPROBES="$PROBES" \
  BORSUK_BENCH_CANDIDATES="$CANDIDATES" \
  BORSUK_BENCH_QUERIES="$QUERIES" \
  BORSUK_BENCH_UNCACHED_QUERIES="$QUERIES" \
  BORSUK_SEGMENT_TABLE_FORMAT=parquet \
  BORSUK_WAL_TABLE_FORMAT=parquet \
  BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
  BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
  BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
  BORSUK_BENCH_READ_ONLY=1 \
  BORSUK_BENCH_RECALL_ONLY=1 \
  BORSUK_BENCH_SKIP_EXACT_RECALL=1 \
  python3 scripts/benchmark_with_resources.py \
    --output "$OUT/resources.csv" \
    --cache-dir "$OUT/cache" \
    --scratch-dir "$OUT/scratch" \
    -- target/release/examples/production_bench

python3 scripts/validate_benchmark_artifacts.py \
  --directory "$OUT" \
  --expected-codec srht-pq-scan \
  --required bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,resources.csv

python3 scripts/render_recall_latency_charts.py \
  --input "$OUT/bench_recall_latency.csv" \
  --dataset synthetic-clustered-100m-96 \
  --output-dir "$OUT/charts" \
  --subtitle 'AWS eu-central-1 · fresh build · packed-code ranges'
python3 scripts/render_resource_charts.py \
  --experiment-root "$OUT" \
  --output-dir "$OUT/charts" \
  --prefix resources

printf 'complete\n' > "$OUT/QUALIFICATION_100M_COMPLETE"
echo "wrote $OUT"
