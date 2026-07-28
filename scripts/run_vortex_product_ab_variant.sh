#!/usr/bin/env bash
# One isolated end-to-end product-format arm. production_bench creates a fresh
# index, opens it with serving metadata prepared, and owns every cache-state
# transition inside the same measured process.
set -euo pipefail

: "${BORSUK_PRODUCT_VARIANT:?set BORSUK_PRODUCT_VARIANT to parquet or vortex}"
: "${BORSUK_PRODUCT_QUERY_PATH:?set BORSUK_PRODUCT_QUERY_PATH to production or segment}"
: "${BORSUK_PRODUCT_DATASET:?set BORSUK_PRODUCT_DATASET}"
: "${BORSUK_PRODUCT_INDEX_URI:?set a fresh BORSUK_PRODUCT_INDEX_URI}"
: "${BORSUK_PRODUCT_VARIANT_ROOT:?set BORSUK_PRODUCT_VARIANT_ROOT}"

SAMPLES="${BORSUK_PRODUCT_SAMPLES:-30}"
VARIANT="$BORSUK_PRODUCT_VARIANT"
QUERY_PATH="$BORSUK_PRODUCT_QUERY_PATH"
VARIANT_ROOT="$BORSUK_PRODUCT_VARIANT_ROOT"
MEASURED_OUTPUT="$VARIANT_ROOT/measured"
BIN="${BORSUK_PRODUCT_BENCH_BIN:-target/release/examples/production_bench}"

case "$VARIANT" in
  parquet|vortex) ;;
  *)
    echo "BORSUK_PRODUCT_VARIANT must be parquet or vortex" >&2
    exit 2
    ;;
esac
case "$QUERY_PATH" in
  production)
    BUILD_INDEX=1
    FORCE_SEGMENT_PATH=0
    ;;
  segment)
    BUILD_INDEX=0
    FORCE_SEGMENT_PATH=1
    ;;
  *)
    echo "BORSUK_PRODUCT_QUERY_PATH must match production|segment" >&2
    exit 2
    ;;
esac
if [[ "$SAMPLES" -lt 30 ]]; then
  echo "publication product A/B requires at least 30 timed queries" >&2
  exit 2
fi
if [[ -e "$VARIANT_ROOT" ]]; then
  echo "refusing to overwrite variant root: $VARIANT_ROOT" >&2
  exit 3
fi
if [[ ! -x "$BIN" ]]; then
  echo "production benchmark binary is missing: $BIN" >&2
  exit 3
fi

mkdir -p "$MEASURED_OUTPUT" "$VARIANT_ROOT/cache-tree/measured"
printf '%s\n' \
  "variant=$VARIANT" \
  "query_path=$QUERY_PATH" \
  "build_index=$BUILD_INDEX" \
  "force_segment_path=$FORCE_SEGMENT_PATH" \
  "segment_table_format=$VARIANT" \
  "index_uri=$BORSUK_PRODUCT_INDEX_URI" \
  "samples=$SAMPLES" \
  "materialized_borsuk_query=true" \
  "external_warmups=0" \
  "production_bench_internal_cache_warmup=true" \
  "serving_metadata_initialized_during_open=true" \
  "scan_codec=srht-pq-scan" \
  "cache_execution=scan" \
  "leaf_capability=pq-scan-only" \
  "concurrency=1,4,16" \
  "max_concurrent_searches=4" \
  "max_concurrent_cell_decodes=24" \
  "ram_budget_bytes=536870912" \
  > "$VARIANT_ROOT/variant.env"

env \
  AWS_REGION="${AWS_REGION:-eu-central-1}" \
  AWS_DEFAULT_REGION="${AWS_REGION:-eu-central-1}" \
  BORSUK_BENCH_URI="$BORSUK_PRODUCT_INDEX_URI" \
  BORSUK_BENCH_DATASET="$BORSUK_PRODUCT_DATASET" \
  BORSUK_SEGMENT_TABLE_FORMAT="$VARIANT" \
  BORSUK_BENCH_BUILD_INDEX="$BUILD_INDEX" \
  BORSUK_BENCH_FORCE_SEGMENT_PATH="$FORCE_SEGMENT_PATH" \
  BORSUK_BENCH_QUERIES="$SAMPLES" \
  BORSUK_BENCH_UNCACHED_QUERIES="$SAMPLES" \
  BORSUK_BENCH_CONCURRENCY=1,4,16 \
  BORSUK_BENCH_CACHE_PROFILE=all \
  BORSUK_BENCH_GLOBAL_SCAN_CODEC=srht-pq-scan \
  BORSUK_BENCH_RECALL_LEAF_MODE=srht-pq-scan \
  BORSUK_BENCH_SERVING_LEAF_MODE=srht-pq-scan \
  BORSUK_BENCH_CACHE_EXECUTION=scan \
  BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only \
  BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
  BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
  BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
  BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES=0 \
  BORSUK_BENCH_READ_ONLY=1 \
  BORSUK_BENCH_NPROBES=1,2,4,8,16,32,64,128,256 \
  BORSUK_BENCH_CANDIDATES=4096 \
  BORSUK_BENCH_CACHE="$VARIANT_ROOT/cache-tree/measured" \
  BORSUK_BENCH_OUTPUT_DIR="$MEASURED_OUTPUT" \
  "$BIN"
