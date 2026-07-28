#!/usr/bin/env bash
# Fresh full-corpus production-profile qualification on real S3.
set -euo pipefail

: "${BORSUK_S3_BUCKET:?set BORSUK_S3_BUCKET to a writable s3:// prefix}"
if [[ "${BORSUK_RUN_FULL_S3:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_FULL_S3=1" >&2
  exit 2
fi

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_FULL_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/full-s3/$RUN_ID}"
QUERIES="${BORSUK_BENCH_QUERIES:-1000}"
UNCACHED_QUERIES="${BORSUK_BENCH_UNCACHED_QUERIES:-$QUERIES}"
SCAN_CODEC="${BORSUK_FULL_SCAN_CODEC:-srht-pq-scan}"
REGION="${AWS_REGION:-eu-central-1}"
KEEP_CASE_DATA="${BORSUK_FULL_KEEP_CASE_DATA:-0}"
SEGMENT_TABLE_FORMAT="${BORSUK_SEGMENT_TABLE_FORMAT:-parquet}"
RECALL_ONLY="${BORSUK_FULL_RECALL_ONLY:-0}"
SKIP_EXACT_RECALL="${BORSUK_FULL_SKIP_EXACT_RECALL:-0}"
FULL_REQUIRED='bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,resources.csv'
RECALL_ONLY_REQUIRED='bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,resources.csv'
if [[ "$RECALL_ONLY" != "0" && "$RECALL_ONLY" != "1" ]]; then
  echo "BORSUK_FULL_RECALL_ONLY must be 0 or 1" >&2
  exit 2
fi
if [[ "$SKIP_EXACT_RECALL" != "0" && "$SKIP_EXACT_RECALL" != "1" ]]; then
  echo "BORSUK_FULL_SKIP_EXACT_RECALL must be 0 or 1" >&2
  exit 2
fi
if [[ "$RECALL_ONLY" == "1" && "$SKIP_EXACT_RECALL" != "1" ]]; then
  echo "recall-only publication profiles must skip unplanned exact scans" >&2
  exit 2
fi
if [[ "$RECALL_ONLY" == "1" ]]; then
  REQUIRED_ARTIFACTS="$RECALL_ONLY_REQUIRED"
else
  REQUIRED_ARTIFACTS="$FULL_REQUIRED"
fi
if [[ -n "${BORSUK_FULL_DATASET_NAMES:-}" ]]; then
  read -r -a DATASET_DIRS <<< "$BORSUK_FULL_DATASET_NAMES"
else
  DATASET_DIRS=(fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960)
  [[ "${RUN_DEEP_IMAGE:-0}" == "1" ]] && DATASET_DIRS+=(deep-image-96)
fi
if ((${#DATASET_DIRS[@]} == 0)); then
  echo "BORSUK_FULL_DATASET_NAMES must select at least one dataset" >&2
  exit 2
fi
for dataset in "${DATASET_DIRS[@]}"; do
  case "$dataset" in
    fashion-mnist-784 | glove-100 | sift-128 | nytimes-256 | gist-960 | deep-image-96) ;;
    *)
      echo "unknown production dataset: $dataset" >&2
      exit 2
      ;;
  esac
done

cd "$(dirname "$0")/.."
if [[ -e "$OUT/coverage.csv" ]]; then
  echo "refusing to overwrite an existing run: $OUT" >&2
  exit 3
fi
mkdir -p "$OUT"
printf '%s\n' 'dataset,status,scan_codec,index_uri,output_dir,resources' > "$OUT/coverage.csv"
cargo build --locked -p borsuk --release --example production_bench

failures=0
for dataset in "${DATASET_DIRS[@]}"; do
  dataset_dir="$DATASETS/$dataset"
  if [[ ! -d "$dataset_dir" ]]; then
    printf '%s\n' "$dataset,missing,$SCAN_CODEC,,," >> "$OUT/coverage.csv"
    failures=$((failures + 1))
    continue
  fi

  dataset_out="$OUT/$dataset"
  cache_dir="$dataset_out/cache"
  scratch_dir="$dataset_out/scratch"
  resources="$dataset_out/resources.csv"
  index_uri="$BORSUK_S3_BUCKET/full-s3/$RUN_ID/$dataset/$SCAN_CODEC"
  mkdir -p "$cache_dir" "$scratch_dir"

  if env \
    AWS_REGION="$REGION" \
    AWS_DEFAULT_REGION="$REGION" \
    BORSUK_BENCH_URI="$index_uri" \
    BORSUK_BENCH_DATASET="$dataset_dir" \
    BORSUK_BENCH_CACHE="$cache_dir" \
    BORSUK_BENCH_OUTPUT_DIR="$dataset_out" \
    BORSUK_BENCH_QUERIES="$QUERIES" \
    BORSUK_BENCH_UNCACHED_QUERIES="$UNCACHED_QUERIES" \
    BORSUK_SEGMENT_TABLE_FORMAT="$SEGMENT_TABLE_FORMAT" \
    BORSUK_BENCH_GLOBAL_SCAN_CODEC="$SCAN_CODEC" \
    BORSUK_BENCH_RECALL_LEAF_MODE="$SCAN_CODEC" \
    BORSUK_BENCH_SERVING_LEAF_MODE="$SCAN_CODEC" \
    BORSUK_BENCH_CACHE_EXECUTION=scan \
    BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only \
    BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
    BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
    BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
    BORSUK_BENCH_READ_ONLY=1 \
    BORSUK_BENCH_RECALL_ONLY="$RECALL_ONLY" \
    BORSUK_BENCH_SKIP_EXACT_RECALL="$SKIP_EXACT_RECALL" \
    python3 scripts/benchmark_with_resources.py \
      --output "$resources" \
      --cache-dir "$cache_dir" \
      --scratch-dir "$scratch_dir" \
      -- target/release/examples/production_bench; then
    python3 scripts/validate_benchmark_artifacts.py \
      --directory "$dataset_out" \
      --expected-codec "$SCAN_CODEC" \
      --required "$REQUIRED_ARTIFACTS"
    status=measured
    python3 scripts/render_recall_latency_charts.py \
      --input "$dataset_out/bench_recall_latency.csv" \
      --dataset "$dataset" \
      --output-dir "$dataset_out/charts/recall-latency" \
      --subtitle 'AWS S3 · fresh source recreation · uncached and disk-cached'
    python3 scripts/render_resource_charts.py \
      --experiment-root "$dataset_out" \
      --output-dir "$dataset_out/charts/resources" \
      --prefix resources
  else
    status=failed
    failures=$((failures + 1))
  fi
  printf '%s\n' "$dataset,$status,$SCAN_CODEC,$index_uri,$dataset_out,$resources" >> "$OUT/coverage.csv"
  if [[ "$KEEP_CASE_DATA" != "1" ]]; then
    rm -rf "$cache_dir" "$scratch_dir"
  fi
done

echo "wrote $OUT/coverage.csv"
if ((failures > 0)); then
  echo "$failures dataset run(s) failed." >&2
  exit 1
fi
