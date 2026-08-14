#!/usr/bin/env bash
# Fresh-build internal method matrix. Every row owns a distinct object prefix;
# the benchmark binary has no index-reuse mode.
set -euo pipefail

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_METHOD_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/standard-method-matrix/$RUN_ID}"
METHODS="${BORSUK_METHODS:-pq-scan srht-pq-scan fast-turboquant-mse-scan fast-turboquant-scan exact flat-scan sq-scan graph vamana-pq}"
DATASET_NAMES="${BORSUK_METHOD_DATASETS:-fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960 deep-image-96}"
EXECUTE="${BORSUK_MATRIX_EXECUTE:-0}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_STANDARD_MATRIX:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_STANDARD_MATRIX=1" >&2
  exit 2
fi
if [[ "$EXECUTE" == "1" && -z "${BORSUK_S3_BUCKET:-}" ]]; then
  echo "paid execution requires BORSUK_S3_BUCKET" >&2
  exit 2
fi
if [[ "$EXECUTE" == "1" && -e "$OUT/coverage.csv" ]]; then
  echo "refusing to overwrite an existing run: $OUT" >&2
  exit 3
fi
if [[ "$EXECUTE" == "1" ]]; then
  cargo build --locked --release -p borsuk --example production_bench
fi

mkdir -p "$OUT"
COVERAGE="$OUT/coverage.csv"
printf '%s\n' 'dataset,method,status,scan_codec,index_capability,index_uri,nprobes,candidates,segment_max_vectors,cache_execution,resource_path' > "$COVERAGE"

probes_for() {
  case "$1" in
    fashion-mnist-784) printf '%s' '1;2;4;6;8;16;32' ;;
    glove-100) printf '%s' '16;32;64;80;96;128' ;;
    sift-128) printf '%s' '1;2;4;8;16;32;64' ;;
    nytimes-256) printf '%s' '16;32;64;72;96;128' ;;
    gist-960) printf '%s' '8;16;32;64;128' ;;
    deep-image-96) printf '%s' '32;64;128;256;512' ;;
    *) echo "unknown dataset: $1" >&2; exit 3 ;;
  esac
}

segment_rows_for() {
  case "$1" in
    fashion-mnist-784) printf '%s' '5349' ;;
    glove-100) printf '%s' '41943' ;;
    sift-128) printf '%s' '32768' ;;
    nytimes-256) printf '%s' '16384' ;;
    gist-960) printf '%s' '4369' ;;
    deep-image-96) printf '%s' '43690' ;;
  esac
}

method_config() {
  scan_codec='srht-pq-scan'
  leaf_mode="$1"
  capability='pq-scan-only'
  cache_execution='scan'
  serving_mode='hybrid'
  skip_recall='0'
  case "$1" in
    pq-scan) scan_codec='pq-scan' ;;
    srht-pq-scan) ;;
    fast-turboquant-mse-scan) scan_codec='fast-turboquant-mse-scan' ;;
    fast-turboquant-scan) scan_codec='fast-turboquant-scan' ;;
    exact) leaf_mode='srht-pq-scan'; serving_mode='exact'; skip_recall='1' ;;
    flat-scan|sq-scan) ;;
    graph|vamana-pq) capability='graph-enabled' ;;
    *) echo "unknown method: $1" >&2; exit 4 ;;
  esac
}

CANDIDATES_SEMICOLON="${BORSUK_METHOD_CANDIDATES:-16;32;64;128;256;512}"
CANDIDATES_CSV="$(printf '%s' "$CANDIDATES_SEMICOLON" | tr ';' ',')"

for dataset in $DATASET_NAMES; do
  dataset_dir="$DATASETS/$dataset"
  [[ -d "$dataset_dir" ]] || { echo "missing dataset directory: $dataset_dir" >&2; exit 3; }
  probes_semicolon="$(probes_for "$dataset")"
  probes_csv="$(printf '%s' "$probes_semicolon" | tr ';' ',')"
  segment_rows="$(segment_rows_for "$dataset")"

  for method in $METHODS; do
    method_config "$method"
    index_uri="${BORSUK_S3_BUCKET:-s3://dry-run}/standard-method/$RUN_ID/$dataset/$method"
    method_out="$OUT/$dataset/$method"
    resource_path="$method_out/resources.csv"
    status='planned'

    if [[ "$EXECUTE" == "1" ]]; then
      mkdir -p "$method_out/cache" "$method_out/scratch"
      env \
        AWS_REGION=eu-central-1 \
        AWS_DEFAULT_REGION=eu-central-1 \
        BORSUK_BENCH_DATASET="$dataset_dir" \
        BORSUK_BENCH_URI="$index_uri" \
        BORSUK_BENCH_CACHE="$method_out/cache" \
        BORSUK_BENCH_OUTPUT_DIR="$method_out" \
        BORSUK_BENCH_LEAF_CAPABILITY="$capability" \
        BORSUK_BENCH_GLOBAL_SCAN_CODEC="$scan_codec" \
        BORSUK_BENCH_RECALL_LEAF_MODE="$leaf_mode" \
        BORSUK_BENCH_SERVING_MODE="$serving_mode" \
        BORSUK_BENCH_SERVING_LEAF_MODE="$leaf_mode" \
        BORSUK_BENCH_CACHE_EXECUTION="$cache_execution" \
        BORSUK_BENCH_NPROBES="$probes_csv" \
        BORSUK_BENCH_CANDIDATES="$CANDIDATES_CSV" \
        BORSUK_BENCH_SEGMENT_MAX="$segment_rows" \
        BORSUK_BENCH_SKIP_RECALL="$skip_recall" \
        BORSUK_BENCH_SKIP_EXACT_RECALL=1 \
        BORSUK_BENCH_MAX_ACTIVE_SEARCHES=4 \
        BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS=24 \
        BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
        BORSUK_BENCH_READ_ONLY=1 \
        python3 scripts/benchmark_with_resources.py \
          --output "$resource_path" \
          --cache-dir "$method_out/cache" \
          --scratch-dir "$method_out/scratch" \
          -- target/release/examples/production_bench
      required='bench_build.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,resources.csv'
      if [[ "$skip_recall" != '1' ]]; then
        required="$required,bench_recall_latency.csv,bench_query_samples.csv"
      fi
      python3 scripts/validate_benchmark_artifacts.py \
        --directory "$method_out" \
        --expected-codec "$scan_codec" \
        --required "$required"
      status='measured'
    fi

    printf '%s\n' "$dataset,$method,$status,$scan_codec,$capability,$index_uri,$probes_semicolon,$CANDIDATES_SEMICOLON,$segment_rows,$cache_execution,$resource_path" >> "$COVERAGE"
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" != "1" ]]; then
  echo "dry run only; set BORSUK_MATRIX_EXECUTE=1 and BORSUK_RUN_STANDARD_MATRIX=1 for paid AWS execution"
fi
