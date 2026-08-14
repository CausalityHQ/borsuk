#!/usr/bin/env bash
# Fresh per-cell cache-graph versus storage-scan qualification. Every profile
# rebuilds from source; graph absence is measured as scan fallback, never as an
# error or an implicit reuse of another profile's artifact.
set -euo pipefail

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_MIXED_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/mixed-cell-graph/$RUN_ID}"
DATASET_NAMES="${BORSUK_MIXED_DATASETS:-fashion-mnist-784 glove-100}"
PROFILES="${BORSUK_MIXED_PROFILES:-srht-d16 srht-d32 srht-d64 mse4-d32}"
EXECUTE="${BORSUK_MIXED_MATRIX_EXECUTE:-0}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_MIXED_MATRIX:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_MIXED_MATRIX=1" >&2
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
printf '%s\n' 'dataset,profile,status,scan_codec,graph_degree,graph_construction_ef,index_uri,nprobes,candidates,segment_max_vectors,cache_execution,resource_path' > "$COVERAGE"

probes_for() {
  case "$1" in
    fashion-mnist-784) printf '%s' '4;6;8;16' ;;
    glove-100) printf '%s' '32;64;96;128' ;;
    sift-128) printf '%s' '4;8;16;32' ;;
    nytimes-256) printf '%s' '32;64;96;128' ;;
    gist-960) printf '%s' '16;32;64;128' ;;
    deep-image-96) printf '%s' '64;128;256;512' ;;
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

profile_config() {
  scan_codec='srht-pq-scan'
  leaf_mode='srht-pq-scan'
  graph_degree='32'
  case "$1" in
    srht-d16) graph_degree='16' ;;
    srht-d32) graph_degree='32' ;;
    srht-d64) graph_degree='64' ;;
    mse4-d32) scan_codec='fast-turboquant-mse-scan'; leaf_mode='fast-turboquant-mse-scan'; graph_degree='32' ;;
    *) echo "unknown mixed profile: $1" >&2; exit 4 ;;
  esac
  graph_construction_ef=$((graph_degree * 4))
}

CANDIDATES_SEMICOLON="${BORSUK_MIXED_CANDIDATES:-64;128;256;512;1024}"
CANDIDATES_CSV="$(printf '%s' "$CANDIDATES_SEMICOLON" | tr ';' ',')"

for dataset in $DATASET_NAMES; do
  dataset_dir="$DATASETS/$dataset"
  [[ -d "$dataset_dir" ]] || { echo "missing dataset directory: $dataset_dir" >&2; exit 3; }
  probes_semicolon="$(probes_for "$dataset")"
  probes_csv="$(printf '%s' "$probes_semicolon" | tr ';' ',')"
  segment_rows="$(segment_rows_for "$dataset")"

  for profile in $PROFILES; do
    profile_config "$profile"
    index_uri="${BORSUK_S3_BUCKET:-s3://dry-run}/mixed-cell-graph/$RUN_ID/$dataset/$profile"
    profile_out="$OUT/$dataset/$profile"
    resource_path="$profile_out/resources.csv"
    status='planned'

    if [[ "$EXECUTE" == "1" ]]; then
      mkdir -p "$profile_out/cache" "$profile_out/scratch"
      env \
        AWS_REGION=eu-central-1 \
        AWS_DEFAULT_REGION=eu-central-1 \
        BORSUK_BENCH_DATASET="$dataset_dir" \
        BORSUK_BENCH_URI="$index_uri" \
        BORSUK_BENCH_CACHE="$profile_out/cache" \
        BORSUK_BENCH_OUTPUT_DIR="$profile_out" \
        BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only \
        BORSUK_BENCH_GLOBAL_SCAN_CODEC="$scan_codec" \
        BORSUK_BENCH_GLOBAL_CELL_GRAPH_DEGREE="$graph_degree" \
        BORSUK_BENCH_GLOBAL_CELL_GRAPH_CONSTRUCTION_EF="$graph_construction_ef" \
        BORSUK_BENCH_CACHE_EXECUTION=auto \
        BORSUK_BENCH_RECALL_LEAF_MODE="$leaf_mode" \
        BORSUK_BENCH_SERVING_LEAF_MODE="$leaf_mode" \
        BORSUK_BENCH_NPROBES="$probes_csv" \
        BORSUK_BENCH_CANDIDATES="$CANDIDATES_CSV" \
        BORSUK_BENCH_SEGMENT_MAX="$segment_rows" \
        BORSUK_BENCH_QUERIES="${BORSUK_MIXED_QUERIES:-100}" \
        BORSUK_BENCH_UNCACHED_QUERIES="${BORSUK_MIXED_UNCACHED_QUERIES:-100}" \
        BORSUK_BENCH_SKIP_EXACT_RECALL=1 \
        BORSUK_BENCH_MAX_ACTIVE_SEARCHES=4 \
        BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS=24 \
        BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
        BORSUK_BENCH_READ_ONLY=1 \
        python3 scripts/benchmark_with_resources.py \
          --output "$resource_path" \
          --cache-dir "$profile_out/cache" \
          --scratch-dir "$profile_out/scratch" \
          -- target/release/examples/production_bench
      python3 scripts/validate_benchmark_artifacts.py \
        --directory "$profile_out" \
        --expected-codec "$scan_codec" \
        --required bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,bench_cache_coverage.csv,resources.csv
      status='measured'
    fi

    printf '%s\n' "$dataset,$profile,$status,$scan_codec,$graph_degree,$graph_construction_ef,$index_uri,$probes_semicolon,$CANDIDATES_SEMICOLON,$segment_rows,auto,$resource_path" >> "$COVERAGE"
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" == "1" ]]; then
  python3 scripts/render_resource_charts.py \
    --experiment-root "$OUT" \
    --output-dir "$OUT/charts/resources" \
    --prefix mixed-cell-graph-resources
fi
