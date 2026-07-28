#!/usr/bin/env bash
# Enumerate or execute a fresh global scan-codec recall/latency/resource matrix.
set -euo pipefail

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_SCAN_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/scan-codec-matrix/$RUN_ID}"
DATASET_NAMES="${BORSUK_SCAN_DATASETS:-fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960 deep-image-96}"
PROFILES="${BORSUK_SCAN_PROFILES:-pq-adaptive pq-32b pq-64b srht-adaptive srht-32b srht-64b fast-turboquant-mse-2bit fast-turboquant-mse-3bit fast-turboquant-mse-4bit fast-turboquant-mse-4bit-shards3 fast-turboquant-prod-2bit fast-turboquant-prod-3bit fast-turboquant-prod-4bit}"
EXECUTE="${BORSUK_SCAN_MATRIX_EXECUTE:-0}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_SCAN_MATRIX:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_SCAN_MATRIX=1" >&2
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
printf '%s\n' 'dataset,profile,status,scan_codec,measured_codec,leaf_mode,pq_code_bytes,turboquant_bits,turboquant_qjl_bits,turboquant_shards,index_uri,nprobes,candidates,segment_max_vectors,cache_execution,cache_states,resource_path' > "$COVERAGE"

probes_for() {
  case "$1" in
    fashion-mnist-784) printf '%s' '1;2;4;6;8;16;32' ;;
    glove-100) printf '%s' '16;32;64;80;96;128' ;;
    sift-128) printf '%s' '1;2;4;8;16;32;64' ;;
    nytimes-256) printf '%s' '16;32;64;72;96;128' ;;
    gist-960) printf '%s' '8;16;32;64;128' ;;
    deep-image-96) printf '%s' '32;64;128;256;512' ;;
  esac
}

# Roughly 16 MiB of float32 source vectors per bounded ingest segment. The
# artifact encoder and query path remain independently bounded by byte budgets.
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
  scan_codec=''
  leaf_mode=''
  pq_code_bytes=''
  turboquant_bits='4'
  turboquant_qjl_bits='0'
  turboquant_shards='1'
  case "$1" in
    pq-adaptive) scan_codec='pq-scan'; leaf_mode='pq-scan' ;;
    pq-32b) scan_codec='pq-scan'; leaf_mode='pq-scan'; pq_code_bytes='32' ;;
    pq-64b) scan_codec='pq-scan'; leaf_mode='pq-scan'; pq_code_bytes='64' ;;
    srht-adaptive) scan_codec='srht-pq-scan'; leaf_mode='srht-pq-scan' ;;
    srht-32b) scan_codec='srht-pq-scan'; leaf_mode='srht-pq-scan'; pq_code_bytes='32' ;;
    srht-64b) scan_codec='srht-pq-scan'; leaf_mode='srht-pq-scan'; pq_code_bytes='64' ;;
    fast-turboquant-mse-2bit) scan_codec='fast-turboquant-mse-scan'; leaf_mode='fast-turboquant-mse-scan'; turboquant_bits='2' ;;
    fast-turboquant-mse-3bit) scan_codec='fast-turboquant-mse-scan'; leaf_mode='fast-turboquant-mse-scan'; turboquant_bits='3' ;;
    fast-turboquant-mse-4bit) scan_codec='fast-turboquant-mse-scan'; leaf_mode='fast-turboquant-mse-scan' ;;
    fast-turboquant-mse-4bit-shards3) scan_codec='fast-turboquant-mse-scan'; leaf_mode='fast-turboquant-mse-scan'; turboquant_shards='3' ;;
    fast-turboquant-prod-2bit) scan_codec='fast-turboquant-scan'; leaf_mode='fast-turboquant-scan'; turboquant_bits='2' ;;
    fast-turboquant-prod-3bit) scan_codec='fast-turboquant-scan'; leaf_mode='fast-turboquant-scan'; turboquant_bits='3' ;;
    fast-turboquant-prod-4bit) scan_codec='fast-turboquant-scan'; leaf_mode='fast-turboquant-scan'; turboquant_bits='4' ;;
    *) echo "unknown scan profile: $1" >&2; exit 4 ;;
  esac
}

CANDIDATES_SEMICOLON="${BORSUK_SCAN_CANDIDATES:-128;256;512;1024;2048;4096}"
CANDIDATES_CSV="$(printf '%s' "$CANDIDATES_SEMICOLON" | tr ';' ',')"

for dataset in $DATASET_NAMES; do
  dataset_dir="$DATASETS/$dataset"
  if [[ ! -d "$dataset_dir" ]]; then
    echo "missing dataset directory: $dataset_dir" >&2
    exit 3
  fi
  probes_semicolon="$(probes_for "$dataset")"
  probes_csv="$(printf '%s' "$probes_semicolon" | tr ';' ',')"
  segment_rows="$(segment_rows_for "$dataset")"

  for profile in $PROFILES; do
    profile_config "$profile"
    index_uri="${BORSUK_S3_BUCKET:-s3://dry-run}/scan-codec/$RUN_ID/$dataset/$profile"
    profile_out="$OUT/$dataset/$profile"
    resource_path="$profile_out/resources.csv"
    status='planned'
    measured_codec='pending'

    if [[ "$EXECUTE" == "1" ]]; then
      mkdir -p "$profile_out/cache" "$profile_out/scratch"
      bench_env=(
        "AWS_REGION=eu-central-1"
        "AWS_DEFAULT_REGION=eu-central-1"
        "BORSUK_BENCH_DATASET=$dataset_dir"
        "BORSUK_BENCH_URI=$index_uri"
        "BORSUK_BENCH_CACHE=$profile_out/cache"
        "BORSUK_BENCH_OUTPUT_DIR=$profile_out"
        "BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only"
        "BORSUK_BENCH_GLOBAL_SCAN_CODEC=$scan_codec"
        "BORSUK_BENCH_TURBOQUANT_BITS=$turboquant_bits"
        "BORSUK_BENCH_TURBOQUANT_QJL_BITS=$turboquant_qjl_bits"
        "BORSUK_BENCH_TURBOQUANT_SHARDS=$turboquant_shards"
        "BORSUK_BENCH_CACHE_EXECUTION=scan"
        "BORSUK_BENCH_RECALL_LEAF_MODE=$leaf_mode"
        "BORSUK_BENCH_SERVING_LEAF_MODE=$leaf_mode"
        "BORSUK_BENCH_NPROBES=$probes_csv"
        "BORSUK_BENCH_CANDIDATES=$CANDIDATES_CSV"
        "BORSUK_BENCH_SEGMENT_MAX=$segment_rows"
        "BORSUK_BENCH_QUERIES=${BORSUK_SCAN_QUERIES:-100}"
        "BORSUK_BENCH_UNCACHED_QUERIES=${BORSUK_SCAN_UNCACHED_QUERIES:-100}"
        "BORSUK_BENCH_SKIP_EXACT_RECALL=${BORSUK_SCAN_SKIP_EXACT_RECALL:-1}"
        "BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4"
        "BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24"
        "BORSUK_BENCH_RAM_BUDGET_BYTES=536870912"
        "BORSUK_BENCH_READ_ONLY=1"
      )
      if [[ -n "$pq_code_bytes" ]]; then
        bench_env+=("BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES=$pq_code_bytes")
      fi
      env "${bench_env[@]}" \
        python3 scripts/benchmark_with_resources.py \
          --output "$resource_path" \
          --cache-dir "$profile_out/cache" \
          --scratch-dir "$profile_out/scratch" \
          -- target/release/examples/production_bench
      python3 scripts/validate_benchmark_artifacts.py \
        --directory "$profile_out" \
        --expected-codec "$scan_codec" \
        --required bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,resources.csv
      measured_codec="$(awk -F, 'NR == 2 { print $1 }' "$profile_out/bench_recall_latency.csv")"
      if [[ "$measured_codec" != "$scan_codec" ]] || ! awk -F, -v expected="$scan_codec" 'NR > 1 && $1 != expected { exit 1 }' "$profile_out/bench_recall_latency.csv"; then
        echo "scan codec mismatch for $dataset/$profile: expected $scan_codec, measured $measured_codec" >&2
        exit 5
      fi
      status='measured'
    fi

    printf '%s\n' "$dataset,$profile,$status,$scan_codec,$measured_codec,$leaf_mode,${pq_code_bytes:-adaptive},$turboquant_bits,$turboquant_qjl_bits,$turboquant_shards,$index_uri,$probes_semicolon,$CANDIDATES_SEMICOLON,$segment_rows,scan,startup;uncached;disk_cached,$resource_path" >> "$COVERAGE"
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" == "1" ]]; then
  mkdir -p "$OUT/charts/resources"
  python3 scripts/render_resource_charts.py \
    --experiment-root "$OUT" \
    --output-dir "$OUT/charts/resources" \
    --prefix scan-codec-resources
  while IFS=, read -r dataset profile status _rest; do
    if [[ "$status" != 'measured' ]]; then
      continue
    fi
    chart_dir="$OUT/charts/recall-latency/$dataset/$profile"
    mkdir -p "$chart_dir"
    python3 scripts/render_recall_latency_charts.py \
      --input "$OUT/$dataset/$profile/bench_recall_latency.csv" \
      --dataset "$dataset" \
      --output-dir "$chart_dir" \
      --subtitle 'AWS eu-central-1 · uncached and disk-cached · exact rerank'
  done < <(tail -n +2 "$COVERAGE")
else
  echo "dry run only; set BORSUK_SCAN_MATRIX_EXECUTE=1 and BORSUK_RUN_SCAN_MATRIX=1 for paid AWS execution"
fi
