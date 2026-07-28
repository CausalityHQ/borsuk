#!/usr/bin/env bash
# Measure the global storage scan and cached per-cell graph path on every
# standard dataset. Each row builds a fresh index from the raw corpus. The
# bounded production profiles and uncapped research ceiling are deliberately
# separate so an overload result can never be mistaken for a safe default.
set -euo pipefail

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_CACHE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/cache-execution-matrix/$RUN_ID}"
DATASET_NAMES="${BORSUK_CACHE_DATASETS:-fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960 deep-image-96}"
STORAGE_CODECS="${BORSUK_CACHE_STORAGE_CODECS:-pq-scan srht-pq-scan fast-turboquant-scan}"
PROFILES="${BORSUK_CACHE_PROFILES:-production-scan production-auto-64m production-auto-128m production-auto-256m production-auto-512m research-auto-uncapped-512m}"
EXECUTE="${BORSUK_CACHE_MATRIX_EXECUTE:-0}"
GRAPH_DEGREE="${BORSUK_CACHE_GRAPH_DEGREE:-16}"
GRAPH_CONSTRUCTION_EF="${BORSUK_CACHE_GRAPH_CONSTRUCTION_EF:-64}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_CACHE_MATRIX:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_CACHE_MATRIX=1" >&2
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
  CARGO_INCREMENTAL=0 cargo build --locked --release -p borsuk --example production_bench
fi

mkdir -p "$OUT"
COVERAGE="$OUT/coverage.csv"
printf '%s\n' 'dataset,profile,profile_class,status,scan_codec,cache_execution,leaf_capability,global_cell_graph_degree,global_cell_graph_construction_ef,global_graph_cache_max_bytes,ram_budget_bytes,max_concurrent_searches,max_concurrent_cell_decodes,concurrency,prefetch_depth,uncached_expected_engine,disk_cached_expected_engine,index_uri,nprobes,candidates,segment_max_vectors,resource_path,cache_coverage_path' > "$COVERAGE"

probes_for() {
  case "$1" in
    fashion-mnist-784) printf '%s' '1;2;4;6;8;16;32' ;;
    glove-100) printf '%s' '16;32;64;80;96;128' ;;
    sift-128) printf '%s' '1;2;4;8;16;32;64' ;;
    nytimes-256) printf '%s' '16;32;64;72;96;128' ;;
    gist-960) printf '%s' '8;16;32;64;128' ;;
    deep-image-96) printf '%s' '32;64;128;256;512' ;;
    *) echo "unknown dataset: $1" >&2; exit 4 ;;
  esac
}

serving_probe_for() {
  case "$1" in
    fashion-mnist-784) printf '%s' '8' ;;
    glove-100) printf '%s' '96' ;;
    sift-128) printf '%s' '32' ;;
    nytimes-256) printf '%s' '96' ;;
    gist-960) printf '%s' '64' ;;
    deep-image-96) printf '%s' '256' ;;
    *) echo "unknown dataset: $1" >&2; exit 4 ;;
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
    *) echo "unknown dataset: $1" >&2; exit 4 ;;
  esac
}

profile_config() {
  profile_class='production'
  cache_execution='scan'
  leaf_capability='pq-scan-only'
  graph_degree='0'
  graph_construction_ef='0'
  graph_cache_max_bytes='0'
  ram_budget_bytes='536870912'
  max_concurrent_searches='4'
  max_concurrent_cell_decodes='24'
  concurrency="${BORSUK_CACHE_PRODUCTION_CONCURRENCY:-1,4,16}"
  prefetch_depth='16'
  disk_cached_expected_engine="$2"
  case "$1" in
    production-scan) ;;
    production-auto-64m) graph_cache_max_bytes='67108864' ;;
    production-auto-128m) graph_cache_max_bytes='134217728' ;;
    production-auto-256m) graph_cache_max_bytes='268435456' ;;
    production-auto-512m) graph_cache_max_bytes='536870912' ;;
    research-auto-uncapped-512m)
      profile_class='research-ceiling'
      graph_cache_max_bytes='536870912'
      ram_budget_bytes='0'
      max_concurrent_searches='0'
      max_concurrent_cell_decodes='0'
      concurrency="${BORSUK_CACHE_RESEARCH_CONCURRENCY:-1,4,16,32}"
      prefetch_depth='64'
      ;;
    *) echo "unknown cache execution profile: $1" >&2; exit 4 ;;
  esac
  if [[ "$graph_cache_max_bytes" != '0' ]]; then
    cache_execution='auto'
    leaf_capability='graph-enabled'
    graph_degree="$GRAPH_DEGREE"
    graph_construction_ef="$GRAPH_CONSTRUCTION_EF"
    disk_cached_expected_engine='graph-or-mixed'
  fi
}

validate_execution_paths() {
  local csv="$1"
  local codec="$2"
  local policy="$3"
  awk -F, -v codec="$codec" -v policy="$policy" '
    NR == 1 { next }
    $10 == "uncached" && ($9 != codec || ($20 + 0) != 0) { exit 1 }
    $10 == "disk_cached" && ($26 + 0) != 0 { exit 1 }
    policy == "scan" && ($20 + 0) != 0 { exit 1 }
    policy == "auto" && $10 == "disk_cached" && ($20 + 0) <= 0 { exit 1 }
  ' "$csv" || {
    echo "cache execution invariant failed for codec=$codec policy=$policy file=$csv" >&2
    exit 5
  }
}

CANDIDATES_SEMICOLON="${BORSUK_CACHE_CANDIDATES:-16;32;64;128;256;512;1024;2048;4096}"
CANDIDATES_CSV="$(printf '%s' "$CANDIDATES_SEMICOLON" | tr ';' ',')"

for dataset in $DATASET_NAMES; do
  dataset_dir="$DATASETS/$dataset"
  if [[ ! -d "$dataset_dir" ]]; then
    echo "missing dataset directory: $dataset_dir" >&2
    exit 3
  fi
  probes_semicolon="$(probes_for "$dataset")"
  probes_csv="$(printf '%s' "$probes_semicolon" | tr ';' ',')"
  serving_probe="$(serving_probe_for "$dataset")"
  segment_rows="$(segment_rows_for "$dataset")"
  for scan_codec in $STORAGE_CODECS; do
    for profile in $PROFILES; do
      profile_config "$profile" "$scan_codec"
      profile_out="$OUT/$dataset/$scan_codec/$profile"
      resource_path="$profile_out/resources.csv"
      cache_coverage_path="$profile_out/bench_cache_coverage.csv"
      status='planned'
      index_uri="${BORSUK_S3_BUCKET:-s3://dry-run}/cache-execution/$RUN_ID/$dataset/$scan_codec/$profile"
      concurrency_field="$(printf '%s' "$concurrency" | tr ',' ';')"

      if [[ "$EXECUTE" == "1" ]]; then
        mkdir -p "$profile_out/cache" "$profile_out/scratch"
        graph_env=()
        if [[ "$graph_degree" != '0' ]]; then
          graph_env+=(
            "BORSUK_BENCH_GLOBAL_CELL_GRAPH_DEGREE=$graph_degree"
            "BORSUK_BENCH_GLOBAL_CELL_GRAPH_CONSTRUCTION_EF=$graph_construction_ef"
          )
        fi
        env \
          AWS_REGION="eu-central-1" \
          AWS_DEFAULT_REGION="eu-central-1" \
          BORSUK_BENCH_DATASET="$dataset_dir" \
          BORSUK_BENCH_URI="$index_uri" \
          BORSUK_BENCH_CACHE="$profile_out/cache" \
          BORSUK_BENCH_OUTPUT_DIR="$profile_out" \
          BORSUK_BENCH_LEAF_CAPABILITY="$leaf_capability" \
          BORSUK_BENCH_GLOBAL_SCAN_CODEC="$scan_codec" \
          BORSUK_BENCH_CACHE_EXECUTION="$cache_execution" \
          BORSUK_BENCH_GLOBAL_CELL_GRAPH_CACHE_MAX_BYTES="$graph_cache_max_bytes" \
          BORSUK_BENCH_RECALL_LEAF_MODE="$scan_codec" \
          BORSUK_BENCH_SERVING_LEAF_MODE="$scan_codec" \
          BORSUK_BENCH_NPROBES="$probes_csv" \
          BORSUK_BENCH_CANDIDATES="$CANDIDATES_CSV" \
          BORSUK_BENCH_SERVING_NPROBE="$serving_probe" \
          BORSUK_BENCH_SERVING_CANDIDATES="${BORSUK_CACHE_SERVING_CANDIDATES:-1024}" \
          BORSUK_BENCH_SERVING_PREFETCH_DEPTH="$prefetch_depth" \
          BORSUK_BENCH_SEGMENT_MAX="$segment_rows" \
          BORSUK_BENCH_QUERIES="${BORSUK_CACHE_QUERIES:-100}" \
          BORSUK_BENCH_UNCACHED_QUERIES="${BORSUK_CACHE_UNCACHED_QUERIES:-100}" \
          BORSUK_BENCH_CONCURRENCY="$concurrency" \
          BORSUK_BENCH_MAX_CONCURRENT_SEARCHES="$max_concurrent_searches" \
          BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES="$max_concurrent_cell_decodes" \
          BORSUK_BENCH_RAM_BUDGET_BYTES="$ram_budget_bytes" \
          BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES=0 \
          "${graph_env[@]}" \
          python3 scripts/benchmark_with_resources.py \
            --output "$resource_path" \
            --cache-dir "$profile_out/cache" \
            --scratch-dir "$profile_out/scratch" \
            -- target/release/examples/production_bench
        python3 scripts/validate_benchmark_artifacts.py \
          --directory "$profile_out" \
          --expected-codec "$scan_codec" \
          --required bench_build.csv,bench_recall_latency.csv,bench_query_samples.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,bench_cache_coverage.csv,bench_write_costs.csv,bench_write_samples.csv,bench_lifecycle.csv,bench_mutation_queries.csv,bench_mutation_query_samples.csv,resources.csv
        validate_execution_paths "$profile_out/bench_cache_states.csv" "$scan_codec" "$cache_execution"
        status='measured'
      fi

      printf '%s\n' "$dataset,$profile,$profile_class,$status,$scan_codec,$cache_execution,$leaf_capability,$graph_degree,$graph_construction_ef,$graph_cache_max_bytes,$ram_budget_bytes,$max_concurrent_searches,$max_concurrent_cell_decodes,$concurrency_field,$prefetch_depth,$scan_codec,$disk_cached_expected_engine,$index_uri,$probes_semicolon,$CANDIDATES_SEMICOLON,$segment_rows,$resource_path,$cache_coverage_path" >> "$COVERAGE"
    done
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" == "1" ]]; then
  mkdir -p "$OUT/charts"
  python3 scripts/render_resource_charts.py \
    --experiment-root "$OUT" \
    --output-dir "$OUT/charts" \
    --prefix cache-resources
  python3 scripts/render_cache_coverage_charts.py \
    --experiment-root "$OUT" \
    --output-dir "$OUT/charts"
else
  echo "dry run only; set BORSUK_CACHE_MATRIX_EXECUTE=1 and BORSUK_RUN_CACHE_MATRIX=1 for paid AWS execution"
fi
