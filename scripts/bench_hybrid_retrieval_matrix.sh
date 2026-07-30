#!/usr/bin/env bash
# Build and measure shared-qrels dense/sparse/text retrieval on AWS/S3.
set -euo pipefail

DATASETS_ROOT="${BORSUK_HYBRID_DATASETS_ROOT:-/tmp/borsuk-hybrid-datasets}"
RUN_ID="${BORSUK_HYBRID_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/hybrid-retrieval/$RUN_ID}"
DATASET_NAMES="${BORSUK_HYBRID_MATRIX_DATASETS:-scifact nfcorpus fiqa scidocs quora msmarco}"
PROFILES="${BORSUK_HYBRID_MATRIX_PROFILES:-pq srht turboquant-mse turboquant-prod}"
MODES="${BORSUK_HYBRID_MATRIX_MODES:-dense sparse text dense+sparse dense+text sparse+text dense+sparse+text}"
SEARCH_POINTS="${BORSUK_HYBRID_SEARCH_POINTS:-128:32 256:64 512:128}"
HOT_FRACTIONS="${BORSUK_HYBRID_HOT_FRACTIONS:-0 0.1 0.25 0.5 0.75 0.9 1}"
FUSIONS="${BORSUK_HYBRID_FUSIONS:-rrf}"
RRF_KS="${BORSUK_HYBRID_RRF_KS:-1 5 10 30 60}"
EXECUTE="${BORSUK_HYBRID_MATRIX_EXECUTE:-0}"
REPETITIONS="${BORSUK_HYBRID_REPETITIONS:-5}"
MASTER_SEED="${BORSUK_HYBRID_MASTER_SEED:-20260726}"
QUERY_LIMIT="${BORSUK_HYBRID_QUERY_LIMIT:-0}"
BENCH_BINARY="${BORSUK_HYBRID_BINARY:-target/release/examples/hybrid_retrieval_bench}"
DENSE_ELEMENT_TYPE="${BORSUK_HYBRID_DENSE_ELEMENT_TYPE:-float32}"
SPARSE_ELEMENT_TYPE="${BORSUK_HYBRID_SPARSE_ELEMENT_TYPE:-float32}"

if [[ "$EXECUTE" == "1" && "${BORSUK_RUN_HYBRID_MATRIX:-0}" != "1" ]]; then
  echo "paid execution requires BORSUK_RUN_HYBRID_MATRIX=1" >&2
  exit 2
fi
if [[ "$EXECUTE" == "1" && -z "${BORSUK_S3_BUCKET:-}" ]]; then
  echo "paid execution requires BORSUK_S3_BUCKET" >&2
  exit 2
fi
if [[ "$EXECUTE" == "1" && -e "$OUT/coverage.csv" ]]; then
  echo "refusing to overwrite existing hybrid run: $OUT" >&2
  exit 3
fi

profile_codec() {
  case "$1" in
    pq) printf '%s' 'pq-scan' ;;
    srht) printf '%s' 'srht-pq-scan' ;;
    turboquant-mse) printf '%s' 'fast-turboquant-mse-scan' ;;
    turboquant-prod) printf '%s' 'fast-turboquant-scan' ;;
    *) echo "unknown hybrid profile: $1" >&2; exit 4 ;;
  esac
}

mkdir -p "$OUT"
COVERAGE="$OUT/coverage.csv"
printf '%s\n' 'stage,dataset,profile,status,scan_codec,index_uri,mode,candidate_depth,max_segments,fusion,rrf_k,target_hot_query_fraction,cache_profile,campaign_repetition,query_seed,artifact_dir,resource_path,dense_element_type,sparse_element_type' > "$COVERAGE"

if [[ "$EXECUTE" == "1" ]]; then
  cargo build --locked --release -p borsuk --example hybrid_retrieval_bench
fi

for dataset in $DATASET_NAMES; do
  dataset_dir="$DATASETS_ROOT/$dataset"
  if [[ ! -d "$dataset_dir" ]]; then
    echo "missing prepared hybrid dataset directory: $dataset_dir" >&2
    exit 3
  fi
  for profile in $PROFILES; do
    codec="$(profile_codec "$profile")"
    index_uri="${BORSUK_S3_BUCKET:-s3://dry-run}/hybrid-retrieval/$RUN_ID/$dataset/$profile/dense-$DENSE_ELEMENT_TYPE/sparse-$SPARSE_ELEMENT_TYPE"
    profile_out="$OUT/$dataset/$profile"
    build_out="$profile_out/build"
    build_resources="$build_out/resources.csv"
    status='planned'

    if [[ "$EXECUTE" == "1" ]]; then
      mkdir -p "$profile_out"
      python3 scripts/validate_hybrid_dataset.py "$dataset_dir" \
        > "$profile_out/dataset-validation.json"
      mkdir -p "$build_out/scratch"
      env \
        AWS_REGION="${AWS_REGION:-eu-central-1}" \
        AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-central-1}" \
        BORSUK_HYBRID_DATASET="$dataset_dir" \
        BORSUK_HYBRID_INDEX_URI="$index_uri" \
        BORSUK_HYBRID_OUTPUT="$build_out" \
        BORSUK_HYBRID_SCAN_CODEC="$codec" \
        BORSUK_HYBRID_DENSE_ELEMENT_TYPE="$DENSE_ELEMENT_TYPE" \
        BORSUK_HYBRID_SPARSE_ELEMENT_TYPE="$SPARSE_ELEMENT_TYPE" \
        BORSUK_HYBRID_BATCH_SIZE="${BORSUK_HYBRID_BATCH_SIZE:-5000}" \
        BORSUK_HYBRID_RAM_BUDGET_BYTES="${BORSUK_HYBRID_RAM_BUDGET_BYTES:-536870912}" \
        python3 scripts/benchmark_with_resources.py \
          --output "$build_resources" \
          --scratch-dir "$build_out/scratch" \
          -- "$BENCH_BINARY" build
      test -s "$build_out/hybrid_build.csv"
      test -s "$build_resources"
      status='measured'
    fi
    printf '%s\n' "build,$dataset,$profile,$status,$codec,$index_uri,,,,,,,,,,${build_out},${build_resources},${DENSE_ELEMENT_TYPE},${SPARSE_ELEMENT_TYPE}" >> "$COVERAGE"

    for mode in $MODES; do
      for search_point in $SEARCH_POINTS; do
        candidate_depth="${search_point%%:*}"
        max_segments="${search_point##*:}"
        if [[ -z "$candidate_depth" || -z "$max_segments" || "$candidate_depth" == "$search_point" ]]; then
          echo "invalid search point '$search_point'; expected candidate_depth:max_segments" >&2
          exit 4
        fi
        for fusion in $FUSIONS; do
          if [[ "$fusion" == "rrf" && "$mode" == *"+"* ]]; then
            fusion_variants="$RRF_KS"
          elif [[ "$fusion" == "rrf" ]]; then
            fusion_variants="1"
          else
            fusion_variants="none"
          fi
          for rrf_k in $fusion_variants; do
            if [[ "$rrf_k" == "none" ]]; then
              fusion_key="$fusion"
              rrf_k_value=""
            else
              fusion_key="$fusion-k${rrf_k}"
              rrf_k_value="$rrf_k"
            fi
            for hot_fraction in $HOT_FRACTIONS; do
              run_key="c${candidate_depth}-p${max_segments}/${fusion_key}/hot-${hot_fraction}"
              for campaign_repetition in $(seq 1 "$REPETITIONS"); do
                query_seed=$((MASTER_SEED + campaign_repetition))
                query_out="$profile_out/query/$mode/$run_key/repetition-$campaign_repetition"
                cache_dir="$query_out/cache"
                resource_path="$query_out/resources.csv"
                query_status='planned'
                cache_profile="mixed-cache-${hot_fraction}"

                if [[ "$EXECUTE" == "1" ]]; then
                  if [[ -e "$query_out" ]]; then
                    echo "refusing to reuse hybrid query directory: $query_out" >&2
                    exit 3
                  fi
                  mkdir -p "$cache_dir" "$query_out/scratch"
                  if [[ "$QUERY_LIMIT" == "0" ]]; then
                    total_queries="$(
                      python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["queries"])' \
                        "$dataset_dir/manifest.json"
                    )"
                  else
                    total_queries="$QUERY_LIMIT"
                  fi
                  env \
                    AWS_REGION="${AWS_REGION:-eu-central-1}" \
                    AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-central-1}" \
                    BORSUK_HYBRID_DATASET="$dataset_dir" \
                    BORSUK_HYBRID_INDEX_URI="$index_uri" \
                    BORSUK_HYBRID_SCAN_CODEC="$codec" \
                    BORSUK_HYBRID_DENSE_ELEMENT_TYPE="$DENSE_ELEMENT_TYPE" \
                    BORSUK_HYBRID_SPARSE_ELEMENT_TYPE="$SPARSE_ELEMENT_TYPE" \
                    BORSUK_HYBRID_MODES="$mode" \
                    BORSUK_HYBRID_FUSION="$fusion" \
                    BORSUK_HYBRID_RRF_K="${rrf_k_value:-1}" \
                    BORSUK_HYBRID_CANDIDATE_DEPTH="$candidate_depth" \
                    BORSUK_HYBRID_MAX_CANDIDATES="$candidate_depth" \
                    BORSUK_HYBRID_MAX_SEGMENTS="$max_segments" \
                    BORSUK_HYBRID_CACHE_DIR="$cache_dir" \
                    BORSUK_HYBRID_CACHE_MAX_BYTES="${BORSUK_HYBRID_CACHE_MAX_BYTES:-21474836480}" \
                    BORSUK_HYBRID_RAM_BUDGET_BYTES="${BORSUK_HYBRID_RAM_BUDGET_BYTES:-536870912}" \
                    BORSUK_HYBRID_MAX_CONCURRENT_SEARCHES="${BORSUK_HYBRID_MAX_CONCURRENT_SEARCHES:-4}" \
                    BORSUK_HYBRID_MAX_CONCURRENT_CELL_DECODES="${BORSUK_HYBRID_MAX_CONCURRENT_CELL_DECODES:-24}" \
                    BORSUK_HYBRID_OUTPUT="$query_out" \
                    BORSUK_HYBRID_QUERY_LIMIT="$total_queries" \
                    BORSUK_HYBRID_QUERY_SEED="$query_seed" \
                    BORSUK_HYBRID_REPETITIONS=1 \
                    BORSUK_HYBRID_PRIME_TARGET_HOT_SET=1 \
                    BORSUK_HYBRID_CACHE_PROFILE="$cache_profile" \
                    BORSUK_HYBRID_TARGET_HOT_FRACTION="$hot_fraction" \
                    python3 scripts/benchmark_with_resources.py \
                      --output "$resource_path" \
                      --cache-dir "$cache_dir" \
                      --scratch-dir "$query_out/scratch" \
                      -- "$BENCH_BINARY" query
                  test -s "$query_out/hybrid_queries.csv"
                  test -s "$query_out/hybrid_summary.csv"
                  test -s "$query_out/hybrid_startup.csv"
                  test -s "$resource_path"
                  rm -rf "$cache_dir" "$query_out/scratch"
                  query_status='measured'
                fi
                printf '%s\n' "query,$dataset,$profile,$query_status,$codec,$index_uri,$mode,$candidate_depth,$max_segments,$fusion,$rrf_k_value,$hot_fraction,$cache_profile,$campaign_repetition,$query_seed,$query_out,$resource_path,${DENSE_ELEMENT_TYPE},${SPARSE_ELEMENT_TYPE}" >> "$COVERAGE"
              done
            done
          done
        done
      done
    done
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" != "1" ]]; then
  echo "dry run only; set BORSUK_HYBRID_MATRIX_EXECUTE=1 and BORSUK_RUN_HYBRID_MATRIX=1 for paid AWS execution"
fi
