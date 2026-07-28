#!/usr/bin/env bash
# Fresh local-control matrix for original TurboQuant, TurboVec, and FAISS.
set -euo pipefail

DATASETS="${DATASETS:-/tmp/borsuk-datasets}"
RUN_ID="${BORSUK_EXTERNAL_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-docs/web/assets/benchmarks/external-controls/$RUN_ID}"
DATASET_NAMES="${BORSUK_EXTERNAL_DATASETS:-fashion-mnist-784 glove-100 sift-128 nytimes-256 gist-960 deep-image-96}"
PROFILES="${BORSUK_EXTERNAL_PROFILES:-dense-tq-mse-2 dense-tq-mse-4 dense-tq-prod-2 dense-tq-prod-4 turbovec-2 turbovec-4 faiss-exact faiss-hnsw-flat faiss-hnsw-pq-2 faiss-hnsw-pq-4 faiss-ivf-pq-2 faiss-ivf-pq-4 faiss-ivf-pq-refine-4}"
SEEDS="${BORSUK_TQ_SEEDS:-17 23 42 101 313 997 2027 4099 8191 65537}"
QUERIES="${BORSUK_EXTERNAL_QUERIES:-100}"
CANDIDATES="${BORSUK_EXTERNAL_CANDIDATES:-16,32,64,128,256,512}"
EXECUTE="${BORSUK_EXTERNAL_EXECUTE:-0}"

if [[ "$EXECUTE" == '1' && "${BORSUK_RUN_EXTERNAL_MATRIX:-0}" != '1' ]]; then
  echo "paid execution requires BORSUK_RUN_EXTERNAL_MATRIX=1" >&2
  exit 2
fi
if [[ "$EXECUTE" == '1' && -e "$OUT/coverage.csv" ]]; then
  echo "refusing to overwrite an existing run: $OUT" >&2
  exit 3
fi

mkdir -p "$OUT"
COVERAGE="$OUT/coverage.csv"
printf '%s\n' 'dataset,profile,seed,status,applicability,output_dir,resource_path' > "$COVERAGE"

dataset_metric() {
  python3 - "$1/meta.json" <<'PY'
import json, sys
print(str(json.load(open(sys.argv[1]))["metric"]).lower())
PY
}

run_control() {
  local dataset="$1" profile="$2" seed="$3" output="$4" resources="$5"
  local command=()
  case "$profile" in
    dense-tq-mse-2) command=(python3 scripts/benchmark_turboquant_reference.py --dataset "$dataset" --output-dir "$output" --variant mse --bit-width 2 --seed "$seed" --queries "$QUERIES" --candidates "$CANDIDATES") ;;
    dense-tq-mse-4) command=(python3 scripts/benchmark_turboquant_reference.py --dataset "$dataset" --output-dir "$output" --variant mse --bit-width 4 --seed "$seed" --queries "$QUERIES" --candidates "$CANDIDATES") ;;
    dense-tq-prod-2) command=(python3 scripts/benchmark_turboquant_reference.py --dataset "$dataset" --output-dir "$output" --variant prod --bit-width 2 --seed "$seed" --queries "$QUERIES" --candidates "$CANDIDATES") ;;
    dense-tq-prod-4) command=(python3 scripts/benchmark_turboquant_reference.py --dataset "$dataset" --output-dir "$output" --variant prod --bit-width 4 --seed "$seed" --queries "$QUERIES" --candidates "$CANDIDATES") ;;
    turbovec-2) command=(python3 scripts/benchmark_turbovec.py --dataset "$dataset" --output-dir "$output" --bit-width 2 --queries "$QUERIES" --candidates "$CANDIDATES") ;;
    turbovec-4) command=(python3 scripts/benchmark_turbovec.py --dataset "$dataset" --output-dir "$output" --bit-width 4 --queries "$QUERIES" --candidates "$CANDIDATES") ;;
    faiss-exact) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method exact --queries "$QUERIES") ;;
    faiss-hnsw-flat) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method hnsw-flat --queries "$QUERIES") ;;
    faiss-hnsw-pq-2) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method hnsw-pq --bits-per-dimension 2 --queries "$QUERIES") ;;
    faiss-hnsw-pq-4) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method hnsw-pq --bits-per-dimension 4 --queries "$QUERIES") ;;
    faiss-ivf-pq-2) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method ivf-pq --bits-per-dimension 2 --queries "$QUERIES") ;;
    faiss-ivf-pq-4) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method ivf-pq --bits-per-dimension 4 --queries "$QUERIES") ;;
    faiss-ivf-pq-refine-4) command=(python3 scripts/benchmark_faiss.py --dataset "$dataset" --output-dir "$output" --method ivf-pq-refine --bits-per-dimension 4 --queries "$QUERIES") ;;
    *) echo "unknown external profile: $profile" >&2; exit 4 ;;
  esac
  mkdir -p "$output/scratch"
  python3 scripts/benchmark_with_resources.py \
    --output "$resources" \
    --cache-dir "$output" \
    --scratch-dir "$output/scratch" \
    -- "${command[@]}"
  python3 scripts/validate_benchmark_artifacts.py \
    --directory "$output" \
    --required build.csv,query.csv,resources.csv
}

for dataset_name in $DATASET_NAMES; do
  dataset="$DATASETS/$dataset_name"
  [[ -d "$dataset" ]] || { echo "missing dataset directory: $dataset" >&2; exit 5; }
  metric="$(dataset_metric "$dataset")"
  for profile in $PROFILES; do
    applicability='all-supported-metrics'
    if [[ "$profile" == turbovec-* && ! "$metric" =~ ^(angular|cosine|inner-product|dot)$ ]]; then
      printf '%s\n' "$dataset_name,$profile,,not-applicable,metric-$metric,," >> "$COVERAGE"
      continue
    fi
    profile_seeds='none'
    if [[ "$profile" == dense-tq-* ]]; then
      profile_seeds="$SEEDS"
    fi
    for seed in $profile_seeds; do
      suffix="$profile"
      [[ "$seed" == 'none' ]] || suffix="$profile/seed-$seed"
      output="$OUT/$dataset_name/$suffix"
      resources="$output/resources.csv"
      status=planned
      if [[ "$EXECUTE" == '1' ]]; then
        run_control "$dataset" "$profile" "$seed" "$output" "$resources"
        status=measured
      fi
      printf '%s\n' "$dataset_name,$profile,$seed,$status,$applicability,$output,$resources" >> "$COVERAGE"
    done
  done
done

echo "wrote $COVERAGE"
if [[ "$EXECUTE" != '1' ]]; then
  echo "dry run only; set BORSUK_EXTERNAL_EXECUTE=1 and BORSUK_RUN_EXTERNAL_MATRIX=1 for execution"
fi
