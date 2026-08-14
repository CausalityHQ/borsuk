#!/usr/bin/env bash
# Build one immutable index (once) and measure one frozen SIMD datatype cell.
set -euo pipefail

cd "$(dirname "$0")/.."

ROOT="${BORSUK_SIMD_ROOT:?set BORSUK_SIMD_ROOT}"
ARCHITECTURE="${BORSUK_SIMD_ARCHITECTURE:?set BORSUK_SIMD_ARCHITECTURE}"
BUILD="${BORSUK_SIMD_BUILD:?set BORSUK_SIMD_BUILD}"
PATH_NAME="${BORSUK_SIMD_PATH:?set BORSUK_SIMD_PATH}"
KIND="${BORSUK_SIMD_KIND:?set BORSUK_SIMD_KIND}"
ELEMENT_TYPE="${BORSUK_SIMD_ELEMENT_TYPE:?set BORSUK_SIMD_ELEMENT_TYPE}"
DATASET="${BORSUK_SIMD_DATASET:?set BORSUK_SIMD_DATASET}"
REPETITION="${BORSUK_SIMD_REPETITION:?set BORSUK_SIMD_REPETITION}"
CACHE_STATE="${BORSUK_SIMD_CACHE_STATE:?set BORSUK_SIMD_CACHE_STATE}"
TARGET_COVERAGE="${BORSUK_SIMD_TARGET_CACHE_COVERAGE_PERCENT:?set BORSUK_SIMD_TARGET_CACHE_COVERAGE_PERCENT}"
CONCURRENCY="${BORSUK_SIMD_CLIENT_CONCURRENCY:?set BORSUK_SIMD_CLIENT_CONCURRENCY}"
QUERY_SEED="${BORSUK_SIMD_QUERY_SEED:?set BORSUK_SIMD_QUERY_SEED}"
INDEX_KEY="${BORSUK_SIMD_INDEX_KEY:?set BORSUK_SIMD_INDEX_KEY}"
INDEX_URI="${BORSUK_SIMD_INDEX_URI:?set BORSUK_SIMD_INDEX_URI}"
OUTPUT="${BORSUK_SIMD_OUTPUT_ROOT:?set BORSUK_SIMD_OUTPUT_ROOT}"
DATASETS_ROOT="${BORSUK_SIMD_DATASETS_ROOT:?set BORSUK_SIMD_DATASETS_ROOT}"
EXPECTED_QUERIES="${BORSUK_SIMD_EXPECTED_QUERIES:?set BORSUK_SIMD_EXPECTED_QUERIES}"
INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set BORSUK_INSTANCE_TYPE}"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:?set BORSUK_SOURCE_SHA256}"
MANIFEST_SHA256="${BORSUK_SIMD_MANIFEST_SHA256:?set BORSUK_SIMD_MANIFEST_SHA256}"
DATASET_IDENTITY_SHA256="${BORSUK_SIMD_DATASET_IDENTITY_SHA256:?set BORSUK_SIMD_DATASET_IDENTITY_SHA256}"
SIMD_TARGET="${BORSUK_SIMD_SIMD_TARGET:?set BORSUK_SIMD_SIMD_TARGET}"
SCALAR_TARGET="${BORSUK_SIMD_SCALAR_TARGET:?set BORSUK_SIMD_SCALAR_TARGET}"
LATE_FRONTIER="${BORSUK_SIMD_LATE_FRONTIER:-128}"

for numeric in "$REPETITION" "$TARGET_COVERAGE" "$CONCURRENCY" "$QUERY_SEED" "$EXPECTED_QUERIES" "$LATE_FRONTIER"; do
  if [[ ! "$numeric" =~ ^[0-9]+$ ]]; then
    echo "SIMD cell numeric identity is invalid: $numeric" >&2
    exit 2
  fi
done
if (( TARGET_COVERAGE > 100 || CONCURRENCY == 0 || EXPECTED_QUERIES == 0 )); then
  echo "SIMD cell coverage/concurrency/query count is invalid" >&2
  exit 2
fi
for digest in "$SOURCE_SHA256" "$MANIFEST_SHA256" "$DATASET_IDENTITY_SHA256"; do
  if [[ ! "$digest" =~ ^[0-9a-f]{64}$ ]]; then
    echo "SIMD cell SHA-256 identity is invalid" >&2
    exit 2
  fi
done

case "$BUILD" in
  simd) TARGET="$SIMD_TARGET" ;;
  scalar-control) TARGET="$SCALAR_TARGET" ;;
  *) echo "unknown SIMD build: $BUILD" >&2; exit 2 ;;
esac
case "$KIND" in
  primary-dense|primary-binary) BINARY_NAME="production_bench" ;;
  named-sparse|text-bm25) BINARY_NAME="hybrid_retrieval_bench" ;;
  late-interaction) BINARY_NAME="market_workload_bench" ;;
  *) echo "unknown SIMD path kind: $KIND" >&2; exit 2 ;;
esac
BINARY="$TARGET/release/examples/$BINARY_NAME"
if [[ ! -x "$BINARY" ]]; then
  echo "missing SIMD cell binary: $BINARY" >&2
  exit 2
fi
BINARY_SHA256="$(sha256sum "$BINARY" | awk '{print $1}')"

DATASET_DIR="$DATASETS_ROOT/$DATASET"
DATASET_IDENTITY="$DATASET_DIR/dataset-identity.json"
if [[ ! -d "$DATASET_DIR" || ! -s "$DATASET_IDENTITY" ]]; then
  echo "missing prepared dataset or dataset identity: $DATASET_DIR" >&2
  exit 2
fi
if [[ "$(sha256sum "$DATASET_IDENTITY" | awk '{print $1}')" != "$DATASET_IDENTITY_SHA256" ]]; then
  echo "SIMD dataset identity drift: $DATASET" >&2
  exit 2
fi

case "$CACHE_STATE" in
  uncached) NATIVE_CACHE_PROFILE="uncached" ;;
  mixed-*) NATIVE_CACHE_PROFILE="mixed_coverage" ;;
  disk-cached) NATIVE_CACHE_PROFILE="disk_cached" ;;
  memory-preloaded) NATIVE_CACHE_PROFILE="memory_preloaded" ;;
  *) echo "unknown SIMD cache state: $CACHE_STATE" >&2; exit 2 ;;
esac
if [[ "$CACHE_STATE" == "memory-preloaded" && "$KIND" != "late-interaction" ]]; then
  echo "memory-preloaded is only valid for late-interaction cells" >&2
  exit 2
fi

if [[ -e "$OUTPUT" ]]; then
  echo "refusing to reuse SIMD cell output: $OUTPUT" >&2
  exit 3
fi
mkdir -p "$OUTPUT/cache" "$OUTPUT/scratch"

BUILD_ROOT="$ROOT/index-state/$BUILD/$PATH_NAME/r$(printf '%02d' "$REPETITION")"
BUILD_MARKER="$BUILD_ROOT/INDEX_COMPLETE"
BUILD_LOCK="$BUILD_ROOT/BUILD_LOCK"
mkdir -p "$BUILD_ROOT"
EXPECTED_INDEX_IDENTITY="$(
  printf '%s\n' \
    "architecture=$ARCHITECTURE" \
    "build=$BUILD" \
    "path=$PATH_NAME" \
    "kind=$KIND" \
    "element_type=$ELEMENT_TYPE" \
    "dataset=$DATASET" \
    "dataset_identity_sha256=$DATASET_IDENTITY_SHA256" \
    "repetition=$REPETITION" \
    "index_key=$INDEX_KEY" \
    "index_uri=$INDEX_URI" \
    "binary_sha256=$BINARY_SHA256" \
    "source_sha256=$SOURCE_SHA256" \
    "manifest_sha256=$MANIFEST_SHA256"
)"

cleanup_path() {
  path="$1"
  case "$path" in
    "$OUTPUT"/*|"$BUILD_ROOT"/*) rm -rf -- "$path" ;;
    *) echo "refusing unsafe SIMD cleanup path: $path" >&2; exit 4 ;;
  esac
}

run_measured() {
  resource_path="$1"
  cache_path="$2"
  scratch_path="$3"
  shift 3
  python3 scripts/benchmark_with_resources.py \
    --output "$resource_path" \
    --cache-dir "$cache_path" \
    --scratch-dir "$scratch_path" \
    -- "$@"
}

if [[ -f "$BUILD_MARKER" ]]; then
  if [[ ! -f "$BUILD_ROOT/index-identity.txt" ]] \
    || [[ "$(cat "$BUILD_ROOT/index-identity.txt")" != "$EXPECTED_INDEX_IDENTITY" ]]; then
    echo "immutable SIMD index identity drift: $BUILD_ROOT" >&2
    exit 3
  fi
else
  if ! mkdir "$BUILD_LOCK" 2>/dev/null; then
    echo "SIMD index build lock already exists: $BUILD_LOCK" >&2
    exit 3
  fi
  build_complete=0
  release_build_lock() {
    exit_status=$?
    if [[ "$build_complete" != "1" ]]; then
      printf '%s\n' "status=failed" "exit_status=$exit_status" > "$BUILD_ROOT/INDEX_FAILED"
    fi
    rmdir "$BUILD_LOCK" 2>/dev/null || true
    return "$exit_status"
  }
  trap release_build_lock EXIT

  mkdir -p "$BUILD_ROOT/output" "$BUILD_ROOT/cache" "$BUILD_ROOT/scratch"
  case "$KIND" in
    primary-dense|primary-binary)
      run_measured \
        "$BUILD_ROOT/resources.csv" \
        "$BUILD_ROOT/cache" \
        "$BUILD_ROOT/scratch" \
        env \
        AWS_REGION="${AWS_REGION:-eu-central-1}" \
        BORSUK_BENCH_DATASET="$DATASET_DIR" \
        BORSUK_BENCH_URI="$INDEX_URI" \
        BORSUK_BENCH_CACHE="$BUILD_ROOT/cache" \
        BORSUK_BENCH_OUTPUT_DIR="$BUILD_ROOT/output" \
        BORSUK_BENCH_VECTOR_ELEMENT_TYPE="$ELEMENT_TYPE" \
        BORSUK_BENCH_GLOBAL_SCAN_CODEC="srht-pq-scan" \
        BORSUK_BENCH_BUILD_INDEX=1 \
        BORSUK_BENCH_BUILD_ONLY=1 \
        BORSUK_BENCH_QUERIES="$EXPECTED_QUERIES" \
        BORSUK_BENCH_QUERY_SEED="$QUERY_SEED" \
        "$BINARY"
      test -s "$BUILD_ROOT/output/bench_build.csv"
      ;;
    named-sparse|text-bm25)
      dense_type="float32"
      sparse_type="float32"
      if [[ "$KIND" == "named-sparse" ]]; then
        sparse_type="$ELEMENT_TYPE"
      fi
      run_measured \
        "$BUILD_ROOT/resources.csv" \
        "$BUILD_ROOT/cache" \
        "$BUILD_ROOT/scratch" \
        env \
        AWS_REGION="${AWS_REGION:-eu-central-1}" \
        BORSUK_HYBRID_DATASET="$DATASET_DIR" \
        BORSUK_HYBRID_INDEX_URI="$INDEX_URI" \
        BORSUK_HYBRID_OUTPUT="$BUILD_ROOT/output" \
        BORSUK_HYBRID_SCAN_CODEC="srht-pq-scan" \
        BORSUK_HYBRID_DENSE_ELEMENT_TYPE="$dense_type" \
        BORSUK_HYBRID_SPARSE_ELEMENT_TYPE="$sparse_type" \
        BORSUK_HYBRID_RAM_BUDGET_BYTES="536870912" \
        "$BINARY" build
      test -s "$BUILD_ROOT/output/hybrid_build.csv"
      ;;
    late-interaction)
      descriptor_type="$(
        python3 - "$DATASET_DIR/dataset.json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["benchmark"]["vector_element_type"])
PY
      )"
      if [[ "$descriptor_type" != "$ELEMENT_TYPE" ]]; then
        echo "late-interaction dataset physical type drift" >&2
        exit 2
      fi
      run_measured \
        "$BUILD_ROOT/resources.csv" \
        "$BUILD_ROOT/cache" \
        "$BUILD_ROOT/scratch" \
        env \
        AWS_REGION="${AWS_REGION:-eu-central-1}" \
        BORSUK_MARKET_DATASET="$DATASET_DIR" \
        BORSUK_MARKET_INDEX_URI="$INDEX_URI" \
        BORSUK_MARKET_OUTPUT="$BUILD_ROOT/output" \
        BORSUK_MARKET_CACHE_DIR="$BUILD_ROOT/cache" \
        BORSUK_MARKET_CACHE_PROFILE="uncached" \
        BORSUK_MARKET_CACHE_COVERAGE_PERCENT=0 \
        BORSUK_MARKET_QUERY_SEED="$QUERY_SEED" \
        BORSUK_MARKET_CLIENT_CONCURRENCY=1 \
        BORSUK_MARKET_MAX_ACTIVE_SEARCHES=4 \
        BORSUK_MARKET_MAX_INFLIGHT_LEAF_READS=24 \
        BORSUK_MARKET_RAM_BUDGET_BYTES=536870912 \
        "$BINARY" late-interaction build
      test -s "$BUILD_ROOT/output/late_interaction_build.csv"
      ;;
  esac
  printf '%s\n' "$EXPECTED_INDEX_IDENTITY" > "$BUILD_ROOT/index-identity.txt"
  printf '%s\n' "status=complete" > "$BUILD_MARKER"
  cleanup_path "$BUILD_ROOT/cache"
  cleanup_path "$BUILD_ROOT/scratch"
  build_complete=1
  trap - EXIT
  rmdir "$BUILD_LOCK"
fi

printf '%s\n' \
  "$EXPECTED_INDEX_IDENTITY" \
  "cache_state=$CACHE_STATE" \
  "target_cache_coverage_percent=$TARGET_COVERAGE" \
  "client_concurrency=$CONCURRENCY" \
  "query_seed=$QUERY_SEED" \
  > "$OUTPUT/cell-environment.txt"

case "$KIND" in
  primary-dense|primary-binary)
    run_measured \
      "$OUTPUT/resources.csv" \
      "$OUTPUT/cache" \
      "$OUTPUT/scratch" \
      env \
      AWS_REGION="${AWS_REGION:-eu-central-1}" \
      BORSUK_BENCH_DATASET="$DATASET_DIR" \
      BORSUK_BENCH_URI="$INDEX_URI" \
      BORSUK_BENCH_CACHE="$OUTPUT/cache" \
      BORSUK_BENCH_OUTPUT_DIR="$OUTPUT" \
      BORSUK_BENCH_VECTOR_ELEMENT_TYPE="$ELEMENT_TYPE" \
      BORSUK_BENCH_GLOBAL_SCAN_CODEC="srht-pq-scan" \
      BORSUK_BENCH_CACHE_EXECUTION="scan" \
      BORSUK_BENCH_FORCE_SEGMENT_PATH=1 \
      BORSUK_BENCH_SERVING_NPROBE=16 \
      BORSUK_BENCH_SERVING_CANDIDATES=256 \
      BORSUK_BENCH_BUILD_INDEX=0 \
      BORSUK_BENCH_SKIP_RECALL=1 \
      BORSUK_BENCH_READ_ONLY=1 \
      BORSUK_BENCH_QUERIES="$EXPECTED_QUERIES" \
      BORSUK_BENCH_QUERY_SEED="$QUERY_SEED" \
      BORSUK_BENCH_REPETITION_ID="r$(printf '%02d' "$REPETITION")" \
      BORSUK_BENCH_CONCURRENCY="$CONCURRENCY" \
      BORSUK_BENCH_CACHE_PROFILE="$NATIVE_CACHE_PROFILE" \
      BORSUK_BENCH_CACHE_COVERAGE_PERCENT="$TARGET_COVERAGE" \
      BORSUK_BENCH_RAM_BUDGET_BYTES="536870912" \
      BORSUK_BENCH_MAX_ACTIVE_SEARCHES="$CONCURRENCY" \
      BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS=24 \
      "$BINARY"
    ;;
  named-sparse|text-bm25)
    mode="sparse"
    sparse_type="$ELEMENT_TYPE"
    if [[ "$KIND" == "text-bm25" ]]; then
      mode="text"
      sparse_type="float32"
    fi
    prime=0
    if (( TARGET_COVERAGE > 0 )); then
      prime=1
    fi
    run_measured \
      "$OUTPUT/resources.csv" \
      "$OUTPUT/cache" \
      "$OUTPUT/scratch" \
      env \
      AWS_REGION="${AWS_REGION:-eu-central-1}" \
      BORSUK_HYBRID_DATASET="$DATASET_DIR" \
      BORSUK_HYBRID_INDEX_URI="$INDEX_URI" \
      BORSUK_HYBRID_OUTPUT="$OUTPUT" \
      BORSUK_HYBRID_CACHE_DIR="$OUTPUT/cache" \
      BORSUK_HYBRID_SCAN_CODEC="srht-pq-scan" \
      BORSUK_HYBRID_DENSE_ELEMENT_TYPE="float32" \
      BORSUK_HYBRID_SPARSE_ELEMENT_TYPE="$sparse_type" \
      BORSUK_HYBRID_MODES="$mode" \
      BORSUK_HYBRID_FUSION="rrf" \
      BORSUK_HYBRID_RRF_K=60 \
      BORSUK_HYBRID_CANDIDATE_DEPTH=100 \
      BORSUK_HYBRID_MAX_CANDIDATES=100 \
      BORSUK_HYBRID_MAX_SEGMENTS=32 \
      BORSUK_HYBRID_QUERY_LIMIT="$EXPECTED_QUERIES" \
      BORSUK_HYBRID_QUERY_SEED="$QUERY_SEED" \
      BORSUK_HYBRID_REPETITIONS=1 \
      BORSUK_HYBRID_CLIENT_CONCURRENCY="$CONCURRENCY" \
      BORSUK_HYBRID_CACHE_PROFILE="$NATIVE_CACHE_PROFILE" \
      BORSUK_HYBRID_TARGET_HOT_FRACTION="$(python3 -c "print($TARGET_COVERAGE / 100)")" \
      BORSUK_HYBRID_PRIME_TARGET_HOT_SET="$prime" \
      BORSUK_HYBRID_RAM_BUDGET_BYTES="536870912" \
      BORSUK_HYBRID_MAX_ACTIVE_SEARCHES="$CONCURRENCY" \
      BORSUK_HYBRID_MAX_INFLIGHT_LEAF_READS=24 \
      "$BINARY" query
    ;;
  late-interaction)
    run_measured \
      "$OUTPUT/resources.csv" \
      "$OUTPUT/cache" \
      "$OUTPUT/scratch" \
      env \
      AWS_REGION="${AWS_REGION:-eu-central-1}" \
      BORSUK_MARKET_DATASET="$DATASET_DIR" \
      BORSUK_MARKET_INDEX_URI="$INDEX_URI" \
      BORSUK_MARKET_OUTPUT="$OUTPUT" \
      BORSUK_MARKET_CACHE_DIR="$OUTPUT/cache" \
      BORSUK_MARKET_CACHE_PROFILE="$NATIVE_CACHE_PROFILE" \
      BORSUK_MARKET_CACHE_COVERAGE_PERCENT="$TARGET_COVERAGE" \
      BORSUK_MARKET_QUERY_SEED="$QUERY_SEED" \
      BORSUK_MARKET_CLIENT_CONCURRENCY="$CONCURRENCY" \
      BORSUK_MARKET_MAX_ACTIVE_SEARCHES="$CONCURRENCY" \
      BORSUK_MARKET_MAX_INFLIGHT_LEAF_READS=24 \
      BORSUK_MARKET_RAM_BUDGET_BYTES=536870912 \
      "$BINARY" late-interaction query
    ;;
esac

python3 scripts/normalize_simd_datatype_cell.py \
  --kind "$KIND" \
  --directory "$OUTPUT" \
  --architecture "$ARCHITECTURE" \
  --instance-type "$INSTANCE_TYPE" \
  --source-sha256 "$SOURCE_SHA256" \
  --manifest-sha256 "$MANIFEST_SHA256" \
  --dataset-identity-sha256 "$DATASET_IDENTITY_SHA256" \
  --build "$BUILD" \
  --binary-sha256 "$BINARY_SHA256" \
  --path "$PATH_NAME" \
  --element-type "$ELEMENT_TYPE" \
  --repetition "$REPETITION" \
  --cache-state "$CACHE_STATE" \
  --target-cache-coverage-percent "$TARGET_COVERAGE" \
  --client-concurrency "$CONCURRENCY" \
  --query-seed "$QUERY_SEED" \
  --expected-queries "$EXPECTED_QUERIES" \
  --late-frontier "$LATE_FRONTIER"

cleanup_path "$OUTPUT/cache"
cleanup_path "$OUTPUT/scratch"
printf '%s\n' "status=complete" > "$OUTPUT/CELL_COMPLETE"
