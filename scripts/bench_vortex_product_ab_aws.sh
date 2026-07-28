#!/usr/bin/env bash
# Fresh Fashion-MNIST end-to-end product A/B: identical production_bench
# configuration, with only the persisted normal-segment table format changed.
set -euo pipefail

: "${BORSUK_PUBLICATION_BUCKET:?set the plain benchmark bucket name}"
: "${BORSUK_VORTEX_PRODUCT_RUN_ID:?set BORSUK_VORTEX_PRODUCT_RUN_ID}"
: "${BORSUK_VORTEX_PRODUCT_RESULT_PREFIX:?set a fresh result prefix}"
: "${BORSUK_VORTEX_PRODUCT_INDEX_PREFIX:?set a fresh index prefix}"
: "${BORSUK_SOURCE_SHA256:?set the exact source archive SHA-256}"

REGION="${AWS_REGION:-eu-central-1}"
DATASET="${BORSUK_VORTEX_PRODUCT_DATASET:-/home/ec2-user/borsuk-datasets/fashion-mnist-784}"
ROOT="${BORSUK_VORTEX_PRODUCT_ROOT:-/home/ec2-user/borsuk-vortex-product-ab/$BORSUK_VORTEX_PRODUCT_RUN_ID}"
LAUNCHED_INSTANCE="${BORSUK_VORTEX_PRODUCT_LAUNCHED_INSTANCE:-0}"
SHUTDOWN="${BORSUK_VORTEX_PRODUCT_SHUTDOWN:-0}"
TOOLCHAIN_BIN="${BORSUK_RUST_TOOLCHAIN_BIN:-/home/ec2-user/.rustup/toolchains/stable-aarch64-unknown-linux-gnu/bin}"
export PATH="$TOOLCHAIN_BIN:/home/ec2-user/.cargo/bin:$PATH"

cd "$(dirname "$0")/.."

if [[ ! "$BORSUK_SOURCE_SHA256" =~ ^[[:xdigit:]]{64}$ ]]; then
  echo "BORSUK_SOURCE_SHA256 must be a 64-character SHA-256 digest" >&2
  exit 2
fi
if [[ ! -d "$DATASET" || ! -s "$DATASET/meta.json" ]]; then
  echo "missing existing Fashion-MNIST dataset: $DATASET" >&2
  exit 3
fi
if [[ -e "$ROOT" ]]; then
  echo "refusing to overwrite local campaign root: $ROOT" >&2
  exit 3
fi
for prefix in "$BORSUK_VORTEX_PRODUCT_RESULT_PREFIX" "$BORSUK_VORTEX_PRODUCT_INDEX_PREFIX"; do
  existing="$(aws --region "$REGION" s3api list-objects-v2 \
    --bucket "$BORSUK_PUBLICATION_BUCKET" \
    --prefix "${prefix%/}/" \
    --max-keys 1 \
    --query KeyCount \
    --output text)"
  if [[ "$existing" != "0" ]]; then
    echo "refusing to overwrite non-empty S3 prefix: s3://$BORSUK_PUBLICATION_BUCKET/$prefix" >&2
    exit 3
  fi
done

mkdir -p "$ROOT/resource-capture" "$ROOT/scratch"
exec > >(tee -a "$ROOT/campaign.log") 2>&1

finish() {
  local status=$?
  aws --region "$REGION" s3 sync \
    "$ROOT" \
    "s3://$BORSUK_PUBLICATION_BUCKET/$BORSUK_VORTEX_PRODUCT_RESULT_PREFIX" \
    --exclude '*/cache-tree/*' \
    --exclude '*/scratch/*' \
    --exclude '*/target/*' \
    --only-show-errors || true
  if [[ "$LAUNCHED_INSTANCE" == "1" && "$SHUTDOWN" == "1" ]]; then
    sudo shutdown -h now >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap finish EXIT

{
  printf '%s\n' \
    "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "run_id=$BORSUK_VORTEX_PRODUCT_RUN_ID" \
    "source_sha256=$BORSUK_SOURCE_SHA256" \
    "dataset=fashion-mnist-784" \
    "dataset_path=$DATASET" \
    "formats=parquet,vortex" \
    "samples=30" \
    "materialized_borsuk_query=true" \
    "index_prefix=s3://$BORSUK_PUBLICATION_BUCKET/$BORSUK_VORTEX_PRODUCT_INDEX_PREFIX" \
    "instance_type=${BORSUK_INSTANCE_TYPE:-unknown}" \
    "local_disk_class=${BORSUK_LOCAL_DISK_CLASS:-unknown}" \
    "resource_scope=per-variant_process-tree_cpu_ram_disk;exclusive-worker_network" \
    "kernel=$(uname -srmo)" \
    "logical_cpus=$(getconf _NPROCESSORS_ONLN)" \
    "memory_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINTS,FSTYPE
} > "$ROOT/environment.txt"
printf '%s\n' \
  "Both product arms materialize normal BORSUK query results." \
  "The only intended variable is segment_table_format=parquet|vortex." \
  "Each arm is rebuilt at a fresh S3 URI; no index, output, or cache is reused." \
  "query_paths=production,segment" \
  "production uses the real resident global-PQ serving path." \
  "segment disables the coarse quantizer to exercise the selected normal-segment decoder." \
  "external_warmups=0" \
  "production_bench_internal_cache_warmup=true" \
  "No external process warmup is used; production_bench prepares serving metadata during open." \
  "Its internal cache-state setup is outside each timed query distribution." \
  > "$ROOT/measurement-contract.txt"

if [[ ! -x "$TOOLCHAIN_BIN/cargo" || ! -x "$TOOLCHAIN_BIN/rustc" ]]; then
  echo "complete preinstalled Rust toolchain is missing at $TOOLCHAIN_BIN" >&2
  exit 2
fi
if ! find /usr/lib64 /usr/lib -name 'libclang.so*' -print -quit 2>/dev/null | grep -q .; then
  command -v dnf >/dev/null 2>&1 || {
    echo "Vortex's Unix I/O dependency requires libclang, and dnf is unavailable" >&2
    exit 2
  }
  sudo dnf install -y clang clang-devel
fi
LIBCLANG_PATH="$(
  find /usr/lib64 /usr/lib -name 'libclang.so*' -printf '%h\n' -quit 2>/dev/null
)"
if [[ -z "$LIBCLANG_PATH" ]]; then
  echo "libclang installation completed without a discoverable shared library" >&2
  exit 2
fi
export LIBCLANG_PATH
printf '%s\n' "libclang_path=$LIBCLANG_PATH" >> "$ROOT/environment.txt"
cargo build --locked --release -p borsuk --example production_bench

for variant in parquet vortex; do
  index_uri="s3://$BORSUK_PUBLICATION_BUCKET/$BORSUK_VORTEX_PRODUCT_INDEX_PREFIX/$variant/fashion-mnist-784/srht-pq-scan"
  for query_path in production segment; do
    variant_root="$ROOT/variants/$variant/$query_path"
    resource_capture="$ROOT/resource-capture/$variant-$query_path.csv"
    env \
      AWS_REGION="$REGION" \
      AWS_DEFAULT_REGION="$REGION" \
      BORSUK_PRODUCT_VARIANT="$variant" \
      BORSUK_PRODUCT_QUERY_PATH="$query_path" \
      BORSUK_SEGMENT_TABLE_FORMAT="$variant" \
      BORSUK_PRODUCT_DATASET="$DATASET" \
      BORSUK_PRODUCT_INDEX_URI="$index_uri" \
      BORSUK_PRODUCT_VARIANT_ROOT="$variant_root" \
      BORSUK_PRODUCT_SAMPLES=30 \
      python3 scripts/benchmark_with_resources.py \
        --output "$resource_capture" \
        --scratch-dir "$ROOT/scratch/$variant-$query_path" \
        --interval-ms 100 \
        --cache-interval-ms 1000 \
        -- bash scripts/run_vortex_product_ab_variant.sh
    cp "$resource_capture" "$variant_root/resources.csv"
  done
done

for variant in parquet vortex; do
  for query_path in production segment; do
    variant_root="$ROOT/variants/$variant/$query_path"
    measured="$variant_root/measured"
    required_artifacts="bench_recall_latency.csv,bench_query_samples.csv,bench_startup.csv,bench_cache_states.csv,bench_concurrency.csv,bench_concurrency_samples.csv,bench_cache_coverage.csv"
    if [[ "$query_path" == "production" ]]; then
      required_artifacts="bench_build.csv,$required_artifacts"
    fi
    python3 scripts/validate_benchmark_artifacts.py \
      --directory "$measured" \
      --expected-codec srht-pq-scan \
      --required "$required_artifacts"

    python3 - "$variant_root" "$variant" "$query_path" <<'PY'
import csv
from pathlib import Path
import sys

root = Path(sys.argv[1])
variant = sys.argv[2]
query_path = sys.argv[3]
measured = root / "measured"

def rows(name):
    with (measured / name).open(newline="") as handle:
        return list(csv.DictReader(handle))

if query_path == "production":
    build = rows("bench_build.csv")
    if len(build) != 1:
        raise SystemExit(f"{variant}/{query_path}: expected one build row")
recall = rows("bench_recall_latency.csv")
samples = rows("bench_query_samples.csv")
cache = rows("bench_cache_states.csv")
concurrency = rows("bench_concurrency.csv")
concurrency_samples = rows("bench_concurrency_samples.csv")
coverage = rows("bench_cache_coverage.csv")
if not recall or any(int(row["samples"]) < 30 for row in recall):
    raise SystemExit(f"{variant}/{query_path}: every recall distribution needs >=30 samples")
if not samples:
    raise SystemExit(f"{variant}/{query_path}: missing raw recall/latency samples")
global_scan_chunks = [int(row["global_scan_chunks"]) for row in samples]
if query_path == "production" and not any(value > 0 for value in global_scan_chunks):
    raise SystemExit(f"{variant}/{query_path}: resident global-PQ scan path was not observed")
if query_path == "segment" and any(value > 0 for value in global_scan_chunks):
    raise SystemExit(f"{variant}/{query_path}: forced segment path leaked into global-PQ scan")
if {row["phase"] for row in cache} != {"uncached", "disk_cached"}:
    raise SystemExit(f"{variant}/{query_path}: cache-state evidence is incomplete")
if any(int(row["queries"]) < 30 for row in cache):
    raise SystemExit(f"{variant}/{query_path}: cache-state distributions need >=30 samples")
if {int(row["workers"]) for row in concurrency} != {1, 4, 16}:
    raise SystemExit(f"{variant}/{query_path}: concurrency matrix is incomplete")
if any(int(row["total_queries"]) < 30 for row in concurrency):
    raise SystemExit(f"{variant}/{query_path}: concurrency distributions need >=30 samples")
if not concurrency_samples or not coverage:
    raise SystemExit(f"{variant}/{query_path}: raw concurrency/cache-coverage evidence is missing")
with (root / "resources.csv").open(newline="") as handle:
    resources = list(csv.DictReader(handle))
required = {
    "cpu_percent", "rss_bytes", "process_read_bytes", "process_write_bytes",
    "cache_disk_bytes", "scratch_disk_bytes", "network_receive_bytes",
    "network_transmit_bytes",
}
if not resources or not required.issubset(resources[0]):
    raise SystemExit(f"{variant}/{query_path}: resource CSV lacks CPU/RAM/disk/network")
if max(float(row["cpu_percent"]) for row in resources) <= 0:
    raise SystemExit(f"{variant}/{query_path}: no CPU activity observed")
if max(int(row["rss_bytes"]) for row in resources) <= 0:
    raise SystemExit(f"{variant}/{query_path}: no RSS observed")
if max(int(row["process_read_bytes"]) + int(row["process_write_bytes"]) for row in resources) <= 0:
    raise SystemExit(f"{variant}/{query_path}: no process disk I/O observed")
if max(int(row["network_receive_bytes"]) + int(row["network_transmit_bytes"]) for row in resources) <= 0:
    raise SystemExit(f"{variant}/{query_path}: no network activity observed")
print(f"validated {variant}/{query_path}: {len(samples)} query and {len(concurrency_samples)} concurrency samples")
PY

    mkdir -p "$variant_root/charts/resources" "$variant_root/charts/recall-latency"
    python3 scripts/render_resource_charts.py \
      --experiment-root "$variant_root" \
      --output-dir "$variant_root/charts/resources" \
      --prefix "$variant-$query_path-resources"
    python3 scripts/render_recall_latency_charts.py \
      --input "$measured/bench_recall_latency.csv" \
      --dataset "fashion-mnist-784-$variant-$query_path" \
      --output-dir "$variant_root/charts/recall-latency" \
      --subtitle "AWS S3 · fresh $variant index · $query_path query path"
    resource_chart="$variant_root/charts/resources/$variant-$query_path-resources-experiment.svg"
    if [[ ! -s "$resource_chart" ]]; then
      echo "resource chart was not generated: $resource_chart" >&2
      exit 5
    fi
    for panel in "CPU utilization" "Process memory" "Disk and cache footprint" "Network I/O"; do
      grep -Fq "$panel" "$resource_chart" || {
        echo "$variant/$query_path resource chart is missing panel: $panel" >&2
        exit 5
      }
    done
    if ! find "$variant_root/charts/recall-latency" -name '*.svg' -type f -size +0c | grep -q .; then
      echo "$variant/$query_path recall/latency chart was not generated" >&2
      exit 5
    fi
  done
done

python3 - "$ROOT" <<'PY'
import csv
from pathlib import Path
import sys

root = Path(sys.argv[1])
out = root / "comparison.csv"
fields = [
    "format", "query_path", "category", "label", "samples", "recall_at_10", "mean_ms",
    "stddev_ms", "p50_ms", "p95_ms", "p99_ms", "qps",
    "segment_bytes", "total_active_index_bytes", "bytes_per_vector",
]
with out.open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    for variant in ("parquet", "vortex"):
        for query_path in ("production", "segment"):
            measured = root / "variants" / variant / query_path / "measured"
            def read(name):
                with (measured / name).open(newline="") as source:
                    return list(csv.DictReader(source))
            if query_path == "production":
                build = read("bench_build.csv")[0]
                writer.writerow({
                    "format": variant,
                    "query_path": query_path,
                    "category": "build",
                    "label": "fresh-index",
                    "segment_bytes": build["segment_bytes"],
                    "total_active_index_bytes": build["total_active_index_bytes"],
                    "bytes_per_vector": build["bytes_per_vector"],
                })
            for row in read("bench_recall_latency.csv"):
                output = {key: row.get(key, "") for key in fields}
                output.update({
                    "format": variant,
                    "query_path": query_path,
                    "category": "recall-latency",
                    "label": f'{row["phase"]}:{row["mode"]}:nprobe={row["nprobe"]}',
                })
                writer.writerow(output)
            for row in read("bench_cache_states.csv"):
                output = {key: row.get(key, "") for key in fields}
                output.update({
                    "format": variant,
                    "query_path": query_path,
                    "category": "cache",
                    "label": row["phase"],
                    "samples": row["queries"],
                })
                writer.writerow(output)
            for row in read("bench_concurrency.csv"):
                output = {key: row.get(key, "") for key in fields}
                output.update({
                    "format": variant,
                    "query_path": query_path,
                    "category": "concurrency",
                    "label": f'workers={row["workers"]}',
                    "samples": row["total_queries"],
                })
                writer.writerow(output)
rows = list(csv.DictReader(out.open(newline="")))
if {row["format"] for row in rows} != {"parquet", "vortex"}:
    raise SystemExit("comparison.csv does not contain both product formats")
if {row["query_path"] for row in rows} != {"production", "segment"}:
    raise SystemExit("comparison.csv does not contain both query paths")
PY

aws --region "$REGION" s3 sync \
  "$ROOT" \
  "s3://$BORSUK_PUBLICATION_BUCKET/$BORSUK_VORTEX_PRODUCT_RESULT_PREFIX" \
  --exclude '*/cache-tree/*' \
  --exclude '*/scratch/*' \
  --only-show-errors
if aws --region "$REGION" s3api head-object \
  --bucket "$BORSUK_PUBLICATION_BUCKET" \
  --key "$BORSUK_VORTEX_PRODUCT_RESULT_PREFIX/VORTEX_PRODUCT_AB_COMPLETE" \
  >/dev/null 2>&1; then
  echo "refusing to overwrite completion checkpoint" >&2
  exit 3
fi
checkpoint_temp="$(mktemp /tmp/borsuk-vortex-product-ab-complete.XXXXXX)"
printf '%s\n' \
  "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "source_sha256=$BORSUK_SOURCE_SHA256" \
  "formats=parquet,vortex" \
  "query_paths=production,segment" \
  "materialized_borsuk_query=true" \
  > "$checkpoint_temp"
aws --region "$REGION" s3 cp \
  "$checkpoint_temp" \
  "s3://$BORSUK_PUBLICATION_BUCKET/$BORSUK_VORTEX_PRODUCT_RESULT_PREFIX/VORTEX_PRODUCT_AB_COMPLETE" \
  --only-show-errors
mv "$checkpoint_temp" "$ROOT/VORTEX_PRODUCT_AB_COMPLETE"
echo "VORTEX_PRODUCT_AB_COMPLETE"
