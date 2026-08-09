#!/usr/bin/env bash
set -euo pipefail

[[ "${BORSUK_RUN_APPROXIMATE_FIRST_QUALIFICATION:-0}" == "1" ]] || {
  echo "set BORSUK_RUN_APPROXIMATE_FIRST_QUALIFICATION=1 to execute" >&2
  exit 2
}

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
manifest="${BORSUK_APPROXIMATE_FIRST_MANIFEST:-$repo_root/docs/research/approximate-first-cohere-1m-local-qualification.json}"
dataset="${BORSUK_APPROXIMATE_FIRST_DATASET:-/data/home/rb/borsuk-datasets/cohere-medium-1M}"
output="${BORSUK_APPROXIMATE_FIRST_OUTPUT:?set BORSUK_APPROXIMATE_FIRST_OUTPUT}"
index_uri="${BORSUK_APPROXIMATE_FIRST_INDEX_URI:?set BORSUK_APPROXIMATE_FIRST_INDEX_URI}"
cache_dir="${BORSUK_APPROXIMATE_FIRST_CACHE_DIR:?set BORSUK_APPROXIMATE_FIRST_CACHE_DIR}"

[[ ! -e "$output" ]] || { echo "output already exists: $output" >&2; exit 2; }
[[ -f "$dataset/dataset.json" ]] || { echo "dataset descriptor missing" >&2; exit 2; }
if [[ -d "$repo_root/.git" ]]; then
  [[ -z "$(git -C "$repo_root" status --porcelain)" ]] || { echo "source tree is dirty" >&2; exit 2; }
  git -C "$repo_root" fetch origin main
  git -C "$repo_root" merge-base --is-ancestor origin/main HEAD || {
    echo "origin/main is not an ancestor of HEAD" >&2
    exit 2
  }
  source_commit="$(git -C "$repo_root" rev-parse HEAD)"
  source_archive_sha256=""
else
  source_commit="${BORSUK_SOURCE_COMMIT:?gitless execution requires BORSUK_SOURCE_COMMIT}"
  source_archive="${BORSUK_SOURCE_ARCHIVE:?gitless execution requires BORSUK_SOURCE_ARCHIVE}"
  source_archive_sha256="${BORSUK_SOURCE_ARCHIVE_SHA256:?gitless execution requires BORSUK_SOURCE_ARCHIVE_SHA256}"
  [[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid source commit" >&2; exit 2; }
  [[ "$source_archive_sha256" =~ ^[0-9a-f]{64}$ ]] || { echo "invalid source archive SHA-256" >&2; exit 2; }
  [[ "$(sha256sum "$source_archive" | awk '{print $1}')" == "$source_archive_sha256" ]] || {
    echo "source archive SHA-256 mismatch" >&2
    exit 2
  }
fi

mkdir -p "$output" "$cache_dir"
failure="$output/APPROXIMATE_FIRST_QUALIFICATION_FAILED"
mark_failure() {
  local status="$1"
  if [[ "$status" -ne 0 ]]; then
    printf 'exit=%s\n' "$status" > "$failure"
  fi
}
trap 'mark_failure $?' EXIT
trap 'mark_failure 130; exit 130' INT
trap 'mark_failure 143; exit 143' TERM

readarray -t protocol < <(python3 - "$manifest" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
print(value["dataset_descriptor_sha256"])
print(value["queries"])
print(value["query_seed"])
print(",".join(map(str, value["nprobes"])))
print(",".join(map(str, value["max_candidates"])))
print(value["scan_codec"])
PY
)
dataset_sha="$(sha256sum "$dataset/dataset.json" | awk '{print $1}')"
[[ "$dataset_sha" == "${protocol[0]}" ]] || { echo "dataset descriptor SHA-256 mismatch" >&2; exit 2; }

export RUSTC_WRAPPER="${RUSTC_WRAPPER:-/usr/local/libexec/devbox-rustc-wrapper}"
export SCCACHE_DIR="${SCCACHE_DIR:-/data/cache/sccache}"
cargo build --locked --release -p borsuk --example production_bench
target_dir="$(cargo metadata --locked --no-deps --format-version=1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="$target_dir/release/examples/production_bench"
[[ -x "$binary" ]] || { echo "production_bench binary missing" >&2; exit 2; }

python3 - "$output/qualification_identity.json" "$manifest" "$dataset_sha" "$binary" "$source_commit" "$source_archive_sha256" <<'PY'
import hashlib, json, sys
path, manifest, dataset_sha, binary, source_commit, source_archive_sha = sys.argv[1:]
identity = {
    "source_commit": source_commit,
    "source_archive_sha256": source_archive_sha or None,
    "manifest_sha256": hashlib.sha256(open(manifest, "rb").read()).hexdigest(),
    "dataset_descriptor_sha256": dataset_sha,
    "binary_sha256": hashlib.sha256(open(binary, "rb").read()).hexdigest(),
    "source_tree_clean": True,
    "origin_main_ancestor": True,
}
open(path, "w").write(json.dumps(identity, indent=2, sort_keys=True) + "\n")
PY

env \
  BORSUK_BENCH_DATASET="$dataset" \
  BORSUK_BENCH_URI="$index_uri" \
  BORSUK_BENCH_CACHE="$cache_dir" \
  BORSUK_BENCH_OUTPUT_DIR="$output" \
  BORSUK_BENCH_LIMIT=0 \
  BORSUK_BENCH_QUERIES="${protocol[1]}" \
  BORSUK_BENCH_QUERY_SEED="${protocol[2]}" \
  BORSUK_BENCH_NPROBES="${protocol[3]}" \
  BORSUK_BENCH_CANDIDATES="${protocol[4]}" \
  BORSUK_BENCH_GLOBAL_SCAN_CODEC="${protocol[5]}" \
  BORSUK_BENCH_CACHE_EXECUTION=scan \
  BORSUK_BENCH_CACHE_PROFILE=uncached \
  BORSUK_BENCH_APPROXIMATE_FIRST_PAIR=1 \
  BORSUK_BENCH_REPETITION_ID=local-r01 \
  "$binary"

set +e
python3 "$repo_root/scripts/validate_approximate_first_qualification.py" \
  "$output" "$manifest" --decision "$output/approximate-first-decision.json"
decision_status=$?
set -e
if [[ $decision_status -eq 0 ]]; then
  printf 'complete\n' > "$output/APPROXIMATE_FIRST_QUALIFICATION_COMPLETE"
elif [[ $decision_status -eq 1 ]]; then
  printf 'rejected\n' > "$output/APPROXIMATE_FIRST_QUALIFICATION_REJECTED"
else
  exit "$decision_status"
fi
rm -f "$failure"
trap - EXIT INT TERM
exit "$decision_status"
