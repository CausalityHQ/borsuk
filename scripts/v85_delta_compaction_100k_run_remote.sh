#!/bin/bash
set -u

if [ "${1:-}" = --describe ]; then
  printf '%s\n' '{"base_rows":90000,"claim_eligible":false,"delta_rows":10000,"evidence_kind":"semantic-local-artifact-screen","page_budget":512,"query_count":32,"rows":100000,"run_counts":[1,10,100],"schema":"borsuk-v85-delta-compaction-screen-matrix-v1"}'
  exit 0
fi
if [ "${1:-}" != "" ]; then
  echo "usage: $0 [--describe]" >&2
  exit 2
fi

: "${V85_SOURCE_ARCHIVE_URI:?}"
: "${V85_SOURCE_ARCHIVE_SHA256:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_OUTPUT_URI:?}"

root=/mnt/v85-delta-compaction-100k
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in dependency.log hashes.log prepare.log build.log screen.time receipt.json summary.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$V85_OUTPUT_URI/evidence/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "$V85_SOURCE_COMMIT" >/tmp/v85-delta-compaction-terminal.json
  aws s3 cp /tmp/v85-delta-compaction-terminal.json "$V85_OUTPUT_URI/terminal.json" --only-show-errors
  unlink /tmp/v85-delta-compaction-terminal.json
  exit "$code"
}
trap publish_terminal EXIT

mkdir -p "$root" && cd "$root" || exit 90
phase=dependencies
export HOME=${HOME:-/root}
dnf install -y -q gcc zstd python3-devel time >dependency.log 2>&1 || exit 91
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal --default-toolchain stable >>dependency.log 2>&1 || exit 92
fi
export PATH="$HOME/.cargo/bin:$PATH"
python3 -m venv .venv >>dependency.log 2>&1 || exit 93
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>dependency.log 2>&1 || exit 93

phase=source
aws s3 cp "$V85_SOURCE_ARCHIVE_URI" source.tar.zst --only-show-errors || exit 94
printf '%s  source.tar.zst\n' "$V85_SOURCE_ARCHIVE_SHA256" | sha256sum -c - >hashes.log 2>&1 || exit 95
mkdir repo
tar --zstd -xf source.tar.zst -C repo || exit 96
(cd repo && cargo build --release --locked -p borsuk-v71 --bin v85_delta_reader) >build.log 2>&1 || exit 97
binary="$root/repo/target/release/v85_delta_reader"

phase=inputs
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 98
aws s3 cp "$base/development-query.parquet" query-source.parquet --only-show-errors || exit 98
aws s3 cp "$base/development-gt100.parquet" gt-source.parquet --only-show-errors || exit 98
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 99
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query-source.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt-source.parquet
HASHES

phase=prepare
export PYTHONPATH="$root/repo" OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16
.venv/bin/python - <<'PY' >prepare.log 2>&1 || exit 100
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.v85_build_delta import canonicalize_evaluation, compute_exact_truth

source = pq.ParquetFile("source.parquet")
batches = []
rows = 0
for batch in source.iter_batches(batch_size=100_000):
    take = min(batch.num_rows, 100_000 - rows)
    batches.append(batch.slice(0, take))
    rows += take
    if rows == 100_000:
        break
if rows != 100_000:
    raise ValueError("semantic screen source prefix differs")
pq.write_table(pa.Table.from_batches(batches), "source-100k.parquet")
canonicalize_evaluation(
    Path("query-source.parquet"), Path("gt-source.parquet"),
    Path("queries.parquet"), Path("truth-placeholder.parquet"),
    dimensions=768, neighbors=100, query_limit=32,
)
compute_exact_truth(
    Path("source-100k.parquet"), Path("query-source.parquet"), Path("truth.parquet"),
    dimensions=768, corpus_rows=100_000, neighbors=100, query_limit=32,
)
PY

phase=screen
/usr/bin/time -v -o screen.time timeout 2400 .venv/bin/python \
  repo/scripts/v85_delta_compaction_screen.py \
  --source source-100k.parquet --queries queries.parquet --truth truth.parquet \
  --binary "$binary" --work screen --output receipt.json \
  --uri-prefix "$V85_OUTPUT_URI/artifacts" >>build.log 2>&1 || exit 101

phase=validate
.venv/bin/python - <<'PY' >summary.json || exit 102
import json
from pathlib import Path
from scripts.v85_qualification import validate_delta_compaction_screen

receipt = json.loads(Path("receipt.json").read_bytes())
summary = validate_delta_compaction_screen(receipt)
print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
PY
phase=complete
exit 0
