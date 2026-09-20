#!/bin/bash
set -u

if [ "${1:-}" = --describe ]; then
  printf '%s\n' '{"base_rows":90000,"claim_eligible":false,"delta_rows":10000,"evidence_kind":"semantic-local-artifact-screen","instance_type":"c7i.8xlarge","replacement_rows":500,"rows":100000,"schema":"borsuk-v85-mutation-screen-matrix-v1","spot_only":true,"tombstone_rows":500}'
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
export V85_OUTPUT_URI

root=/mnt/v85-mutation-100k
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in dependency.log hashes.log prepare.log screen.log screen.time receipt.json summary.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$V85_OUTPUT_URI/evidence/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "$V85_SOURCE_COMMIT" >/tmp/v85-mutation-terminal.json
  aws s3 cp /tmp/v85-mutation-terminal.json "$V85_OUTPUT_URI/terminal.json" --only-show-errors
  unlink /tmp/v85-mutation-terminal.json
  exit "$code"
}
trap publish_terminal EXIT

mkdir -p "$root" && cd "$root" || exit 90
phase=dependencies
export HOME=${HOME:-/root}
dnf install -y -q zstd python3-devel time >dependency.log 2>&1 || exit 91
python3 -m venv .venv >>dependency.log 2>&1 || exit 92
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>dependency.log 2>&1 || exit 92

phase=source
aws s3 cp "$V85_SOURCE_ARCHIVE_URI" source.tar.zst --only-show-errors || exit 93
printf '%s  source.tar.zst\n' "$V85_SOURCE_ARCHIVE_SHA256" | sha256sum -c - >hashes.log 2>&1 || exit 94
mkdir repo
tar --zstd -xf source.tar.zst -C repo || exit 95

phase=inputs
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 96
printf '%s  source.parquet\n' \
  2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86 | \
  sha256sum -c - >>hashes.log 2>&1 || exit 97

phase=prepare
export PYTHONPATH="$root/repo" OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16
.venv/bin/python - <<'PY' >prepare.log 2>&1 || exit 98
import pyarrow as pa
import pyarrow.parquet as pq

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
    raise ValueError("mutation screen source prefix differs")
pq.write_table(pa.Table.from_batches(batches), "source-100k.parquet")
print("prepared", rows)
PY

phase=screen
/usr/bin/time -v -o screen.time timeout 1800 .venv/bin/python - <<'PY' >screen.log 2>&1 || exit 99
from pathlib import Path
import os
from scripts.v85_mutation_screen import MutationScreenRequest, run_mutation_screen

run_mutation_screen(
    MutationScreenRequest(
        source=Path("source-100k.parquet"),
        work=Path("screen"),
        output=Path("receipt.json"),
        uri_prefix=f"{os.environ['V85_OUTPUT_URI'].rstrip('/')}/artifacts",
        base_rows=90_000,
        delta_rows=10_000,
        dimensions=768,
        page_rows=256,
        router_cells=256,
        base_runs=1,
        delta_runs=100,
        seed=85,
    )
)
PY

phase=validate
.venv/bin/python - <<'PY' >summary.json || exit 100
import json
from pathlib import Path
from scripts.v85_qualification import validate_mutation_screen

summary = validate_mutation_screen(json.loads(Path("receipt.json").read_bytes()))
print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
PY
phase=complete
exit 0
