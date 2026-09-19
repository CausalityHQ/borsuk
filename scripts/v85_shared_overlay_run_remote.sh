#!/bin/bash
set -u

: "${V85_SCREEN_URI:?}"
: "${V85_SCREEN_SHA256:?}"
: "${V85_OUTPUT_URI:?}"
: "${V85_SOURCE_COMMIT:?}"
: "${V85_ROWS:=1000000}"
: "${V85_BASE_ROWS:=900000}"
: "${V85_QUERY_COUNT:=32}"
: "${V85_ORACLE_ONLY:=0}"

root=/mnt/v85-shared-overlay
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  for name in dependency.log hashes.log run.log time.log result.json; do
    [ -f "$name" ] && aws s3 cp "$name" "$V85_OUTPUT_URI/$name" --only-show-errors
  done
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "$V85_SOURCE_COMMIT" >/tmp/v85-shared-overlay-terminal.json
  aws s3 cp /tmp/v85-shared-overlay-terminal.json "$V85_OUTPUT_URI/terminal.json" --only-show-errors
  unlink /tmp/v85-shared-overlay-terminal.json
  exit "$code"
}
trap publish_terminal EXIT

mkdir -p "$root" && cd "$root" || exit 90
phase=dependencies
dnf install -y -q time python3-pip >dependency.log 2>&1 || exit 91
python3 -m venv .venv >>dependency.log 2>&1 || exit 91
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>dependency.log 2>&1 || exit 91

phase=inputs
aws s3 cp "$V85_SCREEN_URI" screen.py --only-show-errors || exit 92
printf '%s  screen.py\n' "$V85_SCREEN_SHA256" | sha256sum -c - >hashes.log 2>&1 || exit 93
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 94
aws s3 cp "$base/development-query.parquet" queries.parquet --only-show-errors || exit 94
aws s3 cp "$base/development-gt100.parquet" truth.parquet --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy layout.npy --only-show-errors || exit 94
sha256sum -c >>hashes.log 2>&1 <<'HASHES' || exit 95
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  queries.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  truth.parquet
32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b  layout.npy
HASHES

phase=screen
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
screen_args=(
  --source source.parquet --queries queries.parquet --ground-truth truth.parquet
  --layout-order layout.npy --rows "$V85_ROWS" --base-rows "$V85_BASE_ROWS"
  --query-count "$V85_QUERY_COUNT" --output result.json
)
if [ "$V85_ORACLE_ONLY" = 1 ]; then
  screen_args+=(--oracle-only)
fi
/usr/bin/time -v -o time.log .venv/bin/python screen.py \
  "${screen_args[@]}" \
  >run.log 2>&1 || exit 96
test -s result.json || exit 97
phase=complete
exit 0
