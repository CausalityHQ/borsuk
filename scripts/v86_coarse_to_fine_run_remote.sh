#!/bin/bash
set -u

: "${V86_SCREEN_URI:?}"
: "${V86_SCREEN_SHA256:?}"
: "${V86_SHARED_URI:?}"
: "${V86_SHARED_SHA256:?}"
: "${V86_OUTPUT_URI:?}"
: "${V86_SOURCE_COMMIT:?}"

root=/mnt/v86-coarse-to-fine
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  if [ "$code" -eq 0 ]; then
    for name in dependency.log hashes.log run.log time.log result.json; do
      if [ ! -s "$name" ] || ! aws s3 cp "$name" "$V86_OUTPUT_URI/$name" --only-show-errors; then
        code=98
        phase=publication_failed
        break
      fi
    done
  else
    for name in dependency.log hashes.log run.log time.log result.json; do
      [ -f "$name" ] && aws s3 cp "$name" "$V86_OUTPUT_URI/$name" --only-show-errors || true
    done
  fi
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "$V86_SOURCE_COMMIT" >/tmp/v86-coarse-to-fine-terminal.json
  if ! aws s3 cp /tmp/v86-coarse-to-fine-terminal.json \
    "$V86_OUTPUT_URI/terminal.json" --only-show-errors; then
    code=99
  fi
  unlink /tmp/v86-coarse-to-fine-terminal.json
  sync
  shutdown -h now || true
  exit "$code"
}
trap publish_terminal EXIT

# Bound dependency installation, downloads, execution, and publication—not
# merely the scientific child process.
shutdown -h +30 >/dev/null 2>&1 || exit 89

mkdir -p "$root/scripts" && cd "$root" || exit 90
phase=dependencies
printf 'phase=dependencies\n' >dependency.log
dnf install -y -q time python3-pip >>dependency.log 2>&1 || exit 91
python3 -m venv .venv >>dependency.log 2>&1 || exit 91
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>dependency.log 2>&1 || exit 91

phase=inputs
aws s3 cp "$V86_SCREEN_URI" scripts/v86_coarse_to_fine_screen.py \
  --only-show-errors || exit 92
aws s3 cp "$V86_SHARED_URI" scripts/v85_shared_overlay_screen.py \
  --only-show-errors || exit 92
printf '%s  scripts/v86_coarse_to_fine_screen.py\n%s  scripts/v85_shared_overlay_screen.py\n' \
  "$V86_SCREEN_SHA256" "$V86_SHARED_SHA256" >hashes.log
sha256sum -c hashes.log >hashes-check.log || exit 93
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 94
aws s3 cp "$base/development-query.parquet" queries.parquet --only-show-errors || exit 94
aws s3 cp "$base/development-gt100.parquet" truth.parquet --only-show-errors || exit 94
aws s3 cp \
  s3://borsuk-bench-453182569524-euc1/research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy \
  layout.npy --only-show-errors || exit 94
cat >>hashes.log <<'HASHES'
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  queries.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  truth.parquet
32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b  layout.npy
HASHES
sha256sum -c hashes.log >hashes-check.log || exit 95

phase=screen
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16
/usr/bin/time -v -o time.log timeout --signal=TERM --kill-after=30s 1200s \
  .venv/bin/python -m scripts.v86_coarse_to_fine_screen \
  --source source.parquet --queries queries.parquet --ground-truth truth.parquet \
  --layout-order layout.npy --output result.json >run.log 2>&1 || exit 96
test -s result.json || exit 97
phase=complete
exit 0
