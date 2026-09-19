#!/bin/bash
set -u

if [ "${1:-}" = "--describe" ]; then
  printf '%s\n' '{"blas_threads":16,"max_wall_seconds":1200,"nomination_rows":256,"queries":32,"query_parallelism":1,"rayon_work_stealing":false,"rescue_page_cap":24,"spot_only":true,"total_arms":4,"worker_model":"fixed-16-thread-blas"}'
  exit 0
fi

if [ "${1:-}" = "--classify-lifecycle" ]; then
  [ "${2:-}" = "spot" ] || exit 1
  printf 'spot\n'
  exit 0
fi

root=/mnt/v93-protected-rescue
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  if [ "$code" -eq 0 ]; then
    for name in dependency.log hashes.log hashes-check.log instance.json run.log time.log result.json; do
      if [ ! -s "$name" ] || ! aws s3 cp "$name" "$V93_OUTPUT_URI/$name" --only-show-errors; then
        code=98
        phase=publication_failed
        break
      fi
    done
  else
    for name in dependency.log hashes.log hashes-check.log instance.json run.log time.log result.json; do
      if [ -n "${V93_OUTPUT_URI:-}" ] && [ -f "$name" ]; then
        aws s3 cp "$name" "$V93_OUTPUT_URI/$name" --only-show-errors || true
      fi
    done
  fi
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "${V93_SOURCE_COMMIT:-unknown}" >/tmp/v93-protected-rescue-terminal.json
  if [ -n "${V93_OUTPUT_URI:-}" ] && ! aws s3 cp /tmp/v93-protected-rescue-terminal.json \
    "$V93_OUTPUT_URI/terminal.json" --only-show-errors; then
    code=99
  fi
  unlink /tmp/v93-protected-rescue-terminal.json
  sync
  shutdown -h now || true
  exit "$code"
}
trap publish_terminal EXIT

shutdown -h +30 >/dev/null 2>&1 || exit 89

for variable in \
  V93_SCREEN_URI V93_SCREEN_SHA256 V93_V92_URI V93_V92_SHA256 \
  V93_V90_URI V93_V90_SHA256 V93_V88_URI V93_V88_SHA256 \
  V93_V87_URI V93_V87_SHA256 V93_V86_URI V93_V86_SHA256 \
  V93_SHARED_URI V93_SHARED_SHA256 V93_OUTPUT_URI V93_SOURCE_COMMIT; do
  eval "value=\${$variable:-}"
  [ -n "$value" ] || exit 86
done

mkdir -p "$root/scripts" && cd "$root" || exit 90

phase=instance
token=$(curl --fail --silent --show-error --request PUT \
  --header 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
  http://169.254.169.254/latest/api/token) || exit 87
metadata() {
  curl --fail --silent --show-error \
    --header "X-aws-ec2-metadata-token: $token" \
    "http://169.254.169.254/latest/meta-data/$1"
}
lifecycle=$(metadata instance-life-cycle) || exit 87
[ "$lifecycle" = spot ] || exit 88
instance_id=$(metadata instance-id) || exit 87
instance_type=$(metadata instance-type) || exit 87
availability_zone=$(metadata placement/availability-zone) || exit 87
printf '{"availability_zone":"%s","instance_id":"%s","instance_type":"%s","lifecycle":"%s"}\n' \
  "$availability_zone" "$instance_id" "$instance_type" "$lifecycle" >instance.json

phase=dependencies
printf 'phase=dependencies\n' >dependency.log
dnf install -y -q time python3-pip >>dependency.log 2>&1 || exit 91
python3 -m venv .venv >>dependency.log 2>&1 || exit 91
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' >>dependency.log 2>&1 || exit 91

phase=inputs
aws s3 cp "$V93_SCREEN_URI" scripts/v93_protected_rescue_screen.py --only-show-errors || exit 92
aws s3 cp "$V93_V92_URI" scripts/v92_wave2_nominated_rows_screen.py --only-show-errors || exit 92
aws s3 cp "$V93_V90_URI" scripts/v90_residual_row_sketch_screen.py --only-show-errors || exit 92
aws s3 cp "$V93_V88_URI" scripts/v88_planner_objective_screen.py --only-show-errors || exit 92
aws s3 cp "$V93_V87_URI" scripts/v87_summary_capacity_screen.py --only-show-errors || exit 92
aws s3 cp "$V93_V86_URI" scripts/v86_coarse_to_fine_screen.py --only-show-errors || exit 92
aws s3 cp "$V93_SHARED_URI" scripts/v85_shared_overlay_screen.py --only-show-errors || exit 92
printf '%s  scripts/v93_protected_rescue_screen.py\n%s  scripts/v92_wave2_nominated_rows_screen.py\n%s  scripts/v90_residual_row_sketch_screen.py\n%s  scripts/v88_planner_objective_screen.py\n%s  scripts/v87_summary_capacity_screen.py\n%s  scripts/v86_coarse_to_fine_screen.py\n%s  scripts/v85_shared_overlay_screen.py\n' \
  "$V93_SCREEN_SHA256" "$V93_V92_SHA256" "$V93_V90_SHA256" \
  "$V93_V88_SHA256" "$V93_V87_SHA256" "$V93_V86_SHA256" \
  "$V93_SHARED_SHA256" >hashes.log
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
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16
/usr/bin/time -v -o time.log timeout --signal=TERM --kill-after=30s 1200s \
  .venv/bin/python -m scripts.v93_protected_rescue_screen \
  --source source.parquet --queries queries.parquet --ground-truth truth.parquet \
  --layout-order layout.npy --output result.json >run.log 2>&1 || exit 96
test -s result.json || exit 97
phase=complete
exit 0
