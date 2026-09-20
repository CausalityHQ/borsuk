#!/bin/bash
set -u

if [ "${1:-}" = "--describe" ]; then
  printf '%s\n' '{"actual_s3":true,"instance_type":"c7i.8xlarge","max_wall_seconds":3000,"queries":16,"query_parallelism":1,"range_read_threads":16,"rayon_work_stealing":false,"spot_only":true,"stage":"frozen-plan-replay"}'
  exit 0
fi

if [ "${1:-}" = "--classify-lifecycle" ]; then
  [ "${2:-}" = "spot" ] || exit 1
  printf 'spot\n'
  exit 0
fi

root=/mnt/v95-real-s3-plan-replay
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  if [ "$code" -eq 0 ]; then
    for name in dependency.log hashes.log hashes-check.log instance.json build.log build-time.log build-receipt.json plan.json replay.log replay-time.log result.json; do
      if [ ! -s "$name" ] || ! aws s3 cp "$name" "$V95_OUTPUT_URI/$name" --only-show-errors; then
        code=98
        phase=publication_failed
        break
      fi
    done
  else
    for name in dependency.log hashes.log hashes-check.log instance.json build.log build-time.log build-receipt.json plan.json replay.log replay-time.log result.json; do
      if [ -n "${V95_OUTPUT_URI:-}" ] && [ -f "$name" ]; then
        aws s3 cp "$name" "$V95_OUTPUT_URI/$name" --only-show-errors || true
      fi
    done
  fi
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "${V95_SOURCE_COMMIT:-unknown}" >/tmp/v95-terminal.json
  if [ -n "${V95_OUTPUT_URI:-}" ] && ! aws s3 cp /tmp/v95-terminal.json \
    "$V95_OUTPUT_URI/terminal.json" --only-show-errors; then
    code=99
  fi
  unlink /tmp/v95-terminal.json
  sync
  shutdown -h now || true
  exit "$code"
}
trap publish_terminal EXIT

shutdown -h +50 >/dev/null 2>&1 || exit 89

for variable in \
  V95_SCREEN_URI V95_SCREEN_SHA256 V95_V94_URI V95_V94_SHA256 \
  V95_V93_URI V95_V93_SHA256 V95_V92_URI V95_V92_SHA256 \
  V95_V90_URI V95_V90_SHA256 V95_V88_URI V95_V88_SHA256 \
  V95_V87_URI V95_V87_SHA256 V95_V86_URI V95_V86_SHA256 \
  V95_SHARED_URI V95_SHARED_SHA256 V95_BUCKET V95_INDEX_PREFIX \
  V95_OUTPUT_URI V95_SOURCE_COMMIT; do
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
[ "$instance_type" = "c7i.8xlarge" ] || exit 88
availability_zone=$(metadata placement/availability-zone) || exit 87
region=$(metadata placement/region) || exit 87
export AWS_REGION="$region" AWS_DEFAULT_REGION="$region"
printf '{"availability_zone":"%s","instance_id":"%s","instance_type":"%s","lifecycle":"%s","region":"%s"}\n' \
  "$availability_zone" "$instance_id" "$instance_type" "$lifecycle" "$region" >instance.json

phase=dependencies
printf 'phase=dependencies\n' >dependency.log
dnf install -y -q time python3-pip >>dependency.log 2>&1 || exit 91
python3 -m venv .venv >>dependency.log 2>&1 || exit 91
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  'numpy==1.26.4' 'pyarrow==17.0.0' 'boto3==1.34.46' >>dependency.log 2>&1 || exit 91

phase=inputs
aws s3 cp "$V95_SCREEN_URI" scripts/v95_real_s3_plan_replay.py --only-show-errors || exit 92
aws s3 cp "$V95_V94_URI" scripts/v94_protected_rescue_confirmation.py --only-show-errors || exit 92
aws s3 cp "$V95_V93_URI" scripts/v93_protected_rescue_screen.py --only-show-errors || exit 92
aws s3 cp "$V95_V92_URI" scripts/v92_wave2_nominated_rows_screen.py --only-show-errors || exit 92
aws s3 cp "$V95_V90_URI" scripts/v90_residual_row_sketch_screen.py --only-show-errors || exit 92
aws s3 cp "$V95_V88_URI" scripts/v88_planner_objective_screen.py --only-show-errors || exit 92
aws s3 cp "$V95_V87_URI" scripts/v87_summary_capacity_screen.py --only-show-errors || exit 92
aws s3 cp "$V95_V86_URI" scripts/v86_coarse_to_fine_screen.py --only-show-errors || exit 92
aws s3 cp "$V95_SHARED_URI" scripts/v85_shared_overlay_screen.py --only-show-errors || exit 92
printf '%s  scripts/v95_real_s3_plan_replay.py\n%s  scripts/v94_protected_rescue_confirmation.py\n%s  scripts/v93_protected_rescue_screen.py\n%s  scripts/v92_wave2_nominated_rows_screen.py\n%s  scripts/v90_residual_row_sketch_screen.py\n%s  scripts/v88_planner_objective_screen.py\n%s  scripts/v87_summary_capacity_screen.py\n%s  scripts/v86_coarse_to_fine_screen.py\n%s  scripts/v85_shared_overlay_screen.py\n' \
  "$V95_SCREEN_SHA256" "$V95_V94_SHA256" "$V95_V93_SHA256" \
  "$V95_V92_SHA256" "$V95_V90_SHA256" "$V95_V88_SHA256" \
  "$V95_V87_SHA256" "$V95_V86_SHA256" "$V95_SHARED_SHA256" >hashes.log
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

phase=prepare
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16
printf 'phase=prepare\n' >build.log
/usr/bin/time -v -o build-time.log timeout --signal=TERM --kill-after=30s 1800s \
  .venv/bin/python -m scripts.v95_real_s3_plan_replay prepare \
  --source source.parquet --queries queries.parquet --ground-truth truth.parquet \
  --layout-order layout.npy --bucket "$V95_BUCKET" \
  --index-prefix "$V95_INDEX_PREFIX" --source-commit "$V95_SOURCE_COMMIT" \
  --scratch /mnt/v95-page-objects --plan plan.json \
  --delta-sq8 delta-sq8.parquet --build-receipt build-receipt.json \
  >>build.log 2>&1 || exit 96
test -s plan.json && test -s delta-sq8.parquet && test -s build-receipt.json || exit 97

unlink source.parquet
unlink layout.npy
test ! -e source.parquet && test ! -e layout.npy || exit 97

phase=replay
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
/usr/bin/time -v -o replay-time.log timeout --signal=TERM --kill-after=30s 600s \
  .venv/bin/python -m scripts.v95_real_s3_plan_replay replay \
  --plan plan.json --queries queries.parquet --ground-truth truth.parquet \
  --delta-sq8 delta-sq8.parquet --read-threads 16 --output result.json \
  >replay.log 2>&1 || exit 98
test -s result.json || exit 99
unlink delta-sq8.parquet
phase=complete
exit 0
