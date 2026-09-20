#!/bin/bash
set -u

if [ "${1:-}" = "--describe" ]; then
  printf '%s\n' '{"actual_s3":true,"instance_type":"c7i.8xlarge","max_wall_seconds":900,"query_order":"reverse","query_parallelism":1,"range_read_threads":16,"rayon_work_stealing":false,"scientific_inputs":["plan.json"],"spot_only":true,"stage":"transport-order-diagnostic"}'
  exit 0
fi

root=/mnt/v96-s3-transport-order
phase=bootstrap

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  cd "$root" 2>/dev/null || true
  if [ "$code" -eq 0 ]; then
    for name in dependency.log hashes.log instance.json replay.log replay-time.log result.json; do
      if [ ! -s "$name" ] || ! aws s3 cp "$name" "$V96_OUTPUT_URI/$name" --only-show-errors; then
        code=98
        phase=publication_failed
        break
      fi
    done
  else
    for name in dependency.log hashes.log instance.json replay.log replay-time.log result.json; do
      if [ -n "${V96_OUTPUT_URI:-}" ] && [ -f "$name" ]; then
        aws s3 cp "$name" "$V96_OUTPUT_URI/$name" --only-show-errors || true
      fi
    done
  fi
  printf '{"exit_code":%d,"phase":"%s","source_commit":"%s"}\n' \
    "$code" "$phase" "${V96_SOURCE_COMMIT:-unknown}" >/tmp/v96-terminal.json
  if [ -n "${V96_OUTPUT_URI:-}" ] && ! aws s3 cp /tmp/v96-terminal.json \
    "$V96_OUTPUT_URI/terminal.json" --only-show-errors; then
    code=99
  fi
  unlink /tmp/v96-terminal.json
  sync
  shutdown -h now || true
  exit "$code"
}
trap publish_terminal EXIT

shutdown -h +15 >/dev/null 2>&1 || exit 89

for variable in V96_SCREEN_URI V96_SCREEN_SHA256 V96_V95_URI V96_V95_SHA256 \
  V96_PLAN_URI V96_PLAN_SHA256 V96_OUTPUT_URI V96_SOURCE_COMMIT; do
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
  'numpy==1.26.4' 'boto3==1.34.46' >>dependency.log 2>&1 || exit 91

phase=inputs
aws s3 cp "$V96_SCREEN_URI" scripts/v96_s3_transport_order_probe.py --only-show-errors || exit 92
aws s3 cp "$V96_V95_URI" scripts/v95_real_s3_plan_replay.py --only-show-errors || exit 92
aws s3 cp "$V96_PLAN_URI" plan.json --only-show-errors || exit 92
printf '%s  scripts/v96_s3_transport_order_probe.py\n%s  scripts/v95_real_s3_plan_replay.py\n%s  plan.json\n' \
  "$V96_SCREEN_SHA256" "$V96_V95_SHA256" "$V96_PLAN_SHA256" >hashes.log
sha256sum -c hashes.log >/dev/null || exit 93

phase=replay
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
/usr/bin/time -v -o replay-time.log timeout --signal=TERM --kill-after=30s 600s \
  .venv/bin/python -m scripts.v96_s3_transport_order_probe \
  --plan plan.json --plan-sha256 "$V96_PLAN_SHA256" --read-threads 16 \
  --source-commit "$V96_SOURCE_COMMIT" --output result.json \
  >replay.log 2>&1 || exit 98
test -s result.json || exit 99
phase=complete
exit 0
