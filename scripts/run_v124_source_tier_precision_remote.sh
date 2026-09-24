#!/bin/bash
# One two-corpus development cell on Causality Spot. No incomplete result is read.
set -euo pipefail
root=/mnt/v124-precision
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V124_WALL_SECONDS))
monitor_pid=
imds_token=
imds() {
  if [ -z "$imds_token" ]; then
    imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
      http://169.254.169.254/latest/api/token) || return
  fi
  curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" \
    "http://169.254.169.254/latest/meta-data/$1"
}
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(imds instance-id || true)
  upload_failed=0
  for path in install.log python-test.log python-test-resources.txt \
      deep-download.log deep-download-resources.txt deep-input-hashes.log \
      deep-eval.log deep-eval-resources.txt deep-evidence.jsonl deep-summary.json \
      relaion-download.log relaion-download-resources.txt relaion-input-hashes.log \
      relaion-eval.log relaion-eval-resources.txt relaion-evidence.jsonl \
      relaion-summary.json worker.log interrupt-stop.txt; do
    if [ -f "$path" ] && ! timeout 900 aws s3 cp "$path" \
        "$V124_OUTPUT_PREFIX/artifacts/$path" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ "$upload_failed" -ne 0 ]; then code=96; phase=evidence-upload; fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=('install.log','python-test.log','python-test-resources.txt',
       'deep-download.log','deep-download-resources.txt','deep-input-hashes.log',
       'deep-eval.log','deep-eval-resources.txt','deep-evidence.jsonl',
       'deep-summary.json','relaion-download.log','relaion-download-resources.txt',
       'relaion-input-hashes.log','relaion-eval.log','relaion-eval-resources.txt',
       'relaion-evidence.jsonl','relaion-summary.json','worker.log',
       'interrupt-stop.txt')
artifacts={}
for name in names:
    path=Path(name)
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024*1024),b''):
                digest.update(block)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({'schema':'borsuk-v124-source-tier-precision-spot-v1',
    'source_commit':os.environ['V124_SOURCE_COMMIT'],
    'v121_terminal_sha256':os.environ['V124_V121_TERMINAL_SHA256'],
    'v116_terminal_sha256':os.environ['V124_V116_TERMINAL_SHA256'],
    'instance_id':sys.argv[3], 'exit_code':code, 'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'elapsed_seconds':int(sys.argv[5])-int(sys.argv[4]),
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V124_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
monitor() {
  while true; do
    if imds spot/instance-action >/dev/null 2>&1; then
      : >interrupt-stop.txt
      if [ -f science-pgid ]; then
        kill -TERM -- "-$(cat science-pgid)" 2>/dev/null || true
      fi
      kill -TERM "$main_pid" 2>/dev/null || true
      return
    fi
    sleep 5
  done
}
run_science() {
  label=$1; shift
  remaining=$((deadline_epoch - $(date +%s)))
  [ "$remaining" -gt 0 ] || exit 124
  setsid timeout --signal=TERM --kill-after=30 "$remaining" \
    /usr/bin/time -v "$@" >"$label.log" 2>"$label-resources.txt" &
  science_pid=$!
  printf '%s\n' "$science_pid" >science-pgid
  status=0
  wait "$science_pid" || status=$?
  rm -f science-pgid
  [ ! -f interrupt-stop.txt ] || exit 97
  [ "$status" -eq 0 ] || return "$status"
}
main_pid=$BASHPID
monitor & monitor_pid=$!
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8
phase=python-test
run_science python-test .venv/bin/python -m unittest -q \
  scripts.test_v124_source_tier_precision
phase=deep-download
# shellcheck disable=SC2016
run_science deep-download bash -e -c '
  aws s3 cp "$V124_DEEP_SOURCE_URI" deep-source.parquet --only-show-errors
  aws s3 cp "$V124_DEEP_LAYOUT_URI" deep-layout.npy --only-show-errors
  aws s3 cp "$V124_DEEP_REQUESTS_URI" deep-requests.jsonl --only-show-errors
  aws s3 cp "$V124_DEEP_TRUTH_URI" deep-truth.parquet --only-show-errors
'
{
  printf '%s  deep-source.parquet\n' "$V124_DEEP_SOURCE_SHA256"
  printf '%s  deep-layout.npy\n' "$V124_DEEP_LAYOUT_SHA256"
  printf '%s  deep-requests.jsonl\n' "$V124_DEEP_REQUESTS_SHA256"
  printf '%s  deep-truth.parquet\n' "$V124_DEEP_TRUTH_SHA256"
} >deep-input-hashes.log
sha256sum -c deep-input-hashes.log
phase=deep-eval
run_science deep-eval .venv/bin/python -m scripts.v124_source_tier_precision \
  --cohort deep-image-96-angular --rows 9990000 --dimensions 96 \
  --source deep-source.parquet --layout deep-layout.npy \
  --requests deep-requests.jsonl --truth deep-truth.parquet \
  --evidence deep-evidence.jsonl --summary deep-summary.json
for path in deep-evidence.jsonl deep-summary.json deep-eval-resources.txt; do
  aws s3 cp "$path" "$V124_OUTPUT_PREFIX/artifacts/$path" --only-show-errors
done
rm -f deep-source.parquet deep-layout.npy deep-requests.jsonl deep-truth.parquet
phase=relaion-download
# shellcheck disable=SC2016
run_science relaion-download bash -e -c '
  aws s3 cp "$V124_RELAION_SOURCE_URI" relaion-source.parquet --only-show-errors
  aws s3 cp "$V124_RELAION_LAYOUT_URI" relaion-layout.npy --only-show-errors
  aws s3 cp "$V124_RELAION_REQUESTS_URI" relaion-requests.jsonl --only-show-errors
  aws s3 cp "$V124_RELAION_TRUTH_URI" relaion-truth.parquet --only-show-errors
'
{
  printf '%s  relaion-source.parquet\n' "$V124_RELAION_SOURCE_SHA256"
  printf '%s  relaion-layout.npy\n' "$V124_RELAION_LAYOUT_SHA256"
  printf '%s  relaion-requests.jsonl\n' "$V124_RELAION_REQUESTS_SHA256"
  printf '%s  relaion-truth.parquet\n' "$V124_RELAION_TRUTH_SHA256"
} >relaion-input-hashes.log
sha256sum -c relaion-input-hashes.log
phase=relaion-eval
run_science relaion-eval .venv/bin/python -m scripts.v124_source_tier_precision \
  --cohort ReLAION-1M --rows 1000000 --dimensions 768 \
  --source relaion-source.parquet --layout relaion-layout.npy \
  --requests relaion-requests.jsonl --truth relaion-truth.parquet \
  --evidence relaion-evidence.jsonl --summary relaion-summary.json
phase=complete
exit 0
