#!/bin/bash
set -euo pipefail

root=/mnt/v110-physical-oracle
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
monitor_pid=
imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
imds() {
  curl -fsS -H "X-aws-ec2-metadata-token: $imds_token" \
    "http://169.254.169.254/latest/meta-data/$1"
}

publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(imds instance-id || true)
  failed_upload=0
  for name in worker.log hashes.log oracle.log oracle-resources.txt \
      evidence.jsonl validation.log validation-resources.txt reduction.json \
      interrupt-stop.txt pressure-stop.txt swap-stop.txt; do
    if [ -f "$name" ] && ! aws s3 cp "$name" "$V110_OUTPUT_PREFIX/artifacts/$name" --only-show-errors; then
      failed_upload=1
    fi
  done
  if [ "$failed_upload" -ne 0 ]; then
    code=96
    phase=evidence-upload
  fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
names=("worker.log","hashes.log","oracle.log","oracle-resources.txt",
       "evidence.jsonl","validation.log","validation-resources.txt",
       "reduction.json","interrupt-stop.txt","pressure-stop.txt","swap-stop.txt")
artifacts={}
for name in names:
    path=Path(name)
    if path.exists():
        body=path.read_bytes()
        artifacts[name]={"sha256":hashlib.sha256(body).hexdigest(),
                         "bytes":len(body),
                         "uri":os.environ["V110_OUTPUT_PREFIX"]+"/artifacts/"+name}
code=int(sys.argv[1]);phase=sys.argv[2]
print(json.dumps({"schema":"borsuk-v110-physical-oracle-terminal-v1",
    "source_commit":os.environ["V110_SOURCE_COMMIT"],
    "attempt":int(os.environ["V110_ATTEMPT"]),"instance_id":sys.argv[3],
    "exit_code":code,"phase":phase,
    "status":"complete" if code==0 and phase=="complete" else "failed",
    "elapsed_seconds":int(sys.argv[5])-int(sys.argv[4]),"artifacts":artifacts},
    sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V110_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now
  exit "$code"
}
trap publish_terminal EXIT

monitor() {
  while true; do
    if imds spot/instance-action >/dev/null 2>&1; then
      : >interrupt-stop.txt
      [ -f science-pgid ] && kill -TERM -- "-$(cat science-pgid)" 2>/dev/null
      return
    fi
    full=$(awk '/^full / {for(i=1;i<=NF;i++) if($i ~ /^avg10=/){split($i,a,"="); print a[2]}}' /proc/pressure/memory)
    if awk -v value="$full" 'BEGIN{exit !(value > 0.50)}'; then
      printf '%s\n' "$full" >pressure-stop.txt
      [ -f science-pgid ] && kill -TERM -- "-$(cat science-pgid)" 2>/dev/null
      return
    fi
    swap=$(awk '/^SwapTotal:/ {total=$2} /^SwapFree:/ {free=$2} END {print total-free}' /proc/meminfo)
    if [ "$swap" -gt 1048576 ]; then
      printf '%s\n' "$swap" >swap-stop.txt
      [ -f science-pgid ] && kill -TERM -- "-$(cat science-pgid)" 2>/dev/null
      return
    fi
    sleep 5
  done
}

run_science() {
  label=$1
  shift
  if [ -f interrupt-stop.txt ] || [ -f pressure-stop.txt ] || [ -f swap-stop.txt ]; then
    exit 97
  fi
  setsid timeout --signal=TERM --kill-after=30 5400 \
    /usr/bin/time -v "$@" >"$label.log" 2>"$label-resources.txt" &
  science_pid=$!
  printf '%s\n' "$science_pid" >science-pgid
  wait "$science_pid"
  unlink science-pgid
}

phase=system-packages
dnf install -y -q time >/dev/null
export HOME=${HOME:-/root}
phase=uv-install
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"
phase=venv
uv venv --python 3.12 .venv
phase=pip-install
uv pip install --python .venv/bin/python 'numpy==1.26.4' 'pyarrow==17.0.0'

phase=input-download
for role in SOURCE TRUTH LAYOUT; do
  case "$role" in
    SOURCE) file=source.parquet ;;
    TRUTH) file=truth.parquet ;;
    LAYOUT) file=layout.npy ;;
  esac
  eval uri=\$V110_${role}_URI
  eval expected_bytes=\$V110_${role}_BYTES
  eval expected_sha=\$V110_${role}_SHA256
  aws s3 cp "$uri" "$file" --only-show-errors
  [ "$(stat -c%s "$file")" = "$expected_bytes" ] || exit 91
  printf '%s  %s\n' "$expected_sha" "$file" | sha256sum -c - >>hashes.log
done

monitor &
monitor_pid=$!
phase=oracle
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8
run_science oracle .venv/bin/python repo/scripts/v110_physical_interval_oracle.py \
  --source source.parquet --truth truth.parquet --layout layout.npy \
  --output evidence.jsonl

phase=validation
run_science validation env PYTHONPATH=repo .venv/bin/python \
  -m scripts.validate_v110_physical_interval_oracle \
  --evidence evidence.jsonl --source source.parquet --truth truth.parquet \
  --layout layout.npy --output reduction.json

phase=complete
exit 0
