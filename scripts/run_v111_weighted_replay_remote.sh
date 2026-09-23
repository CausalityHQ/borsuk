#!/bin/bash
set -euo pipefail

root=/mnt/v111-weighted-replay
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
  artifact_upload_failed=0
  for name in hashes.log manifest-export.log prefix.log prefix-evidence.jsonl prefix-reduction.json \
      prefix-resources.txt prefix-validation.log prefix-validation-resources.txt \
      full.log full-evidence.jsonl full-reduction.json full-resources.txt \
      full-validation.log full-validation-resources.txt \
      worker.log decision.json interrupt-stop.txt pressure-stop.txt swap-stop.txt; do
    if [ -f "$name" ] && ! aws s3 cp "$name" "$V111_OUTPUT_PREFIX/artifacts/$name" --only-show-errors; then
      artifact_upload_failed=1
    fi
  done
  if [ "$artifact_upload_failed" -ne 0 ]; then
    code=96
    phase=evidence-upload
  fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib,json,os,sys
from pathlib import Path
code=int(sys.argv[1]); phase=sys.argv[2]
names=("hashes.log", "manifest-export.log", "prefix.log", "prefix-evidence.jsonl",
       "prefix-reduction.json", "prefix-resources.txt", "prefix-validation.log",
       "prefix-validation-resources.txt", "full.log", "full-evidence.jsonl",
       "full-reduction.json", "full-resources.txt", "full-validation.log",
       "full-validation-resources.txt", "worker.log", "decision.json",
       "interrupt-stop.txt", "pressure-stop.txt", "swap-stop.txt")
artifacts={}
for name in names:
    path=Path(name)
    if path.exists():
        data=path.read_bytes()
        artifacts[name]={"sha256":hashlib.sha256(data).hexdigest(),
                         "bytes":len(data),
                         "uri":os.environ["V111_OUTPUT_PREFIX"]+"/artifacts/"+name}
print(json.dumps({
    "schema":"borsuk-v111-weighted-terminal-v1",
    "attempt":int(os.environ["V111_ATTEMPT"]),
    "source_commit":os.environ["V111_SOURCE_COMMIT"],
    "instance_id":sys.argv[3], "exit_code":code,
    "phase":phase, "status":"complete" if code==0 and phase=="complete" else "failed",
    "elapsed_seconds":int(sys.argv[5])-int(sys.argv[4]),
    "artifacts":artifacts,
},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V111_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
  setsid timeout --signal=TERM --kill-after=30 "$V111_WALL_SECONDS" \
    /usr/bin/time -v "$@" >"$label.log" 2>"$label-resources.txt" &
  science_pid=$!
  printf '%s\n' "$science_pid" >science-pgid
  wait "$science_pid"
  unlink science-pgid
  if [ -f interrupt-stop.txt ] || [ -f pressure-stop.txt ] || [ -f swap-stop.txt ]; then
    exit 97
  fi
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
for role in SOURCE QUERIES TRUTH LAYOUT SQ8; do
  case "$role" in
    SOURCE) file=source.parquet ;;
    QUERIES) file=queries.parquet ;;
    TRUTH) file=truth.parquet ;;
    LAYOUT) file=layout.npy ;;
    SQ8) file=sq8.bin ;;
  esac
  eval uri=\$V111_${role}_URI
  eval expected_bytes=\$V111_${role}_BYTES
  eval expected_sha=\$V111_${role}_SHA256
  aws s3 cp "$uri" "$file" --only-show-errors
  [ "$(stat -c%s "$file")" = "$expected_bytes" ] || exit 91
  printf '%s  %s\n' "$expected_sha" "$file" | sha256sum -c - >>hashes.log
done

phase=manifest
export OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=32
monitor &
monitor_pid=$!
run_science manifest-export .venv/bin/python repo/scripts/v77_export_manifest.py \
  --source source.parquet --development-query queries.parquet \
  --ground-truth truth.parquet --layout-order layout.npy --output manifest.bin

phase=prefix
run_science prefix env PYTHONPATH=repo .venv/bin/python -m scripts.v111_weighted_reader_replay \
  --manifest manifest.bin --sq8 sq8.bin --output prefix-evidence.jsonl \
  --queries 200 --regions 1024
run_science prefix-validation env PYTHONPATH=repo .venv/bin/python -m scripts.validate_v111_weighted_reader \
  --evidence prefix-evidence.jsonl --manifest manifest.bin --sq8 sq8.bin \
  --output prefix-reduction.json

phase=prefix-decision
.venv/bin/python - <<'PY'
import json
from pathlib import Path
p=json.loads(Path('prefix-reduction.json').read_text())
matched=p['historical_prefix_200']['matches_v77_v78_control']
good=(p['weighted']['recall100_ppm']>=990000 and p['weighted']['p05_hits']>=90
      and p['weighted']['cap_violations']==0)
matched=(matched and p['historical_prefix_200']['hits']==19832
         and p['capped']['hits']==19739)
decision='advance-full' if matched and good else ('harness-mismatch' if not matched else 'stop-weighted-planner')
Path('decision.json').write_text(json.dumps({'schema':'borsuk-v111-decision-v1',
    'prefix_decision':decision,'prefix_evidence_sha256':p['evidence_sha256']},
    sort_keys=True,separators=(',',':'))+'\n')
PY
decision=$(.venv/bin/python -c 'import json;print(json.load(open("decision.json"))["prefix_decision"])')
if [ "$decision" != advance-full ]; then
  phase=complete
  exit 0
fi

phase=full
run_science full env PYTHONPATH=repo .venv/bin/python -m scripts.v111_weighted_reader_replay \
  --manifest manifest.bin --sq8 sq8.bin --output full-evidence.jsonl \
  --queries 1000 --regions 1024
run_science full-validation env PYTHONPATH=repo .venv/bin/python -m scripts.validate_v111_weighted_reader \
  --evidence full-evidence.jsonl --manifest manifest.bin --sq8 sq8.bin \
  --output full-reduction.json

phase=complete
exit 0
