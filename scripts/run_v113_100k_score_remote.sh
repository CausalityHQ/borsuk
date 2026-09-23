#!/bin/bash
# One preregistered ReLAION-100k score-fidelity cell. Queries arrive after build.
set -euo pipefail
root=/mnt/v113-100k-score
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V113_WALL_SECONDS))
monitor_pid=
imds_token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
imds() {
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
  for name in hashes.log install.log build.log build-resources.txt \
      score.log score-resources.txt validate.log validate-resources.txt \
      evidence.jsonl reduction.json worker.log interrupt-stop.txt; do
    if [ -f "$name" ] && ! aws s3 cp "$name" \
        "$V113_OUTPUT_PREFIX/artifacts/$name" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ -d artifact ]; then
    aws s3 sync artifact "$V113_OUTPUT_PREFIX/artifacts/artifact/" \
      --only-show-errors || upload_failed=1
  fi
  if [ "$upload_failed" -ne 0 ]; then
    code=96
    phase=evidence-upload
  fi
  ended_epoch=$(date +%s)
  python3 - "$code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import hashlib, json, os, sys
from pathlib import Path
names=("hashes.log", "install.log", "build.log", "build-resources.txt",
       "score.log", "score-resources.txt", "validate.log",
       "validate-resources.txt", "evidence.jsonl", "reduction.json",
       "worker.log", "interrupt-stop.txt")
files={}
for name in names:
    path=Path(name)
    if path.is_file():
        files[name]={"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                     "bytes":path.stat().st_size}
for path in sorted(Path("artifact").glob("*")) if Path("artifact").exists() else []:
    if path.is_file():
        files["artifact/"+path.name]={"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                                      "bytes":path.stat().st_size}
code=int(sys.argv[1]); phase=sys.argv[2]
print(json.dumps({
    "schema":"borsuk-v113-100k-spot-v1", "attempt":1,
    "source_commit":os.environ["V113_SOURCE_COMMIT"],
    "instance_id":sys.argv[3], "exit_code":code, "phase":phase,
    "status":"complete" if code==0 and phase=="complete" else "failed",
    "elapsed_seconds":int(sys.argv[5])-int(sys.argv[4]),
    "artifacts":files,
},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V113_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM

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
  label=$1
  shift
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
monitor &
monitor_pid=$!
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16

phase=source
aws s3 cp "$V113_SOURCE_URI" source.parquet --only-show-errors
[ "$(stat -c%s source.parquet)" = "$V113_SOURCE_BYTES" ] || exit 91
printf '%s  source.parquet\n' "$V113_SOURCE_SHA256" | sha256sum -c - >>hashes.log

phase=build
run_science build .venv/bin/python -m scripts.v113_100k_score_screen build \
  --source source.parquet --source-sha256 "$V113_SOURCE_SHA256" \
  --artifact artifact
[ -f artifact/seal.json ] || exit 92

# Query bytes and paths are unavailable to the builder above.
phase=query-download
aws s3 cp "$V113_QUERY_URI" queries.parquet --only-show-errors
[ "$(stat -c%s queries.parquet)" = "$V113_QUERY_BYTES" ] || exit 93
printf '%s  queries.parquet\n' "$V113_QUERY_SHA256" | sha256sum -c - >>hashes.log

phase=score
run_science score .venv/bin/python -m scripts.v113_100k_score_screen score \
  --artifact artifact --source-sha256 "$V113_SOURCE_SHA256" \
  --queries queries.parquet --query-sha256 "$V113_QUERY_SHA256" \
  --output evidence.jsonl --summary reduction.json

phase=validate
run_science validate .venv/bin/python -m scripts.validate_v113_100k_score_screen \
  --artifact artifact --source source.parquet \
  --source-sha256 "$V113_SOURCE_SHA256" \
  --queries queries.parquet --query-sha256 "$V113_QUERY_SHA256" \
  --evidence evidence.jsonl --summary reduction.json

phase=complete
exit 0
