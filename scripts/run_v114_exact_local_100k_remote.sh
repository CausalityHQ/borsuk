#!/bin/bash
# One source-frozen, query-blind V114 exact-local correctness attempt.
set -euo pipefail
root=/mnt/v114-exact-local-100k
cd "$root"
exec >worker.log 2>&1
phase=bootstrap
started_epoch=$(date +%s)
deadline_epoch=$((started_epoch + V114_WALL_SECONDS))
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
  for name in hashes.log install.log compile.log compile-resources.txt \
      build.log build-resources.txt prepare.log prepare-resources.txt \
      score.log score-resources.txt compare.log compare-resources.txt \
      validate.log validate-resources.txt requests.jsonl reference.jsonl \
      rust.jsonl reduction.json worker.log interrupt-stop.txt; do
    if [ -f "$name" ] && ! aws s3 cp "$name" \
        "$V114_OUTPUT_PREFIX/artifacts/$name" --only-show-errors; then
      upload_failed=1
    fi
  done
  if [ -d mirror ]; then
    aws s3 sync mirror/ "$V114_OUTPUT_PREFIX/artifacts/mirror/" \
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
names=("hashes.log", "install.log", "compile.log", "compile-resources.txt",
       "build.log", "build-resources.txt", "prepare.log", "prepare-resources.txt",
       "score.log", "score-resources.txt", "compare.log", "compare-resources.txt",
       "validate.log", "validate-resources.txt", "requests.jsonl",
       "reference.jsonl", "rust.jsonl", "reduction.json", "worker.log",
       "interrupt-stop.txt")
files={}
for name in names:
    path=Path(name)
    if path.is_file():
        files[name]={"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                     "bytes":path.stat().st_size}
for path in sorted(Path("mirror").glob("*")) if Path("mirror").exists() else []:
    if path.is_file():
        files["mirror/"+path.name]={"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                                     "bytes":path.stat().st_size}
code=int(sys.argv[1]); phase=sys.argv[2]
print(json.dumps({
    "schema":"borsuk-v114-exact-local-100k-spot-v1", "attempt":1,
    "source_commit":os.environ["V114_SOURCE_COMMIT"],
    "upstream_artifact":os.environ["V114_V113_ARTIFACT_URI"],
    "instance_id":sys.argv[3], "exit_code":code, "phase":phase,
    "status":"complete" if code==0 and phase=="complete" else "failed",
    "elapsed_seconds":int(sys.argv[5])-int(sys.argv[4]),
    "artifacts":files,
},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "$V114_OUTPUT_PREFIX/terminal.json" --only-show-errors
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
dnf install -y -q python3.12 python3.12-pip tar gzip time gcc >install.log 2>&1
python3.12 -m venv .venv >>install.log 2>&1
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 boto3 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup"
export CARGO_HOME="$root/.cargo"
export CARGO_TARGET_DIR="$root/cargo-target"
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo"
export OMP_NUM_THREADS=8 OPENBLAS_NUM_THREADS=8 MKL_NUM_THREADS=8

phase=compile
run_science compile "$CARGO_HOME/bin/cargo" build --release --locked -j 8 \
  --manifest-path repo/scripts/v114_score_rust/Cargo.toml

phase=source-artifact
aws s3 sync "$V114_V113_ARTIFACT_URI/" artifact/ --only-show-errors
printf '%s  artifact/seal.json\n' "$V114_V113_SEAL_SHA256" | sha256sum -c - >>hashes.log

# Development query bytes and paths are unavailable to this build phase.
phase=build
run_science build .venv/bin/python -m scripts.v114_exact_local_100k build \
  --artifact artifact --mirror mirror
[ -f mirror/source.json ] || exit 92

phase=query-download
aws s3 cp "$V114_QUERY_URI" queries.parquet --only-show-errors
[ "$(stat -c%s queries.parquet)" = "$V114_QUERY_BYTES" ] || exit 93
printf '%s  queries.parquet\n' "$V114_QUERY_SHA256" | sha256sum -c - >>hashes.log

phase=prepare
run_science prepare .venv/bin/python -m scripts.v114_exact_local_100k prepare \
  --artifact artifact --mirror mirror --queries queries.parquet \
  --requests requests.jsonl --reference reference.jsonl

phase=score
run_science score "$CARGO_TARGET_DIR/release/borsuk-v114-score-gate" \
  mirror/manifest.json mirror/sq8.bin mirror/blocks.sha256 requests.jsonl
cp score.log rust.jsonl

phase=compare
run_science compare .venv/bin/python -m scripts.v114_exact_local_100k compare \
  --reference reference.jsonl --actual rust.jsonl --summary reduction.json

phase=validate
run_science validate .venv/bin/python -m scripts.validate_v114_exact_local_100k \
  --artifact artifact --mirror mirror --queries queries.parquet \
  --requests requests.jsonl --reference reference.jsonl \
  --rust rust.jsonl --summary reduction.json

phase=complete
exit 0
