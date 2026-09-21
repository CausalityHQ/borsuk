#!/bin/bash
set -euo pipefail

root=/mnt/bounded-reader-1m
cd "$root"
phase=bootstrap
science_pid=
monitor_pid=
exit_code=0
started_epoch=$(date +%s)

publish_terminal() {
  exit_code=$?
  trap - EXIT
  set +e
  [ -n "$monitor_pid" ] && kill "$monitor_pid" 2>/dev/null
  [ -n "$monitor_pid" ] && wait "$monitor_pid" 2>/dev/null
  instance_id=$(curl -fsS http://169.254.169.254/latest/meta-data/instance-id || true)
  for name in result.json samples.parquet reduction.json resources.txt worker.log pressure-stop.txt swap-stop.txt interrupt-stop.txt; do
    [ -f "$name" ] && aws s3 cp "$name" "$BOUNDED_OUTPUT_PREFIX/$name" --only-show-errors
  done
  ended_epoch=$(date +%s)
  python3 - "$exit_code" "$phase" "$instance_id" "$started_epoch" "$ended_epoch" >terminal.json <<'PY'
import json, os, sys
wall_seconds = int(sys.argv[5]) - int(sys.argv[4])
rate = int(os.environ["BOUNDED_SPOT_PRICE_MICROS"])
print(json.dumps({
    "schema": "borsuk-bounded-reader-terminal-v1",
    "source_commit": os.environ["BOUNDED_SOURCE_COMMIT"],
    "instance_id": sys.argv[3],
    "exit_code": int(sys.argv[1]),
    "phase": sys.argv[2],
    "status": "complete" if int(sys.argv[1]) == 0 else "failed",
    "wall_seconds": wall_seconds,
    "spot_price_usd_per_hour_micros": rate,
    "cost_usd_micros": (wall_seconds * rate + 3599) // 3600,
    "result_sha256": open("result.sha256").read().split()[0] if os.path.exists("result.sha256") else None,
    "samples_sha256": open("samples.sha256").read().split()[0] if os.path.exists("samples.sha256") else None,
    "reduction_sha256": open("reduction.sha256").read().split()[0] if os.path.exists("reduction.sha256") else None,
}, sort_keys=True, separators=(",", ":")))
PY
  aws s3 cp terminal.json "$BOUNDED_OUTPUT_PREFIX/terminal.json" --only-show-errors
  shutdown -h now
  exit "$exit_code"
}
trap publish_terminal EXIT

phase=dependencies
dnf install -y -q gcc python3-pip time >/dev/null
export HOME=${HOME:-/root}
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi
export PATH="$HOME/.cargo/bin:$PATH"
python3 -m venv .venv
.venv/bin/pip install --disable-pip-version-check --quiet numpy==2.4.2 pyarrow==24.0.0

phase=input-download
aws s3 cp "$BOUNDED_SOURCE_URI" source.parquet --only-show-errors
aws s3 cp "$BOUNDED_QUERIES_URI" queries.parquet --only-show-errors
aws s3 cp "$BOUNDED_TRUTH_URI" truth.parquet --only-show-errors
aws s3 cp "$BOUNDED_LAYOUT_URI" layout.npy --only-show-errors
for role in SOURCE QUERIES TRUTH LAYOUT; do
  eval expected_bytes=\$BOUNDED_${role}_BYTES
  eval expected_sha=\$BOUNDED_${role}_SHA256
  case "$role" in
    SOURCE) file=source.parquet ;;
    QUERIES) file=queries.parquet ;;
    TRUTH) file=truth.parquet ;;
    LAYOUT) file=layout.npy ;;
  esac
  [ "$(stat -c%s "$file")" = "$expected_bytes" ] || exit 91
  printf '%s  %s\n' "$expected_sha" "$file" | sha256sum -c -
done

phase=sq8-authentication
aws s3 cp "$BOUNDED_SQ8_URI" sq8.bin --only-show-errors
[ "$(stat -c%s sq8.bin)" = "$BOUNDED_SQ8_BYTES" ]
printf '%s  sq8.bin\n' "$BOUNDED_SQ8_SHA256" >sq8.sha256
sha256sum -c sq8.sha256
unlink sq8.bin

phase=build
(cd repo && cargo build --release --locked -p borsuk-v71)

phase=manifest
.venv/bin/python repo/scripts/v77_export_manifest.py \
  --source source.parquet --development-query queries.parquet \
  --ground-truth truth.parquet --layout-order layout.npy --output manifest.bin
manifest_sha256=$(sha256sum manifest.bin | cut -d' ' -f1)

monitor() {
  while kill -0 "$science_pid" 2>/dev/null; do
    if curl -fsS http://169.254.169.254/latest/meta-data/spot/instance-action >/dev/null 2>&1; then
      : >interrupt-stop.txt
      kill -TERM -- "-$science_pid" 2>/dev/null
      return
    fi
    # Read the memory-pressure `full avg10` signal, not free-memory heuristics.
    full=$(awk '/^full / {for(i=1;i<=NF;i++) if($i ~ /^avg10=/){split($i,a,"="); print a[2]}}' /proc/pressure/memory)
    if awk -v value="$full" 'BEGIN{exit !(value > 0.50)}'; then
      printf '%s\n' "$full" >pressure-stop.txt
      kill -TERM -- "-$science_pid" 2>/dev/null
      return
    fi
    swap=$(awk '/^SwapTotal:/ {total=$2} /^SwapFree:/ {free=$2} END {print total-free}' /proc/meminfo)
    if [ "$swap" -gt 1048576 ]; then
      printf '%s\n' "$swap" >swap-stop.txt
      kill -TERM -- "-$science_pid" 2>/dev/null
      return
    fi
    sleep 5
  done
}

phase=science
export AWS_DEFAULT_REGION=eu-central-1 BORSUK_V71_REGION=eu-central-1
export BORSUK_V71_URI="$BOUNDED_SQ8_URI" BORSUK_V71_MANIFEST="$root/manifest.bin"
export BORSUK_V71_OUTPUT="$root/result.json" BORSUK_V71_SAMPLES="$root/samples.parquet"
export BORSUK_SOURCE_COMMIT="$BOUNDED_SOURCE_COMMIT" BORSUK_MANIFEST_SHA256="$manifest_sha256"
export BORSUK_SQ8_SHA256="$BOUNDED_SQ8_SHA256"
ulimit -v "$BOUNDED_VIRTUAL_MEMORY_KIB"
setsid timeout --signal=TERM --kill-after=30 "$BOUNDED_WALL_SECONDS" \
  /usr/bin/time -v repo/target/release/v71_native_reader >worker.log 2>resources.txt &
science_pid=$!
monitor &
monitor_pid=$!
wait "$science_pid" || exit_code=$?
science_pid=
wait "$monitor_pid" || true
monitor_pid=
[ "$exit_code" -eq 0 ] || exit "$exit_code"

phase=reduction
PYTHONPATH=repo .venv/bin/python - <<'PY'
from pathlib import Path
from scripts.reduce_bounded_reader_result import canonical_reduction_bytes, reduce_bounded_reader_result
receipt = reduce_bounded_reader_result(Path("result.json"), Path("samples.parquet"), Path("truth.parquet"))
Path("reduction.json").write_bytes(canonical_reduction_bytes(receipt))
PY
sha256sum result.json >result.sha256
sha256sum samples.parquet >samples.sha256
sha256sum reduction.json >reduction.sha256
phase=complete
exit 0
