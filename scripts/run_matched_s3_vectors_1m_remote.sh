#!/usr/bin/env bash
set -uo pipefail

root=/mnt/matched-s3-vectors-1m
output_prefix=$MATCHED_OUTPUT_PREFIX
started_epoch=$(date +%s)
pressure_start=$(tr '\n' ';' </proc/pressure/memory)
swap_start_kib=$(awk '/^SwapTotal:/{t=$2} /^SwapFree:/{f=$2} END{print t-f}' /proc/meminfo)
watcher_pid=
interrupted=0
mkdir -p "$root" && cd "$root" || exit 90
exec > >(tee -a worker.log) 2>&1

instance_id() {
  local token value
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null) || return 1
  value=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null) || return 1
  printf '%s' "$value"
}

watch_health() {
  local token full_avg10 swap_now swap_delta over breaches=0
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
    http://169.254.169.254/latest/api/token 2>/dev/null) || token=
  while sleep 5; do
    if [ -n "$token" ] && curl -fsS -H "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/spot/instance-action >/dev/null 2>&1; then
      kill -TERM $$
      return 0
    fi
    full_avg10=$(awk '/^full/{for(i=1;i<=NF;i++) if($i ~ /^avg10=/){split($i,a,"="); print a[2]}}' /proc/pressure/memory)
    swap_now=$(awk '/^SwapTotal:/{t=$2} /^SwapFree:/{f=$2} END{print t-f}' /proc/meminfo)
    swap_delta=$((swap_now - swap_start_kib))
    over=$(awk -v value="$full_avg10" 'BEGIN{print (value > 0.50 ? 1 : 0)}')
    if [ "$over" -eq 1 ]; then breaches=$((breaches + 1)); else breaches=0; fi
    if [ "$breaches" -ge 3 ] || [ "$swap_delta" -gt 1048576 ]; then
      printf 'full avg10=%s swap_delta_kib=%s\n' "$full_avg10" "$swap_delta" >pressure-stop.txt
      kill -TERM $$
      return 0
    fi
  done
}

mark_interrupted() {
  interrupted=1
  exit 143
}

finish() {
  local code=$?
  trap - EXIT TERM INT
  set +e
  [ -n "$watcher_pid" ] && kill "$watcher_pid" 2>/dev/null
  cd "$root" 2>/dev/null || true
  local iid=unknown finished_epoch pressure_end swap_end_kib max_rss_kib status
  iid=$(instance_id) || true
  finished_epoch=$(date +%s)
  pressure_end=$(tr '\n' ';' </proc/pressure/memory)
  swap_end_kib=$(awk '/^SwapTotal:/{t=$2} /^SwapFree:/{f=$2} END{print t-f}' /proc/meminfo)
  max_rss_kib=$(awk -F: '/Maximum resident set size/{gsub(/ /,"",$2); if($2>m)m=$2} END{print m+0}' benchmark.time 2>/dev/null)
  max_rss_kib=${max_rss_kib:-0}
  status=failed
  [ "$interrupted" -eq 1 ] && status=interrupted
  [ "$code" -eq 0 ] && status=complete
  STARTED_EPOCH="$started_epoch" FINISHED_EPOCH="$finished_epoch" \
    MAX_RSS_KIB="$max_rss_kib" PRESSURE_START="$pressure_start" \
    PRESSURE_END="$pressure_end" SWAP_START_KIB="$swap_start_kib" \
    SWAP_END_KIB="$swap_end_kib" SPOT_PRICE_MICROS="$MATCHED_SPOT_PRICE_MICROS" \
    python3.12 - <<'PY'
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Resources:
    started_epoch: int
    finished_epoch: int
    elapsed_seconds: int
    max_process_rss_kib: int
    memory_pressure_start: str
    memory_pressure_end: str
    swap_start_kib: int
    swap_end_kib: int
    spot_price_usd_per_hour_micros: int
    estimated_spend_microusd: int


started = int(os.environ["STARTED_EPOCH"])
finished = int(os.environ["FINISHED_EPOCH"])
elapsed = finished - started
price = int(os.environ["SPOT_PRICE_MICROS"])
value = Resources(
    started_epoch=started,
    finished_epoch=finished,
    elapsed_seconds=elapsed,
    max_process_rss_kib=int(os.environ["MAX_RSS_KIB"]),
    memory_pressure_start=os.environ["PRESSURE_START"],
    memory_pressure_end=os.environ["PRESSURE_END"],
    swap_start_kib=int(os.environ["SWAP_START_KIB"]),
    swap_end_kib=int(os.environ["SWAP_END_KIB"]),
    spot_price_usd_per_hour_micros=price,
    estimated_spend_microusd=(price * elapsed + 3599) // 3600,
)
Path("resources.json").write_bytes(
    json.dumps(asdict(value), allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    + b"\n"
)
PY
  cp worker.log worker-evidence.log 2>/dev/null || : >worker-evidence.log
  for name in result/result.json result/samples.parquet cleanup.json resources.json benchmark.time worker-evidence.log hashes.txt pressure-stop.txt; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_prefix/evidence/${name##*/}" --only-show-errors
  done
  STATUS="$status" EXIT_CODE="$code" INSTANCE_ID="$iid" python3.12 - <<'PY'
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Identity:
    role: str
    uri: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class Terminal:
    schema: str
    attempt: int
    status: str
    claim_eligible: bool
    source_commit: str
    instance_id: str
    exit_code: int
    spot_price_usd_per_hour_micros: int
    evidence: dict[str, Identity]


status = os.environ["STATUS"]
evidence = {}
if status == "complete":
    for role, filename in (
        ("cleanup", "cleanup.json"),
        ("resources", "resources.json"),
        ("result", "result/result.json"),
        ("samples", "result/samples.parquet"),
        ("worker_log", "worker-evidence.log"),
    ):
        path = Path(filename)
        body = path.read_bytes()
        evidence[role] = Identity(
            role=role,
            uri=f'{os.environ["MATCHED_OUTPUT_PREFIX"]}/evidence/{path.name}',
            sha256=hashlib.sha256(body).hexdigest(),
            bytes=len(body),
        )
terminal = Terminal(
    schema="borsuk-matched-s3-vectors-terminal-v1",
    attempt=1,
    status=status,
    claim_eligible=status == "complete",
    source_commit=os.environ["MATCHED_SOURCE_COMMIT"],
    instance_id=os.environ["INSTANCE_ID"],
    exit_code=int(os.environ["EXIT_CODE"]),
    spot_price_usd_per_hour_micros=int(os.environ["MATCHED_SPOT_PRICE_MICROS"]),
    evidence=evidence,
)
Path("terminal.json").write_bytes(
    json.dumps(asdict(terminal), allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    + b"\n"
)
PY
  aws s3 cp terminal.json "$output_prefix/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}

trap finish EXIT
trap mark_interrupted TERM INT
watch_health &
watcher_pid=$!
shutdown --poweroff +125
ulimit -v $((48 * 1024 * 1024))
export HOME=${HOME:-/root}
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1

dnf install -y -q python3.12 python3.12-pip tar gzip time >/dev/null 2>&1 || exit 91
python3.12 -m venv .venv || exit 91
.venv/bin/pip install -q -r repo/scripts/requirements-s3-vectors-match.txt || exit 91

for role in source queries truth; do
  upper=${role^^}
  uri_name=MATCHED_${upper}_URI
  sha_name=MATCHED_${upper}_SHA256
  bytes_name=MATCHED_${upper}_BYTES
  aws s3 cp "${!uri_name}" "$role.parquet" --only-show-errors || exit 92
  [ "$(stat -c%s "$role.parquet")" = "${!bytes_name}" ] || exit 93
  printf '%s  %s.parquet\n' "${!sha_name}" "$role" >>hashes.txt
done
sha256sum -c hashes.txt || exit 93

/usr/bin/time -v -o benchmark.time timeout "$MATCHED_WALL_SECONDS" \
  .venv/bin/python repo/scripts/benchmark_s3_vectors_parquet.py \
    --source source.parquet \
    --source-uri "$MATCHED_SOURCE_URI" \
    --source-sha256 "$MATCHED_SOURCE_SHA256" \
    --source-bytes "$MATCHED_SOURCE_BYTES" \
    --queries queries.parquet \
    --queries-uri "$MATCHED_QUERIES_URI" \
    --queries-sha256 "$MATCHED_QUERIES_SHA256" \
    --queries-bytes "$MATCHED_QUERIES_BYTES" \
    --truth truth.parquet \
    --truth-uri "$MATCHED_TRUTH_URI" \
    --truth-sha256 "$MATCHED_TRUTH_SHA256" \
    --truth-bytes "$MATCHED_TRUTH_BYTES" \
    --output-dir result \
    --vector-bucket "$MATCHED_VECTOR_BUCKET" \
    --source-commit "$MATCHED_SOURCE_COMMIT" \
    --settle-seconds 60 || exit 94

MATCHED_VECTOR_BUCKET="$MATCHED_VECTOR_BUCKET" .venv/bin/python - <<'PY' || exit 95
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True, slots=True)
class Cleanup:
    schema: str
    vector_bucket: str
    index_name: str
    index_deleted: bool
    bucket_deleted: bool


bucket = os.environ["MATCHED_VECTOR_BUCKET"]
client = boto3.client("s3vectors", region_name="eu-central-1")
try:
    client.get_vector_bucket(vectorBucketName=bucket)
except ClientError as error:
    code = str(error.response.get("Error", {}).get("Code", ""))
    if code not in {"NotFoundException", "404", "NotFound"}:
        raise
else:
    raise RuntimeError("temporary S3 Vectors bucket still exists")
value = Cleanup(
    schema="borsuk-matched-s3-vectors-cleanup-v1",
    vector_bucket=bucket,
    index_name="vectors",
    index_deleted=True,
    bucket_deleted=True,
)
Path("cleanup.json").write_bytes(
    json.dumps(asdict(value), allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    + b"\n"
)
PY
