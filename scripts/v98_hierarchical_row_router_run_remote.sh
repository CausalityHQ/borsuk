set -uo pipefail

root=/mnt/v98-hierarchical-g1
phase=bootstrap
interrupted=0
watcher_pid=
started_epoch=$(date +%s)
output_prefix=$V98_OUTPUT_PREFIX
pressure_start=$(tr '\n' ';' </proc/pressure/memory)
swap_start_kib=$(awk '/^SwapTotal:/{t=$2} /^SwapFree:/{f=$2} END{print t-f}' /proc/meminfo)
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
    over=$(awk -v value="$full_avg10" 'BEGIN{print value > 0.50 ? 1 : 0}')
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
  local iid=unknown
  iid=$(instance_id) || true
  local finished_epoch pressure_end swap_end_kib max_rss_kib status
  finished_epoch=$(date +%s)
  pressure_end=$(tr '\n' ';' </proc/pressure/memory)
  swap_end_kib=$(awk '/^SwapTotal:/{t=$2} /^SwapFree:/{f=$2} END{print t-f}' /proc/meminfo)
  max_rss_kib=$(awk -F: '/Maximum resident set size/{gsub(/ /,"",$2); if($2>m)m=$2} END{print m+0}' producer.time rescore.time 2>/dev/null)
  status=failed
  [ "$interrupted" -eq 1 ] && status=interrupted
  [ "$code" -eq 0 ] && status=complete
  STARTED_EPOCH="$started_epoch" FINISHED_EPOCH="$finished_epoch" \
    MAX_RSS_KIB="$max_rss_kib" PRESSURE_START="$pressure_start" \
    PRESSURE_END="$pressure_end" SWAP_START_KIB="$swap_start_kib" \
    SWAP_END_KIB="$swap_end_kib" SPOT_PRICE_MICROS="$V98_SPOT_PRICE_MICROS" \
    python3.12 - <<'PY'
import json, os
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
    json.dumps(asdict(value), allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
)
PY
  cp worker.log worker-evidence.log 2>/dev/null || : >worker-evidence.log
  for name in result.json rescore.json resources.json producer.time rescore.time producer.log rescore.log worker-evidence.log hashes.txt pressure-stop.txt; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_prefix/evidence/$name" --only-show-errors
  done
  STATUS="$status" EXIT_CODE="$code" INSTANCE_ID="$iid" python3.12 - <<'PY'
import hashlib, json, os
from dataclasses import asdict, dataclass
from pathlib import Path

@dataclass(frozen=True, slots=True)
class Identity:
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
    critique_result_sha256: str
    instance_id: str
    exit_code: int
    spot_price_usd_per_hour_micros: int
    evidence: dict[str, Identity]

status = os.environ["STATUS"]
evidence = {}
if status == "complete":
    for role, filename in (
        ("result", "result.json"),
        ("rescore", "rescore.json"),
        ("resources", "resources.json"),
        ("worker_log", "worker-evidence.log"),
    ):
        path = Path(filename)
        body = path.read_bytes()
        evidence[role] = Identity(
            uri=f'{os.environ["V98_OUTPUT_PREFIX"]}/evidence/{filename}',
            sha256=hashlib.sha256(body).hexdigest(),
            bytes=len(body),
        )
terminal = Terminal(
    schema="borsuk-v98-spot-terminal-v1",
    attempt=1,
    status=status,
    claim_eligible=status == "complete",
    source_commit=os.environ["V98_SOURCE_COMMIT"],
    critique_result_sha256=os.environ["V98_CRITIQUE_SHA256"],
    instance_id=os.environ["INSTANCE_ID"],
    exit_code=int(os.environ["EXIT_CODE"]),
    spot_price_usd_per_hour_micros=int(os.environ["V98_SPOT_PRICE_MICROS"]),
    evidence=evidence,
)
Path("terminal.json").write_bytes(
    json.dumps(asdict(terminal), allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
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
export OMP_NUM_THREADS=16 OPENBLAS_NUM_THREADS=16 MKL_NUM_THREADS=16

phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time >/dev/null 2>&1 || exit 91
python3.12 -m venv .venv || exit 91
.venv/bin/pip install -q numpy==1.26.4 pyarrow==17.0.0 || exit 91

phase=source
[ -f source.tar.gz ] || aws s3 cp "$V98_SOURCE_ARCHIVE_URI" source.tar.gz --only-show-errors || exit 92
[ "$(stat -c%s source.tar.gz)" = "$V98_SOURCE_ARCHIVE_BYTES" ] || exit 93
printf '%s  source.tar.gz\n' "$V98_SOURCE_ARCHIVE_SHA256" >hashes.txt
sha256sum -c hashes.txt || exit 93
[ -d repo ] || { mkdir repo && tar -xzf source.tar.gz -C repo; } || exit 94

phase=inputs
for role in source queries truth generation base delta; do
  upper=${role^^}
  uri_name=V98_${upper}_URI
  sha_name=V98_${upper}_SHA256
  bytes_name=V98_${upper}_BYTES
  case "$role" in
    source) filename=source.parquet ;;
    queries) filename=queries.parquet ;;
    truth) filename=truth.parquet ;;
    generation) filename=generation.json ;;
    base) filename=base.arrow ;;
    delta) filename=delta.arrow ;;
  esac
  aws s3 cp "${!uri_name}" "$filename" --only-show-errors || exit 95
  [ "$(stat -c%s "$filename")" = "${!bytes_name}" ] || exit 96
  printf '%s  %s\n' "${!sha_name}" "$filename" >>hashes.txt
done
sha256sum -c hashes.txt || exit 96

export PYTHONPATH="$root/repo"
phase=producer
/usr/bin/time -v -o producer.time timeout "$V98_WALL_SECONDS" .venv/bin/python - <<'PY' >producer.log 2>&1 || exit 97
import os
from pathlib import Path
from scripts.v97_row_width_screen import ObjectIdentity, ScreenAuthority, load_screen_inputs, page_map_sha256
from scripts.v98_hierarchical_row_router import HierarchyConfig, canonical_v98_result_bytes, evaluate_v98

roles = ("source", "queries", "truth", "generation", "base", "delta")
names = {"source":"source.parquet", "queries":"queries.parquet", "truth":"truth.parquet", "generation":"generation.json", "base":"base.arrow", "delta":"delta.arrow"}
identities = {
    role: ObjectIdentity(
        uri=os.environ[f"V98_{role.upper()}_URI"],
        sha256=os.environ[f"V98_{role.upper()}_SHA256"],
        bytes=int(os.environ[f"V98_{role.upper()}_BYTES"]),
    )
    for role in roles
}
inputs = load_screen_inputs(
    **{role: Path(names[role]) for role in roles}, identities=identities,
    dimensions=768, neighbors=100, query_count=int(os.environ["V98_QUERY_COUNT"]), seed=7216,
)
authority = ScreenAuthority(
    source_commit=os.environ["V98_SOURCE_COMMIT"],
    critique_result_sha256=os.environ["V98_CRITIQUE_SHA256"],
    page_map_sha256=page_map_sha256(inputs), dimensions=768, seed=7216, identities=identities,
)
Path("page_map.sha256").write_text(authority.page_map_sha256 + "\n")
config = HierarchyConfig(
    pages_per_root=int(os.environ["V98_PAGES_PER_ROOT"]),
    maximum_root_groups=int(os.environ["V98_MAXIMUM_ROOT_GROUPS"]),
    maximum_exposed_pages=int(os.environ["V98_MAXIMUM_EXPOSED_PAGES"]),
    retained_pages=int(os.environ["V98_RETAINED_PAGES"]),
    maximum_scanned_rows=int(os.environ["V98_MAXIMUM_SCANNED_ROWS"]),
    shortlist_rows=int(os.environ["V98_SHORTLIST_ROWS"]),
    maximum_gets=int(os.environ["V98_MAXIMUM_GETS"]),
    maximum_bytes=int(os.environ["V98_MAXIMUM_BYTES"]),
)
Path("result.json").write_bytes(canonical_v98_result_bytes(evaluate_v98(inputs, authority, config)))
PY

phase=rescore
result_sha=$(sha256sum result.json | cut -d' ' -f1)
RESULT_SHA="$result_sha" /usr/bin/time -v -o rescore.time .venv/bin/python - <<'PY' >rescore.log 2>&1 || exit 98
import os
from pathlib import Path
from scripts.v97_row_width_screen import ObjectIdentity, ScreenAuthority
from scripts.v98_hierarchical_row_router import HierarchyConfig
from scripts.v98_hierarchical_row_router_rescore import canonical_v98_rescore_bytes, rescore_v98_result

roles = ("source", "queries", "truth", "generation", "base", "delta")
identities = {
    role: ObjectIdentity(
        uri=os.environ[f"V98_{role.upper()}_URI"],
        sha256=os.environ[f"V98_{role.upper()}_SHA256"],
        bytes=int(os.environ[f"V98_{role.upper()}_BYTES"]),
    )
    for role in roles
}
authority = ScreenAuthority(
    source_commit=os.environ["V98_SOURCE_COMMIT"], critique_result_sha256=os.environ["V98_CRITIQUE_SHA256"],
    page_map_sha256=Path("page_map.sha256").read_text().strip(), dimensions=768, seed=7216, identities=identities,
)
config = HierarchyConfig(
    pages_per_root=int(os.environ["V98_PAGES_PER_ROOT"]), maximum_root_groups=int(os.environ["V98_MAXIMUM_ROOT_GROUPS"]),
    maximum_exposed_pages=int(os.environ["V98_MAXIMUM_EXPOSED_PAGES"]), retained_pages=int(os.environ["V98_RETAINED_PAGES"]),
    maximum_scanned_rows=int(os.environ["V98_MAXIMUM_SCANNED_ROWS"]), shortlist_rows=int(os.environ["V98_SHORTLIST_ROWS"]),
    maximum_gets=int(os.environ["V98_MAXIMUM_GETS"]), maximum_bytes=int(os.environ["V98_MAXIMUM_BYTES"]),
)
summary = rescore_v98_result(Path("result.json"), os.environ["RESULT_SHA"], expected_authority=authority, expected_config=config)
Path("rescore.json").write_bytes(canonical_v98_rescore_bytes(summary))
PY

phase=complete
exit 0
