"""Create-only Spot worker for the closed 100k returned-recall replay."""

from __future__ import annotations

import json

from scripts.launch_native_geometric_layout_spot import SpotLayoutPlan, _q, _s3_location
from scripts.launch_native_rotated_two_bit_spot import PRIOR_MEMBERSHIP
from scripts.native_two_bit_returned_replay import LEGACY_TERMINAL_SHA256

LEGACY_PREFIX = (
    "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
    "80ddf40533aefd3c24b7d3ea4539887aa94f91bb/"
    "runs/relaion-100k-dev1000-a0001"
)
LEGACY_FILES = (
    ("terminal.json", LEGACY_PREFIX + "/terminal.json", LEGACY_TERMINAL_SHA256, 4679),
    ("groups.bin", LEGACY_PREFIX + "/artifacts/groups.bin",
     "57f122df99a20612c7ed0afa13883709469ce6736287488593b9c411d8bbfa14", 20000832),
    ("mean.bin", LEGACY_PREFIX + "/artifacts/mean.bin",
     "197a8ead172a5a41e029c8f0c9d39b981150c69fc3b0367316fb89c920eb1beb", 3072),
    ("seal.json", LEGACY_PREFIX + "/artifacts/seal.json",
     "ee0d87790e8e65970340126dfea62f4eb625910df974a992ee5af58411f8067e", 5283),
    ("evidence.json", LEGACY_PREFIX + "/artifacts/evidence.json",
     "219f7cee65e9930ed06bd7297b681962e9eaaf9d4b24dc7a36c5fff24a81bbdf", 4587410),
)
ARTIFACT_FILES = {
    "returned-evidence": "evaluation/returned-evidence.json",
    "returned-result": "evaluation/returned-result.json",
    "returned-validation": "evaluation/returned-validation.json",
    "evaluate-resources": "evaluate-resources.txt",
    "evaluate-peak": "evaluate-peak.txt",
    "validate-resources": "validate-resources.txt",
    "validate-peak": "validate-peak.txt",
}


def _download(uri: str, sha256: str, size: int, filename: str) -> str:
    return "\n".join((
        f"aws s3 cp {_q(uri)} {_q(filename)} --only-show-errors",
        f'[ "$(stat -c%s {_q(filename)})" = {_q(size)} ]',
        f"printf '%s  {filename}\\n' {_q(sha256)} | sha256sum -c -",
    ))


def returned_worker_script(plan: SpotLayoutPlan) -> str:
    """Download complete authorities, replay without network, then close."""
    bucket, prefix = _s3_location(plan.output_prefix)
    downloads = "\n".join((
        _download(plan.source.uri, plan.source.sha256, plan.source.encoded_bytes,
                  "source.parquet"),
        _download(PRIOR_MEMBERSHIP.uri, PRIOR_MEMBERSHIP.sha256,
                  PRIOR_MEMBERSHIP.encoded_bytes, "membership.parquet"),
        _download(plan.queries.uri, plan.queries.sha256, plan.queries.encoded_bytes,
                  "queries.parquet"),
        _download(plan.truth.uri, plan.truth.sha256, plan.truth.encoded_bytes,
                  "truth.parquet"),
        *(_download(uri, digest, size, name) for name, uri, digest, size in LEGACY_FILES),
    ))
    script = r"""#!/bin/bash
set -euo pipefail
root=/mnt/native-two-bit-returned
output=@OUTPUT@
bucket=@BUCKET@
prefix=@PREFIX@
phase=bootstrap
status=failed
started=$(date +%s)
MAXIMUM_RSS_BYTES=3221225472
WALL_SECONDS=@WALL@
mkdir -p "$root" && cd "$root"
exec 2>worker-stderr.log
publish_artifact() {
  aws s3api put-object --bucket "$bucket" --key "$prefix/artifacts/$1" \
    --body "$1" --if-none-match '*' --no-cli-pager >/dev/null
}
run_capped() {
  setsid "$@" & pid=$!
  peak=0
  while kill -0 "$pid" 2>/dev/null; do
    rss=$(ps -eo pid=,ppid=,rss= | awk -v root="$pid" '
      { parent[$1]=$2; rss[$1]=$3 }
      END {
        selected[root]=1; changed=1
        while(changed) {
          changed=0
          for (p in rss) if (!selected[p] && selected[parent[p]]) {
            selected[p]=1; changed=1
          }
        }
        for (p in selected) if (selected[p]) total+=rss[p]
        printf "%.0f", total*1024
      }')
    if [ "$rss" -gt "$peak" ]; then peak=$rss; fi
    if [ "$rss" -gt "$MAXIMUM_RSS_BYTES" ]; then
      kill -TERM -- "-$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      printf '%s\n' "$peak" > "$phase-peak.txt"
      return 137
    fi
    sleep 1
  done
  rc=0; wait "$pid" || rc=$?
  printf '%s\n' "$peak" > "$phase-peak.txt"
  return "$rc"
}
terminal() {
  rc=$?; trap - EXIT; set +e; ended=$(date +%s)
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null || true)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
  if [ "$status" != complete ]; then
    for diagnostic in worker-stderr.log install.log evaluate-resources.txt \
      evaluate-peak.txt validate-resources.txt validate-peak.txt; do
      if [ -f "$diagnostic" ]; then
        aws s3api put-object --bucket "$bucket" \
          --key "$prefix/diagnostics/$diagnostic" --body "$diagnostic" \
          --if-none-match '*' --no-cli-pager >/dev/null 2>&1 || true
      fi
    done
  fi
  STATUS="$status" PHASE="$phase" EXIT_CODE="$rc" STARTED="$started" \
  ENDED="$ended" INSTANCE_ID="$instance_id" OUTPUT_PREFIX="$output" \
  SOURCE_COMMIT=@COMMIT@ ARCHIVE_URI=@ARCHIVE_URI@ ARCHIVE_SHA=@ARCHIVE_SHA@ \
  ARCHIVE_BYTES=@ARCHIVE_BYTES@ REQUIREMENTS_SHA=@REQUIREMENTS_SHA@ \
  ATTEMPT=@ATTEMPT@ python3 - <<'PY'
import hashlib,json,os,pathlib
files=@ARTIFACTS@
def ident(path,role):
    body=pathlib.Path(path).read_bytes()
    return {"encoded_bytes":len(body),"role":role,"sha256":hashlib.sha256(body).hexdigest(),
            "uri":os.environ["OUTPUT_PREFIX"]+"/artifacts/"+path}
complete=os.environ["STATUS"]=="complete" and int(os.environ["EXIT_CODE"])==0
terminal={
  "schema":"borsuk-two-bit-returned-terminal-v1",
  "status":os.environ["STATUS"],"phase":os.environ["PHASE"],
  "exit_code":int(os.environ["EXIT_CODE"]),
  "elapsed_seconds":int(os.environ["ENDED"])-int(os.environ["STARTED"]),
  "instance_id":os.environ.get("INSTANCE_ID",""),
  "source_commit":os.environ["SOURCE_COMMIT"],
  "source_archive":{"uri":os.environ["ARCHIVE_URI"],"sha256":os.environ["ARCHIVE_SHA"],
                    "encoded_bytes":int(os.environ["ARCHIVE_BYTES"])},
  "requirements_sha256":os.environ["REQUIREMENTS_SHA"],
  "attempt":int(os.environ["ATTEMPT"]),"claim_eligible":False,
  "artifacts":{role:ident(path,role) for role,path in files.items()} if complete else {},
}
pathlib.Path("terminal.json").write_text(json.dumps(terminal,sort_keys=True,separators=(",",":"))+"\n")
PY
  aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" \
    --body terminal.json --if-none-match '*' --no-cli-pager >/dev/null 2>&1 || true
  sudo shutdown -h now || true
  exit "$rc"
}
trap terminal EXIT
trap 'exit 143' TERM INT
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time util-linux >install.log 2>&1
swapoff -a
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
phase=source
@ARCHIVE_DOWNLOAD@
mkdir repo && tar -xzf source.tar.gz -C repo
[ "$(cat repo/.borsuk-source-commit)" = @COMMIT@ ]
printf '%s  repo/scripts/requirements-format-bench.txt\n' @REQUIREMENTS_SHA@ | sha256sum -c -
chmod -R a+rX repo
python3.12 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  -r repo/scripts/requirements-format-bench.txt
@INPUT_DOWNLOADS@
chmod 0444 source.parquet membership.parquet queries.parquet truth.parquet \
  terminal.json groups.bin mean.bin seal.json evidence.json
mkdir evaluation && chown nobody:nobody evaluation
phase=evaluate
run_capped timeout "$WALL_SECONDS" unshare --net --fork /usr/bin/time -v \
  -o evaluate-resources.txt setpriv --reuid=nobody --regid=nobody --clear-groups \
  env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 \
  OMP_NUM_THREADS=32 "$root/.venv/bin/python" \
  -m scripts.native_two_bit_returned_replay --root "$root" --out "$root/evaluation"
for name in evaluation/returned-evidence.json evaluation/returned-result.json \
  evaluate-resources.txt evaluate-peak.txt; do publish_artifact "$name"; done
phase=validate
run_capped timeout "$WALL_SECONDS" unshare --net --fork /usr/bin/time -v \
  -o validate-resources.txt setpriv --reuid=nobody --regid=nobody --clear-groups \
  env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 \
  OMP_NUM_THREADS=32 "$root/.venv/bin/python" \
  -m scripts.validate_native_two_bit_returned_replay --root "$root" --out "$root/evaluation"
for name in evaluation/returned-validation.json validate-resources.txt \
  validate-peak.txt; do publish_artifact "$name"; done
phase=resource-gate
python3 - <<'PY'
import json,pathlib,re
root=pathlib.Path('.')
for phase in ('evaluate','validate'):
    text=(root/f'{phase}-resources.txt').read_text()
    rss=re.search(r'Maximum resident set size \(kbytes\):\s*(\d+)',text)
    swaps=re.search(r'Swaps:\s*(\d+)',text)
    if rss is None or swaps is None: raise ValueError('returned resources missing')
    if int(rss.group(1))*1024>3221225472 or int(swaps.group(1))!=0:
        raise ValueError('returned resource cap differs')
    if int((root/f'{phase}-peak.txt').read_text())>3221225472:
        raise ValueError('returned process-tree cap differs')
result=json.loads((root/'evaluation/returned-result.json').read_bytes())
validation=json.loads((root/'evaluation/returned-validation.json').read_bytes())
if validation.get('valid') is not True or result.get('decision') not in \
        ('advance-fidelity-only','stop-two-bit-sole-scorer'):
    raise ValueError('returned decision differs')
PY
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
status=complete
phase=complete
"""
    replacements = {
        "@OUTPUT@": _q(plan.output_prefix.rstrip("/")),
        "@BUCKET@": _q(bucket),
        "@PREFIX@": _q(prefix),
        "@WALL@": str(plan.wall_seconds),
        "@COMMIT@": _q(plan.source_commit),
        "@ATTEMPT@": str(plan.attempt),
        "@ARCHIVE_URI@": _q(plan.source_archive.uri),
        "@ARCHIVE_BYTES@": str(plan.source_archive.encoded_bytes),
        "@ARCHIVE_SHA@": _q(plan.source_archive.sha256),
        "@REQUIREMENTS_SHA@": _q(plan.requirements_sha256),
        "@ARTIFACTS@": json.dumps(ARTIFACT_FILES, sort_keys=True),
        "@ARCHIVE_DOWNLOAD@": _download(
            plan.source_archive.uri, plan.source_archive.sha256,
            plan.source_archive.encoded_bytes, "source.tar.gz",
        ),
        "@INPUT_DOWNLOADS@": downloads,
    }
    for marker, value in replacements.items():
        expected_count = 2 if marker in {"@COMMIT@", "@REQUIREMENTS_SHA@"} else 1
        if script.count(marker) != expected_count:
            raise ValueError(f"returned worker marker differs: {marker}")
        script = script.replace(marker, value)
    if len(script.encode()) > 16_384:
        raise ValueError("returned Spot user data exceeds EC2 limit")
    return script
