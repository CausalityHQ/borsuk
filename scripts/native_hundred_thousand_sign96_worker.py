"""Create-only Spot worker for the sign96/source 100k range gate."""

from __future__ import annotations

import json

from scripts.launch_native_geometric_layout_spot import SpotLayoutPlan, _q
from scripts.launch_native_rotated_two_bit_spot import (
    PRIOR_MEMBERSHIP,
    PRIOR_PAGES,
    PRIOR_TREE,
)
from scripts.native_hundred_thousand_sign96_cell import (
    OPQ_PLANS_BYTES,
    OPQ_PLANS_SHA256,
    PRIOR_CODE_SEAL_BYTES,
    PRIOR_CODE_SEAL_SHA256,
)

PRIOR_CODE_SEAL_URI = (
    "s3://borsuk-bench-453182569524-euc1/research/native-rotated-two-bit/"
    "80ddf40533aefd3c24b7d3ea4539887aa94f91bb/"
    "runs/relaion-100k-dev1000-a0001/artifacts/seal.json"
)
OPQ_PLANS_URI = (
    "s3://borsuk-bench-453182569524-euc1/research/native-hundred-thousand-opq8-router/"
    "87236c5765186fb8ab977ef86f87a6b1047effb1/"
    "runs/relaion-100k-dev1000-a0001/artifacts/plans.json"
)
ARTIFACT_FILES = {
    "sign-mean": "sign96/mean.bin",
    "sign-groups": "sign96/groups.bin",
    "sign-seal": "sign96/seal.json",
    "source-seal": "sign96-source-seal.json",
    "plans": "sign96-plans.json",
    "plan-seal": "sign96-plan-seal.json",
    "evidence": "evaluation/sign96-evidence.json",
    "result": "evaluation/sign96-result.json",
    "validation": "evaluation/sign96-validation.json",
    "decision": "evaluation/sign96-decision.json",
    **{
        f"{phase}-resources": f"{phase}-resources.txt"
        for phase in ("construct", "plan", "evaluate", "validate")
    },
    **{
        f"{phase}-peak": f"{phase}-peak.txt"
        for phase in ("construct", "plan", "evaluate", "validate")
    },
}


def _download(uri: str, sha256: str, size: int, name: str) -> str:
    return "\n".join((
        f"aws s3 cp {_q(uri)} {_q(name)} --only-show-errors",
        f'[ "$(stat -c%s {_q(name)})" = {_q(size)} ]',
        f"printf '%s  {name}\\n' {_q(sha256)} | sha256sum -c -",
    ))


def _replace(script: str, marker: str, value: str, *, count: int = 1) -> str:
    if script.count(marker) != count:
        raise ValueError(f"sign96 worker marker differs: {marker}")
    return script.replace(marker, value)


def sign96_worker_script(plan: SpotLayoutPlan) -> str:
    """Build the sealed source, query, truth, validation and terminal phases."""
    source_downloads = "\n".join((
        _download(plan.source.uri, plan.source.sha256, plan.source.encoded_bytes, "source.parquet"),
        _download(PRIOR_MEMBERSHIP.uri, PRIOR_MEMBERSHIP.sha256,
                  PRIOR_MEMBERSHIP.encoded_bytes, "membership.parquet"),
        _download(PRIOR_TREE.uri, PRIOR_TREE.sha256, PRIOR_TREE.encoded_bytes, "tree.parquet"),
        _download(PRIOR_PAGES.uri, PRIOR_PAGES.sha256, PRIOR_PAGES.encoded_bytes, "pages.parquet"),
        _download(PRIOR_CODE_SEAL_URI, PRIOR_CODE_SEAL_SHA256,
                  PRIOR_CODE_SEAL_BYTES, "prior-code-seal.json"),
        "mkdir -p opq",
        _download(OPQ_PLANS_URI, OPQ_PLANS_SHA256, OPQ_PLANS_BYTES, "opq/plans.json"),
    ))
    script = r"""#!/bin/bash
set -euo pipefail
root=/mnt/native-sign96-range
output=@OUTPUT@
bucket=@BUCKET@
prefix=@PREFIX@
phase=bootstrap
status=failed
failure_reason=
started=$(date +%s)
MAXIMUM_RSS_BYTES=3154116608
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
      failure_reason=resource_cap
      kill -TERM -- "-$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      printf '%s\n' "$peak" > "$phase-peak.txt"
      return 137
    fi
    sleep 1
  done
  rc=0; wait "$pid" || rc=$?
  printf '%s\n' "$peak" > "$phase-peak.txt"
  if [ "$rc" -eq 124 ]; then failure_reason=phase_timeout; fi
  return "$rc"
}
terminal() {
  rc=$?; trap - EXIT; set +e; ended=$(date +%s)
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null || true)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
  if [ "$status" != complete ]; then
    aws s3api put-object --bucket "$bucket" --key "$prefix/diagnostics/worker-stderr.log" \
      --body worker-stderr.log --if-none-match '*' --no-cli-pager >/dev/null 2>&1 || true
    for diagnostic in construct-resources.txt construct-peak.txt \
      plan-resources.txt plan-peak.txt evaluate-resources.txt evaluate-peak.txt \
      validate-resources.txt validate-peak.txt; do
      if [ -s "$diagnostic" ]; then
        aws s3api put-object --bucket "$bucket" --key "$prefix/diagnostics/$diagnostic" \
          --body "$diagnostic" --if-none-match '*' --no-cli-pager >/dev/null 2>&1 || true
      fi
    done
  fi
  STATUS="$status" FAILURE_REASON="$failure_reason" PHASE="$phase" \
  EXIT_CODE="$rc" STARTED="$started" ENDED="$ended" \
  INSTANCE_ID="$instance_id" OUTPUT_PREFIX="$output" SOURCE_COMMIT=@COMMIT@ \
  ARCHIVE_URI=@ARCHIVE_URI@ ARCHIVE_SHA=@ARCHIVE_SHA@ ARCHIVE_BYTES=@ARCHIVE_BYTES@ \
  REQUIREMENTS_SHA=@REQUIREMENTS_SHA@ ATTEMPT=@ATTEMPT@ python3 - <<'PY'
import hashlib,json,os,pathlib
files=@ARTIFACTS@
def ident(path,role):
    body=pathlib.Path(path).read_bytes()
    return {"encoded_bytes":len(body),"role":role,"sha256":hashlib.sha256(body).hexdigest(),
            "uri":os.environ["OUTPUT_PREFIX"]+"/artifacts/"+path}
complete=os.environ["STATUS"]=="complete" and int(os.environ["EXIT_CODE"])==0
decision=json.loads(pathlib.Path("evaluation/sign96-decision.json").read_bytes()) if complete else {}
terminal={
  "schema":"borsuk-hundred-thousand-sign96-terminal-v1",
  "status":os.environ["STATUS"],"phase":os.environ["PHASE"],
  "failure_reason":os.environ["FAILURE_REASON"],
  "exit_code":int(os.environ["EXIT_CODE"]),
  "elapsed_seconds":int(os.environ["ENDED"])-int(os.environ["STARTED"]),
  "instance_id":os.environ.get("INSTANCE_ID",""),
  "source_commit":os.environ["SOURCE_COMMIT"],
  "source_archive":{"uri":os.environ["ARCHIVE_URI"],"sha256":os.environ["ARCHIVE_SHA"],
                    "encoded_bytes":int(os.environ["ARCHIVE_BYTES"])},
  "requirements_sha256":os.environ["REQUIREMENTS_SHA"],
  "attempt":int(os.environ["ATTEMPT"]),
  "claim_eligible":False,
  "decision":decision.get("decision","") if complete else "",
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
@SOURCE_ARCHIVE_DOWNLOAD@
mkdir repo && tar -xzf source.tar.gz -C repo
[ "$(cat repo/.borsuk-source-commit)" = @COMMIT@ ]
printf '%s  repo/scripts/requirements-format-bench.txt\n' @REQUIREMENTS_SHA@ | sha256sum -c -
python3.12 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check --quiet \
  -r repo/scripts/requirements-format-bench.txt
@SOURCE_DOWNLOADS@
chmod 0444 source.parquet membership.parquet tree.parquet pages.parquet prior-code-seal.json opq/plans.json
phase=construct
run_capped timeout "$WALL_SECONDS" unshare --net --fork env -i PATH="$PATH" \
  PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  /usr/bin/time -v -o construct-resources.txt "$root/.venv/bin/python" \
  -m scripts.native_hundred_thousand_sign96_cell construct --root "$root"
for name in sign96/mean.bin sign96/groups.bin sign96/seal.json sign96-source-seal.json \
  construct-resources.txt construct-peak.txt; do publish_artifact "$name"; done
phase=plan
@QUERY_DOWNLOAD@
chmod 0444 queries.parquet
run_capped timeout "$WALL_SECONDS" unshare --net --fork env -i PATH="$PATH" \
  PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  /usr/bin/time -v -o plan-resources.txt "$root/.venv/bin/python" \
  -m scripts.native_hundred_thousand_sign96_cell plan --root "$root"
for name in sign96-plans.json sign96-plan-seal.json plan-resources.txt plan-peak.txt; do
  publish_artifact "$name"
done
phase=evaluate
@TRUTH_DOWNLOAD@
chmod 0444 truth.parquet
mkdir -p evaluation
run_capped timeout "$WALL_SECONDS" unshare --net --fork env -i PATH="$PATH" \
  PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  /usr/bin/time -v -o evaluate-resources.txt "$root/.venv/bin/python" \
  -m scripts.native_hundred_thousand_sign96_cell evaluate --root "$root" --out "$root/evaluation"
for name in evaluation/sign96-evidence.json evaluation/sign96-result.json \
  evaluate-resources.txt evaluate-peak.txt; do publish_artifact "$name"; done
phase=validate
run_capped timeout "$WALL_SECONDS" unshare --net --fork env -i PATH="$PATH" \
  PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  /usr/bin/time -v -o validate-resources.txt "$root/.venv/bin/python" \
  -m scripts.validate_native_hundred_thousand_sign96_cell --root "$root" --out "$root/evaluation"
for name in evaluation/sign96-validation.json validate-resources.txt validate-peak.txt; do
  publish_artifact "$name"
done
phase=decision
.venv/bin/python - <<'PY'
import hashlib,json,pathlib,re
root=pathlib.Path(".")
cap=3154116608
resources={}
for phase in ("construct","plan","evaluate","validate"):
    text=(root/f"{phase}-resources.txt").read_text()
    rss=re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)",text)
    swaps=re.search(r"Swaps:\s*(\d+)",text)
    if rss is None or swaps is None: raise ValueError("sign96 resource receipt differs")
    resources[phase]={"maximum_rss_bytes":int(rss.group(1))*1024,
                      "tree_peak_bytes":int((root/f"{phase}-peak.txt").read_text().strip()),
                      "swaps":int(swaps.group(1))}
result_bytes=(root/"evaluation/sign96-result.json").read_bytes()
validation_bytes=(root/"evaluation/sign96-validation.json").read_bytes()
result=json.loads(result_bytes)
validation=json.loads(validation_bytes)
resource_pass=all(item["maximum_rss_bytes"]<=cap and item["tree_peak_bytes"]<=cap
                  and item["swaps"]==0 for item in resources.values())
if validation.get("valid") is not True: raise ValueError("sign96 validator differs")
decision={"schema":"borsuk-hundred-thousand-sign96-decision-v1",
          "decision":"advance" if result["quality_advance_candidate"] and resource_pass else "reject",
          "quality_advance_candidate":result["quality_advance_candidate"],
          "resource_pass":resource_pass,"resource_cap_bytes":cap,"resources":resources,
          "result_sha256":hashlib.sha256(result_bytes).hexdigest(),
          "validation_sha256":hashlib.sha256(validation_bytes).hexdigest()}
(root/"evaluation/sign96-decision.json").write_text(json.dumps(decision,sort_keys=True,separators=(",",":"))+"\n")
PY
publish_artifact evaluation/sign96-decision.json
phase=complete
status=complete
"""
    from scripts.launch_native_geometric_layout_spot import _s3_location

    bucket, prefix = _s3_location(plan.output_prefix)
    replacements = {
        "@OUTPUT@": _q(plan.output_prefix.rstrip("/")),
        "@BUCKET@": _q(bucket),
        "@PREFIX@": _q(prefix),
        "@WALL@": _q(plan.wall_seconds),
        "@COMMIT@": _q(plan.source_commit),
        "@ARCHIVE_URI@": _q(plan.source_archive.uri),
        "@ARCHIVE_SHA@": _q(plan.source_archive.sha256),
        "@ARCHIVE_BYTES@": _q(plan.source_archive.encoded_bytes),
        "@REQUIREMENTS_SHA@": _q(plan.requirements_sha256),
        "@ATTEMPT@": _q(plan.attempt),
        "@ARTIFACTS@": json.dumps(ARTIFACT_FILES, sort_keys=True),
        "@SOURCE_ARCHIVE_DOWNLOAD@": _download(
            plan.source_archive.uri, plan.source_archive.sha256,
            plan.source_archive.encoded_bytes, "source.tar.gz",
        ),
        "@SOURCE_DOWNLOADS@": source_downloads,
        "@QUERY_DOWNLOAD@": _download(
            plan.queries.uri, plan.queries.sha256, plan.queries.encoded_bytes,
            "queries.parquet",
        ),
        "@TRUTH_DOWNLOAD@": _download(
            plan.truth.uri, plan.truth.sha256, plan.truth.encoded_bytes,
            "truth.parquet",
        ),
    }
    for marker, value in replacements.items():
        script = _replace(
            script, marker, value,
            count=2 if marker in ("@COMMIT@", "@REQUIREMENTS_SHA@") else 1,
        )
    if len(script.encode()) > 16_384:
        raise ValueError("sign96 Spot user data exceeds EC2 limit")
    return script
