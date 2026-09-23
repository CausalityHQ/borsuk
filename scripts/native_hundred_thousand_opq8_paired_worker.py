#!/usr/bin/env python3
"""Generate the paired 100k OPQ8/two-bit actual S3 read worker."""

from __future__ import annotations

from scripts.launch_native_geometric_layout_spot import _q, _s3_location
from scripts.native_hundred_thousand_opq8_cell import (
    HISTORICAL_EVIDENCE,
    PRIOR_CODE_SEAL,
)
from scripts.native_hundred_thousand_opq8_paired_cell import OPQ_SOURCE, TWO_BIT_SOURCE
from scripts.native_page_microcluster_cell import FROZEN_INPUTS
from scripts.native_rotated_two_bit_cell import PRIOR_PAGES, PRIOR_TREE


def _download(identity: object, name: str) -> str:
    return " ".join((
        "download_auth", _q(identity.uri), _q(name),
        _q(identity.encoded_bytes), _q(identity.sha256),
    ))


def paired_worker_script(plan: object) -> str:
    from scripts.launch_native_one_million_selector_spot import BUCKET, artifact_names

    script = """#!/bin/bash
set -euo pipefail
root=/mnt/native-hundred-thousand-opq8-paired
output=@OUTPUT@
archive_uri=@ARCHIVE_URI@
phase=bootstrap
status=failed
started=$(date +%s)
MAXIMUM_RSS_BYTES=@RSS@
mkdir -p "$root" && cd "$root"
exec 2>worker-stderr.log
download_auth() {
  aws s3 cp "$1" "$2" --only-show-errors
  [ "$(stat -c%s "$2")" = "$3" ]
  printf '%s  %s\n' "$4" "$2" | sha256sum -c -
}
active_pid=0
broker_pid=0
stop_active() {
  if [ "$active_pid" -gt 0 ]; then
    kill -TERM -- "-$active_pid" 2>/dev/null || true
    sleep 3
    kill -KILL -- "-$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
    active_pid=0
  fi
}
run_capped() {
  local resource_file="$4" peak_bytes=0 samples=0
  setsid "$@" & pid=$!; active_pid=$pid
  while kill -0 "$pid" 2>/dev/null; do
    rss_bytes=$(ps -eo pid=,ppid=,rss= | awk -v root="$pid" -v broker="$broker_pid" '{ parent[$1]=$2; rss[$1]=$3 } END { selected[root]=1; changed=1; while(changed) { changed=0; for (p in rss) if (!selected[p] && selected[parent[p]]) { selected[p]=1; changed=1 } } for (p in selected) if (selected[p]) total+=rss[p]; if (broker > 0 && !selected[broker]) total+=rss[broker]; printf "%.0f", total * 1024 }')
    (( samples += 1 ))
    if [ "$rss_bytes" -gt "$peak_bytes" ]; then peak_bytes="$rss_bytes"; fi
    if [ "$rss_bytes" -gt "$((MAXIMUM_RSS_BYTES - 67108864))" ]; then
      stop_active
      printf 'Maximum sampled process-tree RSS (bytes): %s\\nProcess-tree RSS samples: %s\\n' "$peak_bytes" "$samples" >> "$resource_file"
      return 137
    fi
    sleep 1
  done
  rc=0; wait "$pid" || rc=$?
  active_pid=0
  printf 'Maximum sampled process-tree RSS (bytes): %s\\nProcess-tree RSS samples: %s\\n' "$peak_bytes" "$samples" >> "$resource_file"
  return "$rc"
}
check_resources() {
  local peak_kib tree_bytes samples swaps
  peak_kib=$(awk -F: 'index($1,"Maximum resident set size") {gsub(/[[:space:]]/,"",$2); print $2}' "$1")
  tree_bytes=$(awk -F: 'index($1,"Maximum sampled process-tree RSS (bytes)") {gsub(/[[:space:]]/,"",$2); print $2}' "$1")
  samples=$(awk -F: 'index($1,"Process-tree RSS samples") {gsub(/[[:space:]]/,"",$2); print $2}' "$1")
  swaps=$(awk -F: 'index($1,"Swaps") {gsub(/[[:space:]]/,"",$2); print $2}' "$1")
  [[ "$peak_kib" =~ ^[0-9]+$ ]] || return 1
  [[ "$tree_bytes" =~ ^[0-9]+$ ]] || return 1
  [[ "$samples" =~ ^[0-9]+$ ]] || return 1
  [[ "$swaps" =~ ^[0-9]+$ ]] || return 1
  (( samples > 0 && swaps == 0 && peak_kib * 1024 + 67108864 <= MAXIMUM_RSS_BYTES && tree_bytes + 67108864 <= MAXIMUM_RSS_BYTES ))
}
publish_artifact() {
  local name="$1"
  aws s3api put-object --bucket @BUCKET@ --key @ARTIFACT_KEY@/"$name" --body "$name" --if-none-match '*' >/dev/null
  mkdir -p readback
  aws s3 cp "$output/artifacts/$name" "readback/$name" --only-show-errors
  cmp -s "$name" "readback/$name"
}
terminal() {
  rc=$?; trap - EXIT; set +e; ended=$(date +%s)
  stop_active
  if [ "$broker_pid" -gt 0 ]; then
    kill -TERM "$broker_pid" 2>/dev/null || true
    wait "$broker_pid" 2>/dev/null || true
    broker_pid=0
  fi
  if [ "$status" != complete ]; then
    aws s3 cp worker-stderr.log "$output/diagnostics/worker-stderr.log" --only-show-errors || true
    for resource in source-resources.txt plan-resources.txt evaluate-resources.txt validate-resources.txt; do
      if [ -f "$resource" ]; then aws s3 cp "$resource" "$output/diagnostics/$resource" --only-show-errors || true; fi
    done
  fi
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null || true)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
  STATUS="$status" PHASE="$phase" EXIT_CODE="$rc" STARTED="$started" ENDED="$ended" SOURCE_COMMIT=@COMMIT@ SOURCE_ARCHIVE_URI="$archive_uri" SOURCE_ARCHIVE_SHA256=@ARCHIVE_SHA@ SOURCE_ARCHIVE_BYTES=@ARCHIVE_BYTES@ REQUIREMENTS_SHA256=@REQUIREMENTS_SHA@ INSTANCE_ID="$instance_id" OUTPUT_PREFIX="$output" python3 - <<'PY'
import hashlib,json,os,pathlib
def ident(path,role):
 body=pathlib.Path(path).read_bytes()
 return {"encoded_bytes":len(body),"role":role,"sha256":hashlib.sha256(body).hexdigest(),"uri":os.environ["OUTPUT_PREFIX"]+"/artifacts/"+path}
files=@ARTIFACT_FILES@
complete=os.environ["STATUS"]=="complete" and int(os.environ["EXIT_CODE"])==0
value={"artifacts":{role:ident(path,role) for role,path in files} if complete else {},"attempt":@ATTEMPT@,"claim_eligible":False,"elapsed_seconds":int(os.environ["ENDED"])-int(os.environ["STARTED"]),"exit_code":int(os.environ["EXIT_CODE"]),"instance_id":os.environ.get("INSTANCE_ID",""),"phase":os.environ["PHASE"],"schema":"borsuk-hundred-thousand-opq8-paired-terminal-v1","source_commit":os.environ["SOURCE_COMMIT"],"source_archive":{"uri":os.environ["SOURCE_ARCHIVE_URI"],"sha256":os.environ["SOURCE_ARCHIVE_SHA256"],"encoded_bytes":int(os.environ["SOURCE_ARCHIVE_BYTES"])},"requirements_sha256":os.environ["REQUIREMENTS_SHA256"],"status":os.environ["STATUS"]}
pathlib.Path("terminal.json").write_bytes(json.dumps(value,sort_keys=True,separators=(",",":")).encode()+bytes([10]))
PY
  if aws s3api put-object --bucket @BUCKET@ --key @TERMINAL_KEY@ --body terminal.json --if-none-match '*' >/dev/null; then
    mkdir -p readback
    if ! aws s3 cp "$output/terminal.json" readback/terminal.json --only-show-errors || ! cmp -s terminal.json readback/terminal.json; then rc=1; fi
  else
    rc=1
  fi
  shutdown -h now || true
  exit "$rc"
}
trap terminal EXIT
trap 'exit 143' TERM INT
phase=install
dnf install -y -q python3.12 python3.12-pip tar gzip time util-linux >install.log 2>&1
swapoff -a
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
phase=source
aws s3 cp "$archive_uri" source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = @ARCHIVE_BYTES@ ]
printf '%s  source.tar.gz\\n' @ARCHIVE_SHA@ | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
[ "$(cat repo/.borsuk-source-commit)" = @COMMIT@ ]
printf '%s  repo/scripts/requirements-format-bench.txt\\n' @REQUIREMENTS_SHA@ | sha256sum -c -
chmod -R a+rX "$root/repo"
python3.12 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check --quiet -r repo/scripts/requirements-format-bench.txt
@SOURCE_COMMANDS@
run_capped /usr/bin/time -v -o source-resources.txt timeout --foreground @WALL@ unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$root/.venv/bin/python" -m scripts.native_hundred_thousand_opq8_paired_cell source --root "$root"
check_resources source-resources.txt
phase=seal
chmod 0444 source.parquet membership.parquet mean.bin groups.bin seal.json sealed.json tree.parquet pages.parquet opq/model.bin opq/codes.bin opq/seal.json opq/prior-code-seal.json source-seal.json
publish_artifact source-seal.json
publish_artifact source-resources.txt
phase=plans
@PLAN_COMMANDS@
run_capped /usr/bin/time -v -o plan-resources.txt timeout --foreground @WALL@ unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$root/.venv/bin/python" -m scripts.native_hundred_thousand_opq8_paired_cell plans --root "$root"
check_resources plan-resources.txt
chmod 0444 opq/plans.json opq/evidence.json plan-seal.json
publish_artifact plan-seal.json
publish_artifact plan-resources.txt
phase=evaluate
@DEVELOPMENT_COMMANDS@
chmod 0444 queries.parquet truth.parquet historical-evidence.json
mkdir evaluation && chown nobody:nobody evaluation
env PYTHONPATH="$root/repo" "$root/.venv/bin/python" -m scripts.native_hundred_thousand_opq8_paired_broker --root "$root" --socket "$root/broker.sock" --audit "$root/broker-audit.json" & broker_pid=$!
for attempt in $(seq 1 100); do
  [ -S "$root/broker.sock" ] && break
  kill -0 "$broker_pid" 2>/dev/null
  sleep 0.1
done
[ -S "$root/broker.sock" ]
run_capped /usr/bin/time -v -o evaluate-resources.txt timeout --foreground @WALL@ unshare --net --fork setpriv --reuid=nobody --regid=nobody --clear-groups env -i PATH="$PATH" PYTHONPATH="$root/repo" BORSUK_RANGE_BROKER_SOCKET="$root/broker.sock" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$root/.venv/bin/python" -m scripts.native_hundred_thousand_opq8_paired_cell evaluate --root "$root" --out "$root/evaluation"
kill -TERM "$broker_pid"
wait "$broker_pid"
broker_pid=0
mv evaluation/evidence.json evidence.json
mv evaluation/result.json result.json
rmdir evaluation
check_resources evaluate-resources.txt
python3 - <<'PY'
import json,pathlib
audit=json.loads(pathlib.Path('broker-audit.json').read_bytes())
metrics=json.loads(pathlib.Path('result.json').read_bytes())['metrics']
assert audit['gets']==metrics['candidate_actual_gets']+metrics['control_actual_gets']
assert audit['bytes']==metrics['candidate_actual_bytes']+metrics['control_actual_bytes']
PY
publish_artifact evidence.json
publish_artifact result.json
publish_artifact broker-audit.json
publish_artifact evaluate-resources.txt
phase=validate
run_capped /usr/bin/time -v -o validate-resources.txt timeout --foreground @WALL@ unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$root/.venv/bin/python" -m scripts.native_hundred_thousand_opq8_paired_cell validate --root "$root" --out "$root"
check_resources validate-resources.txt
publish_artifact validation.json
publish_artifact validate-resources.txt
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
status=complete
phase=complete
"""
    replacements = {
        "@OUTPUT@": _q(plan.output_prefix.rstrip("/")),
        "@RSS@": str(plan.maximum_rss_bytes),
        "@BUCKET@": _q(BUCKET),
        "@ARTIFACT_KEY@": _q(_s3_location(plan.output_prefix)[1] + "/artifacts"),
        "@TERMINAL_KEY@": _q(_s3_location(plan.output_prefix)[1] + "/terminal.json"),
        "@ARTIFACT_FILES@": repr(tuple(artifact_names("paired").items())),
        "@ATTEMPT@": str(plan.attempt),
        "@COMMIT@": _q(plan.source_commit),
        "@ARCHIVE_URI@": _q(plan.source_archive.uri),
        "@ARCHIVE_SHA@": _q(plan.source_archive.sha256),
        "@ARCHIVE_BYTES@": str(plan.source_archive.encoded_bytes),
        "@REQUIREMENTS_SHA@": _q(plan.requirements_sha256),
        "@WALL@": str(plan.wall_seconds),
        "@SOURCE_COMMANDS@": "\n".join((
            _download(FROZEN_INPUTS.layout.source, "source.parquet"),
            _download(FROZEN_INPUTS.membership, "membership.parquet"),
            _download(PRIOR_CODE_SEAL, "seal.json"),
            "mkdir opq",
            "cp seal.json opq/prior-code-seal.json",
            _download(OPQ_SOURCE["model"], "opq/model.bin"),
            _download(OPQ_SOURCE["codes"], "opq/codes.bin"),
            _download(OPQ_SOURCE["seal"], "opq/seal.json"),
            _download(TWO_BIT_SOURCE["mean"], "mean.bin"),
            _download(TWO_BIT_SOURCE["groups"], "groups.bin"),
            _download(TWO_BIT_SOURCE["sealed"], "sealed.json"),
            _download(PRIOR_TREE, "tree.parquet"),
            _download(PRIOR_PAGES, "pages.parquet"),
        )),
        "@PLAN_COMMANDS@": "\n".join((
            _download(OPQ_SOURCE["plans"], "opq/plans.json"),
            _download(OPQ_SOURCE["evidence"], "opq/evidence.json"),
        )),
        "@DEVELOPMENT_COMMANDS@": "\n".join((
            _download(FROZEN_INPUTS.queries, "queries.parquet"),
            _download(FROZEN_INPUTS.truth, "truth.parquet"),
            _download(HISTORICAL_EVIDENCE, "historical-evidence.json"),
        )),
    }
    for marker, value in replacements.items():
        script = script.replace(marker, value)
    if len(script.encode()) > 16_384:
        raise ValueError(f"OPQ8 paired Spot worker exceeds EC2 user-data limit: {len(script.encode())}")
    return script
