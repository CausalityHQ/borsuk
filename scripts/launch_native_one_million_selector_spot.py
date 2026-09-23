#!/usr/bin/env python3
"""Launch one immutable ReLAION-1M source-only selector on Causality Spot."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import time
from collections.abc import Sequence
from datetime import UTC, datetime

from scripts.launch_native_geometric_layout_spot import (
    DEFAULT_TARGETS,
    SourceArchiveIdentity,
    _atomic_put,
    _q,
    _s3_location,
    _terminate_and_wait,
)
from scripts.launch_native_rotated_two_bit_spot import _instance_state
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)

BUCKET = "borsuk-bench-453182569524-euc1"
ARTIFACTS = {
    "centroids": "centroids.bin",
    "membership": "membership.bin",
    "seal": "seal.json",
    "evidence": "evidence.json",
    "result": "result.json",
    "validation": "validation.json",
    "construct-resources": "construct-resources.txt",
    "evaluate-resources": "evaluate-resources.txt",
    "validate-resources": "validate-resources.txt",
}


def artifact_names(kind: str) -> dict[str, str]:
    if kind == "two_bit_code_wave":
        return {
            "page-map": "page-map.bin", "page-groups": "page-groups.bin",
            "page-bytes": "page-bytes.bin", "seal": "seal.json",
            "two-bit-code-wave-plans": "two-bit-code-wave-plans.json",
            "two-bit-code-wave-plan-seal": "two-bit-code-wave-plan-seal.json",
            "two-bit-code-wave-evidence": "two-bit-code-wave-evidence.json",
            "two-bit-code-wave-result": "two-bit-code-wave-result.json",
            "two-bit-code-wave-validation": "two-bit-code-wave-validation.json",
            "construct-resources": "construct-resources.txt",
            "plan-resources": "plan-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    if kind == "source_range":
        return {
            "page-map": "page-map.bin", "page-groups": "page-groups.bin",
            "page-bytes": "page-bytes.bin", "seal": "seal.json",
            "row-pages": "row-pages.bin", "range-seal": "range-seal.json",
            "source-range-plans": "source-range-plans.json",
            "source-range-plan-seal": "source-range-plan-seal.json",
            "source-range-evidence": "source-range-evidence.json",
            "source-range-result": "source-range-result.json",
            "source-range-validation": "source-range-validation.json",
            "construct-resources": "construct-resources.txt",
            "plan-resources": "plan-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    if kind == "data_range":
        return {
            "page-map": "page-map.bin", "page-groups": "page-groups.bin",
            "page-bytes": "page-bytes.bin", "seal": "seal.json",
            "row-pages": "row-pages.bin", "range-seal": "range-seal.json",
            "range-plans": "range-plans.json", "range-plan-seal": "range-plan-seal.json",
            "range-evidence": "range-evidence.json", "range-result": "range-result.json",
            "range-validation": "range-validation.json",
            "construct-resources": "construct-resources.txt",
            "plan-resources": "plan-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    if kind == "page_oracle":
        return {
            "page-map": "page-map.bin", "page-groups": "page-groups.bin",
            "page-bytes": "page-bytes.bin", "seal": "seal.json",
            "evidence": "evidence.json", "result": "result.json",
            "validation": "validation.json",
            "construct-resources": "construct-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    if kind == "opq8_1m":
        return {
            "model": "model.bin", "codes": "codes.bin", "membership": "membership.bin",
            "seal": "seal.json", "plans": "plans.json", "plan-seal": "plan-seal.json",
            "evidence": "evidence.json", "result": "result.json", "validation": "validation.json",
            "construct-resources": "construct-resources.txt",
            "plan-resources": "plan-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    if kind == "paired":
        return {
            "source-seal": "source-seal.json", "plan-seal": "plan-seal.json",
            "evidence": "evidence.json",
            "result": "result.json", "validation": "validation.json",
            "broker-audit": "broker-audit.json",
            "source-resources": "source-resources.txt",
            "plan-resources": "plan-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    if kind == "opq8":
        return {
            "model": "model.bin", "codes": "codes.bin", "seal": "seal.json",
            "plans": "plans.json", "evidence": "evidence.json",
            "result": "result.json", "validation": "validation.json",
            "construct-resources": "construct-resources.txt",
            "plan-resources": "plan-resources.txt",
            "evaluate-resources": "evaluate-resources.txt",
            "validate-resources": "validate-resources.txt",
        }
    return {
        **ARTIFACTS,
        **({"layout": "layout.json"} if kind == "layout" else {}),
        **({"moments": "moments.bin"} if kind == "mass" else {}),
    }


@dataclasses.dataclass(frozen=True, slots=True)
class SelectorSpotPlan:
    source_commit: str
    source_archive: SourceArchiveIdentity
    requirements_sha256: str
    output_prefix: str
    selector_kind: str = "group"
    profile: str = "causality"
    attempt: int = 1
    image_id: str = "ami-06121aa3085b6f918"
    security_group_id: str = "sg-0b1fd3e4fbde4af0d"
    instance_profile_arn: str = "arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile"
    instance_type: str = "c7i.8xlarge"
    volume_gib: int = 100
    wall_seconds: int = 7_200
    maximum_rss_bytes: int = 3 * 1024**3


def build_plan(**values: object) -> SelectorSpotPlan:
    plan = SelectorSpotPlan(**values)
    if plan.selector_kind not in {"group", "page", "range", "byte", "pq96", "pq80", "layout", "mass", "opq8", "paired", "opq8_1m", "page_oracle", "data_range", "source_range", "two_bit_code_wave"}:
        raise ValueError("one-million selector kind differs")
    expected = (
        f"s3://{BUCKET}/research/native-hundred-thousand-opq8-paired/"
        f"{plan.source_commit}/runs/relaion-100k-dev1000-a{plan.attempt:04d}"
        if plan.selector_kind == "paired" else
        f"s3://{BUCKET}/research/native-hundred-thousand-opq8-router/"
        f"{plan.source_commit}/runs/relaion-100k-dev1000-a{plan.attempt:04d}"
        if plan.selector_kind == "opq8" else
        f"s3://{BUCKET}/research/native-one-million-opq8-selector/"
        f"{plan.source_commit}/runs/relaion-1m-dev1000-a{plan.attempt:04d}"
        if plan.selector_kind == "opq8_1m" else
        f"s3://{BUCKET}/research/native-one-million-page-oracle-selector/"
        f"{plan.source_commit}/runs/relaion-1m-dev1000-a{plan.attempt:04d}"
        if plan.selector_kind == "page_oracle" else
        f"s3://{BUCKET}/research/native-one-million-data-range-selector/"
        f"{plan.source_commit}/runs/relaion-1m-dev1000-a{plan.attempt:04d}"
        if plan.selector_kind == "data_range" else
        f"s3://{BUCKET}/research/native-one-million-source-range-diagnostic/"
        f"{plan.source_commit}/runs/relaion-1m-dev1000-a{plan.attempt:04d}"
        if plan.selector_kind == "source_range" else
        f"s3://{BUCKET}/research/native-one-million-{plan.selector_kind}-selector/"
        f"{plan.source_commit}/runs/relaion-1m-dev1000-a{plan.attempt:04d}"
    )
    if (
        len(plan.source_commit) != 40
        or any(character not in "0123456789abcdef" for character in plan.source_commit)
        or plan.output_prefix.rstrip("/") != expected
        or plan.profile != "causality"
        or not 1 <= plan.attempt <= 9999
        or len(plan.requirements_sha256) != 64
        or any(character not in "0123456789abcdef" for character in plan.requirements_sha256)
        or plan.wall_seconds != (14_400 if plan.selector_kind == "opq8_1m" else 7_200)
        or plan.maximum_rss_bytes != 3 * 1024**3
    ):
        raise ValueError("one-million selector Spot plan differs")
    return plan


def _download_commands(identities: dict[str, object], names: dict[str, str]) -> str:
    lines: list[str] = []
    for role, filename in names.items():
        identity = identities[role]
        lines.extend((
            f"aws s3 cp {_q(identity.uri)} {_q(filename)} --only-show-errors",
            f"[ \"$(stat -c%s {_q(filename)})\" = {_q(identity.bytes)} ]",
            f"printf '%s  {filename}\\n' {_q(identity.sha256)} | sha256sum -c -",
        ))
    return "\n".join(lines)


def _terminal_schema(kind: str) -> str:
    if kind == "two_bit_code_wave":
        return "borsuk-one-million-two-bit-code-wave-terminal-v1"
    if kind == "source_range":
        return "borsuk-one-million-source-range-terminal-v1"
    if kind == "data_range":
        return "borsuk-one-million-opq8-data-range-terminal-v1"
    if kind == "page_oracle":
        return "borsuk-one-million-opq8-page-oracle-terminal-v1"
    if kind == "opq8_1m":
        return "borsuk-one-million-opq8-terminal-v1"
    if kind == "paired":
        return "borsuk-hundred-thousand-opq8-paired-terminal-v1"
    if kind == "opq8":
        return "borsuk-hundred-thousand-opq8-terminal-v1"
    return (
        "borsuk-one-million-selector-terminal-v1" if kind == "group"
        else f"borsuk-one-million-{kind}-selector-terminal-v1"
    )


def worker_script(plan: SelectorSpotPlan) -> str:
    """Generate a compact phase-separated worker with durable failure logs."""
    if plan.selector_kind == "two_bit_code_wave":
        from scripts.native_one_million_two_bit_code_projection_worker import (
            two_bit_code_wave_worker_script,
        )

        return two_bit_code_wave_worker_script(plan)
    if plan.selector_kind == "source_range":
        from scripts.native_one_million_source_range_worker import (
            source_range_worker_script,
        )

        return source_range_worker_script(plan)
    if plan.selector_kind == "data_range":
        from scripts.native_one_million_data_range_worker import (
            data_range_worker_script,
        )

        return data_range_worker_script(plan)
    if plan.selector_kind == "page_oracle":
        from scripts.native_one_million_page_oracle_worker import (
            page_oracle_worker_script,
        )

        return page_oracle_worker_script(plan)
    if plan.selector_kind == "paired":
        from scripts.native_hundred_thousand_opq8_paired_worker import (
            paired_worker_script,
        )

        return paired_worker_script(plan)
    if plan.selector_kind in {"opq8", "opq8_1m"}:
        from scripts.native_hundred_thousand_opq8_worker import opq8_worker_script

        return opq8_worker_script(plan)
    script = """#!/bin/bash
set -euo pipefail
root=/mnt/native-one-million-selector
output=@OUTPUT@
phase=bootstrap
status=failed
started=$(date +%s)
MAXIMUM_RSS_BYTES=@RSS@
mkdir -p "$root" && cd "$root"
exec 2>worker-stderr.log
run_capped() {
  local resource_file="$4" peak_bytes=0 samples=0
  setsid "$@" & pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    rss_bytes=$(ps -eo pid=,ppid=,rss= | awk -v root="$pid" '{ parent[$1]=$2; rss[$1]=$3 } END { selected[root]=1; changed=1; while(changed) { changed=0; for (p in rss) if (!selected[p] && selected[parent[p]]) { selected[p]=1; changed=1 } } for (p in selected) if (selected[p]) total+=rss[p]; printf "%.0f", total * 1024 }')
    (( samples += 1 ))
    if [ "$rss_bytes" -gt "$peak_bytes" ]; then peak_bytes="$rss_bytes"; fi
    if [ "$rss_bytes" -gt "$((MAXIMUM_RSS_BYTES - 67108864))" ]; then
      kill -TERM -- "-$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      printf 'Maximum sampled process-tree RSS (bytes): %s\\nProcess-tree RSS samples: %s\\n' "$peak_bytes" "$samples" >> "$resource_file"
      return 137
    fi
    sleep 1
  done
  rc=0; wait "$pid" || rc=$?
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
  rm -f "readback/$name"
}
terminal() {
  rc=$?; trap - EXIT; set +e; ended=$(date +%s)
  if [ "$status" != complete ]; then
    aws s3 cp worker-stderr.log "$output/diagnostics/worker-stderr.log" --only-show-errors || true
    for resource in construct-resources.txt evaluate-resources.txt validate-resources.txt; do
      if [ -f "$resource" ]; then aws s3 cp "$resource" "$output/diagnostics/$resource" --only-show-errors || true; fi
    done
  fi
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token 2>/dev/null || true)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
  STATUS="$status" PHASE="$phase" EXIT_CODE="$rc" STARTED="$started" ENDED="$ended" SOURCE_COMMIT=@COMMIT@ SOURCE_ARCHIVE_URI=@ARCHIVE_URI@ SOURCE_ARCHIVE_SHA256=@ARCHIVE_SHA@ SOURCE_ARCHIVE_BYTES=@ARCHIVE_BYTES@ REQUIREMENTS_SHA256=@REQUIREMENTS_SHA@ INSTANCE_ID="$instance_id" OUTPUT_PREFIX="$output" python3 - <<'PY'
import hashlib,json,os,pathlib
def ident(path,role):
 body=pathlib.Path(path).read_bytes()
 return {"encoded_bytes":len(body),"role":role,"sha256":hashlib.sha256(body).hexdigest(),"uri":os.environ["OUTPUT_PREFIX"]+"/artifacts/"+path}
files=@ARTIFACT_FILES@
complete=os.environ["STATUS"]=="complete" and int(os.environ["EXIT_CODE"])==0
value={"artifacts":{role:ident(path,role) for role,path in files} if complete else {},"attempt":@ATTEMPT@,"claim_eligible":False,"elapsed_seconds":int(os.environ["ENDED"])-int(os.environ["STARTED"]),"exit_code":int(os.environ["EXIT_CODE"]),"instance_id":os.environ.get("INSTANCE_ID",""),"phase":os.environ["PHASE"],"schema":"@TERMINAL_SCHEMA@","source_commit":os.environ["SOURCE_COMMIT"],"source_archive":{"uri":os.environ["SOURCE_ARCHIVE_URI"],"sha256":os.environ["SOURCE_ARCHIVE_SHA256"],"encoded_bytes":int(os.environ["SOURCE_ARCHIVE_BYTES"])},"requirements_sha256":os.environ["REQUIREMENTS_SHA256"],"status":os.environ["STATUS"]}
pathlib.Path("terminal.json").write_bytes(json.dumps(value,sort_keys=True,separators=(",",":")).encode()+bytes([10]))
PY
  if aws s3api put-object --bucket @BUCKET@ --key @TERMINAL_KEY@ --body terminal.json --if-none-match '*' >/dev/null; then
    mkdir -p readback
    if ! aws s3 cp "$output/terminal.json" readback/terminal.json --only-show-errors || ! cmp -s terminal.json readback/terminal.json; then rc=1; fi
  else
    rc=1
  fi
  rm -f source.parquet base.arrow delta.arrow queries.parquet truth.parquet
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
aws s3 cp @ARCHIVE_URI@ source.tar.gz --only-show-errors
[ "$(stat -c%s source.tar.gz)" = @ARCHIVE_BYTES@ ]
printf '%s  source.tar.gz\\n' @ARCHIVE_SHA@ | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
[ "$(cat repo/.borsuk-source-commit)" = @COMMIT@ ]
printf '%s  repo/scripts/requirements-format-bench.txt\\n' @REQUIREMENTS_SHA@ | sha256sum -c -
chmod -R a+rX "$root/repo"
python3.12 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check --quiet -r repo/scripts/requirements-format-bench.txt
@SOURCE_COMMANDS@
phase=construct
run_capped /usr/bin/time -v -o construct-resources.txt timeout @WALL@ unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 "$root/.venv/bin/python" -m @CELL_MODULE@ construct --root "$root"
phase=seal
chmod 0444 source.parquet generation.json base.arrow delta.arrow router.arrow centroids.bin membership.bin seal.json @LAYOUT_CHMOD@
publish_artifact centroids.bin
publish_artifact membership.bin
publish_artifact seal.json
@LAYOUT_PUBLISH@
publish_artifact construct-resources.txt
phase=evaluate
@DEVELOPMENT_COMMANDS@
chmod 0444 queries.parquet truth.parquet
mkdir evaluation && chown nobody:nobody evaluation
run_capped /usr/bin/time -v -o evaluate-resources.txt timeout @WALL@ unshare --net --fork setpriv --reuid=nobody --regid=nobody --clear-groups env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 "$root/.venv/bin/python" -m @CELL_MODULE@ evaluate --root "$root" --out "$root/evaluation"
mv evaluation/evidence.json evidence.json
mv evaluation/result.json result.json
rmdir evaluation
publish_artifact evidence.json
publish_artifact result.json
publish_artifact evaluate-resources.txt
phase=validate
run_capped /usr/bin/time -v -o validate-resources.txt timeout @WALL@ unshare --net --fork env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=32 OMP_NUM_THREADS=32 "$root/.venv/bin/python" -m @CELL_MODULE@ validate --root "$root" --out "$root"
publish_artifact validation.json
publish_artifact validate-resources.txt
phase=resource-gate
for resource in construct-resources.txt evaluate-resources.txt validate-resources.txt; do check_resources "$resource"; done
awk '$1 == "SwapTotal:" {exit ($2 != 0)}' /proc/meminfo
status=complete
phase=complete
"""
    source_names = {
        "source": "source.parquet", "generation": "generation.json",
        "base": "base.arrow", "delta": "delta.arrow", "router": "router.arrow",
    }
    development_names = {"queries": "queries.parquet", "truth": "truth.parquet"}
    replacements = {
        "@OUTPUT@": _q(plan.output_prefix.rstrip("/")),
        "@CELL_MODULE@": (
            "scripts.native_one_million_page_dispersion_mass_cell" if plan.selector_kind == "mass"
            else
            "scripts.native_one_million_geometric_group_order_cell" if plan.selector_kind == "layout"
            else
            "scripts.native_one_million_pq80_projection_cell" if plan.selector_kind == "pq80"
            else
            "scripts.native_one_million_pq96_projection_cell" if plan.selector_kind == "pq96"
            else
            "scripts.native_one_million_byte_ceiling_cell" if plan.selector_kind == "byte"
            else
            "scripts.native_one_million_range_selector_cell" if plan.selector_kind == "range"
            else "scripts.native_one_million_page_selector_cell" if plan.selector_kind == "page"
            else "scripts.native_one_million_selector_cell"
        ),
        "@TERMINAL_SCHEMA@": _terminal_schema(plan.selector_kind),
        "@ARTIFACT_FILES@": repr(tuple(artifact_names(plan.selector_kind).items())),
        "@LAYOUT_CHMOD@": (
            "layout.json" if plan.selector_kind == "layout"
            else "moments.bin" if plan.selector_kind == "mass" else ""
        ),
        "@LAYOUT_PUBLISH@": (
            "publish_artifact layout.json" if plan.selector_kind == "layout"
            else "publish_artifact moments.bin" if plan.selector_kind == "mass" else ""
        ),
        "@ATTEMPT@": str(plan.attempt),
        "@BUCKET@": _q(BUCKET),
        "@ARTIFACT_KEY@": _q(_s3_location(plan.output_prefix)[1] + "/artifacts"),
        "@TERMINAL_KEY@": _q(_s3_location(plan.output_prefix)[1] + "/terminal.json"),
        "@RSS@": str(plan.maximum_rss_bytes),
        "@COMMIT@": _q(plan.source_commit),
        "@ARCHIVE_URI@": _q(plan.source_archive.uri),
        "@ARCHIVE_BYTES@": str(plan.source_archive.encoded_bytes),
        "@ARCHIVE_SHA@": _q(plan.source_archive.sha256),
        "@REQUIREMENTS_SHA@": _q(plan.requirements_sha256),
        "@SOURCE_COMMANDS@": _download_commands(SOURCE_IDENTITIES, source_names),
        "@DEVELOPMENT_COMMANDS@": _download_commands(DEVELOPMENT_IDENTITIES, development_names),
        "@WALL@": str(plan.wall_seconds),
    }
    for marker, value in replacements.items():
        script = script.replace(marker, value)
    if len(script.encode()) > 16_384:
        raise ValueError("one-million selector Spot user data exceeds EC2 limit")
    return script


def build_launch_specs(plan: SelectorSpotPlan) -> list[dict[str, object]]:
    user_data = worker_script(plan)
    specs = []
    for target in DEFAULT_TARGETS:
        token = hashlib.sha256(
            f"one-million-{plan.selector_kind}-selector:{plan.source_commit}:{target.availability_zone}:a{plan.attempt:04d}".encode()
        ).hexdigest()[:32]
        specs.append({
            "BlockDeviceMappings": [{"DeviceName": "/dev/xvda", "Ebs": {
                "DeleteOnTermination": True, "Encrypted": True,
                "VolumeSize": plan.volume_gib, "VolumeType": "gp3",
            }}],
            "ClientToken": f"native-1m-{plan.selector_kind}-" + token,
            "IamInstanceProfile": {"Arn": plan.instance_profile_arn},
            "ImageId": plan.image_id,
            "InstanceInitiatedShutdownBehavior": "terminate",
            "InstanceMarketOptions": {"MarketType": "spot", "SpotOptions": {
                "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time",
            }},
            "InstanceType": plan.instance_type,
            "MaxCount": 1, "MinCount": 1,
            "NetworkInterfaces": [{
                "AssociatePublicIpAddress": True, "DeviceIndex": 0,
                "Groups": [plan.security_group_id], "SubnetId": target.subnet_id,
            }],
            "TagSpecifications": [{"ResourceType": "instance", "Tags": [
                {"Key": "Name", "Value": (
                    "borsuk-native-hundred-thousand-opq8-router"
                    if plan.selector_kind == "opq8"
                    else "borsuk-native-hundred-thousand-opq8-paired"
                    if plan.selector_kind == "paired"
                    else f"borsuk-native-one-million-{plan.selector_kind}-selector"
                )},
                {"Key": "BorsukAttempt", "Value": f"a{plan.attempt:04d}"},
            ]}],
            "UserData": user_data,
        })
    return specs


def _validate_terminal_bytes(
    body: bytes, plan: SelectorSpotPlan, instance_id: str
) -> dict[str, object]:
    try:
        terminal = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("selector terminal JSON differs") from error
    if (
        body != (json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n").encode()
        or type(terminal) is not dict
        or set(terminal) != {"artifacts", "attempt", "claim_eligible", "elapsed_seconds", "exit_code", "instance_id", "phase", "schema", "source_commit", "source_archive", "requirements_sha256", "status"}
        or terminal["schema"] != _terminal_schema(plan.selector_kind)
        or terminal["attempt"] != plan.attempt
        or terminal["claim_eligible"] is not False
        or terminal["source_commit"] != plan.source_commit
        or terminal["source_archive"] != dataclasses.asdict(plan.source_archive)
        or terminal["requirements_sha256"] != plan.requirements_sha256
        or terminal["instance_id"] != instance_id
        or type(terminal["elapsed_seconds"]) is not int
        or terminal["elapsed_seconds"] < 0
        or type(terminal["exit_code"]) is not int
    ):
        raise ValueError("selector terminal authority differs")
    complete = terminal["status"] == terminal["phase"] == "complete" and terminal["exit_code"] == 0
    if not complete:
        if terminal["status"] != "failed" or terminal["exit_code"] == 0 or terminal["artifacts"] != {}:
            raise ValueError("selector failed terminal differs")
        return terminal
    artifacts = terminal["artifacts"]
    names = artifact_names(plan.selector_kind)
    if type(artifacts) is not dict or set(artifacts) != set(names):
        raise ValueError("selector terminal artifact roster differs")
    for role, name in names.items():
        identity = artifacts[role]
        if (
            type(identity) is not dict
            or set(identity) != {"role", "uri", "sha256", "encoded_bytes"}
            or identity["role"] != role
            or identity["uri"] != plan.output_prefix.rstrip("/") + "/artifacts/" + name
            or type(identity["encoded_bytes"]) is not int
            or identity["encoded_bytes"] <= 0
            or type(identity["sha256"]) is not str
            or len(identity["sha256"]) != 64
            or any(character not in "0123456789abcdef" for character in identity["sha256"])
        ):
            raise ValueError("selector terminal artifact identity differs")
    return terminal


def _readback_artifacts(
    s3: object, bucket: str, prefix: str, terminal: dict[str, object], *, kind: str = "group",
) -> None:
    if terminal["status"] != "complete":
        return
    for role, filename in artifact_names(kind).items():
        identity = terminal["artifacts"][role]
        body = s3.get_object(Bucket=bucket, Key=f"{prefix}/artifacts/{filename}")["Body"].read()
        if len(body) != identity["encoded_bytes"] or hashlib.sha256(body).hexdigest() != identity["sha256"]:
            raise ValueError("selector " + role + " readback differs")


def _controller_terminal(plan: SelectorSpotPlan, instance_id: str) -> dict[str, object]:
    """Record an interrupted or boot-failed attempt with no worker terminal."""
    return {
        "artifacts": {}, "attempt": plan.attempt, "claim_eligible": False,
        "elapsed_seconds": 0, "exit_code": 1, "instance_id": instance_id,
        "phase": "controller", "schema": _terminal_schema(plan.selector_kind),
        "source_commit": plan.source_commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
        "status": "failed",
    }


def launch_and_monitor(plan: SelectorSpotPlan) -> dict[str, object]:
    import boto3

    session = boto3.Session(profile_name=plan.profile, region_name="eu-central-1")
    ec2 = session.client("ec2")
    s3 = session.client("s3")
    bucket, prefix = _s3_location(plan.output_prefix)
    existing = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/", MaxKeys=1)
    if existing.get("KeyCount", 0) or existing.get("Contents"):
        raise ValueError("selector immutable attempt already exists")
    if plan.selector_kind in {"opq8", "paired"}:
        from scripts.native_hundred_thousand_opq8_cell import (
            HISTORICAL_EVIDENCE,
            PRIOR_CODE_SEAL,
        )
        from scripts.native_page_microcluster_cell import FROZEN_INPUTS

        if plan.selector_kind == "paired":
            from scripts.native_hundred_thousand_opq8_paired_cell import (
                OPQ_SOURCE,
                TWO_BIT_SOURCE,
            )
            from scripts.native_rotated_two_bit_cell import PRIOR_PAGES, PRIOR_TREE

            source_inputs = {
                "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
                "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
                "prior-code-seal": dataclasses.asdict(PRIOR_CODE_SEAL),
                **{f"opq-{name}": dataclasses.asdict(identity) for name, identity in OPQ_SOURCE.items() if name in {"model", "codes", "seal"}},
                **{f"two-bit-{name}": dataclasses.asdict(identity) for name, identity in TWO_BIT_SOURCE.items()},
                "tree": dataclasses.asdict(PRIOR_TREE),
                "pages": dataclasses.asdict(PRIOR_PAGES),
            }
            development_inputs = {
                "queries": dataclasses.asdict(FROZEN_INPUTS.queries),
                "truth": dataclasses.asdict(FROZEN_INPUTS.truth),
                "historical-evidence": dataclasses.asdict(HISTORICAL_EVIDENCE),
                "opq-plans": dataclasses.asdict(OPQ_SOURCE["plans"]),
                "opq-evidence": dataclasses.asdict(OPQ_SOURCE["evidence"]),
            }
        else:
            source_inputs = {
                "source": dataclasses.asdict(FROZEN_INPUTS.layout.source),
                "membership": dataclasses.asdict(FROZEN_INPUTS.membership),
                "prior-code-seal": dataclasses.asdict(PRIOR_CODE_SEAL),
            }
            development_inputs = {
                "queries": dataclasses.asdict(FROZEN_INPUTS.queries),
                "truth": dataclasses.asdict(FROZEN_INPUTS.truth),
                "historical-evidence": dataclasses.asdict(HISTORICAL_EVIDENCE),
            }
    elif plan.selector_kind == "opq8_1m":
        from scripts.native_one_million_opq8_cell import (
            CONTROL_SOURCE,
            HISTORICAL_EVIDENCE,
            MODEL_IDENTITY,
        )

        source_inputs = {
            **{role: dataclasses.asdict(value) for role, value in SOURCE_IDENTITIES.items()},
            "opq-model": dataclasses.asdict(MODEL_IDENTITY),
            **{f"control-{role}": dataclasses.asdict(identity) for role, identity in CONTROL_SOURCE.items()},
        }
        development_inputs = {
            **{role: dataclasses.asdict(value) for role, value in DEVELOPMENT_IDENTITIES.items()},
            "historical-evidence": dataclasses.asdict(HISTORICAL_EVIDENCE),
        }
    elif plan.selector_kind in {"page_oracle", "two_bit_code_wave"}:
        from scripts.native_one_million_page_oracle_cell import PRIOR, PRIOR_PREFIX

        source_inputs = {
            **{role: dataclasses.asdict(SOURCE_IDENTITIES[role]) for role in ("generation", "base", "delta")},
            **{f"prior-{role}": {"uri": f"{PRIOR_PREFIX}{details[0]}", "sha256": details[1], "bytes": details[2]}
               for role, details in PRIOR.items()},
        }
        if plan.selector_kind == "two_bit_code_wave":
            import scripts.native_one_million_two_bit_code_projection_cell as code_wave

            source_inputs.update({
                role: {"uri": code_wave.PRIOR_PREFIX + path, "sha256": digest, "bytes": size}
                for role, path, digest, size in (
                    ("prior-range-terminal", "terminal.json", code_wave.PRIOR_TERMINAL_SHA256, 5580),
                    ("prior-range-plans", "artifacts/range-plans.json", code_wave.PRIOR_PLAN_SHA256, 40009368),
                    ("prior-range-plan-seal", "artifacts/range-plan-seal.json", code_wave.PRIOR_PLAN_SEAL_SHA256, 501),
                    ("prior-range-seal", "artifacts/range-seal.json", code_wave.PRIOR_RANGE_SEAL_SHA256, 682),
                )
            })
        development_inputs = {
            role: dataclasses.asdict(value) for role, value in DEVELOPMENT_IDENTITIES.items()
        }
    elif plan.selector_kind in {"data_range", "source_range"}:
        from scripts.native_one_million_data_range_cell import (
            PRIOR_CODES,
            PRIOR_MEMBERSHIP,
            PRIOR_MODEL,
        )
        from scripts.native_one_million_page_oracle_cell import PRIOR, PRIOR_PREFIX

        source_inputs = {
            **({"source": dataclasses.asdict(SOURCE_IDENTITIES["source"])} if plan.selector_kind == "source_range" else {}),
            **{role: dataclasses.asdict(SOURCE_IDENTITIES[role]) for role in ("generation", "base", "delta")},
            **{f"prior-{role}": {"uri": f"{PRIOR_PREFIX}{details[0]}", "sha256": details[1], "bytes": details[2]}
               for role, details in PRIOR.items()},
            **{role: {"uri": f"{PRIOR_PREFIX}artifacts/{filename}", "sha256": identity[0], "bytes": identity[1]}
               for role, filename, identity in (("codes", "codes.bin", PRIOR_CODES), ("model", "model.bin", PRIOR_MODEL), ("membership", "membership.bin", PRIOR_MEMBERSHIP))},
        }
        development_inputs = {
            role: dataclasses.asdict(value) for role, value in DEVELOPMENT_IDENTITIES.items()
        }
    else:
        source_inputs = {role: dataclasses.asdict(value) for role, value in SOURCE_IDENTITIES.items()}
        development_inputs = {role: dataclasses.asdict(value) for role, value in DEVELOPMENT_IDENTITIES.items()}
    reservation = {
        "schema": (
            "borsuk-hundred-thousand-opq8-paired-reservation-v1"
            if plan.selector_kind == "paired"
            else "borsuk-one-million-opq8-page-oracle-reservation-v1"
            if plan.selector_kind == "page_oracle"
            else "borsuk-one-million-opq8-data-range-reservation-v1"
            if plan.selector_kind == "data_range"
            else "borsuk-one-million-source-range-reservation-v1"
            if plan.selector_kind == "source_range"
            else f"borsuk-one-million-{plan.selector_kind}-selector-reservation-v1"
        ),
        "attempt": plan.attempt, "source_commit": plan.source_commit,
        "source_archive": dataclasses.asdict(plan.source_archive),
        "requirements_sha256": plan.requirements_sha256,
        "source_inputs": source_inputs,
        "development_inputs": development_inputs,
    }
    _atomic_put(
        s3, bucket=bucket, key=f"{prefix}/reservation.json",
        body=(json.dumps(reservation, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )
    instance_id = None
    capacity_markers = ("InsufficientInstanceCapacity", "MaxSpotInstanceCountExceeded")
    for spec in build_launch_specs(plan):
        try:
            response = ec2.run_instances(**spec)
        except Exception as error:
            if any(marker in str(error) for marker in capacity_markers):
                continue
            # A lost response can follow a successful launch. The same EC2
            # client token makes one retry idempotent; then reconcile by token.
            try:
                response = ec2.run_instances(**spec)
            except Exception as recovery_error:
                matches = ec2.describe_instances(Filters=[{
                    "Name": "client-token", "Values": [spec["ClientToken"]],
                }])
                found = [
                    item["InstanceId"]
                    for reservation in matches.get("Reservations", [])
                    for item in reservation.get("Instances", [])
                ]
                if len(found) != 1:
                    raise error from recovery_error
                response = {"Instances": [{"InstanceId": found[0]}]}
        instance_id = response["Instances"][0]["InstanceId"]
        break
    if instance_id is None:
        raise RuntimeError("one-million selector Spot capacity unavailable")
    deadline = time.monotonic() + plan.wall_seconds + 900
    terminated = False
    try:
        while True:
            try:
                body = s3.get_object(Bucket=bucket, Key=f"{prefix}/terminal.json")["Body"].read()
            except Exception as error:
                code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                state = _instance_state(ec2, instance_id)
                if state in {"terminated", "shutting-down", "stopped"}:
                    raise RuntimeError(f"selector instance {instance_id} ended without terminal") from error
                if time.monotonic() >= deadline:
                    raise TimeoutError("selector terminal deadline exceeded") from error
                time.sleep(15)
                continue
            terminal = _validate_terminal_bytes(body, plan, instance_id)
            _readback_artifacts(s3, bucket, prefix, terminal, kind=plan.selector_kind)
            return terminal
    except Exception as error:
        _terminate_and_wait(ec2, instance_id)
        terminated = True
        terminal = _controller_terminal(plan, instance_id)
        try:
            _atomic_put(
                s3, bucket=bucket, key=f"{prefix}/terminal.json",
                body=(json.dumps(terminal, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            )
        except Exception:
            # A worker terminal may already exist; the create-only write cannot replace it.
            pass
        failure = {
            "schema": "borsuk-one-million-selector-controller-failure-v1",
            "source_commit": plan.source_commit,
            "selector_kind": plan.selector_kind,
            "attempt": plan.attempt,
            "instance_id": instance_id,
            "observed_at": datetime.now(UTC).isoformat(),
            "error_type": type(error).__name__,
            "error": str(error),
        }
        try:
            _atomic_put(
                s3, bucket=bucket, key=f"{prefix}/controller-failure.json",
                body=(json.dumps(failure, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            )
        except Exception:
            pass
        raise
    finally:
        if not terminated:
            _terminate_and_wait(ec2, instance_id)


def parse_args(argv: Sequence[str] | None = None) -> SelectorSpotPlan:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive-uri", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--source-archive-bytes", type=int, required=True)
    parser.add_argument("--requirements-sha256", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--selector-kind", choices=("group", "page", "range", "byte", "pq96", "pq80", "layout", "mass", "opq8", "paired", "opq8_1m", "page_oracle", "data_range", "source_range", "two_bit_code_wave"), default="group")
    parser.add_argument("--attempt", type=int, default=1)
    args = parser.parse_args(argv)
    return build_plan(
        source_commit=args.source_commit,
        source_archive=SourceArchiveIdentity(
            args.source_archive_uri, args.source_archive_sha256, args.source_archive_bytes,
        ),
        requirements_sha256=args.requirements_sha256,
        output_prefix=args.output_prefix,
        selector_kind=args.selector_kind,
        attempt=args.attempt,
        wall_seconds=14_400 if args.selector_kind == "opq8_1m" else 7_200,
    )


def main(argv: Sequence[str] | None = None) -> None:
    terminal = launch_and_monitor(parse_args(argv))
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    if terminal["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
