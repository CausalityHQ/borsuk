#!/usr/bin/env python3
"""Launch one evidence-bound V23 incidence phase on ephemeral EC2 Spot."""

from __future__ import annotations

import argparse
import base64
import dataclasses
import decimal
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import tempfile
import time
import traceback
from collections.abc import Sequence
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPECTED_AWS_ACCOUNT = "453182569524"
PROFILE = "causality"
REGION = "eu-central-1"
INSTANCE_TYPE = "c7g.8xlarge"
AMI_ID = "ami-07bcecd13a160173f"
SUBNET_ID = "subnet-034528fbd6977848f"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
KEY_NAME = "borsuk-bench"
INSTANCE_PROFILE = "borsuk-bench-profile"
BUCKET = "borsuk-bench-453182569524-euc1"
MANIFEST_RELATIVE = "scripts/fixtures/v23_incidence_training_manifest.json"
FROZEN_PAGE_ROSTER_URI = (
    "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/"
    "r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/"
    "bench_v23_pages.json"
)
FROZEN_PAGE_ROSTER_SHA256 = (
    "276dfa1914fc1cfa980a0d5037fd8f3d53f7a3e35d4ae64c863956b9095c4303"
)
FROZEN_PAGE_ROSTER_BYTES = 12_825_166
FROZEN_PAGE_URI = (
    "s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/"
    "r01-6846520de9e7ffcfb93d5efd/runtime-v23-d2/arms/0000/attempts/0001/pages"
)
FROZEN_D1_REPORT_SHA256 = (
    "91717a4077c8a7d6b909f1f8d14f59d6a6d422a29e06b3d665a02c29743cbc39"
)
FROZEN_PAGE_GENERATION = (
    "b20f22206edd140fdd5474a3786f3f1a6ff51fa5f9d5f1be9363092156cb74ec"
)
FROZEN_TREE_RECEIPT_URI = (
    "s3://borsuk-bench-453182569524-euc1/research/v23-leaf-page-incidence/"
    "a321c473cb38a3b38c4757a50acf14e144b0441b0ca4bbbe7a8c7f3baaef78cc/"
    "v23-incidence-tree-20260831T120514Z/tree-receipt.json"
)
FROZEN_TREE_RECEIPT_SHA256 = (
    "c1af5ab84ef20797ffe52fa0a93872008df817c142957f009895c8b7fc853a99"
)
FROZEN_TREE_RECEIPT_BYTES = 26_106
SUPPORTED_PHASES = ("tree-training", "posting-construction")
BLOCKED_PHASES = (
    "development-evaluation",
    "holdout-binding",
    "holdout-evaluation",
)
LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
LOWER_GIT_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
INSTANCE_WALL_STOP_SECONDS = 21_600
MEMORY_PSI_PATH = pathlib.Path("/proc/pressure/memory")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode() + b"\n"


def _require_token(label: str, value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
        raise ValueError(f"{label} differs")
    return value


def _require_sha256(label: str, value: str) -> str:
    if LOWER_SHA256.fullmatch(value) is None:
        raise ValueError(f"{label} differs")
    return value


def _require_source_commit(value: str) -> str:
    if LOWER_GIT_SHA1.fullmatch(value) is None:
        raise ValueError("source commit differs")
    return value


def _manifest() -> dict[str, object]:
    path = ROOT / MANIFEST_RELATIVE
    raw = path.read_bytes()
    value = json.loads(raw)
    if raw != _canonical_bytes(value):
        raise ValueError("construction manifest canonical bytes differ")
    if (
        type(value) is not dict
        or value.get("schema") != "borsuk-v23-incidence-manifest-v1"
        or value.get("phase") != "tree-training"
        or value.get("claim_eligible") is not False
        or type(value.get("ordered_inputs")) is not list
        or len(value["ordered_inputs"]) != 59
    ):
        raise ValueError("construction manifest authority differs")
    return value


def build_posting_manifest(
    *,
    tree_receipt_bytes: bytes,
    tree_receipt_identity: dict[str, object],
    roster_bytes: bytes,
    roster_identity: dict[str, object],
) -> bytes:
    """Bind one completed tree and the frozen page roster without page reads."""

    identity_keys = {
        "digest",
        "digest_algorithm",
        "encoded_bytes",
        "generation",
        "role",
        "uri",
    }

    def authenticate_identity(
        identity: dict[str, object], role: str, raw: bytes
    ) -> None:
        if (
            type(identity) is not dict
            or set(identity) != identity_keys
            or identity.get("role") != role
            or identity.get("digest_algorithm") != "sha256"
            or identity.get("encoded_bytes") != len(raw)
            or identity.get("digest") != hashlib.sha256(raw).hexdigest()
            or type(identity.get("generation")) is not str
            or not identity["generation"]
            or type(identity.get("uri")) is not str
            or not identity["uri"].startswith("s3://")
        ):
            raise ValueError(f"{role} authority differs")

    authenticate_identity(tree_receipt_identity, "parent-receipt", tree_receipt_bytes)
    authenticate_identity(roster_identity, "page-roster", roster_bytes)
    if (
        tree_receipt_identity["uri"] != FROZEN_TREE_RECEIPT_URI
        or tree_receipt_identity["digest"] != FROZEN_TREE_RECEIPT_SHA256
        or tree_receipt_identity["encoded_bytes"] != FROZEN_TREE_RECEIPT_BYTES
    ):
        raise ValueError("parent receipt authority differs")
    if (
        roster_identity["uri"] != FROZEN_PAGE_ROSTER_URI
        or roster_identity["digest"] != FROZEN_PAGE_ROSTER_SHA256
        or roster_identity["encoded_bytes"] != FROZEN_PAGE_ROSTER_BYTES
    ):
        raise ValueError("page roster authority differs")

    try:
        tree_receipt = json.loads(tree_receipt_bytes)
        roster = json.loads(roster_bytes)
        tree_is_canonical = tree_receipt_bytes == _canonical_bytes(tree_receipt)
        roster_is_canonical = roster_bytes == _canonical_bytes(roster)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("posting authority JSON differs") from error

    receipt_keys = {
        "claim_eligible",
        "executable_sha256",
        "final_progress_sha256",
        "fma_backend",
        "network_namespace_inode",
        "ordered_inputs",
        "outputs",
        "parent_receipt_sha256",
        "phase",
        "preflight_evidence",
        "probes",
        "run_mode",
        "schema",
        "stop",
    }
    expected_probes = {
        "allowlisted_inputs_opened": True,
        "forbidden_roles_absent": True,
        "network_canary_denied": True,
        "network_namespace_changed": True,
        "output_writable": True,
    }
    if (
        not tree_is_canonical
        or type(tree_receipt) is not dict
        or set(tree_receipt) != receipt_keys
        or tree_receipt["schema"] != "borsuk-v23-incidence-receipt-v3"
        or tree_receipt["claim_eligible"] is not False
        or tree_receipt["phase"] != "tree-training"
        or tree_receipt["run_mode"] != "execute"
        or tree_receipt["stop"] is not None
        or tree_receipt["preflight_evidence"] is not None
        or tree_receipt["probes"] != expected_probes
        or type(tree_receipt["network_namespace_inode"]) is not int
        or tree_receipt["network_namespace_inode"] <= 0
        or type(tree_receipt["ordered_inputs"]) is not list
        or not tree_receipt["ordered_inputs"]
        or type(tree_receipt["outputs"]) is not list
        or len(tree_receipt["outputs"]) != 1
        or type(tree_receipt["parent_receipt_sha256"]) is not str
        or LOWER_SHA256.fullmatch(tree_receipt["parent_receipt_sha256"]) is None
        or type(tree_receipt["final_progress_sha256"]) is not str
        or LOWER_SHA256.fullmatch(tree_receipt["final_progress_sha256"]) is None
        or type(tree_receipt["executable_sha256"]) is not str
        or LOWER_SHA256.fullmatch(tree_receipt["executable_sha256"]) is None
        or tree_receipt["fma_backend"]
        not in {"aarch64-neon-fma", "x86-avx-fma"}
    ):
        raise ValueError("tree receipt authority differs")
    tree_identity = tree_receipt["outputs"][0]
    if (
        type(tree_identity) is not dict
        or set(tree_identity) != identity_keys
        or tree_identity.get("role") != "incidence-tree"
        or tree_identity.get("digest_algorithm") != "blake3"
        or type(tree_identity.get("digest")) is not str
        or LOWER_SHA256.fullmatch(tree_identity["digest"]) is None
        or tree_identity.get("generation") != f"content-{tree_identity['digest']}"
        or type(tree_identity.get("encoded_bytes")) is not int
        or tree_identity["encoded_bytes"] <= 0
        or type(tree_identity.get("uri")) is not str
        or not tree_identity["uri"].startswith("s3://")
    ):
        raise ValueError("incidence tree authority differs")
    construction_bytes = (ROOT / MANIFEST_RELATIVE).read_bytes()
    construction_digest = hashlib.sha256(construction_bytes).hexdigest()
    expected_construction = {
        "digest": construction_digest,
        "digest_algorithm": "sha256",
        "encoded_bytes": len(construction_bytes),
        "generation": f"unversioned-sha256:{construction_digest}",
        "role": "construction-manifest",
        "uri": f"git://borsuk/{MANIFEST_RELATIVE}",
    }
    construction_inputs = [
        identity
        for identity in tree_receipt["ordered_inputs"]
        if type(identity) is dict and identity.get("role") == "construction-manifest"
    ]
    if construction_inputs != [expected_construction]:
        raise ValueError("tree receipt authority differs")

    roster_keys = {
        "claim_eligible",
        "d1_report_sha256",
        "dataset_id",
        "document_kind",
        "index_id",
        "page_uri",
        "pages",
        "schema",
        "source_archive_sha256",
        "stage",
    }
    base = _manifest()
    if (
        not roster_is_canonical
        or type(roster) is not dict
        or set(roster) != roster_keys
        or roster["schema"] != "borsuk-v23-pages-v1"
        or roster["document_kind"] != "publication-v3-v23-page-roster"
        or roster["claim_eligible"] is not False
        or roster["stage"] != "d2"
        or roster["dataset_id"] != base["dataset_id"]
        or roster["index_id"] != base["index_id"]
        or roster["source_archive_sha256"] != base["source_archive_sha256"]
        or roster["d1_report_sha256"] != FROZEN_D1_REPORT_SHA256
        or roster["page_uri"] != FROZEN_PAGE_URI
        or type(roster["pages"]) is not list
        or len(roster["pages"]) != 28_282
    ):
        raise ValueError("page roster authority differs")

    page_keys = {
        "checksum",
        "code_width",
        "dimensions",
        "encoded_bytes",
        "family",
        "generation_checksum",
        "metric",
        "page_ordinal",
        "path",
        "primary_rows",
        "replicated_rows",
    }
    generation = list(bytes.fromhex(FROZEN_PAGE_GENERATION))
    page_inputs = []
    page_checksums = set()
    encoded_bytes = 0
    primary_rows = 0
    replicated_rows = 0
    for ordinal, page in enumerate(roster["pages"]):
        checksum = page.get("checksum") if type(page) is dict else None
        if (
            type(page) is not dict
            or set(page) != page_keys
            or type(checksum) is not str
            or LOWER_SHA256.fullmatch(checksum) is None
            or page["page_ordinal"] != ordinal
            or type(page["page_ordinal"]) is not int
            or page["path"] != f"pages/{checksum}"
            or page["family"] != "f16-flat"
            or page["code_width"] != 192
            or page["dimensions"] != 96
            or page["metric"] != "cosine"
            or page["generation_checksum"] != generation
            or type(page["encoded_bytes"]) is not int
            or page["encoded_bytes"] <= 0
            or type(page["primary_rows"]) is not int
            or page["primary_rows"] <= 0
            or type(page["replicated_rows"]) is not int
            or page["replicated_rows"] < 0
            or checksum in page_checksums
        ):
            raise ValueError("page roster authority differs")
        page_checksums.add(checksum)
        encoded_bytes += page["encoded_bytes"]
        primary_rows += page["primary_rows"]
        replicated_rows += page["replicated_rows"]
        page_inputs.append(
            {
                "authority_kind": "phase-object",
                "identity": {
                    "digest": checksum,
                    "digest_algorithm": "blake3",
                    "encoded_bytes": page["encoded_bytes"],
                    "generation": f"unversioned-blake3:{checksum}",
                    "role": f"page-body-{ordinal:05}",
                    "uri": f"{FROZEN_PAGE_URI}/{page['path']}",
                },
            }
        )
    if (
        encoded_bytes != 3_780_639_674
        or primary_rows != 9_990_000
        or replicated_rows != 8_630_111
    ):
        raise ValueError("page roster authority differs")

    def phase_object(identity: dict[str, object]) -> dict[str, object]:
        return {
            "authority_kind": "phase-object",
            "identity": identity,
        }
    manifest = {
        "algorithm": base["algorithm"],
        "claim_eligible": False,
        "dataset_id": base["dataset_id"],
        "index_id": base["index_id"],
        "ordered_inputs": [
            phase_object(tree_receipt_identity),
            phase_object(tree_identity),
            phase_object(roster_identity),
            *page_inputs,
        ],
        "parent_receipt_sha256": tree_receipt_identity["digest"],
        "phase": "posting-construction",
        "schema": "borsuk-v23-incidence-manifest-v1",
        "source_archive_sha256": base["source_archive_sha256"],
        "source_commit": base["source_commit"],
    }
    return _canonical_bytes(manifest)


def build_launch_plan(
    *, phase: str, run_id: str, source_commit: str
) -> dict[str, object]:
    """Return the mutation-free launch plan without touching AWS."""

    _require_token("run ID", run_id)
    _require_source_commit(source_commit)
    if phase not in SUPPORTED_PHASES:
        if phase in BLOCKED_PHASES:
            raise ValueError(f"{phase} has no committed immutable phase manifest")
        raise ValueError("unknown incidence phase")
    manifest = _manifest()
    if phase == "tree-training":
        preflight_input_count = 1
        execute_input_count = len(manifest["ordered_inputs"])
        parent_receipt_uri = None
        page_roster_uri = None
    else:
        preflight_input_count = 3 + 256
        execute_input_count = 3 + 28_282
        parent_receipt_uri = FROZEN_TREE_RECEIPT_URI
        page_roster_uri = FROZEN_PAGE_ROSTER_URI
    return {
        "aws_account_id": EXPECTED_AWS_ACCOUNT,
        "aws_profile": PROFILE,
        "blocked_phases": list(BLOCKED_PHASES),
        "construction_manifest": MANIFEST_RELATIVE,
        "d3_allowed": False,
        "execute_input_count": execute_input_count,
        "instance_count": 1,
        "billable_wall_stop_seconds": INSTANCE_WALL_STOP_SECONDS,
        "instance_type": INSTANCE_TYPE,
        "maximum_compute_cost_usd": 5.0,
        "outer_wall_stop_seconds": 16_200,
        "phase": phase,
        "parent_receipt_uri": parent_receipt_uri,
        "page_roster_uri": page_roster_uri,
        "preflight_input_count": preflight_input_count,
        "progress_stop_seconds": 300,
        "psi_full_immediate": 0.79,
        "psi_full_sustained": 0.50,
        "purchase_option": "spot",
        "region": REGION,
        "root_volume_delete_on_termination": True,
        "root_volume_gib": 200,
        "rss_stop_bytes": 2 << 30,
        "run_id": run_id,
        "source_commit": source_commit,
        "supported_phases": list(SUPPORTED_PHASES),
        "swap_delta_stop_bytes": 256 << 20,
        "wall_stop_seconds": 7200,
    }


def build_launch_spec(
    *, phase: str, run_id: str, source_commit: str, user_data: str
) -> dict[str, object]:
    """Build the exact one-time Spot request."""

    _require_token("run ID", run_id)
    _require_source_commit(source_commit)
    if phase not in SUPPORTED_PHASES:
        raise ValueError("unsupported incidence phase")
    if not user_data.startswith("#!/") or "shutdown -h now" not in user_data:
        raise ValueError("worker user data does not self-terminate")
    if len(user_data.encode()) > 16_384:
        raise ValueError("worker user data length exceeds EC2 authority")
    client_token = "borsuk-v23-" + hashlib.sha256(
        f"{source_commit}:{run_id}".encode()
    ).hexdigest()[:48]
    return {
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                    "VolumeSize": 200,
                    "VolumeType": "gp3",
                },
            }
        ],
        "ImageId": AMI_ID,
        "ClientToken": client_token,
        "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate",
            },
        },
        "InstanceType": INSTANCE_TYPE,
        "KeyName": KEY_NAME,
        "MaxCount": 1,
        "MetadataOptions": {
            "HttpEndpoint": "enabled",
            "HttpPutResponseHopLimit": 1,
            "HttpTokens": "required",
        },
        "MinCount": 1,
        "SecurityGroupIds": [SECURITY_GROUP_ID],
        "SubnetId": SUBNET_ID,
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": f"borsuk-v23-incidence-{run_id}"},
                    {"Key": "Project", "Value": "BorsukBenchmark"},
                    {"Key": "Campaign", "Value": "v23-leaf-page-incidence"},
                    {"Key": "Phase", "Value": phase},
                    {"Key": "RunId", "Value": run_id},
                    {"Key": "SourceCommit", "Value": source_commit},
                    {"Key": "AutoTerminate", "Value": "true"},
                ],
            }
        ],
        "UserData": user_data,
    }


def build_worker_script(
    *,
    phase: str,
    run_id: str,
    source_commit: str,
    source_uri: str,
    source_sha256: str,
    result_uri: str,
    spot_price_usd_per_hour: str,
) -> str:
    """Return a bootstrap that builds, runs once, publishes, and terminates."""

    if phase not in SUPPORTED_PHASES:
        raise ValueError("unsupported incidence phase")
    _require_token("run ID", run_id)
    _require_source_commit(source_commit)
    _require_sha256("source archive", source_sha256)
    if not source_uri.startswith("s3://") or not result_uri.startswith("s3://"):
        raise ValueError("worker S3 authority differs")
    quoted = {
        "phase": shlex.quote(phase),
        "run": shlex.quote(run_id),
        "commit": shlex.quote(source_commit),
        "source_uri": shlex.quote(source_uri),
        "source_sha": shlex.quote(source_sha256),
        "result_uri": shlex.quote(result_uri),
        "price": shlex.quote(spot_price_usd_per_hour),
    }
    return f"""#!/usr/bin/env bash
set -euo pipefail
phase={quoted['phase']}
run_id={quoted['run']}
source_commit={quoted['commit']}
source_uri={quoted['source_uri']}
source_sha256={quoted['source_sha']}
result_uri={quoted['result_uri']}
spot_price={quoted['price']}
workspace=/var/lib/borsuk-v23-incidence/source
evidence=/var/lib/borsuk-v23-incidence/evidence
scratch_root=/var/lib/borsuk-v23-incidence/scratch
archive=/var/lib/borsuk-v23-incidence/source.tar
posting_bootstrap=/var/lib/borsuk-v23-incidence/posting-bootstrap
posting_tree_receipt="$posting_bootstrap/tree-receipt.json"
posting_roster="$posting_bootstrap/page-roster.json"
posting_manifest="$posting_bootstrap/posting-manifest.json"
worker_log=/var/lib/borsuk-v23-incidence/worker.log
phase_log="$evidence/phase.log"
phase_journal="$evidence/phase-journal.txt"
unit="borsuk-v23-incidence-$run_id"
status=99
export HOME=/root PATH=/root/.cargo/bin:$PATH
mkdir -p "$evidence" "$scratch_root"
exec >>"$worker_log" 2>&1
put_once() {{
  local path="$1" name="$2" bucket prefix
  bucket="${{result_uri#s3://}}"; bucket="${{bucket%%/*}}"
  prefix="${{result_uri#s3://$bucket/}}"
  aws s3api put-object --bucket "$bucket" --key "$prefix/$name" --body "$path" \
    --expected-bucket-owner {EXPECTED_AWS_ACCOUNT} --if-none-match '*' >/dev/null
}}
finish() {{
  status=$?
  trap - EXIT
  set +e
  publish_status=0
  complete_attempted=0
  primary_evidence_attempted=0
  if [[ "$status" -eq 0 && ! -f "$evidence/spot-interruption.json" ]] && {{
    [[ "$phase" == "tree-training" && -f "$evidence/tree-receipt.json" && -f "$evidence/incidence-tree.bin" ]] ||
    [[ "$phase" == "posting-construction" && -f "$evidence/posting-receipt.json" && -f "$evidence/incidence-postings-one.bin" && -f "$evidence/incidence-postings-two.bin" ]]
  }}; then
    if [[ "$phase" == "tree-training" ]]; then
    python3 - "$evidence/ATTEMPT_COMPLETE.json" "$run_id" "$source_commit" "$source_sha256" "$spot_price" "$evidence/tree-receipt.json" "$evidence/incidence-tree.bin" "$binary" <<'PY'
import hashlib,json,os,sys
path,run_id,commit,archive_sha,price,receipt,tree,binary=sys.argv[1:]
def identity(source):
    digest=hashlib.sha256()
    with open(source,"rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""): digest.update(chunk)
    return {{"encoded_bytes":os.path.getsize(source),"sha256":digest.hexdigest()}}
value={{"binary":identity(binary),"claim_eligible":False,"incidence_tree":identity(tree),"phase":"tree-training","purchase_option":"spot","receipt":identity(receipt),"run_id":run_id,"schema":"borsuk-v23-incidence-attempt-complete-v1","source_archive_sha256":archive_sha,"source_commit":commit,"spot_price_usd_per_hour":price,"status":"complete"}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\\n")
PY
    primary_evidence_attempted=1
    put_once "$evidence/binary.json" binary.json || publish_status=86
    put_once "$binary" incidence-executable || publish_status=86
    put_once "$evidence/preflight-receipt.json" preflight-receipt.json || publish_status=86
    put_once "$evidence/progress.json" progress.json || publish_status=86
    put_once "$evidence/incidence-tree.bin" incidence-tree.bin || publish_status=86
    put_once "$evidence/tree-receipt.json" tree-receipt.json || publish_status=86
    else
    python3 - "$evidence/ATTEMPT_COMPLETE.json" "$run_id" "$source_commit" "$source_sha256" "$spot_price" "$evidence/posting-receipt.json" "$evidence/incidence-postings-one.bin" "$evidence/incidence-postings-two.bin" "$binary" <<'PY'
import hashlib,json,os,sys
path,run_id,commit,archive_sha,price,receipt,one,two,binary=sys.argv[1:]
def identity(source):
    digest=hashlib.sha256()
    with open(source,"rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""): digest.update(chunk)
    return {{"encoded_bytes":os.path.getsize(source),"sha256":digest.hexdigest()}}
value={{"binary":identity(binary),"claim_eligible":False,"incidence_postings_one":identity(one),"incidence_postings_two":identity(two),"phase":"posting-construction","purchase_option":"spot","receipt":identity(receipt),"run_id":run_id,"schema":"borsuk-v23-incidence-attempt-complete-v1","source_archive_sha256":archive_sha,"source_commit":commit,"spot_price_usd_per_hour":price,"status":"complete"}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\\n")
PY
    primary_evidence_attempted=1
    put_once "$evidence/binary.json" binary.json || publish_status=86
    put_once "$binary" incidence-executable || publish_status=86
    put_once "$evidence/preflight-receipt.json" preflight-receipt.json || publish_status=86
    put_once "$evidence/progress.json" progress.json || publish_status=86
    put_once "$evidence/incidence-postings-one.bin" incidence-postings-one.bin || publish_status=86
    put_once "$evidence/incidence-postings-two.bin" incidence-postings-two.bin || publish_status=86
    put_once "$evidence/posting-receipt.json" posting-receipt.json || publish_status=86
    fi
  else
    publish_status="$status"
    [[ "$publish_status" -ne 0 ]] || publish_status=87
  fi
  [[ -f "$evidence/spot-interruption.json" ]] && put_once "$evidence/spot-interruption.json" spot-interruption.json || true
  [[ -f "$evidence/interruption-monitor-failed.json" ]] && put_once "$evidence/interruption-monitor-failed.json" interruption-monitor-failed.json || true
  for evidence_name in binary.json preflight-receipt.json preflight-staging-receipt.json execute-staging-receipt.json progress.json phase.log phase-journal.txt phase-traceback.txt phase-failure.json; do
    if [[ "$primary_evidence_attempted" -eq 1 ]]; then
      case "$evidence_name" in
        binary.json|preflight-receipt.json|progress.json) continue ;;
      esac
    fi
    [[ -f "$evidence/$evidence_name" ]] && put_once "$evidence/$evidence_name" "$evidence_name" || true
  done
  [[ -f "$worker_log" ]] && put_once "$worker_log" worker.log || true
  if [[ "$publish_status" -eq 0 ]]; then
    complete_attempted=1
    put_once "$evidence/ATTEMPT_COMPLETE.json" ATTEMPT_COMPLETE.json || publish_status=86
  fi
  if [[ -f "$evidence/spot-interruption.json" && "$complete_attempted" -eq 0 ]]; then
    python3 - "$evidence/ATTEMPT_INTERRUPTED.json" "$run_id" "$source_commit" "$source_sha256" "$phase" <<'PY'
import json,sys
path,run_id,commit,archive_sha,phase=sys.argv[1:]
value={{"claim_eligible":False,"phase":phase,"run_id":run_id,"schema":"borsuk-v23-incidence-attempt-interrupted-v1","source_archive_sha256":archive_sha,"source_commit":commit,"status":"interrupted"}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\\n")
PY
    put_once "$evidence/ATTEMPT_INTERRUPTED.json" ATTEMPT_INTERRUPTED.json || true
  elif [[ "$publish_status" -ne 0 && "$complete_attempted" -eq 0 ]]; then
    python3 - "$evidence/ATTEMPT_FAILED.json" "$run_id" "$source_commit" "$source_sha256" "$publish_status" "$phase" <<'PY'
import json,sys
path,run_id,commit,archive_sha,status,phase=sys.argv[1:]
value={{"claim_eligible":False,"phase":phase,"run_id":run_id,"schema":"borsuk-v23-incidence-attempt-failed-v1","source_archive_sha256":archive_sha,"source_commit":commit,"status":"failed","worker_exit":int(status)}}
open(path,"wb").write(json.dumps(value,sort_keys=True,separators=(",", ":")).encode()+b"\\n")
PY
    put_once "$evidence/ATTEMPT_FAILED.json" ATTEMPT_FAILED.json || true
  fi
  if [[ "$phase" == "posting-construction" ]]; then
    rm -f "$posting_tree_receipt" "$posting_roster" "$posting_manifest"
    rmdir "$posting_bootstrap" 2>/dev/null || true
  fi
  rm -f "$archive"
  shutdown -h now
  exit "$publish_status"
}}
trap finish EXIT
shutdown --poweroff +360

watch_spot_interruption() {{
  local action
  while sleep 2; do
    if action="$(curl --fail --silent --header "X-aws-ec2-metadata-token: $interruption_token" http://169.254.169.254/latest/meta-data/spot/instance-action 2>/dev/null)"; then
      printf '%s\n' "$action" >"$evidence/spot-interruption.json"
      systemctl stop "$unit" || true
      return
    fi
  done
}}

dnf install -y gcc gcc-c++ make cmake perl pkgconf-pkg-config openssl-devel clang python3 python3-pip util-linux git tar gzip
aws s3api put-object --generate-cli-skeleton input | python3 -c 'import json,sys; value=json.load(sys.stdin); assert "IfNoneMatch" in value'
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.91.0
python3 -m pip install uv==0.8.17
aws s3 cp "$source_uri" "$archive" --only-show-errors
test "$(sha256sum "$archive" | awk '{{print $1}}')" = "$source_sha256"
mkdir -p "$workspace"
tar -xf "$archive" -C "$workspace"
cd "$workspace"
test "$(cat .borsuk-source-commit)" = "$source_commit"
export CARGO_BUILD_JOBS=32 CARGO_INCREMENTAL=0
cargo build --locked --release -p borsuk --example v23_leaf_page_incidence_falsifier
binary="$(find target -type f -path '*/release/examples/v23_leaf_page_incidence_falsifier' -perm -0100 -print -quit 2>/dev/null || true)"
test -n "$binary"
binary_sha256="$(sha256sum "$binary" | awk '{{print $1}}')"
binary_bytes="$(stat -c %s "$binary")"
printf '{{"binary_bytes":%s,"binary_sha256":"%s","schema":"borsuk-v23-incidence-binary-v1","source_commit":"%s"}}\n' "$binary_bytes" "$binary_sha256" "$source_commit" >"$evidence/binary.json"
"$(command -v uv)" python install 3.12
"$(command -v uv)" venv --python 3.12 /opt/borsuk-incidence-venv
"$(command -v uv)" pip install --python /opt/borsuk-incidence-venv/bin/python --requirement scripts/requirements-format-bench.txt
/opt/borsuk-incidence-venv/bin/python scripts/launch_v23_incidence_spot.py --offline-probe

worker_mode=(--worker-tree)
if [[ "$phase" == "posting-construction" ]]; then
  mkdir "$posting_bootstrap"
  aws s3 cp {shlex.quote(FROZEN_TREE_RECEIPT_URI)} "$posting_tree_receipt" --only-show-errors
  test "$(stat -c %s "$posting_tree_receipt")" -eq {FROZEN_TREE_RECEIPT_BYTES}
  test "$(sha256sum "$posting_tree_receipt" | awk '{{print $1}}')" = {FROZEN_TREE_RECEIPT_SHA256}
  aws s3 cp {shlex.quote(FROZEN_PAGE_ROSTER_URI)} "$posting_roster" --only-show-errors
  test "$(stat -c %s "$posting_roster")" -eq {FROZEN_PAGE_ROSTER_BYTES}
  test "$(sha256sum "$posting_roster" | awk '{{print $1}}')" = {FROZEN_PAGE_ROSTER_SHA256}
  /opt/borsuk-incidence-venv/bin/python scripts/launch_v23_incidence_spot.py --build-posting-manifest \
    --tree-receipt "$posting_tree_receipt" --page-roster "$posting_roster" \
    --posting-manifest-output "$posting_manifest"
  worker_mode=(--worker-posting --posting-manifest "$posting_manifest")
fi

if ! interruption_token="$(curl --fail --silent --request PUT --header 'X-aws-ec2-metadata-token-ttl-seconds: 21600' http://169.254.169.254/latest/api/token)"; then
  printf '{{"schema":"borsuk-v23-incidence-interruption-monitor-failed-v1"}}\n' >"$evidence/interruption-monitor-failed.json"
  false
fi
watch_spot_interruption & interruption_watcher=$!
set +e
systemd-run --wait --collect --unit="$unit" \
  --property=MemoryMax=3G --property=MemorySwapMax=0 --property=RuntimeMaxSec=16200 \
  --property=StandardOutput=append:$phase_log --property=StandardError=append:$phase_log \
  --working-directory="$workspace" --setenv=PYTHONPATH="$workspace" --setenv=TMPDIR="$scratch_root" \
  --setenv=AWS_REGION={REGION} --setenv=AWS_DEFAULT_REGION={REGION} \
  /opt/borsuk-incidence-venv/bin/python scripts/launch_v23_incidence_spot.py "${{worker_mode[@]}}" \
  --binary "$binary" --binary-sha256 "$binary_sha256" \
  --evidence-directory "$evidence" --output-uri-prefix "$result_uri"
phase_status=$?
journalctl --no-pager -o short-iso -u "$unit" >"$phase_journal" 2>&1 || true
[[ -f "$phase_log" ]] && sync "$phase_log" || true
set -e
kill "$interruption_watcher" 2>/dev/null || true
wait "$interruption_watcher" 2>/dev/null || true
test "$phase_status" -eq 0
"""


def _run(command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, capture_output=True, text=True, **kwargs)


def _aws(arguments: Sequence[str]) -> str:
    return _run(
        ["aws", "--profile", PROFILE, "--region", REGION, *arguments]
    ).stdout.strip()


def _git_source_commit() -> str:
    override = os.environ.get("BORSUK_SOURCE_COMMIT")
    if override is not None:
        return _require_source_commit(override)
    return _run(["git", "rev-parse", "HEAD"], cwd=ROOT).stdout.strip()


def _assert_clean_pushed_source(source_commit: str) -> None:
    if _run(["git", "status", "--porcelain"], cwd=ROOT).stdout:
        raise RuntimeError("launch requires a clean source tree")
    _run(["git", "fetch", "--quiet", "origin", "main"], cwd=ROOT)
    remote = _run(["git", "rev-parse", "origin/main"], cwd=ROOT).stdout.strip()
    if source_commit != remote:
        raise RuntimeError("launch source must equal origin/main")


def _build_source_archive(source_commit: str, archive: pathlib.Path) -> str:
    _require_source_commit(source_commit)
    if not archive.is_absolute() or archive.exists():
        raise ValueError("source archive path differs")
    _run(
        [
            "git",
            "archive",
            "--format=tar",
            f"--add-virtual-file=.borsuk-source-commit:{source_commit}",
            source_commit,
            "-o",
            str(archive),
        ],
        cwd=ROOT,
    )
    return hashlib.sha256(archive.read_bytes()).hexdigest()


def _upload_source(source_commit: str, run_id: str) -> tuple[str, str]:
    with tempfile.NamedTemporaryFile(
        prefix="borsuk-v23-incidence-source-", suffix=".tar", delete=False
    ) as archive:
        archive_path = pathlib.Path(archive.name)
    archive_path.unlink()
    try:
        digest = _build_source_archive(source_commit, archive_path)
        length = archive_path.stat().st_size
        checksum_base64 = base64.b64encode(bytes.fromhex(digest)).decode()
        key = f"research/v23-leaf-page-incidence/source/{digest}.tar"
        uri = f"s3://{BUCKET}/{key}"
        try:
            _aws(
                [
                    "s3api",
                    "put-object",
                    "--bucket",
                    BUCKET,
                    "--key",
                    key,
                    "--body",
                    str(archive_path),
                    "--expected-bucket-owner",
                    EXPECTED_AWS_ACCOUNT,
                    "--checksum-algorithm",
                    "SHA256",
                    "--checksum-sha256",
                    checksum_base64,
                    "--metadata",
                    f"borsuk-sha256={digest}",
                    "--if-none-match",
                    "*",
                ]
            )
        except subprocess.CalledProcessError:
            observed = json.loads(
                _aws(
                [
                    "s3api",
                    "head-object",
                    "--bucket",
                    BUCKET,
                    "--key",
                    key,
                    "--expected-bucket-owner",
                    EXPECTED_AWS_ACCOUNT,
                    "--checksum-mode",
                    "ENABLED",
                    "--query",
                    "{ContentLength:ContentLength,ChecksumSHA256:ChecksumSHA256,Metadata:Metadata}",
                    "--output",
                    "json",
                ]
            )
            )
            if (
                observed
                != {
                    "ContentLength": length,
                    "ChecksumSHA256": checksum_base64,
                    "Metadata": {"borsuk-sha256": digest},
                }
            ):
                raise RuntimeError("existing source archive authority differs") from None
        return uri, digest
    finally:
        if archive_path.exists():
            archive_path.unlink()


def _spot_price() -> str:
    zone = _aws(
        [
            "ec2",
            "describe-subnets",
            "--subnet-ids",
            SUBNET_ID,
            "--query",
            "Subnets[0].AvailabilityZone",
            "--output",
            "text",
        ]
    )
    return _aws(
        [
            "ec2",
            "describe-spot-price-history",
            "--instance-types",
            INSTANCE_TYPE,
            "--product-descriptions",
            "Linux/UNIX",
            "--availability-zone",
            zone,
            "--start-time",
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "--max-items",
            "1",
            "--query",
            "SpotPriceHistory[0].SpotPrice",
            "--output",
            "text",
        ]
    )


def _maximum_compute_cost(spot_price_usd_per_hour: str) -> decimal.Decimal:
    try:
        price = decimal.Decimal(spot_price_usd_per_hour)
    except decimal.InvalidOperation as error:
        raise ValueError("Spot price differs") from error
    if not price.is_finite() or price <= 0:
        raise ValueError("Spot price differs")
    return price * decimal.Decimal(INSTANCE_WALL_STOP_SECONDS) / decimal.Decimal(3600)


def _validate_terminal_bytes(
    raw: bytes,
    run_id: str,
    source_commit: str,
    phase: str,
) -> str:
    if phase not in SUPPORTED_PHASES:
        raise ValueError("attempt terminal phase differs")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("attempt terminal JSON differs") from error
    if raw != _canonical_bytes(value) or type(value) is not dict:  # noqa: E721
        raise ValueError("attempt terminal canonical bytes differ")
    common = {
        "claim_eligible": False,
        "phase": phase,
        "run_id": run_id,
        "source_commit": source_commit,
    }
    if any(value.get(key) != expected for key, expected in common.items()):
        raise ValueError("attempt terminal authority differs")
    status = value.get("status")
    if status == "failed":
        if (
            set(value)
            != {
                "claim_eligible",
                "phase",
                "run_id",
                "schema",
                "source_archive_sha256",
                "source_commit",
                "status",
                "worker_exit",
            }
            or value["schema"] != "borsuk-v23-incidence-attempt-failed-v1"
            or LOWER_SHA256.fullmatch(value.get("source_archive_sha256", "")) is None
            or type(value.get("worker_exit")) is not int  # noqa: E721
        ):
            raise ValueError("failed terminal authority differs")
        return status
    if status == "interrupted":
        if (
            set(value)
            != {
                "claim_eligible",
                "phase",
                "run_id",
                "schema",
                "source_archive_sha256",
                "source_commit",
                "status",
            }
            or value["schema"] != "borsuk-v23-incidence-attempt-interrupted-v1"
            or LOWER_SHA256.fullmatch(value.get("source_archive_sha256", "")) is None
        ):
            raise ValueError("interrupted terminal authority differs")
        return status
    if status == "complete":
        artifact_roles = (
            ("incidence_tree",)
            if phase == "tree-training"
            else ("incidence_postings_one", "incidence_postings_two")
        )
        if (
            set(value)
            != {
                "binary",
                "claim_eligible",
                "phase",
                "purchase_option",
                "receipt",
                "run_id",
                "schema",
                "source_archive_sha256",
                "source_commit",
                "spot_price_usd_per_hour",
                "status",
            }
            | set(artifact_roles)
            or value["schema"] != "borsuk-v23-incidence-attempt-complete-v1"
            or value["purchase_option"] != "spot"
            or LOWER_SHA256.fullmatch(value.get("source_archive_sha256", "")) is None
        ):
            raise ValueError("complete terminal authority differs")
        for role in ("binary", *artifact_roles, "receipt"):
            identity = value.get(role)
            if (
                type(identity) is not dict  # noqa: E721
                or set(identity) != {"encoded_bytes", "sha256"}
                or type(identity["encoded_bytes"]) is not int  # noqa: E721
                or identity["encoded_bytes"] <= 0
                or LOWER_SHA256.fullmatch(identity["sha256"]) is None
            ):
                raise ValueError("complete terminal object identity differs")
        try:
            if type(value["spot_price_usd_per_hour"]) is not str:  # noqa: E721
                raise ValueError
            _maximum_compute_cost(value["spot_price_usd_per_hour"])
        except (TypeError, ValueError) as error:
            raise ValueError("complete terminal Spot price differs") from error
        return status
    raise ValueError("attempt terminal status differs")


def _terminate_instance(instance_id: str) -> None:
    _aws(
        [
            "ec2",
            "terminate-instances",
            "--instance-ids",
            instance_id,
            "--output",
            "json",
        ]
    )


def monitor_attempt(
    instance_id: str,
    result_uri: str,
    run_id: str,
    source_commit: str,
    phase: str,
    *,
    poll_seconds: float = 15.0,
    wall_seconds: float = 21_600.0,
) -> str:
    """Observe only terminal markers and instance state, then terminate once."""

    if not instance_id.startswith("i-") or not result_uri.startswith(f"s3://{BUCKET}/"):
        raise ValueError("attempt monitor authority differs")
    prefix = result_uri.removeprefix(f"s3://{BUCKET}/").rstrip("/")
    started = time.monotonic()
    while time.monotonic() - started < wall_seconds:
        for marker, expected in (
            ("ATTEMPT_COMPLETE.json", "complete"),
            ("ATTEMPT_INTERRUPTED.json", "interrupted"),
            ("ATTEMPT_FAILED.json", "failed"),
        ):
            key = f"{prefix}/{marker}"
            head = subprocess.run(
                [
                    "aws",
                    "--profile",
                    PROFILE,
                    "--region",
                    REGION,
                    "s3api",
                    "head-object",
                    "--bucket",
                    BUCKET,
                    "--key",
                    key,
                    "--expected-bucket-owner",
                    EXPECTED_AWS_ACCOUNT,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if head.returncode != 0:
                continue
            with tempfile.NamedTemporaryFile(delete=False) as handle:
                terminal_path = pathlib.Path(handle.name)
            try:
                _aws(
                    [
                        "s3api",
                        "get-object",
                        "--bucket",
                        BUCKET,
                        "--key",
                        key,
                        "--expected-bucket-owner",
                        EXPECTED_AWS_ACCOUNT,
                        str(terminal_path),
                    ]
                )
                observed = _validate_terminal_bytes(
                    terminal_path.read_bytes(), run_id, source_commit, phase
                )
            finally:
                terminal_path.unlink(missing_ok=True)
            if observed != expected:
                raise RuntimeError("attempt terminal filename differs")
            _terminate_instance(instance_id)
            return observed
        state = _aws(
            [
                "ec2",
                "describe-instances",
                "--instance-ids",
                instance_id,
                "--query",
                "Reservations[0].Instances[0].State.Name",
                "--output",
                "text",
            ]
        )
        if state in {"shutting-down", "terminated", "stopped", "stopping"}:
            raise RuntimeError(f"Spot worker exited without terminal marker: {state}")
        time.sleep(poll_seconds)
    _terminate_instance(instance_id)
    raise TimeoutError("V23 incidence attempt exceeded the controller wall cap")


def launch(phase: str, run_id: str, source_commit: str) -> tuple[str, str]:
    """Launch exactly one worker and return its instance ID and result URI."""

    build_launch_plan(phase=phase, run_id=run_id, source_commit=source_commit)
    _assert_clean_pushed_source(source_commit)
    account = _aws(["sts", "get-caller-identity", "--query", "Account", "--output", "text"])
    if account != EXPECTED_AWS_ACCOUNT:
        raise RuntimeError("AWS account differs")
    active = _aws(
        [
            "ec2",
            "describe-instances",
            "--filters",
            "Name=instance-state-name,Values=pending,running",
            "Name=tag:Name,Values=borsuk-*",
            "--query",
            "Reservations[].Instances[].InstanceId",
            "--output",
            "text",
        ]
    )
    if active and active != "None":
        raise RuntimeError(f"another V23 incidence worker is active: {active}")
    source_uri, source_sha = _upload_source(source_commit, run_id)
    result_uri = (
        f"s3://{BUCKET}/research/v23-leaf-page-incidence/"
        f"{source_sha}/{run_id}"
    )
    _run(
        [
            "python3",
            "scripts/benchmark_s3.py",
            "assert-empty",
            "--profile",
            PROFILE,
            "--uri",
            result_uri,
        ],
        cwd=ROOT,
    )
    price = _spot_price()
    try:
        projected_compute_cost = _maximum_compute_cost(price)
    except ValueError as error:
        raise RuntimeError("Spot price differs") from error
    if projected_compute_cost > decimal.Decimal("5.0"):
        raise RuntimeError(
            f"projected Spot compute cost exceeds $5 ceiling: {projected_compute_cost:.2f}"
        )
    worker = build_worker_script(
        phase=phase,
        run_id=run_id,
        source_commit=source_commit,
        source_uri=source_uri,
        source_sha256=source_sha,
        result_uri=result_uri,
        spot_price_usd_per_hour=price,
    )
    spec = build_launch_spec(
        phase=phase,
        run_id=run_id,
        source_commit=source_commit,
        user_data=worker,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
        json.dump(spec, handle, separators=(",", ":"), sort_keys=True)
        handle.flush()
        instance_id = _aws(
            [
                "ec2",
                "run-instances",
                "--cli-input-json",
                f"file://{handle.name}",
                "--query",
                "Instances[0].InstanceId",
                "--output",
                "text",
            ]
        )
    if not instance_id.startswith("i-"):
        raise RuntimeError("Spot launch returned no instance")
    print(
        _canonical_bytes(
            {
                "instance_id": instance_id,
                "result_uri": result_uri,
                "run_id": run_id,
                "source_archive_sha256": source_sha,
                "source_commit": source_commit,
                "spot_price_usd_per_hour": price,
                "status": "launched",
            }
        ).decode(),
        end="",
    )
    return instance_id, result_uri


def _identity(role: str, path: pathlib.Path, uri: str) -> Any:
    from scripts.run_v23_leaf_page_incidence_falsifier import AuthenticatedInput

    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    return AuthenticatedInput(
        role=role,
        source=path,
        uri=uri,
        digest_algorithm="sha256",
        digest=digest,
        encoded_bytes=len(raw),
        generation=f"unversioned-sha256:{digest}",
    )


def _write_bulk_manifest(source: pathlib.Path, target: pathlib.Path, full: bool) -> None:
    value = json.loads(source.read_bytes())
    if not full:
        if value.get("phase") == "tree-training":
            value["ordered_inputs"] = [value["ordered_inputs"][1]]
        elif value.get("phase") == "posting-construction":
            value["ordered_inputs"] = value["ordered_inputs"][: 3 + 256]
        else:
            raise ValueError("preflight bulk manifest phase differs")
    target.write_bytes(_canonical_bytes(value))


def _validate_tree_progress_binding(
    receipt: dict[str, object], progress_path: pathlib.Path
) -> None:
    _validate_phase_progress_binding(receipt, progress_path, "tree-training")


def _validate_phase_progress_binding(
    receipt: dict[str, object], progress_path: pathlib.Path, phase: str
) -> None:
    from scripts.run_v23_leaf_page_incidence_falsifier import (
        AuthenticatedProgressMonitor,
    )

    if progress_path.is_symlink() or not progress_path.is_file():
        raise ValueError(f"{phase} progress binding differs")
    raw = progress_path.read_bytes()
    try:
        _, completed_units, _ = AuthenticatedProgressMonitor(phase).observe(raw)
        final_record = json.loads(raw.splitlines()[-1])
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"{phase} progress binding differs") from error
    if (
        final_record["sequence"] <= 0
        or completed_units != final_record["total_units"]
        or receipt.get("final_progress_sha256") != hashlib.sha256(raw).hexdigest()
    ):
        raise ValueError(f"{phase} progress binding differs")


def _rewrite_tree_receipt_uri(
    receipt_path: pathlib.Path, tree_path: pathlib.Path, output_uri: str
) -> None:
    import blake3

    if (
        receipt_path.is_symlink()
        or not receipt_path.is_file()
        or tree_path.is_symlink()
        or not tree_path.is_file()
        or re.fullmatch(r"s3://[A-Za-z0-9._-]+/[A-Za-z0-9._/-]+/incidence-tree\.bin", output_uri)
        is None
    ):
        raise ValueError("tree handoff output URI differs")
    raw = receipt_path.read_bytes()
    value = json.loads(raw)
    if raw != _canonical_bytes(value) or type(value) is not dict:  # noqa: E721
        raise ValueError("tree receipt canonical bytes differ")
    outputs = value.get("outputs")
    if type(outputs) is not list or len(outputs) != 1 or type(outputs[0]) is not dict:  # noqa: E721
        raise ValueError("tree receipt output authority differs")
    identity = outputs[0]
    tree_bytes = tree_path.read_bytes()
    if (
        identity.get("role") != "incidence-tree"
        or identity.get("digest_algorithm") != "blake3"
        or identity.get("encoded_bytes") != len(tree_bytes)
        or identity.get("digest") != blake3.blake3(tree_bytes).hexdigest()
        or identity.get("uri") != f"file://{tree_path}"
    ):
        raise ValueError("tree receipt output URI or bytes differ")
    identity["uri"] = output_uri
    temporary = receipt_path.with_name(f".{receipt_path.name}.rewrite")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        rewritten = _canonical_bytes(value)
        offset = 0
        while offset < len(rewritten):
            written = os.write(descriptor, rewritten[offset:])
            if written <= 0:
                raise OSError("tree receipt rewrite made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, receipt_path)


def _rewrite_posting_receipt_uris(
    receipt_path: pathlib.Path,
    output_directory: pathlib.Path,
    output_uri_prefix: str,
) -> dict[str, pathlib.Path]:
    import blake3

    if (
        receipt_path.is_symlink()
        or not receipt_path.is_file()
        or output_directory.is_symlink()
        or not output_directory.is_dir()
        or re.fullmatch(r"s3://[A-Za-z0-9._-]+/[A-Za-z0-9._/-]+", output_uri_prefix)
        is None
    ):
        raise ValueError("posting output URI or bytes differ")
    raw = receipt_path.read_bytes()
    value = json.loads(raw)
    outputs = value.get("outputs") if type(value) is dict else None
    roles = ("incidence-postings-one", "incidence-postings-two")
    if (
        raw != _canonical_bytes(value)
        or type(outputs) is not list
        or len(outputs) != len(roles)
        or [item.get("role") if type(item) is dict else None for item in outputs]
        != list(roles)
    ):
        raise ValueError("posting output URI or bytes differ")
    paths: dict[str, pathlib.Path] = {}
    for role, identity in zip(roles, outputs, strict=True):
        path = pathlib.Path(str(identity.get("uri", "")).removeprefix("file://"))
        digest = blake3.blake3()
        if (
            not str(identity.get("uri", "")).startswith("file://")
            or path.parent != output_directory
            or path.is_symlink()
            or not path.is_file()
        ):
            raise ValueError("posting output URI or bytes differ")
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        observed_digest = digest.hexdigest()
        if (
            set(identity)
            != {
                "digest",
                "digest_algorithm",
                "encoded_bytes",
                "generation",
                "role",
                "uri",
            }
            or identity["digest_algorithm"] != "blake3"
            or identity["digest"] != observed_digest
            or identity["encoded_bytes"] != path.stat().st_size
            or identity["generation"] != f"content-{observed_digest}"
        ):
            raise ValueError("posting output URI or bytes differ")
        identity["uri"] = f"{output_uri_prefix.rstrip('/')}/{role}.bin"
        paths[role] = path
    temporary = receipt_path.with_name(f".{receipt_path.name}.rewrite")
    _write_exclusive(temporary, _canonical_bytes(value))
    os.replace(temporary, receipt_path)
    return paths


def _stage(manifest: pathlib.Path, directory: pathlib.Path, receipt: pathlib.Path) -> None:
    import boto3

    from scripts.stage_v23_leaf_page_incidence_inputs import stage_manifest

    client = boto3.client("s3", region_name=REGION)
    stage_manifest(manifest, directory, receipt, client)


def _phase_policy(
    *,
    phase: str,
    binary: pathlib.Path,
    binary_sha256: str,
    manifest: pathlib.Path,
    bulk_manifest: pathlib.Path,
    staging: pathlib.Path,
    staging_receipt: pathlib.Path,
    scratch: pathlib.Path,
    output: pathlib.Path,
    preflight_receipt: pathlib.Path | None,
) -> Any:
    from scripts.run_v23_leaf_page_incidence_falsifier import (
        AuthenticatedDirectory,
        OfflinePhasePolicy,
        build_phase_argv,
    )

    manifest_raw = manifest.read_bytes()
    manifest_value = json.loads(manifest_raw)
    parent_receipt_sha256 = manifest_value.get("parent_receipt_sha256")
    if (
        manifest_raw != _canonical_bytes(manifest_value)
        or manifest_value.get("phase") != phase
        or (
            phase == "tree-training"
            and parent_receipt_sha256 is not None
        )
        or (
            phase != "tree-training"
            and (
                type(parent_receipt_sha256) is not str
                or LOWER_SHA256.fullmatch(parent_receipt_sha256) is None
            )
        )
    ):
        raise ValueError("phase manifest authority differs")
    manifest_role = (
        "construction-manifest" if phase == "tree-training" else "phase-manifest"
    )
    manifest_uri = (
        f"git://borsuk/{MANIFEST_RELATIVE}"
        if phase == "tree-training"
        else f"file://{manifest}"
    )
    inputs = [
        _identity(manifest_role, manifest, manifest_uri),
        _identity("bulk-manifest", bulk_manifest, f"file://{bulk_manifest}"),
        _identity("staging-receipt", staging_receipt, f"file://{staging_receipt}"),
    ]
    if preflight_receipt is not None:
        inputs.append(
            _identity("preflight-receipt", preflight_receipt, f"file://{preflight_receipt}")
        )
    binary_bytes = binary.read_bytes()
    if hashlib.sha256(binary_bytes).hexdigest() != binary_sha256:
        raise ValueError("worker binary authority differs")
    policy = OfflinePhasePolicy(
        phase=phase,
        executable=binary,
        executable_sha256=binary_sha256,
        executable_bytes=len(binary_bytes),
        inputs=tuple(inputs),
        scratch=scratch,
        output=output,
        parent_receipt_sha256=parent_receipt_sha256,
        directory_capabilities=(
            AuthenticatedDirectory(
                role="bulk-inputs",
                source=staging,
                manifest_role="bulk-manifest",
                staging_receipt_role="staging-receipt",
            ),
        ),
        phase_argv=(),
    )
    return dataclasses.replace(policy, phase_argv=build_phase_argv(policy))


def _write_exclusive(path: pathlib.Path, payload: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("exclusive evidence write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _preserve_worker_failure(
    evidence: pathlib.Path,
    phase: str,
    stage: str,
    error: BaseException,
    partial_receipts: Sequence[tuple[pathlib.Path, str]],
) -> None:
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    preservation_errors: list[str] = []
    failure_artifacts = (
        (
            evidence / "phase-traceback.txt",
            traceback.format_exc().encode("utf-8"),
        ),
        (
            evidence / "phase-failure.json",
            _canonical_bytes(
                {
                    "claim_eligible": False,
                    "exception_type": type(error).__name__,
                    "message": str(error),
                    "phase": phase,
                    "stage": stage,
                }
            ),
        ),
    )
    for target, payload in failure_artifacts:
        try:
            _write_exclusive(target, payload)
        except BaseException as preservation_error:
            preservation_errors.append(
                f"{target.name}: {type(preservation_error).__name__}: "
                f"{preservation_error}"
            )
    for source, name in partial_receipts:
        try:
            if not source.exists():
                continue
            if source.is_symlink() or not source.is_file():
                raise ValueError("partial receipt evidence differs")
            payload = source.read_bytes()
            target = evidence / name
            if target.exists():
                if (
                    target.is_symlink()
                    or not target.is_file()
                    or target.read_bytes() != payload
                ):
                    raise ValueError("partial receipt evidence differs")
                continue
            _write_exclusive(target, payload)
        except BaseException as preservation_error:
            preservation_errors.append(
                f"{name}: {type(preservation_error).__name__}: {preservation_error}"
            )
    if preservation_errors:
        raise RuntimeError(
            "worker failure evidence preservation failed: "
            + "; ".join(preservation_errors)
        )


def worker_tree(
    binary: pathlib.Path,
    binary_sha256: str,
    evidence: pathlib.Path,
    output_uri_prefix: str,
) -> int:
    """Stage and execute one preflight followed by one tree-training phase."""

    from scripts.run_v23_leaf_page_incidence_falsifier import MonitorLimits, run_phase

    manifest = (ROOT / MANIFEST_RELATIVE).resolve()
    root = pathlib.Path(tempfile.mkdtemp(prefix="v23-incidence-tree-"))
    known = (
        "preflight-manifest.json",
        "execute-manifest.json",
        "preflight-staging-receipt.json",
        "execute-staging-receipt.json",
    )
    preflight_staging = root / "preflight-inputs"
    execute_staging = root / "execute-inputs"
    preflight_scratch = root / "preflight-scratch"
    execute_scratch = root / "execute-scratch"
    preflight_output = root / "preflight-output"
    execute_output = root / "execute-output"
    stage = "initialization"
    preflight_manifest = root / known[0]
    execute_manifest = root / known[1]
    preflight_receipt = root / known[2]
    execute_receipt = root / known[3]
    phase_preflight_receipt = preflight_output / "receipt.json"
    execute_progress = execute_output / "progress.json"
    try:
        for path in (
            preflight_scratch,
            execute_scratch,
            preflight_output,
            execute_output,
        ):
            path.mkdir()
        stage = "preflight-manifest"
        _write_bulk_manifest(manifest, preflight_manifest, False)
        stage = "preflight-staging"
        _stage(preflight_manifest, preflight_staging, preflight_receipt)
        stage = "preflight-policy"
        preflight_policy = _phase_policy(
            phase="tree-training",
            binary=binary,
            binary_sha256=binary_sha256,
            manifest=manifest,
            bulk_manifest=preflight_manifest,
            staging=preflight_staging,
            staging_receipt=preflight_receipt,
            scratch=preflight_scratch,
            output=preflight_output,
            preflight_receipt=None,
        )
        stage = "preflight-run"
        preflight_status = run_phase(preflight_policy, MonitorLimits())
        if preflight_status != 0:
            raise RuntimeError(f"tree preflight failed with exit {preflight_status}")
        if not phase_preflight_receipt.is_file():
            raise RuntimeError("tree preflight receipt is absent")
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "preflight-receipt.json").write_bytes(
            phase_preflight_receipt.read_bytes()
        )
        stage = "execute-manifest"
        _write_bulk_manifest(manifest, execute_manifest, True)
        stage = "execute-staging"
        _stage(execute_manifest, execute_staging, execute_receipt)
        stage = "execute-policy"
        execute_policy = _phase_policy(
            phase="tree-training",
            binary=binary,
            binary_sha256=binary_sha256,
            manifest=manifest,
            bulk_manifest=execute_manifest,
            staging=execute_staging,
            staging_receipt=execute_receipt,
            scratch=execute_scratch,
            output=execute_output,
            preflight_receipt=phase_preflight_receipt,
        )
        stage = "execute-run"
        execute_status = run_phase(execute_policy, MonitorLimits())
        if execute_status != 0:
            raise RuntimeError(f"tree execution failed with exit {execute_status}")
        stage = "execute-receipt"
        final_receipt = execute_output / "receipt.json"
        if not final_receipt.is_file():
            raise RuntimeError("tree receipt is absent")
        if execute_progress.is_symlink() or not execute_progress.is_file():
            raise RuntimeError("tree progress is absent")
        receipt_value = json.loads(final_receipt.read_bytes())
        _validate_tree_progress_binding(receipt_value, execute_progress)
        _write_exclusive(evidence / "progress.json", execute_progress.read_bytes())
        outputs = receipt_value.get("outputs")
        if type(outputs) is not list or len(outputs) != 1:  # noqa: E721
            raise RuntimeError("tree receipt output authority differs")
        output_uri = outputs[0].get("uri") if type(outputs[0]) is dict else None
        if type(output_uri) is not str or not output_uri.startswith("file://"):  # noqa: E721
            raise RuntimeError("tree output URI differs")
        tree_path = pathlib.Path(output_uri.removeprefix("file://"))
        if tree_path.parent != execute_output or not tree_path.is_file():
            raise RuntimeError("tree output path differs")
        _rewrite_tree_receipt_uri(
            final_receipt,
            tree_path,
            f"{output_uri_prefix.rstrip('/')}/incidence-tree.bin",
        )
        os.replace(tree_path, evidence / "incidence-tree.bin")
        os.replace(final_receipt, evidence / "tree-receipt.json")
        return 0
    except BaseException as error:
        try:
            _preserve_worker_failure(
                evidence,
                "tree-training",
                stage,
                error,
                (
                    (preflight_receipt, "preflight-staging-receipt.json"),
                    (execute_receipt, "execute-staging-receipt.json"),
                    (phase_preflight_receipt, "preflight-receipt.json"),
                    (execute_progress, "progress.json"),
                ),
            )
        except BaseException:
            traceback.print_exc()
        raise
    finally:
        # Scientific cleanup is fail-closed: only the private mkdtemp tree is removed.
        import shutil

        shutil.rmtree(root)


def worker_posting(
    binary: pathlib.Path,
    binary_sha256: str,
    evidence: pathlib.Path,
    output_uri_prefix: str,
    manifest_bytes: bytes,
) -> int:
    """Stage and execute one monitored posting-construction phase."""

    from scripts.run_v23_leaf_page_incidence_falsifier import MonitorLimits, run_phase

    root = pathlib.Path(tempfile.mkdtemp(prefix="v23-incidence-posting-"))
    manifest = root / "phase-manifest.json"
    preflight_manifest = root / "preflight-manifest.json"
    execute_manifest = root / "execute-manifest.json"
    preflight_receipt = root / "preflight-staging-receipt.json"
    execute_receipt = root / "execute-staging-receipt.json"
    preflight_staging = root / "preflight-inputs"
    execute_staging = root / "execute-inputs"
    preflight_scratch = root / "preflight-scratch"
    execute_scratch = root / "execute-scratch"
    preflight_output = root / "preflight-output"
    execute_output = root / "execute-output"
    phase_preflight_receipt = preflight_output / "receipt.json"
    execute_progress = execute_output / "progress.json"
    stage = "initialization"
    try:
        _write_exclusive(manifest, manifest_bytes)
        manifest_value = json.loads(manifest_bytes)
        if (
            manifest_bytes != _canonical_bytes(manifest_value)
            or manifest_value.get("phase") != "posting-construction"
            or manifest_value.get("parent_receipt_sha256")
            != FROZEN_TREE_RECEIPT_SHA256
        ):
            raise ValueError("posting parent receipt authority differs")
        for path in (
            preflight_scratch,
            execute_scratch,
            preflight_output,
            execute_output,
        ):
            path.mkdir()
        stage = "preflight-manifest"
        _write_bulk_manifest(manifest, preflight_manifest, False)
        stage = "preflight-staging"
        _stage(preflight_manifest, preflight_staging, preflight_receipt)
        stage = "preflight-policy"
        preflight_policy = _phase_policy(
            phase="posting-construction",
            binary=binary,
            binary_sha256=binary_sha256,
            manifest=manifest,
            bulk_manifest=preflight_manifest,
            staging=preflight_staging,
            staging_receipt=preflight_receipt,
            scratch=preflight_scratch,
            output=preflight_output,
            preflight_receipt=None,
        )
        stage = "preflight-run"
        preflight_status = run_phase(preflight_policy, MonitorLimits())
        if preflight_status != 0:
            raise RuntimeError(f"posting preflight failed with exit {preflight_status}")
        if phase_preflight_receipt.is_symlink() or not phase_preflight_receipt.is_file():
            raise RuntimeError("posting preflight receipt is absent")
        evidence.mkdir(parents=True, exist_ok=True)
        _write_exclusive(
            evidence / "preflight-receipt.json",
            phase_preflight_receipt.read_bytes(),
        )
        stage = "execute-manifest"
        _write_bulk_manifest(manifest, execute_manifest, True)
        stage = "execute-staging"
        _stage(execute_manifest, execute_staging, execute_receipt)
        stage = "execute-policy"
        execute_policy = _phase_policy(
            phase="posting-construction",
            binary=binary,
            binary_sha256=binary_sha256,
            manifest=manifest,
            bulk_manifest=execute_manifest,
            staging=execute_staging,
            staging_receipt=execute_receipt,
            scratch=execute_scratch,
            output=execute_output,
            preflight_receipt=phase_preflight_receipt,
        )
        stage = "execute-run"
        execute_status = run_phase(execute_policy, MonitorLimits())
        if execute_status != 0:
            raise RuntimeError(f"posting execution failed with exit {execute_status}")
        stage = "execute-receipt"
        final_receipt = execute_output / "receipt.json"
        if final_receipt.is_symlink() or not final_receipt.is_file():
            raise RuntimeError("posting receipt is absent")
        receipt_value = json.loads(final_receipt.read_bytes())
        _validate_phase_progress_binding(
            receipt_value,
            execute_progress,
            "posting-construction",
        )
        paths = _rewrite_posting_receipt_uris(
            final_receipt,
            execute_output,
            output_uri_prefix,
        )
        _write_exclusive(evidence / "progress.json", execute_progress.read_bytes())
        for role, path in paths.items():
            os.replace(path, evidence / f"{role}.bin")
        os.replace(final_receipt, evidence / "posting-receipt.json")
        return 0
    except BaseException as error:
        try:
            _preserve_worker_failure(
                evidence,
                "posting-construction",
                stage,
                error,
                (
                    (preflight_receipt, "preflight-staging-receipt.json"),
                    (execute_receipt, "execute-staging-receipt.json"),
                    (phase_preflight_receipt, "preflight-receipt.json"),
                    (execute_progress, "progress.json"),
                ),
            )
        except BaseException:
            traceback.print_exc()
        raise
    finally:
        import shutil

        shutil.rmtree(root)


def offline_probe() -> int:
    """Prove memory-pressure visibility and an isolated network namespace."""

    if __package__:
        from scripts.run_v23_leaf_page_incidence_falsifier import (
            _memory_psi_full_avg10,
        )
    else:  # Direct ``python scripts/...`` execution.
        from run_v23_leaf_page_incidence_falsifier import _memory_psi_full_avg10

    _memory_psi_full_avg10(MEMORY_PSI_PATH)
    program = """
import socket
sock=socket.socket(); sock.settimeout(0.2)
try: sock.connect(('169.254.169.254',80))
except OSError: pass
else: raise RuntimeError('network namespace is not isolated')
"""
    completed = subprocess.run(
        [
            "unshare",
            "--net",
            "--pid",
            "--fork",
            sys.executable,
            "-c",
            program,
        ],
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("V23 incidence offline probe failed")
    return 0


def _build_posting_manifest_file(
    tree_receipt_path: pathlib.Path,
    roster_path: pathlib.Path,
    output_path: pathlib.Path,
) -> None:
    if (
        tree_receipt_path.is_symlink()
        or not tree_receipt_path.is_file()
        or roster_path.is_symlink()
        or not roster_path.is_file()
        or output_path.exists()
        or output_path.parent.is_symlink()
        or not output_path.parent.is_dir()
    ):
        raise ValueError("posting manifest file boundary differs")
    tree_receipt_bytes = tree_receipt_path.read_bytes()
    roster_bytes = roster_path.read_bytes()

    def identity(role: str, uri: str, raw: bytes) -> dict[str, object]:
        digest = hashlib.sha256(raw).hexdigest()
        return {
            "digest": digest,
            "digest_algorithm": "sha256",
            "encoded_bytes": len(raw),
            "generation": f"unversioned-sha256:{digest}",
            "role": role,
            "uri": uri,
        }

    manifest_bytes = build_posting_manifest(
        tree_receipt_bytes=tree_receipt_bytes,
        tree_receipt_identity=identity(
            "parent-receipt", FROZEN_TREE_RECEIPT_URI, tree_receipt_bytes
        ),
        roster_bytes=roster_bytes,
        roster_identity=identity("page-roster", FROZEN_PAGE_ROSTER_URI, roster_bytes),
    )
    _write_exclusive(output_path, manifest_bytes)


def parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--phase", choices=SUPPORTED_PHASES + BLOCKED_PHASES)
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    private = parser.add_mutually_exclusive_group()
    private.add_argument("--worker-tree", action="store_true")
    private.add_argument("--worker-posting", action="store_true")
    private.add_argument("--build-posting-manifest", action="store_true")
    private.add_argument("--offline-probe", action="store_true")
    parser.add_argument("--binary", type=pathlib.Path)
    parser.add_argument("--binary-sha256")
    parser.add_argument("--evidence-directory", type=pathlib.Path)
    parser.add_argument("--output-uri-prefix")
    parser.add_argument("--tree-receipt", type=pathlib.Path)
    parser.add_argument("--page-roster", type=pathlib.Path)
    parser.add_argument("--posting-manifest-output", type=pathlib.Path)
    parser.add_argument("--posting-manifest", type=pathlib.Path)
    parsed = parser.parse_args(arguments)
    if parsed.worker_tree:
        if any(
            (
                parsed.phase,
                parsed.run_id,
                parsed.dry_run,
                parsed.tree_receipt,
                parsed.page_roster,
                parsed.posting_manifest_output,
                parsed.posting_manifest,
            )
        ) or not all(
            (
                parsed.binary,
                parsed.binary_sha256,
                parsed.evidence_directory,
                parsed.output_uri_prefix,
            )
        ):
            parser.error("worker tree arguments differ")
    elif parsed.worker_posting:
        if any(
            (
                parsed.phase,
                parsed.run_id,
                parsed.dry_run,
                parsed.tree_receipt,
                parsed.page_roster,
                parsed.posting_manifest_output,
            )
        ) or not all(
            (
                parsed.binary,
                parsed.binary_sha256,
                parsed.evidence_directory,
                parsed.output_uri_prefix,
                parsed.posting_manifest,
            )
        ):
            parser.error("worker posting arguments differ")
    elif parsed.build_posting_manifest:
        if any(
            (
                parsed.phase,
                parsed.run_id,
                parsed.dry_run,
                parsed.binary,
                parsed.binary_sha256,
                parsed.evidence_directory,
                parsed.output_uri_prefix,
                parsed.posting_manifest,
            )
        ) or not all(
            (
                parsed.tree_receipt,
                parsed.page_roster,
                parsed.posting_manifest_output,
            )
        ):
            parser.error("posting manifest arguments differ")
    elif parsed.offline_probe:
        if any(
            (
                parsed.phase,
                parsed.run_id,
                parsed.dry_run,
                parsed.binary,
                parsed.binary_sha256,
                parsed.evidence_directory,
                parsed.output_uri_prefix,
                parsed.tree_receipt,
                parsed.page_roster,
                parsed.posting_manifest_output,
                parsed.posting_manifest,
            )
        ):
            parser.error("offline probe arguments differ")
    elif (
        not parsed.phase
        or not parsed.run_id
        or any(
            (
                parsed.binary,
                parsed.binary_sha256,
                parsed.evidence_directory,
                parsed.output_uri_prefix,
                parsed.tree_receipt,
                parsed.page_roster,
                parsed.posting_manifest_output,
                parsed.posting_manifest,
            )
        )
    ):
        parser.error("controller arguments differ")
    return parsed


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = parse_args(arguments)
    if parsed.worker_tree:
        return worker_tree(
            parsed.binary.resolve(),
            _require_sha256("binary SHA-256", parsed.binary_sha256),
            parsed.evidence_directory.resolve(),
            parsed.output_uri_prefix,
        )
    if parsed.worker_posting:
        if parsed.posting_manifest.is_symlink() or not parsed.posting_manifest.is_file():
            raise ValueError("posting manifest path differs")
        return worker_posting(
            parsed.binary.resolve(),
            _require_sha256("binary SHA-256", parsed.binary_sha256),
            parsed.evidence_directory.resolve(),
            parsed.output_uri_prefix,
            parsed.posting_manifest.read_bytes(),
        )
    if parsed.build_posting_manifest:
        _build_posting_manifest_file(
            parsed.tree_receipt.resolve(),
            parsed.page_roster.resolve(),
            parsed.posting_manifest_output.resolve(),
        )
        return 0
    if parsed.offline_probe:
        return offline_probe()
    source_commit = _git_source_commit()
    plan = build_launch_plan(
        phase=parsed.phase, run_id=parsed.run_id, source_commit=source_commit
    )
    if parsed.dry_run:
        sys.stdout.buffer.write(_canonical_bytes(plan))
        return 0
    instance_id, result_uri = launch(parsed.phase, parsed.run_id, source_commit)
    try:
        status = monitor_attempt(
            instance_id,
            result_uri,
            parsed.run_id,
            source_commit,
            parsed.phase,
        )
    except BaseException:
        _terminate_instance(instance_id)
        raise
    if status != "complete":
        raise RuntimeError(f"V23 incidence {parsed.phase} attempt failed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        traceback.print_exc()
        raise SystemExit(1) from error
