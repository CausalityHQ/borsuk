#!/usr/bin/env python3
"""Immutable Publication V3 build/runtime worker construction."""

from __future__ import annotations

import json
import shlex
import textwrap
from copy import deepcopy
from dataclasses import dataclass

try:
    from scripts.publication_v3_protocol import (
        build_schedule_document,
        validate_manifest,
    )
except ModuleNotFoundError:
    from publication_v3_protocol import (
        build_schedule_document,
        validate_manifest,
    )


@dataclass(frozen=True)
class ExecutionJob:
    cell: dict[str, object]
    role: str
    attempt: int
    cell_tag: str
    terminal_prefix: str
    complete_uri: str
    failed_uri: str
    index_uri: str

    @property
    def terminal_uri(self) -> str:
        return self.complete_uri

    @property
    def failure_uri(self) -> str:
        return self.failed_uri

    @classmethod
    def _new(
        cls, cell: dict[str, object], *, role: str, attempt: int
    ) -> "ExecutionJob":
        if role not in {"build", "runtime"} or not 0 < attempt <= 9_999:
            raise ValueError("execution job role or attempt is invalid")
        result_prefix = str(cell["result_prefix"]).rstrip("/")
        cell_id = str(cell["cell_id"])
        terminal_prefix = f"{result_prefix}/{role}/attempts/{attempt:04d}"
        marker = role.upper()
        return cls(
            cell=cell,
            role=role,
            attempt=attempt,
            cell_tag=f"{role}-{cell_id}",
            terminal_prefix=terminal_prefix,
            complete_uri=f"{terminal_prefix}/{marker}_TERMINAL_COMPLETE.json",
            failed_uri=f"{terminal_prefix}/{marker}_TERMINAL_FAILED.json",
            index_uri=str(cell["index_prefix"]),
        )

    @classmethod
    def build(cls, cell: dict[str, object], *, attempt: int) -> "ExecutionJob":
        return cls._new(cell, role="build", attempt=attempt)

    @classmethod
    def runtime(cls, cell: dict[str, object], *, attempt: int) -> "ExecutionJob":
        return cls._new(cell, role="runtime", attempt=attempt)


def qualification_cell(
    manifest: dict[str, object],
    *,
    dataset_id: str,
    workload_kind: str,
    build_attempt: int | None = None,
) -> dict[str, object]:
    """Select the canonical first BORSUK cell from a partially staged manifest."""

    schedule = build_schedule_document(validate_manifest(manifest))
    matches = [
        cell
        for cell in schedule["cells"]
        if cell["system"] == "borsuk"
        and cell["repetition_id"] == "r01"
        and cell["dataset"]["id"] == dataset_id
        and cell["workload"]["kind"] == workload_kind
    ]
    if len(matches) != 1:
        raise ValueError("qualification cell is not uniquely scheduled")
    result = matches[0]
    if build_attempt is None:
        return result
    if not 0 < build_attempt <= 9_999:
        raise ValueError("qualification build attempt is invalid")
    result = deepcopy(result)
    index_root, index_name = str(result["index_prefix"]).rsplit("/", 1)
    result["index_prefix"] = (
        f"{index_root}/build-attempts/{build_attempt:04d}/{index_name}"
    )
    return result


def _q(value: object) -> str:
    return shlex.quote(str(value))


def _j(value: object) -> str:
    return json.dumps(str(value))


def _common_prelude(
    *,
    source_revision: str,
    source_uri: str,
    source_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
    protocol_uri: str,
    protocol_sha256: str,
    terminal_prefix: str,
    complete_marker: str,
    failed_marker: str,
) -> str:
    return textwrap.dedent(
        f"""\
        set -euo pipefail
        export AWS_REGION=eu-central-1 AWS_DEFAULT_REGION=eu-central-1
        work=/var/lib/borsuk-publication
        mkdir -p "$work"
        complete=0
        stage=preflight
        detail_log="$work/worker.log"
        put_immutable() {{
          local path=$1 uri=$2 digest checksum bucket key
          digest=$(sha256sum "$path" | awk '{{print $1}}')
          checksum=$(openssl dgst -sha256 -binary "$path" | base64 -w0)
          bucket=${{uri#s3://}}; bucket=${{bucket%%/*}}; key=${{uri#s3://$bucket/}}
          aws s3api put-object --bucket "$bucket" --key "$key" --body "$path" \
            --expected-bucket-owner 453182569524 --server-side-encryption AES256 \
            --checksum-algorithm SHA256 --checksum-sha256 "$checksum" \
            --metadata "borsuk-sha256=$digest" --if-none-match '*' >/dev/null
        }}
        fail() {{
          status=$?
          trap - EXIT
          if [[ $complete -eq 0 ]]; then
            printf '{{"schema_version":1,"status":"failed","exit_code":%d,"stage":"%s"}}\n' "$status" "$stage" >"$work/failed.json" || true
            put_immutable "$work/failed.json" {_q(terminal_prefix + "/" + failed_marker)} || true
            failure_source="$detail_log"
            if [[ ! -s "$failure_source" ]]; then
              failure_source="$work/worker.log"
            fi
            if [[ -f "$failure_source" ]]; then
              tail -c 65536 "$failure_source" >"$work/FAILURE.log" || true
              if [[ -s "$work/FAILURE.log" ]]; then
                put_immutable "$work/FAILURE.log" {_q(terminal_prefix + "/FAILURE.log")} || true
              fi
            fi
          fi
          exit "$status"
        }}
        trap fail EXIT
        exec > >(tee -a "$work/worker.log") 2>&1
        source_uri={_q(source_uri)}
        manifest_uri={_q(manifest_uri)}
        protocol_uri={_q(protocol_uri)}
        aws s3 cp "$source_uri" "$work/source.tar.gz" --only-show-errors
        aws s3 cp "$manifest_uri" "$work/manifest.json" --only-show-errors
        aws s3 cp "$protocol_uri" "$work/protocol.json" --only-show-errors
        test "$(sha256sum "$work/source.tar.gz" | awk '{{print $1}}')" = {_q(source_sha256)}
        test "$(sha256sum "$work/manifest.json" | awk '{{print $1}}')" = {_q(manifest_sha256)}
        test "$(sha256sum "$work/protocol.json" | awk '{{print $1}}')" = {_q(protocol_sha256)}
        mkdir -p "$work/source"
        tar -xzf "$work/source.tar.gz" -C "$work/source"
        printf '%s\n' {_q(source_revision)} >"$work/source/.borsuk-source-revision"
        """
    )


def build_worker_script(
    *,
    job: ExecutionJob,
    source_uri: str,
    source_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
    protocol_uri: str,
    protocol_sha256: str,
    attempt_id: str,
    terminal_prefix: str,
    purchase_option: str = "spot",
) -> str:
    if job.role != "build":
        raise ValueError("build worker requires a build job")
    if purchase_option != "spot":
        raise ValueError("build worker must use Spot")
    cell = {**job.cell, "index_prefix": job.index_uri}
    dataset = cell["dataset"]
    source = dataset["source"]
    if source.get("state") not in {"staged", "generated"}:
        raise ValueError("build worker dataset is not executable")
    prelude = _common_prelude(
        source_revision=str(cell["source"]["git_commit"]),
        source_uri=source_uri,
        source_sha256=source_sha256,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha256,
        protocol_uri=protocol_uri,
        protocol_sha256=protocol_sha256,
        terminal_prefix=terminal_prefix,
        complete_marker="BUILD_TERMINAL_COMPLETE.json",
        failed_marker="BUILD_TERMINAL_FAILED.json",
    )
    dataset_step = ""
    if source["state"] == "staged":
        dataset_step = f'aws s3 cp {_q(source["url"] + "/")} "$work/cell/dataset/" --recursive --only-show-errors'
    return prelude + textwrap.dedent(
        f"""\
        stage=attest-purchase
        token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
        instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
        instance_purchase_option=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-life-cycle)
        test "$instance_purchase_option" = {_q(purchase_option)}
        stage=provision
        dnf install -y gcc gcc-c++ git make cmake openssl-devel python3.12 python3.12-pip xz
        python3.12 -m venv "$work/venv"
        "$work/venv/bin/pip" install -r "$work/source/scripts/requirements-format-bench.txt"
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
        source /root/.cargo/env
        cd "$work/source"
        stage=compile
        cargo build --locked --release --example production_bench --example generate_synthetic_dataset
        stage=stage-dataset
        mkdir -p "$work/cell/dataset"
        {dataset_step}
        stage=preflight-index
        index_uri={_q(job.index_uri)}
        index_bucket=${{index_uri#s3://}}; index_bucket=${{index_bucket%%/*}}
        index_prefix=${{index_uri#s3://$index_bucket/}}/
        object_count=$(aws s3api list-objects-v2 --bucket "$index_bucket" --prefix "$index_prefix" --max-items 1 \
          --expected-bucket-owner 453182569524 --query 'length(Contents || `[]`)' --output text)
        if [[ "$object_count" != 0 ]]; then
          echo 'refusing nonempty scheduled index prefix' >&2
          exit 2
        fi
        stage=publish-binary
        binary="$work/source/target/release/examples/production_bench"
        binary_sha=$(sha256sum "$binary" | awk '{{print $1}}')
        put_immutable "$binary" {_q(terminal_prefix + "/production_bench")}
        printf '{{"schema_version":1,"sha256":"%s","bytes":%s}}\n' "$binary_sha" "$(stat -c %s "$binary")" >"$work/BINARY_COMPLETE.json"
        put_immutable "$work/BINARY_COMPLETE.json" {_q(terminal_prefix + "/BINARY_COMPLETE.json")}
        stage=build-index
        detail_log="$work/cell/build/step-00.log"
        "$work/venv/bin/python" scripts/run_publication_v3_cell.py "$work/protocol.json" "$work/cell" \
          --mode build --manifest "$work/manifest.json" --source-archive-sha256 {_q(source_sha256)} \
          --dataset-materialization-sha256 {_q(source.get("sha256", "0" * 64))} \
          --attempt-id {_q(attempt_id)} --instance-identity "$instance_id" \
          --generator "$work/source/target/release/examples/generate_synthetic_dataset" --borsuk-bench "$binary"
        stage=seal-index
        "$work/venv/bin/python" scripts/seal_publication_v3_index.py \
          --index-uri {_q(cell["index_prefix"])} --logical-rows {_q(dataset["scale"]["rows"])} \
          --logical-cells {_q(cell["index_profile"]["logical_cells"])} \
          --roster-output "$work/INDEX_OBJECTS.json" --inventory-output "$work/INDEX_INVENTORY.json" \
          --region eu-central-1
        "$work/venv/bin/python" scripts/run_publication_v3_cell.py "$work/protocol.json" "$work/cell" \
          --mode seal --manifest "$work/manifest.json" --source-archive-sha256 {_q(source_sha256)} \
          --dataset-materialization-sha256 {_q(source.get("sha256", "0" * 64))} \
          --attempt-id {_q(attempt_id)} --instance-identity "$instance_id" \
          --build-complete "$work/cell/BUILD_COMPLETE.json" --object-roster "$work/INDEX_OBJECTS.json"
        stage=publish-receipts
        for name in BUILD_COMPLETE.json INDEX_COMPLETE.json INDEX_OBJECTS.json INDEX_INVENTORY.json; do
          path="$work/$name"; [[ -f "$path" ]] || path="$work/cell/$name"
          put_immutable "$path" {_q(terminal_prefix)}/$name
        done
        printf '{{"schema_version":1,"status":"complete","role":"build","attempt":{job.attempt},"attempt_id":{_j(attempt_id)},"instance_id":"%s","source_archive_sha256":{_j(source_sha256)},"manifest_sha256":{_j(manifest_sha256)},"protocol_sha256":{_j(protocol_sha256)},"index_uri":{_j(job.index_uri)},"binary_sha256":"%s","purchase_option":"%s"}}\n' "$instance_id" "$binary_sha" "$instance_purchase_option" >"$work/complete.json"
        put_immutable "$work/complete.json" {_q(terminal_prefix + "/BUILD_TERMINAL_COMPLETE.json")}
        complete=1
        """
    )


def runtime_worker_script(
    *,
    job: ExecutionJob,
    source_uri: str,
    source_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
    protocol_uri: str,
    protocol_sha256: str,
    build_prefix: str,
    binary_sha256: str,
    attempt_id: str,
    terminal_prefix: str,
    purchase_option: str = "spot",
) -> str:
    if job.role != "runtime":
        raise ValueError("runtime worker requires a runtime job")
    if purchase_option not in {"spot", "on-demand"}:
        raise ValueError("runtime purchase option must be spot or on-demand")
    cell = job.cell
    source = cell["dataset"]["source"]
    if source.get("state") != "staged":
        raise ValueError("read qualification runtime requires a staged dataset")
    prelude = _common_prelude(
        source_revision=str(cell["source"]["git_commit"]),
        source_uri=source_uri,
        source_sha256=source_sha256,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha256,
        protocol_uri=protocol_uri,
        protocol_sha256=protocol_sha256,
        terminal_prefix=terminal_prefix,
        complete_marker="RUNTIME_TERMINAL_COMPLETE.json",
        failed_marker="RUNTIME_TERMINAL_FAILED.json",
    )
    return prelude + textwrap.dedent(
        f"""\
        stage=attest-purchase
        token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
        instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
        instance_purchase_option=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-life-cycle)
        test "$instance_purchase_option" = {_q(purchase_option)}
        stage=provision
        dnf install -y git python3.12 python3.12-pip util-linux xfsprogs
        python3.12 -m venv "$work/venv"
        "$work/venv/bin/pip" install boto3==1.34.46
        stage=mount-cache
        root_source=$(findmnt -n -o SOURCE /)
        root_parent=$(lsblk -no PKNAME "$root_source")
        root_device=${{root_parent:+/dev/$root_parent}}
        [[ -n "$root_device" ]] || root_device=$root_source
        cache_device=$(lsblk -dpno NAME,TYPE | awk '$2=="disk" {{print $1}}' | while read -r candidate; do
          if [[ "$candidate" != "$root_device" ]]; then printf '%s\n' "$candidate"; break; fi
        done)
        test -b "$cache_device"
        mkfs.xfs -f "$cache_device" >/dev/null
        cache_mount="$work/cell/cache"
        mkdir -p "$cache_mount"
        mount -o noatime "$cache_device" "$cache_mount"
        swapoff -a
        stage=verify-index
        aws s3 cp {_q(build_prefix + "/production_bench")} "$work/production_bench" --only-show-errors
        test "$(sha256sum "$work/production_bench" | awk '{{print $1}}')" = {_q(binary_sha256)}
        chmod 700 "$work/production_bench"
        for name in INDEX_COMPLETE.json INDEX_OBJECTS.json INDEX_INVENTORY.json; do
          aws s3 cp {_q(build_prefix)}/$name "$work/$name" --only-show-errors
        done
        "$work/venv/bin/python" "$work/source/scripts/observe_publication_v3_index.py" \
          --index-uri {_q(job.index_uri)} --roster "$work/INDEX_OBJECTS.json" \
          --output "$work/INDEX_INVENTORY.json" --region eu-central-1
        mkdir -p "$work/cell/runtime-dataset"
        for name in meta.json test.parquet neighbors.parquet; do
          aws s3 cp {_q(source["url"])}/$name "$work/cell/runtime-dataset/$name" --only-show-errors
        done
        stage=execute-runtime
        detail_log="$work/cell/runtime/step-00.log"
        mkdir -p "$(dirname "$detail_log")"
        systemd-run --quiet --wait --collect --service-type=exec \
          -p MemoryMax=8589934592 -p MemorySwapMax=0 \
          -p StandardOutput=append:$detail_log -p StandardError=append:$detail_log \
          /usr/bin/python3.12 "$work/source/scripts/run_publication_v3_cell.py" "$work/protocol.json" "$work/cell" \
          --mode runtime --manifest "$work/manifest.json" --source-archive-sha256 {_q(source_sha256)} \
          --dataset-materialization-sha256 {_q(source["sha256"])} --attempt-id {_q(attempt_id)} \
          --instance-identity "$instance_id" --purchase-option "$instance_purchase_option" \
          --borsuk-bench "$work/production_bench" \
          --index-receipt "$work/INDEX_COMPLETE.json" --object-roster "$work/INDEX_OBJECTS.json" \
          --index-inventory "$work/INDEX_INVENTORY.json"
        stage=publish-receipts
        put_immutable "$work/cell/RESULT_COMPLETE.json" {_q(terminal_prefix + "/RESULT_COMPLETE.json")}
        put_immutable "$work/cell/RUNTIME_ATTESTATION.json" {_q(terminal_prefix + "/RUNTIME_ATTESTATION.json")}
        printf '{{"schema_version":1,"status":"complete","role":"runtime","attempt":{job.attempt},"attempt_id":{_j(attempt_id)},"instance_id":"%s","source_archive_sha256":{_j(source_sha256)},"manifest_sha256":{_j(manifest_sha256)},"protocol_sha256":{_j(protocol_sha256)},"binary_sha256":{_j(binary_sha256)},"purchase_option":"%s"}}\n' "$instance_id" "$instance_purchase_option" >"$work/complete.json"
        put_immutable "$work/complete.json" {_q(terminal_prefix + "/RUNTIME_TERMINAL_COMPLETE.json")}
        complete=1
        """
    )
