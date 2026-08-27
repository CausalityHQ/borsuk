#!/usr/bin/env python3
"""Immutable Publication V3 build/runtime worker construction."""

from __future__ import annotations

import json
import shlex
import textwrap
from copy import deepcopy
from dataclasses import dataclass

try:
    from scripts.publication_v3_aws import worker_failure_trap_script
    from scripts.publication_v3_protocol import (
        build_schedule_document,
        canonical_json_bytes,
        validate_manifest,
    )
except ModuleNotFoundError:
    from publication_v3_aws import worker_failure_trap_script
    from publication_v3_protocol import (
        build_schedule_document,
        canonical_json_bytes,
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
    def runtime(
        cls,
        cell: dict[str, object],
        *,
        attempt: int,
        profile: str = "recall",
        arm_index: int = 0,
        diagnostic: bool = False,
        v21_feasibility: bool = False,
    ) -> "ExecutionJob":
        if profile not in {"recall", "concurrency", "lifecycle"}:
            raise ValueError("runtime execution profile is invalid")
        if (
            type(v21_feasibility) is not bool
            or arm_index < 0
            or diagnostic
            and profile not in {"recall", "lifecycle"}
            or v21_feasibility
            and (diagnostic or profile != "recall" or arm_index != 0)
        ):
            raise ValueError("runtime execution arm identity is invalid")
        job = cls._new(cell, role="runtime", attempt=attempt)
        if v21_feasibility:
            namespace = "runtime-v21-feasibility"
        elif profile == "lifecycle" and diagnostic:
            namespace = "runtime-lifecycle-diagnostic"
        elif profile == "recall" and diagnostic:
            namespace = "runtime-read-diagnostic"
        else:
            namespace = f"runtime-{profile}"
        terminal_prefix = (
            f"{str(cell['result_prefix']).rstrip('/')}/{namespace}/"
            f"arms/{arm_index:04d}/attempts/{attempt:04d}"
        )
        return cls(
            cell=job.cell,
            role=job.role,
            attempt=job.attempt,
            cell_tag=f"{namespace}-{cell['cell_id']}-arm-{arm_index:04d}",
            terminal_prefix=terminal_prefix,
            complete_uri=f"{terminal_prefix}/RUNTIME_TERMINAL_COMPLETE.json",
            failed_uri=f"{terminal_prefix}/RUNTIME_TERMINAL_FAILED.json",
            index_uri=job.index_uri,
        )


def borsuk_cell(
    manifest: dict[str, object],
    *,
    workload_id: str,
    dataset_id: str,
    repetition_id: str,
    build_attempt: int | None = None,
) -> dict[str, object]:
    """Select one exact frozen BORSUK schedule cell."""

    schedule = build_schedule_document(validate_manifest(manifest))
    matches = [
        cell
        for cell in schedule["cells"]
        if cell["system"] == "borsuk"
        and cell["repetition_id"] == repetition_id
        and cell["dataset"]["id"] == dataset_id
        and cell["workload"]["id"] == workload_id
    ]
    if len(matches) != 1:
        raise ValueError("BORSUK cell is not uniquely scheduled")
    result = matches[0]
    if build_attempt is None:
        return result
    if not 0 < build_attempt <= 9_999:
        raise ValueError("BORSUK build attempt is invalid")
    result = deepcopy(result)
    index_root, index_name = str(result["index_prefix"]).rsplit("/", 1)
    result["index_prefix"] = (
        f"{index_root}/build-attempts/{build_attempt:04d}/{index_name}"
    )
    return result


def qualification_cell(
    manifest: dict[str, object],
    *,
    dataset_id: str,
    workload_kind: str,
    build_attempt: int | None = None,
) -> dict[str, object]:
    """Select the canonical first BORSUK cell from a partially staged manifest."""

    normalized = validate_manifest(manifest)
    workloads = [
        workload
        for workload in normalized["workloads"]
        if workload["kind"] == workload_kind and dataset_id in workload["dataset_ids"]
    ]
    if len(workloads) != 1:
        raise ValueError("qualification cell is not uniquely scheduled")
    return borsuk_cell(
        normalized,
        workload_id=str(workloads[0]["id"]),
        dataset_id=dataset_id,
        repetition_id="r01",
        build_attempt=build_attempt,
    )


def _q(value: object) -> str:
    return shlex.quote(str(value))


def _j(value: object) -> str:
    return json.dumps(str(value))


def worker_immutable_upload_function() -> str:
    """Return the retry-safe immutable S3 publisher used by paid workers."""

    return textwrap.dedent(
        """\
        put_immutable() {
          local path=$1 uri=$2 digest checksum bucket key attempt error_log attempt_log
          local remote_checksum put_status command_timeout remaining head_timeout
          digest=$(sha256sum "$path" | awk '{print $1}')
          checksum=$(openssl dgst -sha256 -binary "$path" | base64 -w0)
          bucket=${uri#s3://}; bucket=${bucket%%/*}; key=${uri#s3://$bucket/}
          error_log="$work/put-immutable-error.log"
          attempt_log="$work/put-immutable-attempt.log"
          : >"$error_log"
          for attempt in 1 2 3; do
            command_timeout=60
            if [[ "$immutable_upload_deadline" -gt 0 ]]; then
              remaining=$((immutable_upload_deadline - SECONDS))
              if [[ "$remaining" -le 0 ]]; then
                printf 'immutable-upload key=%s attempt=%s publish-budget-exhausted\n' \
                  "$key" "$attempt" >>"$error_log"
                break
              fi
              if [[ "$remaining" -lt "$command_timeout" ]]; then
                command_timeout=$remaining
              fi
            fi
            if timeout "$command_timeout" aws s3api put-object --bucket "$bucket" --key "$key" --body "$path" \
              --expected-bucket-owner 453182569524 --server-side-encryption AES256 \
              --checksum-algorithm SHA256 --checksum-sha256 "$checksum" \
              --metadata "borsuk-sha256=$digest" --if-none-match '*' \
              >/dev/null 2>"$attempt_log"; then
              rm -f "$error_log" "$attempt_log"
              return 0
            else
              put_status=$?
            fi
            {
              printf 'immutable-upload key=%s attempt=%s status=%s\n' \
                "$key" "$attempt" "$put_status"
              tail -c 16384 "$attempt_log" || true
            } >>"$error_log"
            if grep -Eq 'PreconditionFailed|(^|[^0-9])412([^0-9]|$)' "$attempt_log"; then
              head_timeout=15
              if [[ "$immutable_upload_deadline" -gt 0 ]]; then
                remaining=$((immutable_upload_deadline - SECONDS))
                if [[ "$remaining" -le 0 ]]; then break; fi
                if [[ "$remaining" -lt "$head_timeout" ]]; then
                  head_timeout=$remaining
                fi
              fi
              remote_checksum=$(timeout "$head_timeout" aws s3api head-object --bucket "$bucket" \
                --key "$key" --expected-bucket-owner 453182569524 \
                --checksum-mode ENABLED --query ChecksumSHA256 --output text \
                2>>"$error_log" || true)
              if [[ "$remote_checksum" = "$checksum" ]]; then
                immutable_upload_reconciliations=$((immutable_upload_reconciliations + 1))
                printf 'immutable-upload key=%s attempt=%s reconciled-412\n' \
                  "$key" "$attempt" >&2
                rm -f "$error_log" "$attempt_log"
                return 0
              fi
            fi
            tail -c 16384 "$attempt_log" >&2 || true
            printf 'immutable-upload key=%s attempt=%s status=%s\n' \
              "$key" "$attempt" "$put_status" >&2
            if [[ "$attempt" -lt 3 ]]; then sleep 1; fi
          done
          tail -c 65536 "$error_log" >"$work/failure-detail.log" || true
          detail_log="$work/failure-detail.log"
          rm -f "$attempt_log"
          return 1
        }
        """
    )


def _common_prelude(
    *,
    source_revision: str,
    source_uri: str,
    source_sha256: str,
    manifest_uri: str,
    manifest_sha256: str,
    protocol_uri: str,
    protocol_sha256: str,
) -> str:
    failure_trap = worker_failure_trap_script()
    return textwrap.dedent(
        f"""\
        set -euo pipefail
        export AWS_REGION=eu-central-1 AWS_DEFAULT_REGION=eu-central-1
        work=/var/lib/borsuk-publication
        mkdir -p "$work"
        complete=0
        stage=preflight
        detail_log="$work/worker.log"
        immutable_upload_deadline=0
        immutable_upload_reconciliations=0
        {worker_immutable_upload_function()}
        {failure_trap}
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
    if source.get("state") not in {"staged", "staged-generated"}:
        raise ValueError("build worker dataset is not executable")
    prelude = _common_prelude(
        source_revision=str(cell["source"]["git_commit"]),
        source_uri=source_uri,
        source_sha256=source_sha256,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha256,
        protocol_uri=protocol_uri,
        protocol_sha256=protocol_sha256,
    )
    dataset_step = ""
    if source["state"] == "staged":
        dataset_step = f'aws s3 cp {_q(source["url"] + "/")} "$work/cell/dataset/" --recursive --only-show-errors'
    else:
        dataset_step = textwrap.dedent(
            f"""\
            aws s3 cp {_q(source["receipt_uri"])} "$work/GENERATED_DATASET_COMPLETE.json" --only-show-errors
            test "$(sha256sum "$work/GENERATED_DATASET_COMPLETE.json" | awk '{{print $1}}')" = {_q(source["receipt_sha256"])}
            "$work/venv/bin/python" scripts/fetch_publication_v3_dataset.py \
              --cell "$work/protocol.json" --receipt "$work/GENERATED_DATASET_COMPLETE.json" \
              --output "$work/cell/dataset" --roles train,query,ground-truth,metadata \
              --region eu-central-1 --owner 453182569524 --workers 32
            """
        )
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
        cargo build --locked --release --example production_bench --example rest_app_bench --example generate_synthetic_dataset
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
        rest_binary="$work/source/target/release/examples/rest_app_bench"
        rest_binary_sha=$(sha256sum "$rest_binary" | awk '{{print $1}}')
        put_immutable "$rest_binary" {_q(terminal_prefix + "/rest_app_bench")}
        printf '{{"schema_version":1,"sha256":"%s","bytes":%s}}\n' "$rest_binary_sha" "$(stat -c %s "$rest_binary")" >"$work/REST_BINARY_COMPLETE.json"
        put_immutable "$work/REST_BINARY_COMPLETE.json" {_q(terminal_prefix + "/REST_BINARY_COMPLETE.json")}
        stage=build-index
        detail_log="$work/cell/build/step-00.log"
        "$work/venv/bin/python" scripts/run_publication_v3_cell.py "$work/protocol.json" "$work/cell" \
          --mode build --manifest "$work/manifest.json" --source-archive-sha256 {_q(source_sha256)} \
          --dataset-materialization-sha256 {_q(source["sha256"])} \
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
          --dataset-materialization-sha256 {_q(source["sha256"])} \
          --attempt-id {_q(attempt_id)} --instance-identity "$instance_id" \
          --build-complete "$work/cell/BUILD_COMPLETE.json" --object-roster "$work/INDEX_OBJECTS.json"
        stage=publish-receipts
        immutable_upload_deadline=$((SECONDS + 600))
        for name in BUILD_COMPLETE.json INDEX_COMPLETE.json INDEX_OBJECTS.json INDEX_INVENTORY.json; do
          path="$work/$name"; [[ -f "$path" ]] || path="$work/cell/$name"
          put_immutable "$path" {_q(terminal_prefix)}/$name
        done
        printf '{{"schema_version":2,"status":"complete","role":"build","attempt":{job.attempt},"attempt_id":{_j(attempt_id)},"instance_id":"%s","source_archive_sha256":{_j(source_sha256)},"manifest_sha256":{_j(manifest_sha256)},"protocol_sha256":{_j(protocol_sha256)},"index_uri":{_j(job.index_uri)},"binary_sha256":"%s","rest_binary_sha256":"%s","purchase_option":"%s","artifact_upload_reconciliations":%s}}\n' "$instance_id" "$binary_sha" "$rest_binary_sha" "$instance_purchase_option" "$immutable_upload_reconciliations" >"$work/complete.json"
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
    binary_sha256: str | None,
    attempt_id: str,
    terminal_prefix: str,
    purchase_option: str = "spot",
    runtime_profile: str = "recall",
    arm_index: int = 0,
    arm: dict[str, object] | None = None,
    disk_cache_max_bytes: int,
    exact_read_max_physical_amplification: int,
    max_active_searches: int,
    max_waiting_searches: int,
    leaf_read_width: int,
    max_inflight_leaf_reads: int,
    max_parallel_decode_rank_tasks: int,
    cpu_threads: int,
    io_threads: int,
    s3_get_concurrency: int,
    ram_budget_bytes: int,
    diagnostic_write_ops: int | None = None,
    diagnostic_timeout_seconds: int | None = None,
    diagnostic_read_nprobes: tuple[int, ...] | None = None,
    diagnostic_read_candidates: tuple[int, ...] | None = None,
    v21_feasibility: bool = False,
    v21_base_authority: dict[str, object] | None = None,
) -> str:
    if job.role != "runtime":
        raise ValueError("runtime worker requires a runtime job")
    if purchase_option not in {"spot", "on-demand"}:
        raise ValueError("runtime purchase option must be spot or on-demand")
    if runtime_profile not in {"recall", "concurrency", "lifecycle"}:
        raise ValueError("runtime profile must be recall, concurrency, or lifecycle")
    if arm_index < 0:
        raise ValueError("runtime arm index must be nonnegative")
    if (
        disk_cache_max_bytes < 0
        or min(
            exact_read_max_physical_amplification,
            max_active_searches,
            max_waiting_searches,
            leaf_read_width,
            max_inflight_leaf_reads,
            max_parallel_decode_rank_tasks,
            cpu_threads,
            io_threads,
            s3_get_concurrency,
            ram_budget_bytes,
        )
        <= 0
    ):
        raise ValueError("runtime resource authority must be positive")
    if (
        exact_read_max_physical_amplification > 5
        or max_active_searches > 64
        or max_waiting_searches > 256
        or leaf_read_width > 1_024
        or max_inflight_leaf_reads > 1_024
        or max_parallel_decode_rank_tasks > cpu_threads
        or cpu_threads > 64
        or s3_get_concurrency > 128
        or not s3_get_concurrency <= io_threads <= 256
        or (runtime_profile == "concurrency" and max_active_searches < 16)
    ):
        raise ValueError("runtime resource authority violates safety bounds")
    workload = job.cell.get("workload")
    workload_kind = workload.get("kind") if isinstance(workload, dict) else None
    read_diagnostic = any(
        value is not None
        for value in (
            diagnostic_read_nprobes,
            diagnostic_read_candidates,
        )
    )
    if (
        type(v21_feasibility) is not bool
        or v21_feasibility
        and (
            read_diagnostic
            or diagnostic_write_ops is not None
            or diagnostic_timeout_seconds is not None
            or runtime_profile != "recall"
            or arm_index != 0
            or not isinstance(v21_base_authority, dict)
        )
        or not v21_feasibility
        and v21_base_authority is not None
    ):
        raise ValueError("V21 feasibility authority is invalid")
    profile_mismatch = (
        (
            runtime_profile == "concurrency"
            and not job.cell_tag.startswith("runtime-concurrency-")
        )
        or (
            runtime_profile == "recall"
            and (
                not job.cell_tag.startswith("runtime-")
                or job.cell_tag.startswith("runtime-concurrency-")
                or job.cell_tag.startswith("runtime-lifecycle-")
                or (
                    read_diagnostic
                    != job.cell_tag.startswith("runtime-read-diagnostic-")
                )
                or (
                    v21_feasibility
                    != job.cell_tag.startswith("runtime-v21-feasibility-")
                )
            )
        )
        or (
            runtime_profile == "lifecycle"
            and not job.cell_tag.startswith("runtime-lifecycle-")
        )
    )
    if profile_mismatch:
        raise ValueError("runtime job identity differs from its execution profile")
    if runtime_profile == "lifecycle":
        if workload_kind != "write-update-delete-compact" or not isinstance(arm, dict):
            raise ValueError("lifecycle runtime requires an exact mutation arm")
    elif workload_kind != "read-recall" or arm is not None:
        raise ValueError("read runtime cannot carry lifecycle authority")
    diagnostic_cell = job.cell
    cell = diagnostic_cell
    base_setup = ""
    index_uri = job.index_uri
    runtime_manifest = '"$work/manifest.json"'
    runtime_protocol = '"$work/protocol.json"'
    runtime_source_sha256 = source_sha256
    index_digest_checks = ""
    if not v21_feasibility and (
        not isinstance(binary_sha256, str)
        or len(binary_sha256) != 64
        or any(character not in "0123456789abcdef" for character in binary_sha256)
    ):
        raise ValueError("runtime binary checksum authority differs")
    binary_setup = textwrap.dedent(
        f"""\
        aws s3 cp {_q(build_prefix + "/production_bench")} "$work/production_bench" --only-show-errors
        test "$(sha256sum "$work/production_bench" | awk '{{print $1}}')" = {_q(binary_sha256)}
        binary_sha={_q(binary_sha256)}
        """
    )
    provision_packages = "git python3.12 python3.12-pip util-linux xfsprogs"
    provision_python = '"$work/venv/bin/pip" install boto3==1.34.46'
    memory_max_bytes = 8_589_934_592
    systemd_wait_option = "--wait"
    systemd_run_options = "--collect"
    cgroup_observation = ""
    if v21_feasibility:
        assert v21_base_authority is not None
        fields = {
            "manifest_uri",
            "manifest_sha256",
            "protocol_uri",
            "protocol_sha256",
            "build_terminal_uri",
            "build_terminal_sha256",
            "build_prefix",
            "source_archive_sha256",
            "cell",
            "index_id",
            "index_uri",
            "index_receipt_sha256",
            "object_roster_sha256",
            "inventory_sha256",
        }
        if set(v21_base_authority) != fields:
            raise ValueError("V21 base-index authority fields differ")
        for field in (
            "manifest_sha256",
            "protocol_sha256",
            "build_terminal_sha256",
            "source_archive_sha256",
            "index_receipt_sha256",
            "object_roster_sha256",
            "inventory_sha256",
        ):
            value = v21_base_authority[field]
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"V21 base-index {field} differs")
        base_cell = v21_base_authority["cell"]
        base_source = base_cell.get("source") if isinstance(base_cell, dict) else None
        if (
            not isinstance(base_cell, dict)
            or base_cell.get("cell_id") is None
            or not isinstance(base_source, dict)
            or base_source.get("archive_sha256")
            != v21_base_authority["source_archive_sha256"]
            or base_cell.get("index_prefix") != v21_base_authority["index_uri"]
            or str(base_cell["index_prefix"]).rstrip("/").rsplit("/", 1)[-1]
            != v21_base_authority["index_id"]
            or v21_base_authority["build_prefix"] != build_prefix
            or binary_sha256 is not None
        ):
            raise ValueError("V21 base-index cell authority differs")
        cell = base_cell
        index_uri = str(v21_base_authority["index_uri"])
        runtime_manifest = '"$work/base-manifest.json"'
        runtime_protocol = '"$work/base-protocol.json"'
        runtime_source_sha256 = str(v21_base_authority["source_archive_sha256"])
        base_setup = textwrap.dedent(
            f"""\
            stage=verify-base-authority
            aws s3 cp {_q(v21_base_authority["manifest_uri"])} "$work/base-manifest.json" --only-show-errors
            test "$(sha256sum "$work/base-manifest.json" | awk '{{print $1}}')" = {_q(v21_base_authority["manifest_sha256"])}
            aws s3 cp {_q(v21_base_authority["protocol_uri"])} "$work/base-protocol.json" --only-show-errors
            test "$(sha256sum "$work/base-protocol.json" | awk '{{print $1}}')" = {_q(v21_base_authority["protocol_sha256"])}
            aws s3 cp {_q(v21_base_authority["build_terminal_uri"])} "$work/BASE_BUILD_TERMINAL_COMPLETE.json" --only-show-errors
            test "$(sha256sum "$work/BASE_BUILD_TERMINAL_COMPLETE.json" | awk '{{print $1}}')" = {_q(v21_base_authority["build_terminal_sha256"])}
            """
        )
        index_digest_checks = textwrap.dedent(
            f"""\
            test "$(sha256sum "$work/INDEX_COMPLETE.json" | awk '{{print $1}}')" = {_q(v21_base_authority["index_receipt_sha256"])}
            test "$(sha256sum "$work/INDEX_OBJECTS.json" | awk '{{print $1}}')" = {_q(v21_base_authority["object_roster_sha256"])}
            test "$(sha256sum "$work/INDEX_INVENTORY.json" | awk '{{print $1}}')" = {_q(v21_base_authority["inventory_sha256"])}
            """
        )
        provision_packages = (
            "gcc gcc-c++ git make cmake openssl-devel python3.12 "
            "python3.12-pip util-linux xfsprogs xz"
        )
        provision_python = (
            '"$work/venv/bin/pip" install -r '
            '"$work/source/scripts/requirements-format-bench.txt"'
        )
        binary_setup = textwrap.dedent(
            """\
            stage=compile-diagnostic
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
            source /root/.cargo/env
            (cd "$work/source" && cargo build --locked --release --example production_bench)
            cp "$work/source/target/release/examples/production_bench" "$work/production_bench"
            binary_sha=$(sha256sum "$work/production_bench" | awk '{print $1}')
            """
        )
        memory_max_bytes = 34_359_738_368
        unit_name = f"borsuk-v21-{job.attempt:04d}.service"
        systemd_wait_option = ""
        systemd_run_options = f"--unit={unit_name} --remain-after-exit"
        cgroup_observation = textwrap.dedent(
            f"""\
            while true; do
              unit_active_state=$(systemctl show {_q(unit_name)} --property=ActiveState --value)
              unit_sub_state=$(systemctl show {_q(unit_name)} --property=SubState --value)
              if [[ "$unit_active_state" == active && "$unit_sub_state" == exited ]]; then
                break
              fi
              if [[ "$unit_active_state" == failed ]]; then
                runtime_status=$(systemctl show {_q(unit_name)} --property=ExecMainStatus --value)
                [[ "$runtime_status" =~ ^[0-9]+$ ]] || runtime_status=1
                (( runtime_status != 0 )) || runtime_status=1
                exit "$runtime_status"
              fi
              sleep 1
            done
            actual_exec_code=$(systemctl show {_q(unit_name)} --property=ExecMainCode --value)
            actual_exec_status=$(systemctl show {_q(unit_name)} --property=ExecMainStatus --value)
            actual_memory_max=$(systemctl show {_q(unit_name)} --property=MemoryMax --value)
            actual_memory_swap_max=$(systemctl show {_q(unit_name)} --property=MemorySwapMax --value)
            actual_memory_peak=$(systemctl show {_q(unit_name)} --property=MemoryPeak --value)
            [[ "$actual_memory_max" =~ ^[0-9]+$ ]]
            [[ "$actual_memory_swap_max" =~ ^[0-9]+$ ]]
            [[ "$actual_memory_peak" =~ ^[0-9]+$ ]]
            test "$actual_exec_code" = 1
            test "$actual_exec_status" = 0
            test "$actual_memory_max" = {memory_max_bytes}
            test "$actual_memory_swap_max" = 0
            test "$actual_memory_peak" -le "$actual_memory_max"
            systemctl stop {_q(unit_name)}
            """
        )
        cache_setup = textwrap.dedent(
            """\
            stage=disable-cache
            mkdir -p "$work/cell/cache"
            swapoff -a
            """
        )
    else:
        cache_setup = textwrap.dedent(
            """\
            stage=mount-cache
            root_source=$(findmnt -n -o SOURCE /)
            root_parent=$(lsblk -no PKNAME "$root_source")
            root_device=${root_parent:+/dev/$root_parent}
            [[ -n "$root_device" ]] || root_device=$root_source
            cache_device=$(lsblk -dpno NAME,TYPE | awk '$2=="disk" {print $1}' | while read -r candidate; do
              if [[ "$candidate" != "$root_device" ]]; then printf '%s\n' "$candidate"; break; fi
            done)
            test -b "$cache_device"
            mkfs.xfs -f "$cache_device" >/dev/null
            cache_mount="$work/cell/cache"
            mkdir -p "$cache_mount"
            mount -o noatime "$cache_device" "$cache_mount"
            swapoff -a
            """
        )
    source = cell["dataset"]["source"]
    if source.get("state") not in {"staged", "staged-generated"}:
        raise ValueError("publication runtime requires a staged dataset")
    prelude = _common_prelude(
        source_revision=str(diagnostic_cell["source"]["git_commit"]),
        source_uri=source_uri,
        source_sha256=source_sha256,
        manifest_uri=manifest_uri,
        manifest_sha256=manifest_sha256,
        protocol_uri=protocol_uri,
        protocol_sha256=protocol_sha256,
    )
    clone_step = ""
    clone_arguments = ""
    if runtime_profile == "lifecycle":
        clone_step = textwrap.dedent(
            f"""\
            stage=clone-index
            printf '%s' {_q(canonical_json_bytes(arm).decode("utf-8"))} >"$work/arm.json"
            "$work/venv/bin/python" "$work/source/scripts/clone_publication_v3_index.py" \
              --cell "$work/protocol.json" --arm "$work/arm.json" \
              --attempt-id {_q(attempt_id)} --base-receipt "$work/INDEX_COMPLETE.json" \
              --base-roster "$work/INDEX_OBJECTS.json" \
              --receipt-output "$work/CLONE_COMPLETE.json" \
              --inventory-output "$work/CLONE_OBJECTS.json" --workers 32
            put_immutable "$work/CLONE_COMPLETE.json" {_q(terminal_prefix + "/CLONE_COMPLETE.json")}
            put_immutable "$work/CLONE_OBJECTS.json" {_q(terminal_prefix + "/CLONE_OBJECTS.json")}
            """
        )
        clone_arguments = (
            ' --clone-receipt "$work/CLONE_COMPLETE.json"'
            ' --clone-inventory "$work/CLONE_OBJECTS.json"'
        )
    if source["state"] == "staged-generated":
        runtime_dataset_step = textwrap.dedent(
            f"""\
            aws s3 cp {_q(source["receipt_uri"])} "$work/GENERATED_DATASET_COMPLETE.json" --only-show-errors
            test "$(sha256sum "$work/GENERATED_DATASET_COMPLETE.json" | awk '{{print $1}}')" = {_q(source["receipt_sha256"])}
            "$work/venv/bin/python" "$work/source/scripts/fetch_publication_v3_dataset.py" \
              --cell "$work/protocol.json" --receipt "$work/GENERATED_DATASET_COMPLETE.json" \
              --output "$work/cell/runtime-dataset" --roles query,ground-truth,metadata \
              --region eu-central-1 --owner 453182569524 --workers 8
            """
        )
    else:
        runtime_dataset_step = textwrap.dedent(
            f"""\
            for name in meta.json test.parquet neighbors.parquet; do
              aws s3 cp {_q(source["url"])}/$name "$work/cell/runtime-dataset/$name" --only-show-errors
            done
            """
        )
    diagnostic_arguments = ""
    diagnostic_validation = ""
    diagnostic_receipt_fields = ""
    read_diagnostic_uploads = ""
    diagnostic_uploads_before_result = ""
    if (diagnostic_write_ops is None) != (diagnostic_timeout_seconds is None):
        raise ValueError("lifecycle diagnostic authority must be supplied atomically")
    if diagnostic_write_ops is not None:
        if (
            runtime_profile != "lifecycle"
            or not 1 <= diagnostic_write_ops <= 50_000
            or diagnostic_timeout_seconds is None
            or diagnostic_timeout_seconds <= 0
        ):
            raise ValueError("lifecycle diagnostic write count is invalid")
        diagnostic_arguments = f" --diagnostic-write-ops {_q(diagnostic_write_ops)}"
        diagnostic_validation = f"""actual_diagnostic_write_ops=$(\"$work/venv/bin/python\" -c 'import json,sys; print(json.load(open(sys.argv[1]))[\"diagnostic_write_ops\"])' \"$work/cell/RESULT_COMPLETE.json\")
actual_claim_eligible=$(\"$work/venv/bin/python\" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))[\"claim_eligible\"]))' \"$work/cell/RESULT_COMPLETE.json\")
test \"$actual_diagnostic_write_ops\" = {diagnostic_write_ops}
test \"$actual_claim_eligible\" = false
diagnostic_result_sha=$(sha256sum \"$work/cell/RESULT_COMPLETE.json\" | awk '{{print $1}}')"""
        diagnostic_receipt_fields = (
            "diagnostic_fields=$(printf ',"
            f'"claim_eligible":false,"diagnostic_write_ops":{diagnostic_write_ops},'
            f'"diagnostic_timeout_seconds":{diagnostic_timeout_seconds},'
            '"diagnostic_result_sha256":"%s"\' "$diagnostic_result_sha")'
        )
    if read_diagnostic:
        if (
            diagnostic_write_ops is not None
            or runtime_profile != "recall"
            or diagnostic_read_nprobes is None
            or diagnostic_read_candidates is None
            or not diagnostic_read_nprobes
            or not diagnostic_read_candidates
            or tuple(sorted(set(diagnostic_read_nprobes))) != diagnostic_read_nprobes
            or tuple(sorted(set(diagnostic_read_candidates)))
            != diagnostic_read_candidates
            or len(diagnostic_read_nprobes) * len(diagnostic_read_candidates) > 32
            or any(
                isinstance(value, bool) or not 1 <= value <= 256
                for value in diagnostic_read_nprobes
            )
            or any(
                isinstance(value, bool) or not 1 <= value <= 16_384
                for value in diagnostic_read_candidates
            )
        ):
            raise ValueError("read diagnostic authority is invalid")
        nprobes = ",".join(str(value) for value in diagnostic_read_nprobes)
        candidates = ",".join(str(value) for value in diagnostic_read_candidates)
        diagnostic_arguments = (
            f" --diagnostic-read-nprobes {_q(nprobes)}"
            f" --diagnostic-read-candidates {_q(candidates)}"
        )
        diagnostic_validation = """actual_claim_eligible=$(\"$work/venv/bin/python\" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))[\"claim_eligible\"]))' \"$work/cell/RESULT_COMPLETE.json\")
test \"$actual_claim_eligible\" = false
for name in bench_query_samples.csv bench_recall_latency.csv; do
  test -s \"$work/cell/runtime-output/$name\"
done
diagnostic_result_sha=$(sha256sum \"$work/cell/RESULT_COMPLETE.json\" | awk '{print $1}')
diagnostic_samples_sha=$(sha256sum \"$work/cell/runtime-output/bench_query_samples.csv\" | awk '{print $1}')
diagnostic_summary_sha=$(sha256sum \"$work/cell/runtime-output/bench_recall_latency.csv\" | awk '{print $1}')"""
        diagnostic_receipt_fields = (
            "diagnostic_fields=$(printf ',"
            '"claim_eligible":false,'
            f'"diagnostic_read_nprobes":{json.dumps(list(diagnostic_read_nprobes))},'
            f'"diagnostic_read_candidates":{json.dumps(list(diagnostic_read_candidates))},'
            '"diagnostic_result_sha256":"%s",'
            '"diagnostic_samples_sha256":"%s",'
            '"diagnostic_summary_sha256":"%s"\' '
            '"$diagnostic_result_sha" "$diagnostic_samples_sha" '
            '"$diagnostic_summary_sha")'
        )
        read_diagnostic_uploads = textwrap.dedent(
            f"""\
            for name in bench_query_samples.csv bench_recall_latency.csv; do
              put_immutable "$work/cell/runtime-output/$name" {_q(terminal_prefix)}/$name
            done
            """
        )
    if v21_feasibility:
        assert v21_base_authority is not None
        diagnostic_arguments = " --v21-feasibility"
        diagnostic_validation = """actual_claim_eligible=$(\"$work/venv/bin/python\" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))[\"claim_eligible\"]))' \"$work/cell/RESULT_COMPLETE.json\")
test \"$actual_claim_eligible\" = false
for name in bench_v21_feasibility_arms.csv bench_v21_feasibility_samples.csv bench_v21_feasibility_summary.json; do
  test -s \"$work/cell/runtime-output/$name\"
done
v21_result_sha=$(sha256sum \"$work/cell/RESULT_COMPLETE.json\" | awk '{print $1}')
v21_arms_sha=$(sha256sum \"$work/cell/runtime-output/bench_v21_feasibility_arms.csv\" | awk '{print $1}')
v21_samples_sha=$(sha256sum \"$work/cell/runtime-output/bench_v21_feasibility_samples.csv\" | awk '{print $1}')
v21_summary_sha=$(sha256sum \"$work/cell/runtime-output/bench_v21_feasibility_summary.json\" | awk '{print $1}')"""
        diagnostic_receipt_fields = (
            "diagnostic_fields=$(printf ',"
            '"claim_eligible":false,"v21_feasibility":true,'
            '"v21_result_sha256":"%s",'
            '"v21_arms_sha256":"%s",'
            '"v21_samples_sha256":"%s",'
            '"v21_summary_sha256":"%s"\' '
            '"$v21_result_sha" "$v21_arms_sha" "$v21_samples_sha" '
            '"$v21_summary_sha")'
        )
        v21_receipt_authority = {
            "base_build_terminal_sha256": v21_base_authority["build_terminal_sha256"],
            "base_manifest_sha256": v21_base_authority["manifest_sha256"],
            "base_protocol_sha256": v21_base_authority["protocol_sha256"],
            "base_source_archive_sha256": v21_base_authority["source_archive_sha256"],
            "base_index_receipt_sha256": v21_base_authority["index_receipt_sha256"],
            "base_object_roster_sha256": v21_base_authority["object_roster_sha256"],
            "base_inventory_sha256": v21_base_authority["inventory_sha256"],
            "base_index_id": v21_base_authority["index_id"],
            "base_index_uri": v21_base_authority["index_uri"],
            "diagnostic_source_archive_sha256": source_sha256,
        }
        v21_authority_fragment = "," + ",".join(
            f"{json.dumps(key)}:{json.dumps(value)}"
            for key, value in v21_receipt_authority.items()
        )
        diagnostic_receipt_fields += (
            "\ndiagnostic_fields=$(printf '%s%s' \"$diagnostic_fields\" "
            f"{_q(v21_authority_fragment)})"
        )
        diagnostic_receipt_fields += (
            "\ndiagnostic_fields=$(printf "
            '\'%s,"memory_max_bytes":%s,"memory_swap_max_bytes":%s,\''
            '\'"memory_peak_bytes":%s\' "$diagnostic_fields" '
            '"$actual_memory_max" "$actual_memory_swap_max" "$actual_memory_peak")'
        )
        diagnostic_uploads_before_result = textwrap.dedent(
            f"""\
            put_immutable "$work/cell/runtime-output/bench_v21_feasibility_arms.csv" {_q(terminal_prefix + "/bench_v21_feasibility_arms.csv")}
            put_immutable "$work/cell/runtime-output/bench_v21_feasibility_samples.csv" {_q(terminal_prefix + "/bench_v21_feasibility_samples.csv")}
            put_immutable "$work/cell/runtime-output/bench_v21_feasibility_summary.json" {_q(terminal_prefix + "/bench_v21_feasibility_summary.json")}
            """
        )
    return prelude + textwrap.dedent(
        f"""\
        stage=attest-purchase
        token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
        instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
        instance_purchase_option=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-life-cycle)
        test "$instance_purchase_option" = {_q(purchase_option)}
        stage=provision
        dnf install -y {provision_packages}
        python3.12 -m venv "$work/venv"
        {provision_python}
        {cache_setup}
        {base_setup}
        {binary_setup}
        chmod 700 "$work/production_bench"
        stage=verify-index
        for name in INDEX_COMPLETE.json INDEX_OBJECTS.json INDEX_INVENTORY.json; do
          aws s3 cp {_q(build_prefix)}/$name "$work/$name" --only-show-errors
        done
        {index_digest_checks}
        "$work/venv/bin/python" "$work/source/scripts/observe_publication_v3_index.py" \
          --index-uri {_q(index_uri)} --roster "$work/INDEX_OBJECTS.json" \
          --output "$work/INDEX_INVENTORY.json" --region eu-central-1
        {clone_step}
        mkdir -p "$work/cell/runtime-dataset"
        {runtime_dataset_step}stage=execute-runtime
        detail_log="$work/cell/runtime/step-00.log"
        mkdir -p "$(dirname "$detail_log")"
        systemd-run --quiet {systemd_wait_option} {systemd_run_options} --service-type=exec \
          -p MemoryMax={memory_max_bytes} -p MemorySwapMax=0 \
          -p StandardOutput=append:$detail_log -p StandardError=append:$detail_log \
          /usr/bin/python3.12 "$work/source/scripts/run_publication_v3_cell.py" {runtime_protocol} "$work/cell" \
          --mode runtime --manifest {runtime_manifest} --source-archive-sha256 {_q(runtime_source_sha256)} \
          --runtime-profile {_q(runtime_profile)} \
          --arm-index {_q(arm_index)} \
          --disk-cache-max-bytes {_q(disk_cache_max_bytes)} \
          --exact-read-max-physical-amplification {_q(exact_read_max_physical_amplification)} \
          --max-active-searches {_q(max_active_searches)} \
          --max-waiting-searches {_q(max_waiting_searches)} \
          --leaf-read-width {_q(leaf_read_width)} \
          --max-inflight-leaf-reads {_q(max_inflight_leaf_reads)} \
          --max-parallel-decode-rank-tasks {_q(max_parallel_decode_rank_tasks)} \
          --cpu-threads {_q(cpu_threads)} \
          --io-threads {_q(io_threads)} \
          --s3-get-concurrency {_q(s3_get_concurrency)} \
          --ram-budget-bytes {_q(ram_budget_bytes)} \
          --dataset-materialization-sha256 {_q(source["sha256"])} --attempt-id {_q(attempt_id)} \
          --instance-identity "$instance_id" --purchase-option "$instance_purchase_option" \
          --borsuk-bench "$work/production_bench" \
          --index-receipt "$work/INDEX_COMPLETE.json" --object-roster "$work/INDEX_OBJECTS.json" \
          --index-inventory "$work/INDEX_INVENTORY.json"{clone_arguments}{diagnostic_arguments}{' --v21-diagnostic-protocol "$work/protocol.json" --v21-diagnostic-manifest "$work/manifest.json"' if v21_feasibility else ''}
        {cgroup_observation}
        {diagnostic_validation}
        stage=publish-receipts
        immutable_upload_deadline=$((SECONDS + 600))
        {diagnostic_uploads_before_result}
        put_immutable "$work/cell/RESULT_COMPLETE.json" {_q(terminal_prefix + "/RESULT_COMPLETE.json")}
        {read_diagnostic_uploads}
        put_immutable "$work/cell/RUNTIME_ATTESTATION.json" {_q(terminal_prefix + "/RUNTIME_ATTESTATION.json")}
        execution_contract="$work/cell/RUNTIME_EXECUTION_CONTRACT.json"
        put_immutable "$execution_contract" {_q(terminal_prefix + "/RUNTIME_EXECUTION_CONTRACT.json")}
        actual_max_active=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_active_searches"])' "$execution_contract")
        actual_max_waiting=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_waiting_searches"])' "$execution_contract")
        actual_leaf_width=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["leaf_read_width"])' "$execution_contract")
        actual_max_leaf_reads=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_inflight_leaf_reads"])' "$execution_contract")
        actual_max_parallel_decode_rank_tasks=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_parallel_decode_rank_tasks"])' "$execution_contract")
        actual_cpu_threads=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["cpu_threads"])' "$execution_contract")
        actual_io_threads=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["io_threads"])' "$execution_contract")
        actual_s3_gets=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["s3_get_concurrency"])' "$execution_contract")
        actual_ram_budget=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["ram_budget_bytes"])' "$execution_contract")
        actual_disk_cache=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["disk_cache_max_bytes"])' "$execution_contract")
        actual_exact_amplification=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["exact_read_max_physical_amplification"])' "$execution_contract")
        actual_runtime_profile=$("$work/venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["runtime_profile"])' "$execution_contract")
        test "$actual_max_active" = {_q(max_active_searches)}
        test "$actual_max_waiting" = {_q(max_waiting_searches)}
        test "$actual_leaf_width" = {_q(leaf_read_width)}
        test "$actual_max_leaf_reads" = {_q(max_inflight_leaf_reads)}
        test "$actual_max_parallel_decode_rank_tasks" = {_q(max_parallel_decode_rank_tasks)}
        test "$actual_cpu_threads" = {_q(cpu_threads)}
        test "$actual_io_threads" = {_q(io_threads)}
        test "$actual_s3_gets" = {_q(s3_get_concurrency)}
        test "$actual_ram_budget" = {_q(ram_budget_bytes)}
        test "$actual_disk_cache" = {_q(disk_cache_max_bytes)}
        test "$actual_exact_amplification" = {_q(exact_read_max_physical_amplification)}
        test "$actual_runtime_profile" = {_q(runtime_profile)}
        execution_contract_sha=$(sha256sum "$execution_contract" | awk '{{print $1}}')
        diagnostic_fields=''
        {diagnostic_receipt_fields}
        concurrency_fields=''
        lifecycle_fields=''
        if [[ {_q(runtime_profile)} == concurrency ]]; then
          for name in bench_concurrency.csv bench_concurrency_samples.csv; do
            put_immutable "$work/cell/runtime-output/$name" {_q(terminal_prefix)}/$name
          done
          concurrency_summary_sha=$(sha256sum "$work/cell/runtime-output/bench_concurrency.csv" | awk '{{print $1}}')
          concurrency_samples_sha=$(sha256sum "$work/cell/runtime-output/bench_concurrency_samples.csv" | awk '{{print $1}}')
          concurrency_fields=$(printf ',"concurrency_summary_sha256":"%s","concurrency_samples_sha256":"%s"' "$concurrency_summary_sha" "$concurrency_samples_sha")
        elif [[ {_q(runtime_profile)} == lifecycle ]]; then
          for name in bench_lifecycle.csv bench_write_costs.csv bench_write_samples.csv bench_mutation_queries.csv bench_mutation_query_samples.csv storage-access.csv; do
            put_immutable "$work/cell/runtime-output/$name" {_q(terminal_prefix)}/$name
          done
          lifecycle_summary_sha=$(sha256sum "$work/cell/runtime-output/bench_lifecycle.csv" | awk '{{print $1}}')
          lifecycle_costs_sha=$(sha256sum "$work/cell/runtime-output/bench_write_costs.csv" | awk '{{print $1}}')
          lifecycle_samples_sha=$(sha256sum "$work/cell/runtime-output/bench_write_samples.csv" | awk '{{print $1}}')
          lifecycle_query_summary_sha=$(sha256sum "$work/cell/runtime-output/bench_mutation_queries.csv" | awk '{{print $1}}')
          lifecycle_query_samples_sha=$(sha256sum "$work/cell/runtime-output/bench_mutation_query_samples.csv" | awk '{{print $1}}')
          lifecycle_storage_trace_sha=$(sha256sum "$work/cell/runtime-output/storage-access.csv" | awk '{{print $1}}')
          lifecycle_fields=$(printf ',"lifecycle_summary_sha256":"%s","lifecycle_costs_sha256":"%s","lifecycle_samples_sha256":"%s","lifecycle_query_summary_sha256":"%s","lifecycle_query_samples_sha256":"%s","lifecycle_storage_trace_sha256":"%s"' "$lifecycle_summary_sha" "$lifecycle_costs_sha" "$lifecycle_samples_sha" "$lifecycle_query_summary_sha" "$lifecycle_query_samples_sha" "$lifecycle_storage_trace_sha")
        fi
        printf '{{"schema_version":5,"status":"complete","role":"runtime","attempt":{job.attempt},"attempt_id":{_j(attempt_id)},"instance_id":"%s","source_archive_sha256":{_j(source_sha256)},"manifest_sha256":{_j(manifest_sha256)},"protocol_sha256":{_j(protocol_sha256)},"binary_sha256":"%s","purchase_option":"%s","runtime_profile":"%s","arm_index":{arm_index},"max_active_searches":%s,"max_waiting_searches":%s,"leaf_read_width":%s,"max_inflight_leaf_reads":%s,"max_parallel_decode_rank_tasks":%s,"cpu_threads":%s,"io_threads":%s,"s3_get_concurrency":%s,"ram_budget_bytes":%s,"disk_cache_max_bytes":%s,"exact_read_max_physical_amplification":%s,"execution_contract_sha256":"%s","artifact_upload_reconciliations":%s%s%s%s}}\n' "$instance_id" "$binary_sha" "$instance_purchase_option" "$actual_runtime_profile" "$actual_max_active" "$actual_max_waiting" "$actual_leaf_width" "$actual_max_leaf_reads" "$actual_max_parallel_decode_rank_tasks" "$actual_cpu_threads" "$actual_io_threads" "$actual_s3_gets" "$actual_ram_budget" "$actual_disk_cache" "$actual_exact_amplification" "$execution_contract_sha" "$immutable_upload_reconciliations" "$diagnostic_fields" "$concurrency_fields" "$lifecycle_fields" >"$work/complete.json"
        put_immutable "$work/complete.json" {_q(terminal_prefix + "/RUNTIME_TERMINAL_COMPLETE.json")}
        complete=1
        """
    )
