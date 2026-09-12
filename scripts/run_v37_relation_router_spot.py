#!/usr/bin/env python3
"""Spot-only orchestration for the V37 relation-router fail-fast campaign."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import pathlib
import re
import shlex
import subprocess
import sys
import time
import urllib.parse
from collections.abc import Callable, Sequence
from typing import Any

EXPECTED_AWS_ACCOUNT = "453182569524"
PROFILE = "causality"
REGION = "eu-central-1"
AMI_ID = "ami-07bcecd13a160173f"
INSTANCE_TYPE = "c7g.8xlarge"
INSTANCE_PROFILE = "borsuk-bench-profile"
SECURITY_GROUP_ID = "sg-0b1fd3e4fbde4af0d"
MEMORY_LIMIT_BYTES = 3 * 1_073_741_824
MEMORY_PSI_FULL_LIMIT = 0.75
SCIENCE_TIMEOUT_SECONDS = 600
WRAPPER_TIMEOUT_SECONDS = 720
PROGRESS_TIMEOUT_SECONDS = 120
MINIMUM_COORDINATE_SCORES_PER_SECOND = 20_000_000
PREFLIGHT_COORDINATE_SCORES = 50_331_648
PREFLIGHT_COORDINATE_SHA256 = (
    "62ebfdbb42fa5e6212437932ebe682970389b15fa8cae1d6e2aa6d8610a41608"
)
_MAX_AUTHORITY_JSON_BYTES = 1_048_576
_CAPACITY_ERRORS = {
    "InsufficientFreeAddressesInSubnet",
    "InsufficientInstanceCapacity",
    "UnfulfillableCapacity",
}

_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LOWER_GIT_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_INSTANCE_ID = re.compile(r"i-[0-9a-f]{8,17}\Z")
_TRAINING_PHASES = {"preflight-training", "build-ownership"}


@dataclasses.dataclass(frozen=True)
class SpotTarget:
    """One preregistered public subnet in an independent availability zone."""

    availability_zone: str
    subnet_id: str


SPOT_TARGETS = (
    SpotTarget("eu-central-1c", "subnet-0a12dbed0ca6fac25"),
    SpotTarget("eu-central-1b", "subnet-00243d923761c047c"),
    SpotTarget("eu-central-1a", "subnet-034528fbd6977848f"),
)


@dataclasses.dataclass(frozen=True)
class V37SpotPlan:
    """Exact immutable launch authority for one V37 phase."""

    phase: str
    run_id: str
    source_commit: str
    source_archive_uri: str
    source_archive_sha256: str
    source_archive_bytes: int
    binary_uri: str
    binary_sha256: str
    binary_bytes: int
    manifest_uri: str
    manifest_sha256: str
    manifest_bytes: int
    output_prefix: str


@dataclasses.dataclass(frozen=True)
class V37MonitorSample:
    """One bounded monitor observation from the sole worker process group."""

    elapsed_seconds: float
    last_progress_seconds: float
    coordinate_scores_per_second: int | None
    rss_bytes: int
    psi_full_avg10: float
    swap_bytes: int


@dataclasses.dataclass(frozen=True)
class V37StagedArtifact:
    """One authenticated local file plus its immutable evidence identity."""

    role: str
    path: pathlib.Path
    uri: str
    sha256: str
    blake3: str
    encoded_bytes: int


@dataclasses.dataclass(frozen=True)
class V37StagedOutput:
    """One named local output and its final immutable S3 object URI."""

    role: str
    path: pathlib.Path
    uri: str


@dataclasses.dataclass(frozen=True)
class V37StagedPhase:
    """Fully authenticated inputs and empty output paths for one local phase."""

    phase: str
    run_id: str
    source_commit: str
    workers: int
    inputs: tuple[V37StagedArtifact, ...]
    outputs: tuple[V37StagedOutput, ...]


@dataclasses.dataclass(frozen=True)
class V37WorkerInvocation:
    """Strict local capabilities admitted to the remote worker."""

    root: pathlib.Path
    plan: pathlib.Path
    manifest: pathlib.Path
    binary: pathlib.Path
    instance_id: str


def parse_v37_worker_args(arguments: Sequence[str]) -> V37WorkerInvocation:
    """Parse the explicit local-only worker boundary without abbreviation."""

    iterator = iter(arguments)
    try:
        next(iterator)
    except StopIteration as error:
        raise ValueError("V37 worker program name is absent") from error
    execute = False
    values: dict[str, str] = {}
    allowed = {"--root", "--plan", "--manifest", "--binary", "--instance-id"}
    for flag in iterator:
        if flag == "--execute-v37-worker":
            if execute:
                raise ValueError("V37 worker execute flag is duplicated")
            execute = True
            continue
        if flag not in allowed or flag in values:
            raise ValueError("V37 worker flag differs")
        try:
            value = next(iterator)
        except StopIteration as error:
            raise ValueError("V37 worker flag value is absent") from error
        if value.startswith("--"):
            raise ValueError("V37 worker flag value differs")
        values[flag] = value
    if not execute or set(values) != allowed:
        raise ValueError("V37 worker capabilities differ")
    paths = [pathlib.Path(values[flag]) for flag in ("--root", "--plan", "--manifest", "--binary")]
    if (
        any(not path.is_absolute() for path in paths)
        or len(set(paths)) != len(paths)
        or _INSTANCE_ID.fullmatch(values["--instance-id"]) is None
    ):
        raise ValueError("V37 worker local authority differs")
    return V37WorkerInvocation(
        root=paths[0],
        plan=paths[1],
        manifest=paths[2],
        binary=paths[3],
        instance_id=values["--instance-id"],
    )


def _s3_uri(value: str, *, prefix: bool = False) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or ".." in pathlib.PurePosixPath(parsed.path).parts
    ):
        raise ValueError("V37 S3 authority differs")
    key = parsed.path[1:]
    if key.endswith("/") != prefix:
        raise ValueError("V37 S3 prefix authority differs")
    return parsed.netloc, key


def phase_input_roles(phase: str) -> tuple[str, ...]:
    """Return the only local input roles admitted to one executable phase."""

    roles = {
        "preflight-training": ("v37-authority",),
        "build-ownership": (
            "v36-authority",
            "v36-execution-authority",
            "v36-receipt",
            "v36-source-registry",
            "v37-authority",
            "source",
        ),
        "evaluate-ceiling": (
            "ceiling-authority",
            "development-ground-truth",
            "ownership-tree",
            "ownership",
        ),
    }
    try:
        return roles[phase]
    except KeyError as error:
        raise ValueError("V37 phase differs") from error


def _phase_output_roles(phase: str) -> tuple[str, ...]:
    if phase == "preflight-training":
        return ()
    if phase == "build-ownership":
        return ("ownership-tree", "ownership")
    if phase == "evaluate-ceiling":
        return ("ceiling",)
    raise ValueError("V37 phase differs")


def _validate_v37_phase_manifest(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise ValueError("V37 phase manifest differs")
    expected_keys = {
        "claim_eligible",
        "inputs",
        "outputs",
        "phase",
        "run_id",
        "schema",
        "source_commit",
        "workers",
    }
    phase = value.get("phase")
    roles = phase_input_roles(str(phase))
    output_roles = _phase_output_roles(str(phase))
    inputs = value.get("inputs")
    outputs = value.get("outputs")
    identity_keys = {"blake3", "encoded_bytes", "role", "sha256", "uri"}
    output_keys = {"role", "uri"}
    if (
        set(value) != expected_keys
        or value.get("schema") != "borsuk-v37-spot-phase-manifest-v1"
        or value.get("claim_eligible") is not False
        or _TOKEN.fullmatch(str(value.get("run_id"))) is None
        or _LOWER_GIT_SHA1.fullmatch(str(value.get("source_commit"))) is None
        or type(value.get("workers")) is not int
        or not 1 <= value["workers"] <= 32
        or type(inputs) is not list
        or type(outputs) is not list
        or [item.get("role") if type(item) is dict else None for item in inputs]
        != list(roles)
        or [item.get("role") if type(item) is dict else None for item in outputs]
        != list(output_roles)
    ):
        raise ValueError("V37 phase manifest authority differs")
    uris: set[str] = set()
    for item in inputs:
        if (
            type(item) is not dict
            or set(item) != identity_keys
            or _LOWER_SHA256.fullmatch(str(item.get("sha256"))) is None
            or _LOWER_SHA256.fullmatch(str(item.get("blake3"))) is None
            or type(item.get("encoded_bytes")) is not int
            or item["encoded_bytes"] <= 0
        ):
            raise ValueError("V37 phase input authority differs")
        _s3_uri(str(item["uri"]))
        if item["uri"] in uris:
            raise ValueError("V37 phase input roles overlap")
        uris.add(str(item["uri"]))
    for item in outputs:
        if type(item) is not dict or set(item) != output_keys:
            raise ValueError("V37 phase output authority differs")
        _s3_uri(str(item["uri"]))
        if item["uri"] in uris:
            raise ValueError("V37 phase input/output roles overlap")
        uris.add(str(item["uri"]))
    return value


def canonical_v37_phase_manifest_bytes(value: object) -> bytes:
    """Validate and encode one exact phase manifest."""

    validated = _validate_v37_phase_manifest(value)
    return (
        json.dumps(validated, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
    )


def _validate_v37_plan_manifest_bytes(
    raw: bytes, plan: V37SpotPlan
) -> dict[str, object]:
    if (
        len(raw) != plan.manifest_bytes
        or hashlib.sha256(raw).hexdigest() != plan.manifest_sha256
    ):
        raise ValueError("V37 worker manifest authority differs")
    try:
        manifest = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 plan manifest JSON differs") from error
    if (
        canonical_v37_phase_manifest_bytes(manifest) != raw
        or manifest["phase"] != plan.phase
        or manifest["run_id"] != plan.run_id
        or manifest["source_commit"] != plan.source_commit
    ):
        raise ValueError("V37 plan manifest differs")
    return manifest


def stage_v37_phase_inputs(
    raw: bytes,
    root: pathlib.Path,
    download: Callable[[str, str, pathlib.Path], None],
) -> V37StagedPhase:
    """Download only exact manifest objects and authenticate before local use."""

    try:
        manifest = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 phase manifest JSON differs") from error
    if canonical_v37_phase_manifest_bytes(manifest) != raw:
        raise ValueError("V37 phase manifest bytes differ")
    input_root = root / "inputs"
    output_root = root / "outputs"
    input_root.mkdir(mode=0o700)
    output_root.mkdir(mode=0o700)
    staged_inputs = []
    import blake3

    for identity in manifest["inputs"]:
        path = input_root / identity["role"]
        bucket, key = _s3_uri(identity["uri"])
        download(bucket, key, path)
        sha256 = hashlib.sha256()
        blake3_digest = blake3.blake3()
        encoded_bytes = 0
        with path.open("rb") as input_file:
            while True:
                chunk = input_file.read(1_048_576)
                if not chunk:
                    break
                encoded_bytes += len(chunk)
                sha256.update(chunk)
                blake3_digest.update(chunk)
        if (
            encoded_bytes != identity["encoded_bytes"]
            or sha256.hexdigest() != identity["sha256"]
            or blake3_digest.hexdigest() != identity["blake3"]
        ):
            raise ValueError(f"V37 {identity['role']} staged bytes differ")
        staged_inputs.append(
            V37StagedArtifact(
                role=identity["role"],
                path=path,
                uri=identity["uri"],
                sha256=identity["sha256"],
                blake3=identity["blake3"],
                encoded_bytes=identity["encoded_bytes"],
            )
        )
    staged_outputs = tuple(
        V37StagedOutput(
            role=output["role"],
            path=output_root / output["role"],
            uri=output["uri"],
        )
        for output in manifest["outputs"]
    )
    return V37StagedPhase(
        phase=manifest["phase"],
        run_id=manifest["run_id"],
        source_commit=manifest["source_commit"],
        workers=manifest["workers"],
        inputs=tuple(staged_inputs),
        outputs=staged_outputs,
    )


def build_v37_binary_command(
    binary: pathlib.Path, staged: V37StagedPhase
) -> list[str]:
    """Build the local-only Rust CLI invocation from authenticated objects."""

    command = [
        str(binary),
        "--execute-v37-local",
        "--mode",
        staged.phase,
        "--workers",
        str(staged.workers),
    ]
    for item in staged.inputs:
        command.extend(
            [
                f"--{item.role}",
                str(item.path),
                f"--{item.role}-uri",
                item.uri,
                f"--{item.role}-sha256",
                item.sha256,
                f"--{item.role}-blake3",
                item.blake3,
                f"--{item.role}-bytes",
                str(item.encoded_bytes),
            ]
        )
    for output in staged.outputs:
        command.extend([f"--{output.role}-output", str(output.path)])
    return command


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
    )


def _local_identity(item: V37StagedArtifact) -> dict[str, object]:
    return {
        "blake3": item.blake3,
        "encoded_bytes": item.encoded_bytes,
        "role": item.role,
        "sha256": item.sha256,
        "uri": item.uri,
    }


def validate_v37_local_result_bytes(
    raw: bytes, staged: V37StagedPhase
) -> dict[str, object]:
    """Recompute native phase evidence before any result is published."""

    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 native result JSON differs") from error
    if type(value) is not dict or _canonical_json_bytes(value) != raw:
        raise ValueError("V37 native result authority differs")
    if staged.phase == "evaluate-ceiling":
        return _validate_v37_ceiling_result(value, raw, staged)
    if staged.phase not in {"preflight-training", "build-ownership"}:
        raise ValueError("V37 native result phase differs")

    evidence_keys = {
        "dimensions",
        "fma_backend",
        "leaf_count",
        "partition_coordinate_scores",
        "partition_coordinate_scores_per_second",
        "partition_scoring_elapsed_ns",
        "rows",
        "training_elapsed_ns",
    }
    identity_keys = {"blake3", "encoded_bytes", "role", "sha256", "uri"}
    result_keys = {
        "artifacts",
        "claim_eligible",
        "inputs",
        "mode",
        "schema",
        "training_evidence",
    }
    if staged.phase == "preflight-training":
        result_keys.add("preflight_evidence")
    if (
        set(value) != result_keys
        or value.get("schema") != "borsuk-v37-local-result-v3"
        or value.get("claim_eligible") is not False
        or value.get("mode") != staged.phase
        or value.get("inputs") != [_local_identity(item) for item in staged.inputs]
        or type(value.get("artifacts")) is not list
        or type(value.get("training_evidence")) is not dict
        or set(value["training_evidence"]) != evidence_keys
    ):
        raise ValueError("V37 native result authority differs")
    expected_artifacts = []
    import blake3

    for output in staged.outputs:
        raw_output = output.path.read_bytes()
        expected_artifacts.append(
            {
                "blake3": blake3.blake3(raw_output).hexdigest(),
                "encoded_bytes": len(raw_output),
                "role": output.role,
                "sha256": hashlib.sha256(raw_output).hexdigest(),
                "uri": f"file://{output.path}",
            }
        )
    if value["artifacts"] != expected_artifacts or any(
        type(item) is not dict or set(item) != identity_keys
        for item in value["artifacts"]
    ):
        raise ValueError("V37 native result artifacts differ")
    evidence = value["training_evidence"]
    numeric = (
        "dimensions",
        "leaf_count",
        "partition_coordinate_scores",
        "partition_coordinate_scores_per_second",
        "partition_scoring_elapsed_ns",
        "rows",
        "training_elapsed_ns",
    )
    if (
        any(type(evidence[key]) is not int or evidence[key] <= 0 for key in numeric)
        or evidence["leaf_count"] < 2
        or evidence["fma_backend"] not in {"aarch64-neon-fma", "x86-avx-fma"}
        or evidence["training_elapsed_ns"] < evidence["partition_scoring_elapsed_ns"]
    ):
        raise ValueError("V37 native training evidence differs")
    throughput = (
        evidence["partition_coordinate_scores"] * 1_000_000_000
        // evidence["partition_scoring_elapsed_ns"]
    )
    if (
        evidence["partition_coordinate_scores_per_second"] != throughput
        or throughput < MINIMUM_COORDINATE_SCORES_PER_SECOND
    ):
        raise ValueError("V37 native partition throughput differs")
    if staged.phase == "preflight-training" and (
        evidence["rows"] != 65_536
        or evidence["dimensions"] != 192
        or evidence["leaf_count"] != 16
        or value["artifacts"]
        or len(staged.inputs) != 1
        or staged.inputs[0].role != "v37-authority"
        or staged.outputs
    ):
        raise ValueError("V37 native preflight evidence differs")
    if staged.phase == "preflight-training":
        preflight = value["preflight_evidence"]
        preflight_keys = {
            "coordinate_generator",
            "coordinate_sha256",
            "projected_construction_bytes",
            "scalar_fused_comparisons",
            "scalar_fused_max_ulp_delta",
        }
        if (
            type(preflight) is not dict
            or set(preflight) != preflight_keys
            or preflight.get("coordinate_generator") != "splitmix23-f32-v1"
            or preflight.get("coordinate_sha256") != PREFLIGHT_COORDINATE_SHA256
            or preflight.get("projected_construction_bytes") != 2_137_615_120
            or preflight.get("scalar_fused_comparisons") != 15_360
            or preflight.get("scalar_fused_max_ulp_delta") != 0
            or evidence["partition_coordinate_scores"] != PREFLIGHT_COORDINATE_SCORES
        ):
            raise ValueError("V37 native preflight evidence differs")
    return value


def _validate_v37_ceiling_result(
    value: dict[str, object], raw: bytes, staged: V37StagedPhase
) -> dict[str, object]:
    identity_keys = {"blake3", "encoded_bytes", "role", "sha256", "uri"}
    authority_keys = {
        "construction_authority",
        "development_ground_truth",
        "gt_neighbors",
        "ownership",
        "ownership_tree",
        "query_count",
        "schema",
        "selected_postings",
    }
    ceiling_keys = {
        "aggregate_gate_ppm",
        "aggregate_recall_ppm",
        "claim_eligible",
        "disposition",
        "gt_neighbors",
        "minimum_gate_ppm",
        "minimum_recall_ppm",
        "passed",
        "samples",
        "schema",
        "selected_postings_limit",
    }
    sample_keys = {
        "hits",
        "query_ordinal",
        "recall_ppm",
        "selected_postings",
    }
    if (
        set(value)
        != {"ceiling", "ceiling_authority", "claim_eligible", "inputs", "schema"}
        or value.get("schema") != "borsuk-v37-bound-ceiling-v1"
        or value.get("claim_eligible") is not False
        or type(value.get("inputs")) is not dict
        or type(value.get("ceiling")) is not dict
    ):
        raise ValueError("V37 native ceiling result authority differs")
    inputs = value["inputs"]
    ceiling = value["ceiling"]
    staged_by_role = {item.role: _local_identity(item) for item in staged.inputs}
    if (
        set(inputs) != authority_keys
        or inputs.get("schema") != "borsuk-v37-ceiling-authority-v1"
        or inputs.get("gt_neighbors") != 100
        or inputs.get("selected_postings") != 14
        or type(inputs.get("query_count")) is not int
        or inputs["query_count"] <= 0
        or value.get("ceiling_authority") != staged_by_role.get("ceiling-authority")
        or inputs.get("development_ground_truth")
        != staged_by_role.get("development-ground-truth")
        or inputs.get("ownership_tree") != staged_by_role.get("ownership-tree")
        or inputs.get("ownership") != staged_by_role.get("ownership")
    ):
        raise ValueError("V37 native ceiling input binding differs")
    construction = inputs.get("construction_authority")
    if (
        type(construction) is not dict
        or set(construction) != identity_keys
        or construction.get("role") != "v37-authority"
        or _LOWER_SHA256.fullmatch(str(construction.get("sha256"))) is None
        or _LOWER_SHA256.fullmatch(str(construction.get("blake3"))) is None
        or type(construction.get("encoded_bytes")) is not int
        or construction["encoded_bytes"] <= 0
        or not str(construction.get("uri", "")).startswith("s3://")
    ):
        raise ValueError("V37 native construction authority differs")
    if (
        set(ceiling) != ceiling_keys
        or ceiling.get("schema") != "borsuk-v37-layout-ceiling-v1"
        or ceiling.get("claim_eligible") is not False
        or ceiling.get("gt_neighbors") != 100
        or ceiling.get("selected_postings_limit") != 14
        or ceiling.get("aggregate_gate_ppm") != 998_000
        or ceiling.get("minimum_gate_ppm") != 800_000
        or type(ceiling.get("samples")) is not list
        or len(ceiling["samples"]) != inputs["query_count"]
        or not ceiling["samples"]
    ):
        raise ValueError("V37 native ceiling authority differs")
    total_hits = 0
    minimum_recall = 1_000_000
    previous_query = -1
    for sample in ceiling["samples"]:
        if type(sample) is not dict or set(sample) != sample_keys:
            raise ValueError("V37 native ceiling sample differs")
        query = sample.get("query_ordinal")
        hits = sample.get("hits")
        selected = sample.get("selected_postings")
        if (
            type(query) is not int
            or query <= previous_query
            or type(hits) is not int
            or not 0 <= hits <= 100
            or type(selected) is not list
            or not 1 <= len(selected) <= 14
            or any(type(posting) is not int or posting < 0 for posting in selected)
            or len(set(selected)) != len(selected)
            or sample.get("recall_ppm") != hits * 10_000
        ):
            raise ValueError("V37 native ceiling sample differs")
        previous_query = query
        total_hits += hits
        minimum_recall = min(minimum_recall, sample["recall_ppm"])
    aggregate = total_hits * 1_000_000 // (len(ceiling["samples"]) * 100)
    passed = aggregate >= 998_000 and minimum_recall >= 800_000
    disposition = "ceiling-passed" if passed else "layout-rejected"
    if (
        ceiling.get("aggregate_recall_ppm") != aggregate
        or ceiling.get("minimum_recall_ppm") != minimum_recall
        or ceiling.get("passed") is not passed
        or ceiling.get("disposition") != disposition
    ):
        raise ValueError("V37 native ceiling aggregate differs")
    if len(staged.outputs) != 1 or staged.outputs[0].role != "ceiling":
        raise ValueError("V37 native ceiling output differs")
    if staged.outputs[0].path.read_bytes() != raw:
        raise ValueError("V37 native ceiling output bytes differ")
    return value


def validate_v37_progress_bytes(
    raw: bytes, previous: dict[str, object] | None
) -> dict[str, object]:
    """Validate one exact native progress snapshot and its predecessor."""

    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 native progress JSON differs") from error
    keys = {
        "completed_internal_nodes",
        "partition_coordinate_scores",
        "schema",
        "sequence",
        "total_internal_nodes",
    }
    if (
        type(value) is not dict
        or _canonical_json_bytes(value) != raw
        or set(value) != keys
        or value.get("schema") != "borsuk-v37-training-progress-v1"
        or any(
            type(value.get(key)) is not int or value[key] <= 0
            for key in keys - {"schema"}
        )
        or value["sequence"] != value["completed_internal_nodes"]
        or value["completed_internal_nodes"] > value["total_internal_nodes"]
    ):
        raise ValueError("V37 native progress authority differs")
    if previous is None:
        if value["sequence"] != 1:
            raise ValueError("V37 native progress initial sequence differs")
    elif (
        value["sequence"] != previous["sequence"] + 1
        or value["completed_internal_nodes"]
        != previous["completed_internal_nodes"] + 1
        or value["partition_coordinate_scores"]
        <= previous["partition_coordinate_scores"]
        or value["total_internal_nodes"] != previous["total_internal_nodes"]
    ):
        raise ValueError("V37 native progress sequence differs")
    return value


def publish_v37_worker_success(
    plan: V37SpotPlan,
    staged: V37StagedPhase,
    *,
    instance_id: str,
    result_raw: bytes,
    final_progress_raw: bytes | None,
    monitor: dict[str, int],
    upload_once: Callable[[str, str, bytes], None],
) -> bytes:
    """Validate and create every successful worker object, terminal last."""

    if (
        staged.phase != plan.phase
        or staged.run_id != plan.run_id
        or staged.source_commit != plan.source_commit
        or not instance_id
    ):
        raise ValueError("V37 staged worker authority differs")
    result = validate_v37_local_result_bytes(result_raw, staged)
    final_progress = None
    if staged.phase in _TRAINING_PHASES:
        if final_progress_raw is None:
            raise ValueError("V37 final progress is absent")
        try:
            final_progress = json.loads(final_progress_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("V37 final progress JSON differs") from error
        progress_keys = {
            "completed_internal_nodes",
            "partition_coordinate_scores",
            "schema",
            "sequence",
            "total_internal_nodes",
        }
        evidence = result["training_evidence"]
        if (
            type(final_progress) is not dict
            or _canonical_json_bytes(final_progress) != final_progress_raw
            or set(final_progress) != progress_keys
            or final_progress.get("schema") != "borsuk-v37-training-progress-v1"
            or final_progress.get("sequence")
            != final_progress.get("completed_internal_nodes")
            or final_progress.get("completed_internal_nodes")
            != final_progress.get("total_internal_nodes")
            or final_progress.get("total_internal_nodes") != evidence["leaf_count"] - 1
            or final_progress.get("partition_coordinate_scores")
            != evidence["partition_coordinate_scores"]
        ):
            raise ValueError("V37 final progress authority differs")
    elif final_progress_raw is not None:
        raise ValueError("V37 ceiling progress must be absent")

    import blake3

    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
    artifacts = []
    for output in staged.outputs:
        expected_uri = f"{plan.output_prefix}{output.role}"
        if output.uri != expected_uri:
            raise ValueError("V37 worker output URI differs")
        body = output.path.read_bytes()
        identity = {
            "blake3": blake3.blake3(body).hexdigest(),
            "encoded_bytes": len(body),
            "role": output.role,
            "sha256": hashlib.sha256(body).hexdigest(),
            "uri": output.uri,
        }
        if identity["encoded_bytes"] == 0:
            raise ValueError("V37 worker output is empty")
        upload_once(bucket, prefix + output.role, body)
        artifacts.append(identity)

    result_identity = {
        "encoded_bytes": len(result_raw),
        "role": "local-result",
        "sha256": hashlib.sha256(result_raw).hexdigest(),
        "uri": f"{plan.output_prefix}local-result.json",
    }
    progress_identity = None
    if final_progress_raw is not None:
        progress_identity = {
            "encoded_bytes": len(final_progress_raw),
            "role": "progress",
            "sha256": hashlib.sha256(final_progress_raw).hexdigest(),
            "uri": f"{plan.output_prefix}progress.json",
        }
    upload_once(bucket, prefix + "local-result.json", result_raw)
    if final_progress_raw is not None:
        upload_once(bucket, prefix + "progress.json", final_progress_raw)
    terminal = {
        "artifacts": artifacts,
        "binary_bytes": plan.binary_bytes,
        "binary_sha256": plan.binary_sha256,
        "binary_uri": plan.binary_uri,
        "claim_eligible": False,
        "instance_id": instance_id,
        "manifest_bytes": plan.manifest_bytes,
        "manifest_sha256": plan.manifest_sha256,
        "manifest_uri": plan.manifest_uri,
        "monitor": monitor,
        "phase": plan.phase,
        "progress": progress_identity,
        "result": result_identity,
        "run_id": plan.run_id,
        "schema": "borsuk-v37-spot-terminal-v2",
        "source_archive_bytes": plan.source_archive_bytes,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_uri": plan.source_archive_uri,
        "source_commit": plan.source_commit,
        "status": "complete",
    }
    terminal_raw = canonical_v37_terminal_bytes(terminal, plan)
    upload_once(bucket, prefix + "ATTEMPT_TERMINAL.json", terminal_raw)
    return terminal_raw


def execute_v37_worker(
    plan: V37SpotPlan,
    *,
    root: pathlib.Path,
    manifest_raw: bytes,
    binary: pathlib.Path,
    instance_id: str,
    download: Callable[[str, str, pathlib.Path], None],
    execute: Callable[
        [list[str], V37StagedPhase, pathlib.Path, pathlib.Path],
        tuple[int, V37MonitorSample],
    ],
    upload_once: Callable[[str, str, bytes], None],
) -> bytes:
    """Execute one authenticated native phase and clean its named local files."""

    _validate_v37_plan_manifest_bytes(manifest_raw, plan)
    if root.exists():
        raise ValueError("V37 worker manifest authority differs")
    root.mkdir(mode=0o700)
    stdout_path = root / "local-result.json"
    progress_log_path = root / "progress.log"
    final_progress_path = root / "progress.json"
    staged: V37StagedPhase | None = None
    try:
        staged = stage_v37_phase_inputs(manifest_raw, root, download)
        command = build_v37_binary_command(binary, staged)
        returncode, sample = execute(command, staged, stdout_path, progress_log_path)
        if returncode != 0:
            raise RuntimeError(f"V37 native worker exited {returncode}")
        stop = classify_v37_monitor_sample(sample)
        if stop is not None:
            raise RuntimeError(f"V37 native worker crossed {stop}")
        result_raw = stdout_path.read_bytes()
        result = validate_v37_local_result_bytes(result_raw, staged)
        final_progress_raw = None
        if staged.phase in _TRAINING_PHASES:
            previous = None
            progress_lines = progress_log_path.read_bytes().splitlines(keepends=True)
            if not progress_lines:
                raise ValueError("V37 native progress is absent")
            for line in progress_lines:
                previous = validate_v37_progress_bytes(line, previous)
            if (
                previous is None
                or previous["completed_internal_nodes"]
                != previous["total_internal_nodes"]
                or previous["partition_coordinate_scores"]
                != result["training_evidence"]["partition_coordinate_scores"]
                or sample.coordinate_scores_per_second
                != result["training_evidence"][
                    "partition_coordinate_scores_per_second"
                ]
            ):
                raise ValueError("V37 native terminal progress differs")
            final_progress_raw = progress_lines[-1]
            final_progress_path.write_bytes(final_progress_raw)
        elif progress_log_path.exists() and progress_log_path.stat().st_size != 0:
            raise ValueError("V37 ceiling progress must be absent")
        monitor = {
            "elapsed_milliseconds": round(sample.elapsed_seconds * 1_000),
            "peak_psi_full_ppm": round(sample.psi_full_avg10 * 1_000_000),
            "peak_rss_bytes": sample.rss_bytes,
            "swap_end_bytes": sample.swap_bytes,
            "swap_start_bytes": 0,
        }
        return publish_v37_worker_success(
            plan,
            staged,
            instance_id=instance_id,
            result_raw=result_raw,
            final_progress_raw=final_progress_raw,
            monitor=monitor,
            upload_once=upload_once,
        )
    finally:
        if staged is not None:
            for item in staged.inputs:
                item.path.unlink(missing_ok=True)
            for output in staged.outputs:
                output.path.unlink(missing_ok=True)
        for path in (stdout_path, progress_log_path, final_progress_path):
            path.unlink(missing_ok=True)
        for directory in (root / "inputs", root / "outputs", root):
            try:
                directory.rmdir()
            except FileNotFoundError:
                pass


def build_v37_spot_plan(**values: Any) -> V37SpotPlan:
    """Validate one launch plan without consulting mutable AWS state."""

    plan = V37SpotPlan(**values)
    if (
        plan.phase not in {
            "preflight-training",
            "build-ownership",
            "evaluate-ceiling",
        }
        or _TOKEN.fullmatch(plan.run_id) is None
        or _LOWER_GIT_SHA1.fullmatch(plan.source_commit) is None
        or _LOWER_SHA256.fullmatch(plan.source_archive_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.binary_sha256) is None
        or _LOWER_SHA256.fullmatch(plan.manifest_sha256) is None
        or min(plan.source_archive_bytes, plan.binary_bytes, plan.manifest_bytes) <= 0
    ):
        raise ValueError("V37 Spot plan authority differs")
    for uri in (plan.source_archive_uri, plan.binary_uri, plan.manifest_uri):
        _s3_uri(uri)
    _s3_uri(plan.output_prefix, prefix=True)
    return plan


def _aws_error_code(error: BaseException) -> str | None:
    response = getattr(error, "response", None)
    if type(response) is not dict or type(response.get("Error")) is not dict:
        return None
    code = response["Error"].get("Code")
    return code if type(code) is str else None


def build_v37_launch_specs(
    plan: V37SpotPlan, *, user_data: str
) -> list[dict[str, Any]]:
    """Build one Spot-only launch request per preregistered availability zone."""

    build_v37_spot_plan(**dataclasses.asdict(plan))
    if not user_data.startswith("#!/") or "shutdown -h now" not in user_data:
        raise ValueError("V37 worker user data differs")
    market = {
        "MarketType": "spot",
        "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate",
            "SpotInstanceType": "one-time",
        },
    }
    specs = []
    for ordinal, target in enumerate(SPOT_TARGETS):
        token_authority = json.dumps(
            {
                "plan": dataclasses.asdict(plan),
                "target": dataclasses.asdict(target),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        specs.append({
            "ImageId": AMI_ID,
            "InstanceType": INSTANCE_TYPE,
            "MinCount": 1,
            "MaxCount": 1,
            "ClientToken": "borsuk-v37-"
            + hashlib.sha256(token_authority + bytes([ordinal])).hexdigest()[:48],
            "SubnetId": target.subnet_id,
            "Placement": {"AvailabilityZone": target.availability_zone},
            "SecurityGroupIds": [SECURITY_GROUP_ID],
            "IamInstanceProfile": {"Name": INSTANCE_PROFILE},
            "InstanceInitiatedShutdownBehavior": "terminate",
            "InstanceMarketOptions": market,
            "MetadataOptions": {
                "HttpEndpoint": "enabled",
                "HttpPutResponseHopLimit": 1,
                "HttpTokens": "required",
            },
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "Encrypted": True,
                        "Iops": 3000,
                        "Throughput": 250,
                        "VolumeSize": 200,
                        "VolumeType": "gp3",
                    },
                }
            ],
            "UserData": user_data,
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": plan.run_id},
                        {"Key": "borsuk-purpose", "Value": f"v37-{plan.phase}"},
                    ],
                }
            ],
        })
    return specs


def validate_v37_relation_predecessors(
    ceiling: dict[str, object], direct: dict[str, object]
) -> None:
    """Admit relation construction only after bound ceiling/direct evidence."""

    bindings = (
        "source_sha256",
        "projection_sha256",
        "ownership_tree_sha256",
        "ownership_sha256",
    )
    if (
        ceiling.get("schema") != "borsuk-v37-ceiling-passed-v1"
        or ceiling.get("passed") is not True
        or direct.get("schema") != "borsuk-v37-direct-failed-v1"
        or direct.get("passed") is not False
        or any(
            type(ceiling.get(key)) is not str
            or _LOWER_SHA256.fullmatch(str(ceiling[key])) is None
            or direct.get(key) != ceiling[key]
            for key in bindings
        )
    ):
        raise ValueError("V37 relation predecessor authority differs")


def validate_v37_preflight_admission(
    *,
    build_plan: V37SpotPlan,
    build_manifest_raw: bytes,
    preflight_manifest_raw: bytes,
    preflight_result_raw: bytes,
    preflight_terminal_raw: bytes,
) -> dict[str, object]:
    """Admit a 1M build only from one bound successful reduced preflight."""

    if build_plan.phase != "build-ownership":
        raise ValueError("V37 preflight successor phase differs")
    if (
        len(build_manifest_raw) != build_plan.manifest_bytes
        or hashlib.sha256(build_manifest_raw).hexdigest()
        != build_plan.manifest_sha256
    ):
        raise ValueError("V37 build manifest authority differs")
    try:
        build_manifest = json.loads(build_manifest_raw)
        terminal = json.loads(preflight_terminal_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 preflight admission JSON differs") from error
    if (
        canonical_v37_phase_manifest_bytes(build_manifest) != build_manifest_raw
        or build_manifest["phase"] != build_plan.phase
        or build_manifest["run_id"] != build_plan.run_id
        or build_manifest["source_commit"] != build_plan.source_commit
    ):
        raise ValueError("V37 preflight successor manifest differs")
    if (
        type(terminal) is not dict
        or terminal.get("phase") != "preflight-training"
        or terminal.get("status") != "complete"
        or type(terminal.get("result")) is not dict
        or not str(terminal["result"].get("uri", "")).endswith("local-result.json")
    ):
        raise ValueError("V37 preflight admission authority differs")
    output_prefix = str(terminal["result"]["uri"]).removesuffix("local-result.json")
    preflight_plan = build_v37_spot_plan(
        phase="preflight-training",
        run_id=terminal.get("run_id"),
        source_commit=terminal.get("source_commit"),
        source_archive_uri=terminal.get("source_archive_uri"),
        source_archive_sha256=terminal.get("source_archive_sha256"),
        source_archive_bytes=terminal.get("source_archive_bytes"),
        binary_uri=terminal.get("binary_uri"),
        binary_sha256=terminal.get("binary_sha256"),
        binary_bytes=terminal.get("binary_bytes"),
        manifest_uri=terminal.get("manifest_uri"),
        manifest_sha256=terminal.get("manifest_sha256"),
        manifest_bytes=terminal.get("manifest_bytes"),
        output_prefix=output_prefix,
    )
    validate_v37_terminal_bytes(
        preflight_terminal_raw, preflight_plan, "complete"
    )
    if (
        preflight_plan.source_commit != build_plan.source_commit
        or preflight_plan.source_archive_uri != build_plan.source_archive_uri
        or preflight_plan.source_archive_sha256 != build_plan.source_archive_sha256
        or preflight_plan.source_archive_bytes != build_plan.source_archive_bytes
        or preflight_plan.binary_uri != build_plan.binary_uri
        or preflight_plan.binary_sha256 != build_plan.binary_sha256
        or preflight_plan.binary_bytes != build_plan.binary_bytes
        or len(preflight_manifest_raw) != preflight_plan.manifest_bytes
        or hashlib.sha256(preflight_manifest_raw).hexdigest()
        != preflight_plan.manifest_sha256
        or len(preflight_result_raw) != terminal["result"].get("encoded_bytes")
        or hashlib.sha256(preflight_result_raw).hexdigest()
        != terminal["result"].get("sha256")
    ):
        raise ValueError("V37 preflight admission binding differs")
    try:
        preflight_manifest = json.loads(preflight_manifest_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 preflight manifest JSON differs") from error
    if canonical_v37_phase_manifest_bytes(preflight_manifest) != preflight_manifest_raw:
        raise ValueError("V37 preflight manifest bytes differ")
    preflight_inputs = tuple(
        V37StagedArtifact(
            role=item["role"],
            path=pathlib.Path("/authenticated") / item["role"],
            uri=item["uri"],
            sha256=item["sha256"],
            blake3=item["blake3"],
            encoded_bytes=item["encoded_bytes"],
        )
        for item in preflight_manifest["inputs"]
    )
    staged = V37StagedPhase(
        phase="preflight-training",
        run_id=preflight_manifest["run_id"],
        source_commit=preflight_manifest["source_commit"],
        workers=preflight_manifest["workers"],
        inputs=preflight_inputs,
        outputs=(),
    )
    result = validate_v37_local_result_bytes(preflight_result_raw, staged)
    build_v37_input = next(
        (item for item in build_manifest["inputs"] if item["role"] == "v37-authority"),
        None,
    )
    if (
        preflight_manifest["source_commit"] != build_plan.source_commit
        or preflight_manifest["workers"] != build_manifest["workers"]
        or result["inputs"] != preflight_manifest["inputs"]
        or build_v37_input != preflight_manifest["inputs"][0]
    ):
        raise ValueError("V37 preflight successor binding differs")
    return result


def classify_v37_monitor_sample(sample: V37MonitorSample) -> str | None:
    """Classify the first registered stop without inferring process outcome."""

    if sample.elapsed_seconds > SCIENCE_TIMEOUT_SECONDS:
        return "science-timeout"
    if sample.last_progress_seconds > PROGRESS_TIMEOUT_SECONDS:
        return "progress-timeout"
    if (
        sample.coordinate_scores_per_second is not None
        and sample.coordinate_scores_per_second
        < MINIMUM_COORDINATE_SCORES_PER_SECOND
    ):
        return "throughput-stop"
    if sample.rss_bytes > MEMORY_LIMIT_BYTES:
        return "rss-stop"
    if sample.psi_full_avg10 > MEMORY_PSI_FULL_LIMIT:
        return "psi-stop"
    if sample.swap_bytes > 0:
        return "swap-stop"
    return None


def _v37_cgroup_root() -> pathlib.Path:
    for line in pathlib.Path("/proc/self/cgroup").read_text().splitlines():
        fields = line.split(":", 2)
        if fields[:2] == ["0", ""]:
            return pathlib.Path("/sys/fs/cgroup") / fields[2].lstrip("/")
    raise RuntimeError("V37 cgroup v2 authority is absent")


def _v37_slice_unit(run_id: str) -> str:
    if _TOKEN.fullmatch(run_id) is None:
        raise ValueError("V37 slice run identity differs")
    suffix = hashlib.sha256(run_id.encode()).hexdigest()[:24]
    return f"borsuk-v37-{suffix}.slice"


def _v37_systemd_control_group(unit: str) -> pathlib.Path:
    result = subprocess.run(
        ["systemctl", "show", "--property=ControlGroup", "--value", unit],
        check=True,
        capture_output=True,
        text=True,
    )
    control_group = result.stdout.strip()
    if (
        not control_group.startswith("/")
        or control_group == "/"
        or ".." in pathlib.PurePosixPath(control_group).parts
    ):
        raise RuntimeError("V37 aggregate cgroup is absent")
    return pathlib.Path("/sys/fs/cgroup") / control_group.lstrip("/")


def _v37_psi_full_avg10() -> float:
    for line in pathlib.Path("/proc/pressure/memory").read_text().splitlines():
        if line.startswith("full "):
            for field in line.split()[1:]:
                if field.startswith("avg10="):
                    return float(field.removeprefix("avg10="))
    raise RuntimeError("V37 memory PSI authority is absent")


def run_v37_native_process(
    command: list[str],
    stdout_path: pathlib.Path,
    progress_path: pathlib.Path,
    *,
    poll_seconds: float = 1.0,
    memory_cgroup: pathlib.Path | None = None,
    terminate_command: list[str] | None = None,
) -> tuple[int, V37MonitorSample]:
    """Run and pressure-monitor one native child while preserving its output."""

    if not command or poll_seconds <= 0 or stdout_path == progress_path:
        raise ValueError("V37 native process authority differs")
    cgroup = memory_cgroup if memory_cgroup is not None else _v37_cgroup_root()
    started = time.monotonic()
    last_progress = started
    observed_lines = 0
    previous_progress = None
    peak_psi = _v37_psi_full_avg10()
    with stdout_path.open("xb") as stdout, progress_path.open("xb") as progress:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=progress,
            close_fds=True,
            start_new_session=True,
        )
        terminated = False

        def terminate() -> None:
            nonlocal terminated
            if terminated:
                return
            terminated = True
            if terminate_command is not None:
                subprocess.run(terminate_command, check=False)
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)

        try:
            cgroup_deadline = time.monotonic() + 30
            while not (cgroup / "memory.current").is_file() or not (
                cgroup / "memory.swap.current"
            ).is_file():
                if process.poll() is not None or time.monotonic() >= cgroup_deadline:
                    raise RuntimeError("V37 native science cgroup is absent")
                time.sleep(min(poll_seconds, 0.1))
            swap_start = int((cgroup / "memory.swap.current").read_text().strip())
            peak_rss = int((cgroup / "memory.current").read_text().strip())
            peak_swap = swap_start

            def read_cgroup_counter(name: str, previous: int) -> int:
                try:
                    return int((cgroup / name).read_text().strip())
                except FileNotFoundError:
                    return previous

            while True:
                returncode = process.poll()
                progress.flush()
                lines = progress_path.read_bytes().splitlines(keepends=True)
                complete_lines = [line for line in lines if line.endswith(b"\n")]
                for line in complete_lines[observed_lines:]:
                    previous_progress = validate_v37_progress_bytes(
                        line, previous_progress
                    )
                    last_progress = time.monotonic()
                observed_lines = len(complete_lines)
                now = time.monotonic()
                peak_rss = max(
                    peak_rss, read_cgroup_counter("memory.current", peak_rss)
                )
                peak_psi = max(peak_psi, _v37_psi_full_avg10())
                peak_swap = max(
                    peak_swap,
                    read_cgroup_counter("memory.swap.current", peak_swap),
                )
                sample = V37MonitorSample(
                    elapsed_seconds=now - started,
                    last_progress_seconds=now - last_progress,
                    coordinate_scores_per_second=None,
                    rss_bytes=peak_rss,
                    psi_full_avg10=peak_psi,
                    swap_bytes=peak_swap - swap_start,
                )
                stop = classify_v37_monitor_sample(sample)
                if stop is not None and returncode is None:
                    terminate()
                    return process.returncode, sample
                if returncode is not None:
                    return returncode, sample
                time.sleep(poll_seconds)
        finally:
            if process.poll() is None:
                terminate()


def build_v37_science_service_command(
    native_command: list[str],
    staged: V37StagedPhase,
    *,
    binary: pathlib.Path,
    inputs: pathlib.Path,
    outputs: pathlib.Path,
) -> list[str]:
    """Wrap native science in one unprivileged file-only systemd service."""

    if (
        not native_command
        or native_command[0] != str(binary)
        or _TOKEN.fullmatch(staged.run_id) is None
        or inputs == outputs
        or not inputs.is_dir()
        or not outputs.is_dir()
    ):
        raise ValueError("V37 native science service authority differs")
    properties = [
        "User=nobody",
        "Group=nogroup",
        "PrivateNetwork=yes",
        "ProtectSystem=strict",
        "ProtectHome=yes",
        "NoNewPrivileges=yes",
        "PrivateDevices=yes",
        "CapabilityBoundingSet=",
        "RestrictAddressFamilies=AF_UNIX",
        f"MemoryMax={MEMORY_LIMIT_BYTES}",
        "MemorySwapMax=0",
        "MemoryAccounting=yes",
        f"RuntimeMaxSec={SCIENCE_TIMEOUT_SECONDS}",
        f"ReadOnlyPaths={inputs}",
        f"ReadOnlyPaths={binary}",
    ]
    if staged.outputs:
        properties.append(f"ReadWritePaths={outputs}")
    command = [
        "systemd-run",
        "--wait",
        "--pipe",
        f"--unit=borsuk-v37-science-{staged.run_id}",
        f"--slice={_v37_slice_unit(staged.run_id)}",
        "--quiet",
    ]
    command.extend(f"--property={property_value}" for property_value in properties)
    command.extend(native_command)
    return command


def execute_v37_native_science(
    native_command: list[str],
    staged: V37StagedPhase,
    stdout_path: pathlib.Path,
    progress_path: pathlib.Path,
    *,
    runner: Callable[
        [list[str], pathlib.Path, pathlib.Path], tuple[int, V37MonitorSample]
    ]
    | None = None,
) -> tuple[int, V37MonitorSample]:
    """Execute native science only through its systemd capability boundary."""

    if not staged.inputs or not native_command:
        raise ValueError("V37 native science paths differ")
    inputs = staged.inputs[0].path.parent
    outputs = inputs.parent / "outputs"
    if (
        not outputs.is_dir()
        or any(item.path.parent != inputs for item in staged.inputs)
        or any(item.path.parent != outputs for item in staged.outputs)
    ):
        raise ValueError("V37 native science paths differ")
    for item in staged.inputs:
        item.path.chmod(0o444)
    inputs.parent.chmod(0o711)
    inputs.chmod(0o555)
    outputs.chmod(0o733 if staged.outputs else 0o555)
    command = build_v37_science_service_command(
        native_command,
        staged,
        binary=pathlib.Path(native_command[0]),
        inputs=inputs,
        outputs=outputs,
    )
    if runner is not None:
        return runner(command, stdout_path, progress_path)
    unit = f"borsuk-v37-science-{staged.run_id}.service"
    cgroup = _v37_systemd_control_group(_v37_slice_unit(staged.run_id))
    try:
        return run_v37_native_process(
            command,
            stdout_path,
            progress_path,
            memory_cgroup=cgroup,
            terminate_command=[
                "systemctl",
                "kill",
                "--kill-who=all",
                "--signal=TERM",
                unit,
            ],
        )
    finally:
        subprocess.run(["systemctl", "reset-failed", unit], check=False)


def build_v37_worker_script(plan: V37SpotPlan) -> str:
    """Render the bounded boot wrapper for one original worker process group."""

    build_v37_spot_plan(**dataclasses.asdict(plan))
    values = json.dumps(dataclasses.asdict(plan), sort_keys=True, separators=(",", ":"))
    encoded = base64.b64encode(values.encode()).decode()
    source_archive_uri = shlex.quote(plan.source_archive_uri)
    binary_uri = shlex.quote(plan.binary_uri)
    manifest_uri = shlex.quote(plan.manifest_uri)
    source_archive_sha256 = shlex.quote(plan.source_archive_sha256)
    binary_sha256 = shlex.quote(plan.binary_sha256)
    manifest_sha256 = shlex.quote(plan.manifest_sha256)
    slice_unit = shlex.quote(_v37_slice_unit(plan.run_id))
    output_bucket, output_prefix = _s3_uri(plan.output_prefix, prefix=True)
    boot_failure_key = shlex.quote(output_prefix + "BOOT_FAILURE.log")
    return f"""#!/bin/bash
set -euo pipefail
umask 077
export V37_PLAN_B64={encoded}
export V37_RSS_LIMIT=3221225472
export V37_PSI_FULL_LIMIT=0.75
export V37_SCIENCE_TIMEOUT=600
export V37_WRAPPER_TIMEOUT=720
export V37_PROGRESS_TIMEOUT=120
export V37_MINIMUM_SCORES_PER_SECOND=20000000
export V37_MEMORY_CURRENT_FILE=memory.current
export V37_MEMORY_SWAP_CURRENT_FILE=memory.swap.current
export V37_MEMORY_PSI_FILE=/proc/pressure/memory
root=/var/lib/borsuk-v37-relation
scratch="$root/scratch"
source="$root/source"
slice={slice_unit}
mkdir -p "$scratch" "$source"
chmod 0711 "$root" "$scratch"
boot_log="$scratch/boot.log"
exec >"$boot_log" 2>&1
systemctl set-property --runtime "$slice" MemoryMax=3221225472 MemorySwapMax=0 MemoryAccounting=yes
archive="$scratch/source.tar.zst"
binary="$scratch/v37-relation-router"
manifest="$scratch/manifest.json"
plan="$scratch/plan.json"
printf '%s' "$V37_PLAN_B64" | base64 -d >"$plan"
validate_boot_object() {{
  local path="$1" expected_bytes="$2" expected_sha256="$3" actual_sha256
  test "$(stat -c %s "$path")" = "$expected_bytes"
  actual_sha256=$(sha256sum "$path")
  test "${{actual_sha256%% *}}" = "$expected_sha256"
}}
cleanup() {{
  status=$?
  trap - EXIT INT TERM
  if test "$status" -ne 0 && test -s "$boot_log"; then
    aws s3api put-object --bucket {shlex.quote(output_bucket)} \
      --key {boot_failure_key} --body "$boot_log" --if-none-match '*' \
      --checksum-algorithm SHA256 >/dev/null || true
  fi
  rm -f "$archive" "$binary" "$manifest" "$plan" "$boot_log"
  rmdir "$scratch" 2>/dev/null || true
  shutdown -h now
  exit "$status"
}}
trap cleanup EXIT INT TERM
aws s3 cp {source_archive_uri} "$archive" --only-show-errors
aws s3 cp {binary_uri} "$binary" --only-show-errors
aws s3 cp {manifest_uri} "$manifest" --only-show-errors
validate_boot_object "$archive" {plan.source_archive_bytes} {source_archive_sha256}
validate_boot_object "$binary" {plan.binary_bytes} {binary_sha256}
validate_boot_object "$manifest" {plan.manifest_bytes} {manifest_sha256}
chmod 0555 "$binary"
tar --zstd -xf "$archive" -C "$source"
dnf install -y python3-pip
python3 -m pip install --no-cache-dir --target "$source/.v37-python" \
  --disable-pip-version-check \
  --requirement "$source/scripts/requirements-v37-relation-router.txt"
imds_token=$(curl --fail --silent --show-error --request PUT \
  --header 'X-aws-ec2-metadata-token-ttl-seconds: 21600' \
  http://169.254.169.254/latest/api/token)
instance_id=$(curl --fail --silent --show-error \
  --header "X-aws-ec2-metadata-token: $imds_token" \
  http://169.254.169.254/latest/meta-data/instance-id)
[[ "$instance_id" =~ ^i-[0-9a-f]{{8,17}}$ ]]
systemd-run --wait --collect --unit="borsuk-v37-{plan.run_id}" --slice="$slice" \
  --property=MemoryMax=3G --property=MemorySwapMax=0 --property=RuntimeMaxSec=600 \
  setsid env PYTHONPATH="$source/.v37-python" python3 \
  "$source/scripts/run_v37_relation_router_spot.py" \
  --execute-v37-worker --root "$root/phase" --plan "$plan" --manifest "$manifest" \
  --binary "$binary" --instance-id "$instance_id"
# Worker publishes exactly one canonical ATTEMPT_TERMINAL.json disposition.
"""


def canonical_v37_terminal_bytes(
    terminal: dict[str, object], plan: V37SpotPlan
) -> bytes:
    """Serialize one exact controller terminal after strict authority checks."""

    if terminal.get("status") == "failed":
        failure_keys = {
            "binary_bytes",
            "binary_sha256",
            "binary_uri",
            "claim_eligible",
            "instance_id",
            "manifest_bytes",
            "manifest_sha256",
            "manifest_uri",
            "phase",
            "reason",
            "run_id",
            "schema",
            "source_archive_bytes",
            "source_archive_sha256",
            "source_archive_uri",
            "source_commit",
            "status",
        }
        reasons = {
            "authority-failure",
            "boot-failure",
            "progress-timeout",
            "psi-stop",
            "rss-stop",
            "science-timeout",
            "swap-stop",
            "throughput-stop",
            "worker-exit",
            "wrapper-timeout",
        }
        if (
            set(terminal) != failure_keys
            or terminal.get("schema") != "borsuk-v37-spot-failed-terminal-v1"
            or terminal.get("claim_eligible") is not False
            or terminal.get("phase") != plan.phase
            or terminal.get("run_id") != plan.run_id
            or terminal.get("source_commit") != plan.source_commit
            or terminal.get("reason") not in reasons
            or type(terminal.get("instance_id")) is not str
            or not terminal["instance_id"]
            or terminal.get("source_archive_uri") != plan.source_archive_uri
            or terminal.get("source_archive_sha256") != plan.source_archive_sha256
            or terminal.get("source_archive_bytes") != plan.source_archive_bytes
            or terminal.get("binary_uri") != plan.binary_uri
            or terminal.get("binary_sha256") != plan.binary_sha256
            or terminal.get("binary_bytes") != plan.binary_bytes
            or terminal.get("manifest_uri") != plan.manifest_uri
            or terminal.get("manifest_sha256") != plan.manifest_sha256
            or terminal.get("manifest_bytes") != plan.manifest_bytes
        ):
            raise ValueError("V37 failed terminal authority differs")
        return _canonical_json_bytes(terminal)

    expected_keys = {
        "artifacts",
        "binary_bytes",
        "binary_sha256",
        "binary_uri",
        "claim_eligible",
        "instance_id",
        "manifest_bytes",
        "manifest_sha256",
        "manifest_uri",
        "monitor",
        "phase",
        "progress",
        "result",
        "run_id",
        "schema",
        "source_archive_bytes",
        "source_archive_sha256",
        "source_archive_uri",
        "source_commit",
        "status",
    }
    identity_keys = {"blake3", "encoded_bytes", "role", "sha256", "uri"}
    result_keys = {"encoded_bytes", "role", "sha256", "uri"}
    monitor_keys = {
        "elapsed_milliseconds",
        "peak_psi_full_ppm",
        "peak_rss_bytes",
        "swap_end_bytes",
        "swap_start_bytes",
    }
    artifacts = terminal.get("artifacts")
    progress = terminal.get("progress")
    result = terminal.get("result")
    monitor = terminal.get("monitor")
    if (
        set(terminal) != expected_keys
        or terminal.get("schema") != "borsuk-v37-spot-terminal-v2"
        or terminal.get("claim_eligible") is not False
        or terminal.get("phase") != plan.phase
        or terminal.get("run_id") != plan.run_id
        or terminal.get("source_commit") != plan.source_commit
        or terminal.get("status") != "complete"
        or type(terminal.get("instance_id")) is not str
        or not terminal["instance_id"]
        or terminal.get("source_archive_uri") != plan.source_archive_uri
        or terminal.get("source_archive_sha256") != plan.source_archive_sha256
        or terminal.get("source_archive_bytes") != plan.source_archive_bytes
        or terminal.get("binary_uri") != plan.binary_uri
        or terminal.get("binary_sha256") != plan.binary_sha256
        or terminal.get("binary_bytes") != plan.binary_bytes
        or terminal.get("manifest_uri") != plan.manifest_uri
        or terminal.get("manifest_sha256") != plan.manifest_sha256
        or terminal.get("manifest_bytes") != plan.manifest_bytes
        or type(artifacts) is not list
        or [item.get("role") if type(item) is dict else None for item in artifacts]
        != list(_phase_output_roles(plan.phase))
        or any(
            type(item) is not dict
            or set(item) != identity_keys
            or _LOWER_SHA256.fullmatch(str(item.get("sha256"))) is None
            or _LOWER_SHA256.fullmatch(str(item.get("blake3"))) is None
            or type(item.get("encoded_bytes")) is not int
            or item["encoded_bytes"] <= 0
            or item.get("uri") != f"{plan.output_prefix}{item.get('role')}"
            for item in artifacts
        )
        or (
            plan.phase in _TRAINING_PHASES
            and (
                type(progress) is not dict
                or set(progress) != result_keys
                or progress.get("role") != "progress"
                or progress.get("uri") != f"{plan.output_prefix}progress.json"
                or _LOWER_SHA256.fullmatch(str(progress.get("sha256"))) is None
                or type(progress.get("encoded_bytes")) is not int
                or progress["encoded_bytes"] <= 0
            )
        )
        or (plan.phase == "evaluate-ceiling" and progress is not None)
        or type(result) is not dict
        or set(result) != result_keys
        or result.get("role") != "local-result"
        or result.get("uri") != f"{plan.output_prefix}local-result.json"
        or any(
            _LOWER_SHA256.fullmatch(str(item.get("sha256"))) is None
            or type(item.get("encoded_bytes")) is not int
            or item["encoded_bytes"] <= 0
            for item in (result,)
        )
        or type(monitor) is not dict
        or set(monitor) != monitor_keys
        or any(type(monitor[key]) is not int or monitor[key] < 0 for key in monitor_keys)
        or monitor["elapsed_milliseconds"] == 0
        or monitor["peak_rss_bytes"] > MEMORY_LIMIT_BYTES
        or monitor["peak_psi_full_ppm"] > round(MEMORY_PSI_FULL_LIMIT * 1_000_000)
        or monitor["swap_end_bytes"] != monitor["swap_start_bytes"]
    ):
        raise ValueError("V37 terminal authority differs")
    return _canonical_json_bytes(terminal)


def publish_v37_worker_failure(
    plan: V37SpotPlan,
    *,
    instance_id: str,
    reason: str,
    upload_once: Callable[[str, str, bytes], None],
) -> bytes:
    """Publish one claim-ineligible failed terminal with no result authority."""

    terminal = {
        "binary_bytes": plan.binary_bytes,
        "binary_sha256": plan.binary_sha256,
        "binary_uri": plan.binary_uri,
        "claim_eligible": False,
        "instance_id": instance_id,
        "manifest_bytes": plan.manifest_bytes,
        "manifest_sha256": plan.manifest_sha256,
        "manifest_uri": plan.manifest_uri,
        "phase": plan.phase,
        "reason": reason,
        "run_id": plan.run_id,
        "schema": "borsuk-v37-spot-failed-terminal-v1",
        "source_archive_bytes": plan.source_archive_bytes,
        "source_archive_sha256": plan.source_archive_sha256,
        "source_archive_uri": plan.source_archive_uri,
        "source_commit": plan.source_commit,
        "status": "failed",
    }
    raw = canonical_v37_terminal_bytes(terminal, plan)
    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)
    upload_once(bucket, prefix + "ATTEMPT_TERMINAL.json", raw)
    return raw


def validate_v37_terminal_bytes(
    raw: bytes, plan: V37SpotPlan, expected_status: str
) -> dict[str, object]:
    """Authenticate exact canonical terminal bytes and expected disposition."""

    try:
        terminal = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 terminal JSON differs") from error
    if (
        type(terminal) is not dict
        or canonical_v37_terminal_bytes(terminal, plan) != raw
        or terminal.get("status") != expected_status
    ):
        raise ValueError("V37 terminal bytes differ")
    return terminal


def _read_v37_exact_s3_object(
    s3_client: Any,
    uri: str,
    *,
    expected_bytes: int | None = None,
    expected_sha256: str | None = None,
    maximum_bytes: int = _MAX_AUTHORITY_JSON_BYTES,
) -> bytes:
    bucket, key = _s3_uri(uri)
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
        ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
        ChecksumMode="ENABLED",
    )
    content_length = response.get("ContentLength")
    if (
        type(content_length) is not int
        or content_length <= 0
        or content_length > maximum_bytes
        or (expected_bytes is not None and content_length != expected_bytes)
    ):
        raise ValueError("V37 S3 object length differs")
    raw = response["Body"].read(content_length + 1)
    if len(raw) != content_length:
        raise ValueError("V37 S3 object body length differs")
    if (
        expected_sha256 is not None
        and hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        raise ValueError("V37 S3 object digest differs")
    return raw


def _admit_v37_build_preflight(
    plan: V37SpotPlan, s3_client: Any, preflight_terminal_uri: str
) -> None:
    if not preflight_terminal_uri.endswith("/ATTEMPT_TERMINAL.json"):
        raise ValueError("V37 preflight terminal URI differs")
    terminal_raw = _read_v37_exact_s3_object(s3_client, preflight_terminal_uri)
    try:
        terminal = json.loads(terminal_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 preflight terminal JSON differs") from error
    result = terminal.get("result") if type(terminal) is dict else None
    if (
        type(result) is not dict
        or type(terminal.get("manifest_uri")) is not str
        or type(terminal.get("manifest_bytes")) is not int
        or type(terminal.get("manifest_sha256")) is not str
        or type(result.get("uri")) is not str
        or type(result.get("encoded_bytes")) is not int
        or type(result.get("sha256")) is not str
    ):
        raise ValueError("V37 preflight terminal authority differs")
    build_manifest_raw = _read_v37_exact_s3_object(
        s3_client,
        plan.manifest_uri,
        expected_bytes=plan.manifest_bytes,
        expected_sha256=plan.manifest_sha256,
    )
    preflight_manifest_raw = _read_v37_exact_s3_object(
        s3_client,
        terminal["manifest_uri"],
        expected_bytes=terminal["manifest_bytes"],
        expected_sha256=terminal["manifest_sha256"],
    )
    preflight_result_raw = _read_v37_exact_s3_object(
        s3_client,
        result["uri"],
        expected_bytes=result["encoded_bytes"],
        expected_sha256=result["sha256"],
    )
    validate_v37_preflight_admission(
        build_plan=plan,
        build_manifest_raw=build_manifest_raw,
        preflight_manifest_raw=preflight_manifest_raw,
        preflight_result_raw=preflight_result_raw,
        preflight_terminal_raw=terminal_raw,
    )


def run_v37_spot_phase(
    plan: V37SpotPlan,
    *,
    preflight_terminal_uri: str | None = None,
    ec2_client: Any,
    s3_client: Any,
    sleep: Callable[[float], None],
    monotonic: Callable[[], float],
) -> str:
    """Launch one original Spot worker, preserve its terminal, and terminate it."""

    bucket, prefix = _s3_uri(plan.output_prefix, prefix=True)

    def upload_once(upload_bucket: str, upload_key: str, body: bytes) -> None:
        s3_client.put_object(
            Bucket=upload_bucket,
            Key=upload_key,
            Body=body,
            ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
            IfNoneMatch="*",
            ChecksumAlgorithm="SHA256",
        )

    def seal_failure(instance: str, reason: str) -> None:
        publish_v37_worker_failure(
            plan,
            instance_id=instance,
            reason=reason,
            upload_once=upload_once,
        )

    def read_terminal() -> bytes | None:
        try:
            response = s3_client.get_object(
                Bucket=bucket,
                Key=prefix + "ATTEMPT_TERMINAL.json",
                ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
                ChecksumMode="ENABLED",
            )
        except (KeyError, FileNotFoundError):
            return None
        except Exception as error:
            if _aws_error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        content_length = response.get("ContentLength")
        if (
            type(content_length) is not int
            or content_length <= 0
            or content_length > _MAX_AUTHORITY_JSON_BYTES
        ):
            raise ValueError("V37 terminal length differs")
        raw = response["Body"].read(content_length + 1)
        if len(raw) != content_length:
            raise ValueError("V37 terminal body length differs")
        return raw

    if read_terminal() is not None:
        raise ValueError("V37 terminal already exists")

    plan_manifest_raw = _read_v37_exact_s3_object(
        s3_client,
        plan.manifest_uri,
        expected_bytes=plan.manifest_bytes,
        expected_sha256=plan.manifest_sha256,
    )
    _validate_v37_plan_manifest_bytes(plan_manifest_raw, plan)

    if plan.phase == "build-ownership":
        if preflight_terminal_uri is None:
            raise ValueError("V37 preflight terminal is required")
        _admit_v37_build_preflight(plan, s3_client, preflight_terminal_uri)
    elif preflight_terminal_uri is not None:
        raise ValueError("V37 preflight terminal is forbidden")

    def classify_terminal(raw: bytes) -> dict[str, object]:
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("V37 terminal JSON differs") from error
        status = value.get("status") if type(value) is dict else None
        if status not in {"complete", "failed"}:
            raise ValueError("V37 terminal status differs")
        return validate_v37_terminal_bytes(raw, plan, status)

    instance_id: str | None = None
    for spec in build_v37_launch_specs(
        plan,
        user_data=build_v37_worker_script(plan),
    ):
        try:
            response = ec2_client.run_instances(**spec)
        except Exception as error:
            if _aws_error_code(error) in _CAPACITY_ERRORS:
                continue
            raise
        instances = response.get("Instances")
        if type(instances) is not list or len(instances) != 1:
            raise ValueError("V37 Spot launch response differs")
        candidate = instances[0].get("InstanceId")
        if type(candidate) is not str or not candidate:
            raise ValueError("V37 Spot instance identity differs")
        instance_id = candidate
        break
    if instance_id is None:
        raise RuntimeError("V37 Spot capacity unavailable")

    started = monotonic()
    terminated = False
    try:
        while monotonic() - started <= WRAPPER_TIMEOUT_SECONDS:
            raw = read_terminal()
            if raw is not None:
                terminal = classify_terminal(raw)
                if terminal["instance_id"] != instance_id:
                    raise ValueError("V37 terminal instance differs")
                uri = f"s3://{bucket}/{prefix}ATTEMPT_TERMINAL.json"
                if terminal["status"] == "failed":
                    raise RuntimeError(f"V37 worker failed at {uri}")
                return uri
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            reservations = response.get("Reservations")
            if type(reservations) is not list or len(reservations) != 1:
                raise ValueError("V37 Spot instance status differs")
            instances = reservations[0].get("Instances")
            if type(instances) is not list or len(instances) != 1:
                raise ValueError("V37 Spot instance status differs")
            state = instances[0].get("State")
            state_name = state.get("Name") if type(state) is dict else None
            if state_name in {"shutting-down", "terminated", "stopped", "stopping"}:
                seal_failure(instance_id, "boot-failure")
                raise RuntimeError("V37 worker failed at boot-failure")
            if state_name not in {"pending", "running"}:
                raise ValueError("V37 Spot instance state differs")
            sleep(5)
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        terminated = True
        raw = read_terminal()
        if raw is not None:
            terminal = classify_terminal(raw)
            if terminal["instance_id"] != instance_id:
                raise ValueError("V37 terminal instance differs")
            terminal_uri = f"s3://{bucket}/{prefix}ATTEMPT_TERMINAL.json"
            if terminal["status"] == "complete":
                return terminal_uri
            raise RuntimeError(f"V37 worker failed at {terminal_uri}")
        seal_failure(instance_id, "wrapper-timeout")
        raise TimeoutError("V37 Spot wrapper timed out")
    finally:
        if not terminated:
            ec2_client.terminate_instances(InstanceIds=[instance_id])


def _load_v37_spot_plan_bytes(raw: bytes) -> V37SpotPlan:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V37 worker plan JSON differs") from error
    expected = {field.name for field in dataclasses.fields(V37SpotPlan)}
    if type(value) is not dict or set(value) != expected:
        raise ValueError("V37 worker plan schema differs")
    plan = build_v37_spot_plan(**value)
    if json.dumps(value, sort_keys=True, separators=(",", ":")).encode() != raw:
        raise ValueError("V37 worker plan bytes differ")
    return plan


def _worker_failure_reason(error: BaseException) -> str:
    message = str(error)
    for reason in (
        "progress-timeout",
        "psi-stop",
        "rss-stop",
        "science-timeout",
        "swap-stop",
        "throughput-stop",
    ):
        if reason in message:
            return reason
    if "worker exited" in message:
        return "worker-exit"
    return "authority-failure"


def run_v37_worker_invocation(invocation: V37WorkerInvocation, s3_client: Any) -> bytes:
    """Adapt exact local boot files and S3 object calls to the pure worker."""

    plan = _load_v37_spot_plan_bytes(invocation.plan.read_bytes())
    manifest_raw = invocation.manifest.read_bytes()
    binary_size = invocation.binary.stat().st_size
    binary_sha256 = hashlib.sha256(invocation.binary.read_bytes()).hexdigest()
    if binary_size != plan.binary_bytes or binary_sha256 != plan.binary_sha256:
        raise ValueError("V37 worker binary authority differs")

    def download(bucket: str, key: str, path: pathlib.Path) -> None:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
            ChecksumMode="ENABLED",
        )
        body = response["Body"]
        written = 0
        with path.open("xb") as output:
            while True:
                chunk = body.read(1_048_576)
                if not chunk:
                    break
                output.write(chunk)
                written += len(chunk)
        if response.get("ContentLength") != written:
            raise ValueError("V37 staged object length differs")

    def upload_once(bucket: str, key: str, body: bytes) -> None:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ExpectedBucketOwner=EXPECTED_AWS_ACCOUNT,
            IfNoneMatch="*",
            ChecksumAlgorithm="SHA256",
        )

    def execute(
        command: list[str],
        _staged: V37StagedPhase,
        stdout_path: pathlib.Path,
        progress_path: pathlib.Path,
    ) -> tuple[int, V37MonitorSample]:
        returncode, sample = execute_v37_native_science(
            command, _staged, stdout_path, progress_path
        )
        if returncode == 0 and _staged.phase in _TRAINING_PHASES:
            try:
                value = json.loads(stdout_path.read_bytes())
                throughput = value["training_evidence"][
                    "partition_coordinate_scores_per_second"
                ]
            except (KeyError, TypeError, json.JSONDecodeError) as error:
                raise ValueError("V37 native throughput evidence differs") from error
            if type(throughput) is not int:
                raise ValueError("V37 native throughput evidence differs")
            sample = dataclasses.replace(
                sample, coordinate_scores_per_second=throughput
            )
        return returncode, sample

    try:
        return execute_v37_worker(
            plan,
            root=invocation.root,
            manifest_raw=manifest_raw,
            binary=invocation.binary,
            instance_id=invocation.instance_id,
            download=download,
            execute=execute,
            upload_once=upload_once,
        )
    except Exception as error:
        publish_v37_worker_failure(
            plan,
            instance_id=invocation.instance_id,
            reason=_worker_failure_reason(error),
            upload_once=upload_once,
        )
        raise


def main(arguments: Sequence[str] | None = None) -> int:
    """Run only the explicit remote-worker boundary."""

    try:
        invocation = parse_v37_worker_args(sys.argv if arguments is None else arguments)
        import boto3
        from botocore.config import Config

        s3_client = boto3.client(
            "s3",
            region_name=REGION,
            config=Config(
                connect_timeout=10,
                read_timeout=60,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        run_v37_worker_invocation(invocation, s3_client)
    except Exception as error:
        print(f"V37 worker failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
