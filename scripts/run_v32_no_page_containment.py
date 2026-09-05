#!/usr/bin/env python3
"""Fail fast on V32 truth containment without reading any page body."""

from __future__ import annotations

import hashlib
import json
import resource
import subprocess
import sys
from argparse import ArgumentParser
from bisect import bisect_right
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

QUERY_COUNT = 32
RECALL_K = 10
V32_GOVERNING_TERMINAL_URI = (
    "s3://borsuk-bench-453182569524-euc1/research/"
    "v32-quality-perfect-s3-serving/af05a46b75212c894fc5208aa768910552ed083d/"
    "attempts/v32-deep-1m-global-containment-l768-20260905T020228Z-a0001/"
    "TERMINAL.json"
)
V32_GOVERNING_TERMINAL_SHA256 = (
    "88226dcc0bc3a6b7034349d95698c0946d500a40b7ba1133bdd418fc5eefb74e"
)
V32_GOVERNING_TERMINAL_BYTES = 262_537


@dataclass(frozen=True)
class LocalArtifact:
    path: Path
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class Root64Metadata:
    """Independent query root orders and complete authenticated leaf population."""

    root_orders: tuple[tuple[int, ...], ...]
    leaf_owners: tuple[int, ...]
    leaf_rows: tuple[int, ...]


@dataclass(frozen=True)
class RegisteredLocalArtifact:
    path: Path
    uri: str
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class V32ContainmentPlan:
    qualifier: Path
    manifest: LocalArtifact
    artifact_dir: Path
    query: LocalArtifact
    logical_sources: LocalArtifact
    truth: LocalArtifact
    truth_receipt: LocalArtifact
    source_rows: int
    query_start: int
    query_count: int
    root_beam: int
    leaf_beam: int
    global_leaf_limit: int | None = None
    virtual_geometric_pages: bool = False
    diagnostic_batch: LocalArtifact | None = None
    governing_terminal: RegisteredLocalArtifact | None = None
    global_geometric_pages: bool = False


def _digest(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _validate_artifact(artifact: LocalArtifact) -> None:
    if (
        not artifact.path.is_absolute()
        or not _digest(artifact.sha256)
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
    ):
        raise ValueError("V32 containment artifact authority differs")


def _validate_registered_artifact(artifact: RegisteredLocalArtifact) -> None:
    if (
        not artifact.path.is_absolute()
        or not artifact.uri.startswith("s3://")
        or "//" in artifact.uri[5:]
        or artifact.uri.endswith("/")
        or not _digest(artifact.sha256)
        or type(artifact.encoded_bytes) is not int
        or artifact.encoded_bytes <= 0
        or artifact.uri != V32_GOVERNING_TERMINAL_URI
        or artifact.sha256 != V32_GOVERNING_TERMINAL_SHA256
        or artifact.encoded_bytes != V32_GOVERNING_TERMINAL_BYTES
    ):
        raise ValueError("V32 registered artifact authority differs")


def _read_governing_terminal(
    plan: V32ContainmentPlan,
) -> tuple[bytes, dict[str, object]]:
    artifact = plan.governing_terminal
    if artifact is None:
        raise ValueError("V32 governing terminal authority is missing")
    _validate_registered_artifact(artifact)
    raw = artifact.path.read_bytes()
    if (
        len(raw) != artifact.encoded_bytes
        or hashlib.sha256(raw).hexdigest() != artifact.sha256
        or not raw.endswith(b"\n")
        or b"\n" in raw[:-1]
    ):
        raise ValueError("V32 governing terminal byte authority differs")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V32 governing terminal JSON differs") from error
    canonical = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    outer_keys = {
        "aggregate_containment_ppm",
        "claim_eligible",
        "failed_gates",
        "global_leaf_limit",
        "leaf_beam",
        "logical_sources_sha256",
        "losses_by_stage",
        "manifest_sha256",
        "maximum_codes_scanned",
        "maximum_leaves_eligible",
        "maximum_leaves_scanned",
        "maximum_peak_query_table_pairs_live",
        "maximum_query_table_pairs_built",
        "maximum_routing_leaf_rows",
        "maximum_selected_page_bytes",
        "maximum_truth_microleaf_rank",
        "minimum_containment_ppm",
        "page_body_reads",
        "perfect_queries",
        "queries",
        "query_count",
        "query_sha256",
        "query_start",
        "reciprocal_rank",
        "root_beam",
        "routing_scope",
        "samples",
        "schema_version",
        "selected_page_hits",
        "source_rows",
        "status",
        "truth_receipt_sha256",
        "truth_sha256",
    }
    query_keys = {
        "baseline_hits",
        "lost_logicals",
        "page_selections",
        "query_ordinal",
        "reciprocal_rank_hits",
        "recovered_logicals",
        "routing",
        "targets",
    }
    if (
        raw != canonical
        or type(value) is not dict
        or set(value) != outer_keys
        or value["schema_version"] != 4
        or value["claim_eligible"] is not False
        or value["page_body_reads"] != 0
        or value["query_count"] != QUERY_COUNT
        or value["query_start"] != plan.query_start
        or value["source_rows"] != plan.source_rows
        or value["root_beam"] != plan.root_beam
        or value["leaf_beam"] != plan.leaf_beam
        or value["global_leaf_limit"] != plan.global_leaf_limit
        or value["routing_scope"] != "global"
        or value["manifest_sha256"] != plan.manifest.sha256
        or value["logical_sources_sha256"] != plan.logical_sources.sha256
        or value["query_sha256"] != plan.query.sha256
        or value["truth_sha256"] != plan.truth.sha256
        or value["truth_receipt_sha256"] != plan.truth_receipt.sha256
        or value["selected_page_hits"] != 308
        or value["aggregate_containment_ppm"] != 962_500
        or value["minimum_containment_ppm"] != 700_000
        or value["perfect_queries"] != 23
        or value["failed_gates"] != ["perfect-containment"]
        or value["status"] != "failed"
        or type(value["queries"]) is not list
        or len(value["queries"]) != QUERY_COUNT
        or type(value["samples"]) is not list
        or len(value["samples"]) != QUERY_COUNT
        or type(value["reciprocal_rank"]) is not dict
        or value["reciprocal_rank"].get("selected_page_hits") != 298
    ):
        raise ValueError("V32 governing terminal authority differs")
    for offset, query in enumerate(value["queries"]):
        ordinal = plan.query_start + offset
        if (
            type(query) is not dict
            or set(query) != query_keys
            or query.get("query_ordinal") != ordinal
            or type(query.get("targets")) is not list
            or len(query["targets"]) != RECALL_K
            or type(query.get("page_selections")) is not dict
            or set(query["page_selections"]) != {"first_distinct", "reciprocal_rank"}
            or type(query.get("routing")) is not dict
        ):
            raise ValueError("V32 governing terminal query evidence differs")
    return raw, value


def _read_scale_manifest(plan: V32ContainmentPlan) -> tuple[int, int, int, int]:
    _validate_artifact(plan.manifest)
    payload = plan.manifest.path.read_bytes()
    if (
        len(payload) != plan.manifest.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != plan.manifest.sha256
        or not payload.endswith(b"\n")
        or b"\n" in payload[:-1]
    ):
        raise ValueError("V32 containment manifest byte authority differs")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V32 containment manifest JSON differs") from error
    expected = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    layout = value.get("layout") if type(value) is dict else None
    routing = value.get("routing") if type(value) is dict else None
    maximum_routing_leaf_rows = (
        layout.get("maximum_routing_leaf_rows") if type(layout) is dict else None
    )
    routing_keys = {
        "algorithm",
        "arms",
        "candidate_depth",
        "page_count",
        "root_beam",
    }
    arms = routing.get("arms") if type(routing) is dict else None
    expected_arms = [
        {"leaf_beam": 64, "maximum_scanned_codes": 65_536},
        {"leaf_beam": 128, "maximum_scanned_codes": 131_072},
        {"leaf_beam": 256, "maximum_scanned_codes": 262_144},
    ]
    scan_budget = {
        64: 65_536,
        128: 131_072,
        256: 262_144,
    }.get(plan.leaf_beam)
    if (
        payload != expected
        or type(layout) is not dict
        or type(routing) is not dict
        or layout.get("source_rows") != plan.source_rows
        or layout.get("page_rows") != 480
        or type(maximum_routing_leaf_rows) is not int
        or not 1 <= maximum_routing_leaf_rows <= 1_024
        or type(layout.get("maximum_code_parent_rows")) is not int
        or not 1 <= layout["maximum_code_parent_rows"] <= 131_072
        or type(layout.get("maximum_routing_leaves_per_root")) is not int
        or layout["maximum_routing_leaves_per_root"] <= 0
        or type(layout.get("projected_resident_bytes")) is not int
        or not 1 <= layout["projected_resident_bytes"] <= 3 * 1_024 * 1_024 * 1_024
        or set(routing) != routing_keys
        or routing.get("algorithm") != "hierarchical-routing-microleaf-pq-v1"
        or routing.get("candidate_depth") != 12_288
        or routing.get("page_count") != 16
        or routing.get("root_beam") != 8
        or arms != expected_arms
        or scan_budget is None
    ):
        raise ValueError("V32 containment scale geometry differs")
    if plan.root_beam not in {8, 16, 32} or (
        plan.global_leaf_limit is not None
        and (plan.global_leaf_limit != 768 or plan.leaf_beam != 256)
    ):
        raise ValueError("V32 containment root beam differs")
    maximum_leaves_eligible = (
        128 * layout["maximum_routing_leaves_per_root"]
        if plan.global_leaf_limit is not None
        else plan.root_beam * layout["maximum_routing_leaves_per_root"]
    )
    return (
        maximum_routing_leaf_rows,
        maximum_leaves_eligible,
        plan.leaf_beam,
        scan_budget,
    )


def _read_truth(
    plan: V32ContainmentPlan, truth_bytes: bytes
) -> tuple[tuple[int, ...], ...]:
    _validate_artifact(plan.manifest)
    _validate_artifact(plan.query)
    _validate_artifact(plan.truth)
    _validate_artifact(plan.truth_receipt)
    if (
        not plan.qualifier.is_absolute()
        or not plan.artifact_dir.is_absolute()
        or plan.source_rows not in {100_000, 1_000_000}
        or type(plan.query_start) is not int
        or plan.query_start < 0
        or plan.query_count != QUERY_COUNT
        or plan.root_beam not in {8, 16, 32}
        or (plan.global_leaf_limit is not None and plan.global_leaf_limit != 768)
        or type(truth_bytes) is not bytes
        or len(truth_bytes) != plan.truth.encoded_bytes
        or hashlib.sha256(truth_bytes).hexdigest() != plan.truth.sha256
    ):
        raise ValueError("V32 containment truth byte authority differs")
    table = pq.read_table(pa.BufferReader(truth_bytes))
    # Prefix truth is already materialized for the registered query window.
    # Its rows are local offsets 0..31; `query_start` continues to address the
    # corresponding rows in the full query Parquet artifact.
    if table.schema.names != ["neighbors_id"] or table.num_rows != QUERY_COUNT:
        raise ValueError("V32 containment truth schema differs")
    field = table.schema.field("neighbors_id")
    item = field.type.value_field if pa.types.is_fixed_size_list(field.type) else None
    if (
        field.nullable
        or item is None
        or item.nullable
        or not pa.types.is_int64(item.type)
        or field.type.list_size != RECALL_K
    ):
        raise ValueError("V32 containment truth schema differs")
    rows = table.column("neighbors_id").to_pylist()
    result: list[tuple[int, ...]] = []
    for row in rows:
        if (
            type(row) is not list
            or len(row) < RECALL_K
            or any(
                type(logical) is not int or not 0 <= logical < plan.source_rows
                for logical in row[:RECALL_K]
            )
            or len(set(row[:RECALL_K])) != RECALL_K
        ):
            raise ValueError("V32 containment truth membership differs")
        result.append(tuple(row[:RECALL_K]))
    _read_truth_receipt(plan, truth_bytes, result)
    return tuple(result)


def _read_truth_receipt(
    plan: V32ContainmentPlan,
    truth_bytes: bytes,
    truth: list[tuple[int, ...]],
) -> None:
    payload = plan.truth_receipt.path.read_bytes()
    if (
        len(payload) != plan.truth_receipt.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != plan.truth_receipt.sha256
        or not payload.endswith(b"\n")
        or b"\n" in payload[:-1]
    ):
        raise ValueError("V32 containment truth receipt byte authority differs")
    try:
        receipt = json.loads(payload)
        manifest = json.loads(plan.manifest.path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V32 containment truth receipt JSON differs") from error
    expected = (
        json.dumps(
            receipt, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    source = manifest.get("source") if type(manifest) is dict else None
    keys = {
        "claim_eligible",
        "corpus_manifest_bytes",
        "corpus_manifest_sha256",
        "corpus_normalization",
        "corpus_shards",
        "distance",
        "query_bytes",
        "query_count",
        "query_normalization",
        "query_sha256",
        "query_start",
        "rank_10_11_tie_queries",
        "schema",
        "shards_read",
        "source_rows",
        "status",
        "tie_break",
        "top_k",
        "truth_bytes",
        "truth_id_space",
        "truth_ids_sha256",
        "truth_row_semantics",
        "truth_sha256",
    }
    truth_ids = b"".join(
        logical.to_bytes(8, "little", signed=True) for row in truth for logical in row
    )
    shards = receipt.get("corpus_shards") if type(receipt) is dict else None
    if (
        payload != expected
        or type(receipt) is not dict
        or set(receipt) != keys
        or receipt["schema"] != "borsuk-v32-prefix-truth-v3"
        or receipt["claim_eligible"] is not False
        or receipt["status"] != "passed"
        or receipt["truth_row_semantics"] != "window-relative"
        or receipt["truth_id_space"] != "source-ordinal"
        or receipt["top_k"] != RECALL_K
        or receipt["distance"] != "squared-l2-f64-fixed-dimension-order"
        or receipt["corpus_normalization"] != "f64-l2-once-to-f32"
        or receipt["query_normalization"] != "f64-l2-twice-to-f32"
        or receipt["tie_break"] != "source-ordinal-ascending"
        or receipt["query_sha256"] != plan.query.sha256
        or receipt["query_bytes"] != plan.query.encoded_bytes
        or receipt["query_start"] != plan.query_start
        or receipt["query_count"] != plan.query_count
        or receipt["source_rows"] != plan.source_rows
        or receipt["truth_sha256"] != plan.truth.sha256
        or receipt["truth_bytes"] != len(truth_bytes)
        or receipt["truth_ids_sha256"] != hashlib.sha256(truth_ids).hexdigest()
        or type(receipt["rank_10_11_tie_queries"]) is not int
        or not 0 <= receipt["rank_10_11_tie_queries"] <= QUERY_COUNT
        or type(shards) is not list
        or not shards
        or receipt["shards_read"] != len(shards)
        or type(source) is not dict
        or receipt["corpus_manifest_sha256"] != source.get("corpus_manifest_sha256")
        or receipt["corpus_manifest_bytes"] != source.get("corpus_manifest_bytes")
    ):
        raise ValueError("V32 containment truth receipt authority differs")


def _read_logical_sources(plan: V32ContainmentPlan) -> tuple[int, ...]:
    _validate_artifact(plan.logical_sources)
    payload = plan.logical_sources.path.read_bytes()
    if (
        len(payload) != plan.logical_sources.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != plan.logical_sources.sha256
    ):
        raise ValueError("V32 containment logical-source byte authority differs")
    try:
        table = pa.ipc.open_file(pa.BufferReader(payload)).read_all()
    except pa.ArrowInvalid as error:
        raise ValueError("V32 containment logical-source Arrow differs") from error
    expected_schema = pa.schema(
        [pa.field("source_ordinal", pa.uint64(), nullable=False)]
    )
    if table.schema != expected_schema or table.num_rows != plan.source_rows:
        raise ValueError("V32 containment logical-source Arrow differs")
    sources = table.column("source_ordinal").to_pylist()
    source_to_logical = [-1] * plan.source_rows
    for logical, source in enumerate(sources):
        if (
            type(source) is not int
            or not 0 <= source < plan.source_rows
            or source_to_logical[source] != -1
        ):
            raise ValueError("V32 containment logical-source permutation differs")
        source_to_logical[source] = logical
    if any(logical < 0 for logical in source_to_logical):
        raise ValueError("V32 containment logical-source permutation differs")
    return tuple(source_to_logical)


def _read_diagnostic_batch(
    plan: V32ContainmentPlan,
    truth: tuple[tuple[int, ...], ...],
    source_to_logical: tuple[int, ...],
) -> None:
    artifact = plan.diagnostic_batch
    if artifact is None:
        raise ValueError("V32 virtual diagnostic batch authority is missing")
    _validate_artifact(artifact)
    payload = artifact.path.read_bytes()
    if (
        len(payload) != artifact.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != artifact.sha256
    ):
        raise ValueError("V32 virtual diagnostic batch byte authority differs")
    try:
        table = pa.ipc.open_file(pa.BufferReader(payload)).read_all()
    except pa.ArrowInvalid as error:
        raise ValueError("V32 virtual diagnostic batch Arrow differs") from error
    truth_type = pa.list_(pa.field("element", pa.uint64(), nullable=False), RECALL_K)
    expected_schema = pa.schema(
        [
            pa.field("query_ordinal", pa.uint64(), nullable=False),
            pa.field("truth_logicals", truth_type, nullable=False),
        ]
    )
    if table.schema != expected_schema or table.num_rows != plan.query_count:
        raise ValueError("V32 virtual diagnostic batch Arrow differs")
    ordinals = table.column("query_ordinal").to_pylist()
    logical_rows = table.column("truth_logicals").to_pylist()
    expected_logicals = [
        [source_to_logical[source] for source in source_row] for source_row in truth
    ]
    if ordinals != list(
        range(plan.query_start, plan.query_start + plan.query_count)
    ) or (logical_rows != expected_logicals):
        raise ValueError("V32 virtual diagnostic batch binding differs")


def _commands(
    plan: V32ContainmentPlan,
    truth: tuple[tuple[int, ...], ...],
    source_to_logical: tuple[int, ...],
    leaf_beam: int,
    *,
    replay_control: bool = False,
    page_budget_ladder: bool = False,
) -> tuple[tuple[str, ...], ...]:
    geometric = (
        plan.virtual_geometric_pages
        or plan.global_geometric_pages
        or page_budget_ladder
    )
    query_count = plan.query_count if geometric else 1
    common = (
        str(plan.qualifier),
        "--execute",
        "--manifest",
        str(plan.manifest.path),
        "--manifest-sha256",
        plan.manifest.sha256,
        "--manifest-bytes",
        str(plan.manifest.encoded_bytes),
        "--artifact-dir",
        str(plan.artifact_dir),
        "--query-parquet",
        str(plan.query.path),
        "--query-sha256",
        plan.query.sha256,
        "--query-bytes",
        str(plan.query.encoded_bytes),
        "--query-count",
        str(query_count),
        "--root-beam",
        str(plan.root_beam),
        "--leaf-beam",
        str(leaf_beam),
        "--candidate-depth",
        "12288",
        "--page-count",
        "16",
        "--k",
        "10",
    )
    scope = (
        ("--global-leaf-limit", str(plan.global_leaf_limit))
        if plan.global_leaf_limit is not None
        else ()
    )
    virtual = ("--virtual-geometric-pages",) if plan.virtual_geometric_pages else ()
    if plan.global_geometric_pages:
        virtual = (
            "--global-replay-control" if replay_control else "--global-geometric-pages",
        )
    if page_budget_ladder:
        virtual = ("--page-budget-ladder",)
    if geometric:
        batch = plan.diagnostic_batch
        if batch is None:
            raise ValueError("V32 virtual diagnostic batch authority is missing")
        return (
            common
            + scope
            + virtual
            + (
                "--query-start",
                str(plan.query_start),
                "--diagnostic-batch-arrow",
                str(batch.path),
                "--diagnostic-batch-sha256",
                batch.sha256,
                "--diagnostic-batch-bytes",
                str(batch.encoded_bytes),
            ),
        )
    return tuple(
        common
        + scope
        + virtual
        + (
            "--query-start",
            str(plan.query_start + offset),
            "--diagnose-logicals",
            ",".join(str(source_to_logical[source]) for source in sources),
        )
        for offset, sources in enumerate(truth)
    )


def build_v32_containment_commands(
    plan: V32ContainmentPlan, truth_bytes: bytes
) -> tuple[tuple[str, ...], ...]:
    """Build one no-page diagnostic invocation per frozen query."""

    _, _, leaf_beam, _ = _read_scale_manifest(plan)
    truth = _read_truth(plan, truth_bytes)
    return _commands(plan, truth, _read_logical_sources(plan), leaf_beam)


def _diagnostics(
    payload: bytes,
    query_ordinal: int,
    truth: tuple[int, ...],
    maximum_leaves_eligible: int,
    leaf_beam: int,
    scan_budget: int,
    global_leaf_limit: int | None,
    root_beam: int,
) -> tuple[
    list[dict[str, object]],
    dict[str, int],
    dict[str, dict[str, object]],
]:
    if (
        type(payload) is not bytes
        or not payload.endswith(b"\n")
        or b"\n" in payload[:-1]
    ):
        raise ValueError("V32 containment diagnostic canonical bytes differ")
    value = json.loads(payload)
    expected = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    if (
        payload != expected
        or type(value) is not dict
        or set(value)
        != {
            "claim_eligible",
            "diagnostics",
            "page_body_reads",
            "page_selections",
            "query_ordinal",
            "routing",
            "schema_version",
            "truth_independent_selection",
        }
        or value["claim_eligible"] is not False
        or value["schema_version"] != 5
        or value["page_body_reads"] != 0
        or value["truth_independent_selection"] is not True
        or value["query_ordinal"] != query_ordinal
        or type(value["diagnostics"]) is not list
        or len(value["diagnostics"]) != RECALL_K
    ):
        raise ValueError("V32 containment diagnostic authority differs")
    diagnostics = value["diagnostics"]
    expected_keys = {
        "candidate_rank",
        "first_unique_page_rank",
        "global_routing_leaf_rank",
        "leaf_ordinal",
        "logical",
        "owner_root_ordinal",
        "owner_root_rank",
        "page_in_retained_pool",
        "page_in_scanned_pool",
        "page_ordinal",
        "page_selected",
        "reciprocal_rank_selected",
        "routing_leaf_rank",
        "stage",
    }
    stages = {"leaf-frontier", "candidate-retention", "page-reducer", "selected-page"}
    for item in diagnostics:
        optional_ranks = (
            (item.get("candidate_rank"), item.get("first_unique_page_rank"))
            if type(item) is dict
            else ()
        )
        if (
            type(item) is not dict
            or set(item) != expected_keys
            or type(item["logical"]) is not int
            or type(item["leaf_ordinal"]) is not int
            or type(item["global_routing_leaf_rank"]) is not int
            or type(item["page_ordinal"]) is not int
            or type(item["owner_root_ordinal"]) is not int
            or type(item["owner_root_rank"]) is not int
            or item["logical"] < 0
            or item["leaf_ordinal"] < 0
            or item["global_routing_leaf_rank"] < 1
            or item["page_ordinal"] < 0
            or item["owner_root_ordinal"] < 0
            or item["owner_root_rank"] < 1
            or type(item["page_in_retained_pool"]) is not bool
            or type(item["page_in_scanned_pool"]) is not bool
            or type(item["page_selected"]) is not bool
            or type(item["reciprocal_rank_selected"]) is not bool
            or (
                item["routing_leaf_rank"] is not None
                and (
                    type(item["routing_leaf_rank"]) is not int
                    or item["routing_leaf_rank"] < 1
                )
            )
            or (
                item["routing_leaf_rank"] is None
                and item["stage"] not in {"leaf-frontier", "selected-page"}
            )
            or item["stage"] not in stages
            or item["page_in_retained_pool"]
            != (item["first_unique_page_rank"] is not None)
            or (item["page_in_retained_pool"] and not item["page_in_scanned_pool"])
            or (item["page_selected"] and not item["page_in_retained_pool"])
            or (item["stage"] == "selected-page") != item["page_selected"]
            or (
                item["stage"] == "candidate-retention"
                and (
                    item["routing_leaf_rank"] is None
                    or item["candidate_rank"] is not None
                )
            )
            or (item["stage"] == "page-reducer" and item["candidate_rank"] is None)
            or any(
                rank is not None and (type(rank) is not int or rank < 0)
                for rank in optional_ranks
            )
        ):
            raise ValueError("V32 containment diagnostic value differs")
    if {item["logical"] for item in diagnostics} != set(truth):
        raise ValueError("V32 containment truth binding differs")
    routing = value["routing"]
    routing_keys = {
        "candidates_retained",
        "codes_scanned",
        "global_leaf_limit",
        "leaves_eligible",
        "leaves_scanned",
        "next_leaf_rows",
        "pages_considered",
        "peak_query_table_pairs_live",
        "query_table_pairs_built",
        "roots_scored",
        "scan_budget",
        "scope",
        "selected_page_bytes",
        "selected_pages",
        "stop_reason",
        "total_routing_leaves",
    }
    if (
        type(routing) is not dict
        or set(routing) != routing_keys
        or any(
            type(routing[key]) is not int or routing[key] < 0
            for key in routing_keys
            - {"global_leaf_limit", "next_leaf_rows", "scope", "stop_reason"}
        )
        or routing["global_leaf_limit"] != global_leaf_limit
        or routing["scan_budget"] != scan_budget
        or routing["scope"]
        != ("global" if global_leaf_limit is not None else "root-gated")
        or not 1 <= routing["codes_scanned"] <= scan_budget
        or routing["candidates_retained"] != min(12_288, routing["codes_scanned"])
        or not (
            1
            <= routing["leaves_scanned"]
            <= min(global_leaf_limit, routing["leaves_eligible"])
            if global_leaf_limit is not None
            else min(leaf_beam, routing["leaves_eligible"])
            <= routing["leaves_scanned"]
            <= routing["leaves_eligible"]
        )
        or routing["leaves_eligible"] > maximum_leaves_eligible
        or routing["peak_query_table_pairs_live"] != 1
        or not 1 <= routing["query_table_pairs_built"] <= routing["leaves_scanned"]
        or routing["roots_scored"] != 128
        or not 16 <= routing["pages_considered"] <= 12_288
        or routing["selected_pages"] != 16
        or routing["selected_page_bytes"] == 0
        or (
            global_leaf_limit is not None
            and routing["leaves_eligible"] != routing["total_routing_leaves"]
        )
        or not 1
        <= routing["total_routing_leaves"]
        <= (
            maximum_leaves_eligible
            if global_leaf_limit is not None
            else maximum_leaves_eligible * 128 // root_beam
        )
    ):
        raise ValueError("V32 containment routing work differs")
    if global_leaf_limit is None:
        if (
            routing["stop_reason"] != "root-gated"
            or routing["next_leaf_rows"] is not None
        ):
            raise ValueError("V32 containment routing stop differs")
    elif routing["stop_reason"] == "all-leaves":
        if (
            routing["leaves_scanned"] != routing["total_routing_leaves"]
            or routing["next_leaf_rows"] is not None
        ):
            raise ValueError("V32 containment routing stop differs")
    elif routing["stop_reason"] == "leaf-limit":
        if (
            routing["leaves_scanned"]
            != min(global_leaf_limit, routing["total_routing_leaves"])
            or routing["next_leaf_rows"] is not None
        ):
            raise ValueError("V32 containment routing stop differs")
    elif routing["stop_reason"] == "scan-budget":
        if (
            type(routing["next_leaf_rows"]) is not int
            or routing["next_leaf_rows"] <= 0
            or routing["leaves_scanned"]
            >= min(global_leaf_limit, routing["total_routing_leaves"])
            or routing["codes_scanned"] + routing["next_leaf_rows"] <= scan_budget
        ):
            raise ValueError("V32 containment routing stop differs")
    else:
        raise ValueError("V32 containment routing stop differs")
    if any(
        item["routing_leaf_rank"] is not None
        and item["routing_leaf_rank"] > routing["leaves_eligible"]
        for item in diagnostics
    ):
        raise ValueError("V32 containment diagnostic value differs")
    if any(
        item["global_routing_leaf_rank"] > routing["total_routing_leaves"]
        or item["owner_root_ordinal"] >= routing["roots_scored"]
        or item["owner_root_rank"] > routing["roots_scored"]
        or (
            global_leaf_limit is not None
            and item["routing_leaf_rank"] != item["global_routing_leaf_rank"]
        )
        or (
            global_leaf_limit is None
            and item["routing_leaf_rank"] is not None
            and item["owner_root_rank"] > root_beam
        )
        for item in diagnostics
    ):
        raise ValueError("V32 containment diagnostic value differs")
    for item in diagnostics:
        routing_leaf_rank = item["routing_leaf_rank"]
        candidate_rank = item["candidate_rank"]
        first_unique_page_rank = item["first_unique_page_rank"]
        target_leaf_scanned = (
            routing_leaf_rank is not None
            and routing_leaf_rank <= routing["leaves_scanned"]
        )
        expected_stage = (
            "selected-page"
            if item["page_selected"]
            else "leaf-frontier"
            if not target_leaf_scanned
            else "candidate-retention"
            if candidate_rank is None
            else "page-reducer"
        )
        if (
            (target_leaf_scanned and not item["page_in_scanned_pool"])
            or item["stage"] != expected_stage
            or (
                candidate_rank is not None
                and (
                    not target_leaf_scanned
                    or not item["page_in_retained_pool"]
                    or candidate_rank >= routing["candidates_retained"]
                    or first_unique_page_rank is None
                    or first_unique_page_rank > candidate_rank
                )
            )
            or (
                first_unique_page_rank is not None
                and first_unique_page_rank >= routing["candidates_retained"]
            )
            or item["page_selected"]
            != (
                first_unique_page_rank is not None
                and first_unique_page_rank < routing["selected_pages"]
            )
            or (
                routing["candidates_retained"] == routing["codes_scanned"]
                and target_leaf_scanned
                and candidate_rank is None
            )
        ):
            raise ValueError("V32 containment diagnostic value differs")
    page_selections = value["page_selections"]
    if type(page_selections) is not dict or set(page_selections) != {
        "first_distinct",
        "reciprocal_rank",
    }:
        raise ValueError("V32 containment page selection differs")
    selected_ordinals: dict[str, set[int]] = {}
    for name, selection in page_selections.items():
        if (
            type(selection) is not dict
            or set(selection) != {"pages", "selected_page_bytes"}
            or type(selection["pages"]) is not list
            or len(selection["pages"]) != routing["selected_pages"]
            or type(selection["selected_page_bytes"]) is not int
            or selection["selected_page_bytes"] <= 0
        ):
            raise ValueError("V32 containment page selection differs")
        ordinals: set[int] = set()
        selected_bytes = 0
        for page in selection["pages"]:
            if (
                type(page) is not dict
                or set(page) != {"encoded_bytes", "ordinal", "sha256"}
                or type(page["encoded_bytes"]) is not int
                or page["encoded_bytes"] <= 0
                or type(page["ordinal"]) is not int
                or page["ordinal"] < 0
                or type(page["sha256"]) is not str
                or not _digest(page["sha256"])
                or page["ordinal"] in ordinals
            ):
                raise ValueError("V32 containment page selection differs")
            ordinals.add(page["ordinal"])
            selected_bytes += page["encoded_bytes"]
        if selected_bytes != selection["selected_page_bytes"]:
            raise ValueError("V32 containment page selection differs")
        selected_ordinals[name] = ordinals
    if page_selections["first_distinct"]["selected_page_bytes"] != routing[
        "selected_page_bytes"
    ] or any(
        (item["stage"] == "selected-page")
        != (item["page_ordinal"] in selected_ordinals["first_distinct"])
        or item["reciprocal_rank_selected"]
        != (item["page_ordinal"] in selected_ordinals["reciprocal_rank"])
        for item in diagnostics
    ):
        raise ValueError("V32 containment page selection differs")
    return diagnostics, routing, page_selections


def _virtual_diagnostics(
    payload: bytes,
    query_ordinal: int,
    truth: tuple[int, ...],
    maximum_leaves_eligible: int,
    leaf_beam: int,
    scan_budget: int,
    global_leaf_limit: int,
    root_beam: int,
) -> tuple[
    list[dict[str, object]],
    dict[str, int],
    dict[str, dict[str, object]],
    dict[str, object],
]:
    if not payload.endswith(b"\n") or b"\n" in payload[:-1]:
        raise ValueError("V32 virtual diagnostic canonical bytes differ")
    value = json.loads(payload)
    expected = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    if (
        payload != expected
        or type(value) is not dict
        or value.get("schema_version") != 6
        or "virtual_geometric" not in value
    ):
        raise ValueError("V32 virtual diagnostic authority differs")
    virtual = value.pop("virtual_geometric")
    value["schema_version"] = 5
    base = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    diagnostics, routing, page_selections = _diagnostics(
        base,
        query_ordinal,
        truth,
        maximum_leaves_eligible,
        leaf_beam,
        scan_budget,
        global_leaf_limit,
        root_beam,
    )
    keys = {
        "candidate_replay_sha256",
        "newly_lost_logicals",
        "page_body_reads",
        "page_rows",
        "projected_selected_bytes",
        "projected_selected_bytes_at_eight",
        "recovered_logicals",
        "selected_pages",
        "selected_pages_at_eight",
        "targets",
        "truth_microleaf_count",
        "truth_virtual_page_count",
        "virtual_layout_sha256",
    }
    if (
        type(virtual) is not dict
        or set(virtual) != keys
        or virtual["page_body_reads"] != 0
        or virtual["page_rows"] != 480
        or virtual["projected_selected_bytes"] != 3_145_728
        or virtual["projected_selected_bytes_at_eight"] != 1_572_864
        or type(virtual["selected_pages"]) is not list
        or len(virtual["selected_pages"]) != 16
        or any(type(page) is not int or page < 0 for page in virtual["selected_pages"])
        or len(set(virtual["selected_pages"])) != 16
        or virtual["selected_pages_at_eight"] != virtual["selected_pages"][:8]
        or type(virtual["targets"]) is not list
        or len(virtual["targets"]) != RECALL_K
        or not _digest(virtual["candidate_replay_sha256"])
        or not _digest(virtual["virtual_layout_sha256"])
    ):
        raise ValueError("V32 virtual diagnostic layout evidence differs")
    selected_pages = set(virtual["selected_pages"])
    selected_pages_at_eight = set(virtual["selected_pages_at_eight"])
    targets = virtual["targets"]
    if any(
        type(target) is not dict
        or set(target) != {"logical", "page_ordinal", "selected", "selected_at_eight"}
        or type(target["logical"]) is not int
        or type(target["page_ordinal"]) is not int
        or target["page_ordinal"] < 0
        or type(target["selected"]) is not bool
        or type(target["selected_at_eight"]) is not bool
        or target["selected"] != (target["page_ordinal"] in selected_pages)
        or target["selected_at_eight"]
        != (target["page_ordinal"] in selected_pages_at_eight)
        for target in targets
    ) or [target["logical"] for target in targets] != [
        target["logical"] for target in diagnostics
    ]:
        raise ValueError("V32 virtual diagnostic target evidence differs")
    expected_recovered = [
        current["logical"]
        for current, treatment in zip(diagnostics, targets, strict=True)
        if not current["page_selected"] and treatment["selected_at_eight"]
    ]
    expected_lost = [
        current["logical"]
        for current, treatment in zip(diagnostics, targets, strict=True)
        if current["page_selected"] and not treatment["selected_at_eight"]
    ]
    truth_microleaves = len({target["leaf_ordinal"] for target in diagnostics})
    truth_pages = len({target["page_ordinal"] for target in targets})
    if (
        virtual["recovered_logicals"] != expected_recovered
        or virtual["newly_lost_logicals"] != expected_lost
        or virtual["truth_microleaf_count"] != truth_microleaves
        or virtual["truth_virtual_page_count"] != truth_pages
    ):
        raise ValueError("V32 virtual diagnostic causal evidence differs")
    return diagnostics, routing, page_selections, virtual


def _virtual_batch_payloads(payload: bytes, query_count: int) -> tuple[bytes, ...]:
    if not payload.endswith(b"\n") or b"\n" in payload[:-1]:
        raise ValueError("V32 virtual diagnostic batch canonical bytes differ")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V32 virtual diagnostic batch JSON differs") from error
    expected = (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    if (
        payload != expected
        or type(value) is not dict
        or set(value)
        != {"claim_eligible", "page_body_reads", "queries", "schema_version"}
        or value["claim_eligible"] is not False
        or value["page_body_reads"] != 0
        or value["schema_version"] != 7
        or type(value["queries"]) is not list
        or len(value["queries"]) != query_count
    ):
        raise ValueError("V32 virtual diagnostic batch authority differs")
    return tuple(
        json.dumps(
            query, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
        for query in value["queries"]
    )


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def run_page_budget_ladder(
    plan: V32ContainmentPlan,
    *,
    invoke: Callable[[tuple[str, ...]], bytes],
) -> bytes:
    """Authenticate local references, invoke once, and independently check coverage."""
    return _run_frontier_replay(plan, invoke=invoke, expanded=False)


def run_expanded_frontier_replay(
    plan: V32ContainmentPlan,
    *,
    invoke: Callable[[tuple[str, ...]], bytes],
) -> bytes:
    """Replay the fixed expanded frontier against the original construction plan."""
    return _run_frontier_replay(plan, invoke=invoke, expanded=True)


def _run_frontier_replay(
    plan: V32ContainmentPlan,
    *,
    invoke: Callable[[tuple[str, ...]], bytes],
    expanded: bool,
) -> bytes:
    if (
        plan.source_rows != 1_000_000
        or plan.query_count != 32
        or plan.global_leaf_limit != 768
        or plan.leaf_beam != 256
        or plan.virtual_geometric_pages
        or plan.global_geometric_pages
        or plan.governing_terminal is not None
    ):
        raise ValueError("V32 page ladder plan differs")
    _, maximum_leaves, leaf_beam, _ = _read_scale_manifest(plan)
    truth = _read_truth(plan, plan.truth.path.read_bytes())
    query = plan.query.path.read_bytes()
    if (
        len(query) != plan.query.encoded_bytes
        or hashlib.sha256(query).hexdigest() != plan.query.sha256
    ):
        raise ValueError("V32 page ladder query bytes differ")
    manifest = json.loads(plan.manifest.path.read_bytes())
    descriptor = manifest.get("diagnostics", {}).get("logical_sources")
    expected = dict(
        file=plan.logical_sources.path.name,
        role="v32-logical-sources-arrow",
        sha256=plan.logical_sources.sha256,
        encoded_bytes=plan.logical_sources.encoded_bytes,
    )
    if descriptor != expected:
        raise ValueError("V32 page ladder logical-source manifest binding differs")
    source_to_logical = _read_logical_sources(plan)
    _read_diagnostic_batch(plan, truth, source_to_logical)
    logical_truth = tuple(tuple(source_to_logical[n] for n in row) for row in truth)
    mapping, registry = read_page_ladder_registry(
        plan.manifest,
        plan.artifact_dir,
        plan.source_rows,
        tuple(n for row in logical_truth for n in row),
    )
    (command,) = _commands(
        plan, truth, source_to_logical, leaf_beam, page_budget_ladder=True
    )
    if expanded:
        arguments = list(command)
        arguments[arguments.index("--global-leaf-limit") + 1] = "1536"
        arguments[arguments.index("--page-budget-ladder")] = (
            "--expanded-frontier-replay"
        )
        command = tuple(arguments)
    del source_to_logical
    payload = invoke(command)
    summary = _validate_page_budget_ladder(
        payload,
        expanded=expanded,
        query_start=plan.query_start,
        truth_logicals=logical_truth,
        logical_pages=mapping,
        registered_pages=registry,
        maximum_leaves_eligible=maximum_leaves,
        root_beam=plan.root_beam,
    )
    return _canonical_bytes(
        dict(
            schema="borsuk-v32-expanded-frontier-v1"
            if expanded
            else "borsuk-v32-page-budget-ladder-v1",
            claim_eligible=False,
            metric="truth-page-containment-not-reranked-recall",
            page_body_reads=0,
            source_rows=plan.source_rows,
            query_start=plan.query_start,
            query_count=plan.query_count,
            manifest_sha256=plan.manifest.sha256,
            query_sha256=plan.query.sha256,
            truth_sha256=plan.truth.sha256,
            truth_receipt_sha256=plan.truth_receipt.sha256,
            logical_sources_sha256=plan.logical_sources.sha256,
            diagnostic_batch_sha256=plan.diagnostic_batch.sha256,
            diagnostic_sha256=hashlib.sha256(payload).hexdigest(),
            summary=summary,
            diagnostic=json.loads(payload),
        )
    )


def read_page_ladder_registry(
    manifest: LocalArtifact,
    artifact_dir: Path,
    source_rows: int,
    requested_logicals: tuple[int, ...],
) -> tuple[dict[int, int], dict[int, dict[str, object]]]:
    """Authenticate the page-range Parquet; map only requested truth IDs.

    Space is proportional to the page registry plus at most 320 truth IDs,
    never a dense logical-row-to-page Python mapping.
    """
    _validate_artifact(manifest)
    raw = manifest.path.read_bytes()
    if (
        len(raw) != manifest.encoded_bytes
        or hashlib.sha256(raw).hexdigest() != manifest.sha256
    ):
        raise ValueError("V32 page registry manifest bytes differ")
    value = json.loads(raw)
    if (
        raw != _canonical_bytes(value)
        or type(value) is not dict
        or not artifact_dir.is_absolute()
        or type(source_rows) is not int
        or source_rows <= 0
        or len(requested_logicals) > 320
        or any(
            type(n) is not int or not 0 <= n < source_rows for n in requested_logicals
        )
        or type(value.get("layout")) is not dict
        or type(value["layout"].get("source_rows")) is not int
        or value["layout"]["source_rows"] != source_rows
    ):
        raise ValueError("V32 page registry manifest differs")
    item = value["layout"].get("page_ranges")
    if (
        type(item) is not dict
        or set(item) != {"file", "role", "sha256", "encoded_bytes"}
        or item["role"] != "v32-page-ranges-parquet"
        or type(item["file"]) is not str
        or not item["file"]
        or Path(item["file"]).name != item["file"]
        or item["file"] in {".", ".."}
        or type(item["sha256"]) is not str
        or not _digest(item["sha256"])
        or type(item["encoded_bytes"]) is not int
        or item["encoded_bytes"] <= 0
    ):
        raise ValueError("V32 page registry descriptor differs")
    path = artifact_dir / item["file"]
    if path.stat().st_size != item["encoded_bytes"]:
        raise ValueError("V32 page registry length differs")
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != item["sha256"]:
        raise ValueError("V32 page registry digest differs")
    table = pq.read_table(pa.BufferReader(payload))
    schema = pa.schema(
        [
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("logical_start", pa.uint64(), nullable=False),
            pa.field("row_count", pa.uint16(), nullable=False),
            pa.field("sha256", pa.string(), nullable=False),
            pa.field("encoded_bytes", pa.uint64(), nullable=False),
            pa.field("primary_rows", pa.uint16(), nullable=False),
            pa.field("replica_rows", pa.uint16(), nullable=False),
        ]
    )
    if (
        table.schema != schema
        or not 1 <= table.num_rows <= source_rows
        or any(c.null_count for c in table.columns)
    ):
        raise ValueError("V32 page registry physical schema differs")
    wanted = sorted(set(requested_logicals))
    target_index = 0
    end = 0
    mapping, pages = {}, {}
    for ordinal, row in enumerate(table.to_pylist()):
        if (
            row["page_ordinal"] != ordinal
            or row["logical_start"] != end
            or not 1 <= row["row_count"] <= 480
            or row["primary_rows"] != row["row_count"]
            or row["replica_rows"] != 0
            or not _digest(row["sha256"])
            or not 0 < row["encoded_bytes"] <= 196608
            or end + row["row_count"] > source_rows
        ):
            raise ValueError("V32 page registry coverage differs")
        end += row["row_count"]
        pages[ordinal] = {
            "ordinal": ordinal,
            **{
                key: row[key]
                for key in ("sha256", "encoded_bytes", "primary_rows", "replica_rows")
            },
        }
        while target_index < len(wanted) and wanted[target_index] < end:
            mapping[wanted[target_index]] = ordinal
            target_index += 1
    if end != source_rows or target_index != len(wanted):
        raise ValueError("V32 page registry coverage differs")
    return mapping, pages


def validate_page_budget_ladder(
    payload: bytes,
    *,
    query_start: int,
    truth_logicals: tuple[tuple[int, ...], ...],
    logical_pages: dict[int, int],
    registered_pages: dict[int, dict[str, object]],
    maximum_leaves_eligible: int,
    root_beam: int,
) -> dict[str, object]:
    return _validate_page_budget_ladder(
        payload,
        query_start=query_start,
        truth_logicals=truth_logicals,
        logical_pages=logical_pages,
        registered_pages=registered_pages,
        maximum_leaves_eligible=maximum_leaves_eligible,
        root_beam=root_beam,
        expanded=False,
    )


def validate_expanded_frontier(
    payload: bytes,
    *,
    query_start: int,
    truth_logicals: tuple[tuple[int, ...], ...],
    logical_pages: dict[int, int],
    registered_pages: dict[int, dict[str, object]],
    maximum_leaves_eligible: int,
    root_beam: int,
) -> dict[str, object]:
    """Independently check the fixed 1536-leaf/524288-code explanatory replay."""
    return _validate_page_budget_ladder(
        payload,
        query_start=query_start,
        truth_logicals=truth_logicals,
        logical_pages=logical_pages,
        registered_pages=registered_pages,
        maximum_leaves_eligible=maximum_leaves_eligible,
        root_beam=root_beam,
        expanded=True,
    )


def root64_metadata_from_bytes(
    roots_bytes: bytes,
    leaves_bytes: bytes,
    routes_bytes: bytes,
    query_bytes: bytes,
    *,
    query_start: int,
) -> Root64Metadata:
    """Parse already-authenticated metadata and reproduce ordered root distances."""
    from scripts.build_v30_reduced_truth import _matrix, _normalize_like_v30

    if type(query_start) is not int or not 0 <= query_start <= 9968:
        raise ValueError("V32 root64 query range differs")
    vector = pa.list_(pa.field("element", pa.float16(), nullable=False), 96)

    def read(raw, schema):
        reader = pa.ipc.open_file(pa.BufferReader(raw))
        if reader.num_record_batches != 1:
            raise ValueError("V32 root64 Arrow batch count differs")
        table = reader.read_all()
        if table.schema != schema or any(c.null_count for c in table.columns):
            raise ValueError("V32 root64 Arrow schema differs")
        return table

    roots = read(roots_bytes, pa.schema([pa.field("centroid", vector, nullable=False)]))
    leaves = read(
        leaves_bytes,
        pa.schema(
            [
                pa.field("root_ordinal", pa.uint16(), nullable=False),
                pa.field("centroid", vector, nullable=False),
            ]
        ),
    )
    routes = read(
        routes_bytes,
        pa.schema(
            [
                pa.field("routing_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("code_parent_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("routing_centroid", vector, nullable=False),
                pa.field("logical_start", pa.uint64(), nullable=False),
                pa.field("row_count", pa.uint64(), nullable=False),
                pa.field("page_start", pa.uint32(), nullable=False),
                pa.field("page_count", pa.uint32(), nullable=False),
            ]
        ),
    )
    if roots.num_rows != 128 or leaves.num_rows != 4096 or not routes.num_rows:
        raise ValueError("V32 root64 hierarchy shape differs")
    for table, name in [
        (roots, "centroid"),
        (leaves, "centroid"),
        (routes, "routing_centroid"),
    ]:
        column = table[name].combine_chunks()
        if column.values.null_count or not np.isfinite(column.values.to_numpy()).all():
            raise ValueError("V32 root64 centroid values differ")
    owners = leaves["root_ordinal"].combine_chunks().to_numpy()
    parents = routes["code_parent_leaf_ordinal"].combine_chunks().to_numpy()
    counts = routes["row_count"].combine_chunks().to_numpy().astype(np.uint64)
    starts = routes["logical_start"].combine_chunks().to_numpy().astype(np.uint64)
    if (
        not np.array_equal(owners, np.arange(4096) // 32)
        or np.any(parents >= 4096)
        or not np.array_equal(
            routes["routing_leaf_ordinal"].to_pylist(), np.arange(len(counts))
        )
        or np.any((counts == 0) | (counts > 1024))
        or int(counts.sum()) != 1_000_000
        or not np.array_equal(starts, np.cumsum(counts) - counts)
    ):
        raise ValueError("V32 root64 ownership or row population differs")
    centers = (
        roots["centroid"]
        .combine_chunks()
        .values.to_numpy()
        .reshape(128, 96)
        .astype(np.float32)
        .astype(np.float64)
    )
    queries = _normalize_like_v30(
        _normalize_like_v30(_matrix(query_bytes, role="query", physical_rows=10000))
    )
    orders = []
    for query in queries[query_start : query_start + 32].astype(np.float64):
        distances = np.zeros(128, dtype=np.float64)
        for dimension in range(96):
            delta = query[dimension] - centers[:, dimension]
            distances += delta * delta
        if not np.isfinite(distances).all():
            raise ValueError("V32 root64 root distances differ")
        orders.append(tuple(int(n) for n in np.lexsort((np.arange(128), distances))))
    return Root64Metadata(
        tuple(orders),
        tuple(int(n) for n in owners[parents]),
        tuple(int(n) for n in counts),
    )


def validate_root64_frontier(
    payload: bytes,
    *,
    query_start: int,
    truth_logicals: tuple[tuple[int, ...], ...],
    logical_pages: dict[int, int],
    registered_pages: dict[int, dict[str, object]],
    metadata: Root64Metadata,
) -> dict[str, object]:
    """Recompute root scope from independently authenticated metadata, not output."""
    if (
        not isinstance(metadata, Root64Metadata)
        or len(metadata.root_orders) != 32
        or not metadata.leaf_rows
        or len(metadata.leaf_rows) != len(metadata.leaf_owners)
        or any(type(n) is not int or not 1 <= n <= 1024 for n in metadata.leaf_rows)
        or sum(metadata.leaf_rows) != 1_000_000
        or any(type(n) is not int or not 0 <= n < 128 for n in metadata.leaf_owners)
        or any(
            len(order) != 128
            or any(type(n) is not int for n in order)
            or sorted(order) != list(range(128))
            for order in metadata.root_orders
        )
    ):
        raise ValueError("V32 root64 metadata differs")
    return _validate_page_budget_ladder(
        payload,
        query_start=query_start,
        truth_logicals=truth_logicals,
        logical_pages=logical_pages,
        registered_pages=registered_pages,
        maximum_leaves_eligible=len(metadata.leaf_rows),
        root_beam=64,
        expanded=False,
        root_metadata=metadata,
    )


def _check_root64_scope(query, targets, work, metadata, offset):
    order = metadata.root_orders[offset]
    expected_roots = list(order[:64])
    observed = query["selected_root_ordinals"]
    if (
        type(observed) is not list
        or any(type(n) is not int for n in observed)
        or observed != expected_roots
    ):
        raise ValueError("V32 root64 selected roots differ")
    membership = set(expected_roots)
    selected = [
        i for i, owner in enumerate(metadata.leaf_owners) if owner in membership
    ]
    rows = sum(metadata.leaf_rows[i] for i in selected)
    if (
        rows > 524288
        or work["codes_scanned"] != rows
        or work["leaves_scanned"] != len(selected)
        or work["leaves_eligible"] != len(selected)
        or work["total_routing_leaves"] != len(metadata.leaf_rows)
    ):
        raise ValueError("V32 root64 complete population differs")
    starts, total = [], 0
    for count in metadata.leaf_rows:
        starts.append(total)
        total += count
    root_ranks = {root: rank + 1 for rank, root in enumerate(order)}
    leaf_ranks = {leaf: rank + 1 for rank, leaf in enumerate(selected)}
    for target in targets:
        logical = target["logical"]
        if not 0 <= logical < total:
            raise ValueError("V32 root64 logical range differs")
        leaf = bisect_right(starts, logical) - 1
        owner = metadata.leaf_owners[leaf]
        if (
            target["leaf_ordinal"] != leaf
            or target["owner_root_ordinal"] != owner
            or target["owner_root_rank"] != root_ranks[owner]
            or target["routing_leaf_rank"] != leaf_ranks.get(leaf)
        ):
            raise ValueError("V32 root64 target scope differs")


def _validate_page_budget_ladder(
    payload: bytes,
    *,
    query_start: int,
    truth_logicals: tuple[tuple[int, ...], ...],
    logical_pages: dict[int, int],
    registered_pages: dict[int, dict[str, object]],
    maximum_leaves_eligible: int,
    root_beam: int,
    expanded: bool,
    root_metadata: Root64Metadata | None = None,
) -> dict[str, object]:
    """Validate coverage only; callers authenticate truth and index registries first.

    The producer's replay digest identifies its capture, not an independent
    reconstruction of ADC scores. No page-read or exact-rerank claim is made.
    """
    value = json.loads(payload)
    if (
        type(query_start) is not int
        or query_start < 0
        or len(truth_logicals) != 32
        or type(value) is not dict
        or set(value)
        != {
            "schema_version",
            "query_start",
            "claim_eligible",
            "page_body_reads",
            "queries",
            "resources",
        }
        or payload != _canonical_bytes(value)
        or type(value["schema_version"]) is not int
        or value["schema_version"]
        != (13 if root_metadata is not None else 12 if expanded else 11)
        or type(value["query_start"]) is not int
        or value["query_start"] != query_start
        or value["claim_eligible"] is not False
        or type(value["page_body_reads"]) is not int
        or value["page_body_reads"] != 0
        or type(value["queries"]) is not list
        or len(value["queries"]) != 32
    ):
        raise ValueError("V32 page ladder envelope differs")
    resources = value["resources"]
    if (
        type(resources) is not dict
        or set(resources) != {"peak_rss_bytes", "phase_wall_ns", "phase_cpu_ns"}
        or any(type(n) is not int or not 0 < n < 2**64 for n in resources.values())
    ):
        raise ValueError("V32 page ladder resources differ")
    totals, byte_totals, minima = [0] * 3, [0] * 3, [1_000_000] * 3
    for offset, query in enumerate(value["queries"]):
        truth = truth_logicals[offset]
        if (
            len(truth) != 10
            or len(set(truth)) != 10
            or any(type(n) is not int or n < 0 for n in truth)
            or type(query) is not dict
            or set(query)
            != (
                {"query_ordinal", "candidate_replay_sha256", "current", "cells"}
                | ({"selected_root_ordinals"} if root_metadata is not None else set())
            )
            or type(query["query_ordinal"]) is not int
            or query["query_ordinal"] != query_start + offset
            or type(query["candidate_replay_sha256"]) is not str
            or not _digest(query["candidate_replay_sha256"])
            or type(query["cells"]) is not list
            or len(query["cells"]) != 3
        ):
            raise ValueError("V32 page ladder query differs")
        targets, _work, selections = _diagnostics(
            _canonical_bytes(query["current"]),
            query_start + offset,
            truth,
            maximum_leaves_eligible,
            maximum_leaves_eligible
            if root_metadata is not None
            else 512
            if expanded
            else 256,
            524288 if expanded or root_metadata is not None else 262144,
            None if root_metadata is not None else 1536 if expanded else 768,
            64 if root_metadata is not None else root_beam,
        )
        if root_metadata is not None:
            _check_root64_scope(query, targets, _work, root_metadata, offset)
        if any(logical_pages.get(t["logical"]) != t["page_ordinal"] for t in targets):
            raise ValueError("V32 page ladder truth ownership differs")
        previous = []
        for index, (cap, cell) in enumerate(
            zip((16, 32, 64), query["cells"], strict=True)
        ):
            if (
                type(cell) is not dict
                or set(cell)
                != {
                    "requested_pages",
                    "selected_page_count",
                    "selected_pages",
                    "selected_page_bytes",
                    "contained_truth_count",
                    "containment_ppm",
                }
                or any(type(cell[k]) is not int for k in cell if k != "selected_pages")
                or cell["requested_pages"] != cap
                or type(cell["selected_pages"]) is not list
                or not 16 <= len(cell["selected_pages"]) <= cap
                or cell["selected_page_count"] != len(cell["selected_pages"])
            ):
                raise ValueError("V32 page ladder cell differs")
            pages = cell["selected_pages"]
            ordinals = set()
            for page in pages:
                if (
                    type(page) is not dict
                    or set(page)
                    != {
                        "ordinal",
                        "sha256",
                        "encoded_bytes",
                        "primary_rows",
                        "replica_rows",
                    }
                    or any(type(page[k]) is not int for k in page if k != "sha256")
                    or not 0 <= page["ordinal"] < 2**32
                    or page["ordinal"] in ordinals
                    or type(page["sha256"]) is not str
                    or not _digest(page["sha256"])
                    or not 0 < page["encoded_bytes"] <= 196608
                    or not 0 <= page["primary_rows"] <= 480
                    or page["replica_rows"] != 0
                    or page != registered_pages.get(page["ordinal"])
                ):
                    raise ValueError("V32 page ladder registered page differs")
                ordinals.add(page["ordinal"])
            if pages[: len(previous)] != previous or (
                index and len(previous) < (16, 32, 64)[index - 1] and pages != previous
            ):
                raise ValueError("V32 page ladder prefix differs")
            if (
                index == 0
                and [
                    {k: p[k] for k in ("ordinal", "sha256", "encoded_bytes")}
                    for p in pages
                ]
                != selections["first_distinct"]["pages"]
            ):
                raise ValueError("V32 page ladder original selection differs")
            for target in targets:
                rank = target["first_unique_page_rank"]
                if len(pages) < cap and rank is not None and rank >= len(pages):
                    raise ValueError("V32 page ladder exhausted population differs")
                if rank is not None and rank < len(pages):
                    if pages[rank]["ordinal"] != target["page_ordinal"]:
                        raise ValueError("V32 page ladder target rank differs")
                elif target["page_ordinal"] in ordinals:
                    raise ValueError("V32 page ladder target membership differs")
            hits = sum(logical_pages[n] in ordinals for n in truth)
            size = sum(p["encoded_bytes"] for p in pages)
            if (
                cell["contained_truth_count"] != hits
                or cell["containment_ppm"] != hits * 100000
                or cell["selected_page_bytes"] != size
            ):
                raise ValueError("V32 page ladder arithmetic differs")
            totals[index] += hits
            byte_totals[index] += size
            minima[index] = min(minima[index], hits * 100000)
            previous = pages
    return {
        "claim_eligible": False,
        "contained_truth_counts": totals,
        "selected_page_bytes": byte_totals,
        "minimum_containment_ppm": minima,
    }


def _global_envelope(payload: bytes, *, treatment: bool) -> dict[str, object]:
    value = json.loads(payload)
    keys = {
        "claim_eligible",
        "page_body_reads",
        "queries",
        "schema_version",
        "resources",
    }
    if treatment:
        keys |= {"layout_algorithm", "page_row_counts"}
    if (
        type(value) is not dict
        or set(value) != keys
        or payload != _canonical_bytes(value)
        or value["claim_eligible"] is not False
        or type(value["page_body_reads"]) is not int
        or value["page_body_reads"] != 0
        or type(value["schema_version"]) is not int
        or value["schema_version"] != (9 if treatment else 10)
        or type(value["queries"]) is not list
        or len(value["queries"]) != 32
    ):
        raise ValueError("V32 global batch authority differs")
    resources = value["resources"]
    if (
        type(resources) is not dict
        or set(resources) != {"peak_rss_bytes", "phase_wall_ns", "phase_cpu_ns"}
        or any(type(v) is not int or v <= 0 for v in resources.values())
    ):
        raise ValueError("V32 global resource evidence differs")
    return value


def _global_control_payloads(
    payload: bytes,
) -> tuple[tuple[bytes, ...], tuple[str, ...]]:
    value = _global_envelope(payload, treatment=False)
    currents = []
    hashes = []
    for ordinal, query in enumerate(value["queries"], 64):
        if (
            type(query) is not dict
            or set(query) != {"candidate_replay_sha256", "current", "pq_work"}
            or type(query["candidate_replay_sha256"]) is not str
            or not _digest(query["candidate_replay_sha256"])
            or type(query["current"]) is not dict
            or type(query["current"].get("query_ordinal")) is not int
            or query["current"]["query_ordinal"] != ordinal
        ):
            raise ValueError("V32 global control replay authority differs")
        _validate_pq_work(query["pq_work"], query["current"].get("routing"))
        currents.append(_canonical_bytes(query["current"]))
        hashes.append(query["candidate_replay_sha256"])
    return tuple(currents), tuple(hashes)


def _validate_pq_work(value: object, routing: object) -> None:
    if (
        type(value) is not dict
        or set(value) != {"base", "high"}
        or type(routing) is not dict
    ):
        raise ValueError("V32 PQ work schema differs")
    parents = routing.get("query_table_pairs_built")
    codes = routing.get("codes_scanned")
    if (
        type(parents) is not int
        or not 1 <= parents <= 768
        or type(codes) is not int
        or not 1 <= codes <= 262144
    ):
        raise ValueError("V32 PQ routing bounds differ")
    rows = 0
    for name, width in (("base", 24), ("high", 48)):
        work = value[name]
        if type(work) is not dict or set(work) != {
            "entries_evaluated",
            "cache_hits",
            "eager_fallbacks",
        }:
            raise ValueError("V32 PQ width schema differs")
        if any(type(n) is not int or not 0 <= n < 2**64 for n in work.values()):
            raise ValueError("V32 PQ work integer differs")
        evaluated, hits, fallbacks = (
            work["entries_evaluated"],
            work["cache_hits"],
            work["eager_fallbacks"],
        )
        if (
            fallbacks > parents
            or not fallbacks * width * 256 <= evaluated <= parents * width * 256
            or (evaluated == 0 and hits != 0)
        ):
            raise ValueError("V32 PQ work bounds differ")
        accesses = evaluated - fallbacks * width * 256 + hits
        if accesses >= 2**64 or accesses % width:
            raise ValueError("V32 PQ row work differs")
        rows += accesses // width
    if rows != codes:
        raise ValueError("V32 PQ scanned-row work differs")


def _global_treatment_payloads(
    payload: bytes, hashes: tuple[str, ...]
) -> tuple[tuple[bytes, ...], list[int]]:
    value = _global_envelope(payload, treatment=True)
    counts = value["page_row_counts"]
    if (
        value["layout_algorithm"] != "v32-global-balanced-cosine-v1"
        or type(counts) is not list
        or len(counts) != 2084
        or any(type(count) is not int or not 1 <= count <= 480 for count in counts)
        or sum(counts) != 1_000_000
    ):
        raise ValueError("V32 global page map authority differs")
    payloads = []
    for ordinal, (query, expected_hash) in enumerate(
        zip(value["queries"], hashes, strict=True), 64
    ):
        if (
            type(query) is not dict
            or query.get("query_ordinal") != ordinal
            or type(query.get("virtual_geometric")) is not dict
        ):
            raise ValueError("V32 global treatment query authority differs")
        virtual = query["virtual_geometric"]
        if virtual.get("candidate_replay_sha256") != expected_hash:
            raise ValueError("V32 global candidate replay differs")
        pages = virtual.get("selected_pages")
        targets = virtual.get("targets")
        if (
            type(pages) is not list
            or type(targets) is not list
            or any(
                type(page) is not int or not 0 <= page < len(counts) for page in pages
            )
            or any(
                type(target) is not dict
                or type(target.get("page_ordinal")) is not int
                or not 0 <= target["page_ordinal"] < len(counts)
                for target in targets
            )
        ):
            raise ValueError("V32 global page reference differs")
        payloads.append(_canonical_bytes(query))
    return tuple(payloads), counts


def run_v32_no_page_containment(
    plan: V32ContainmentPlan,
    truth_bytes: bytes,
    *,
    invoke: Callable[[tuple[str, ...]], bytes],
) -> bytes:
    """Run and independently reduce the page-free scale containment gate."""

    if plan.global_geometric_pages and (
        plan.virtual_geometric_pages
        or plan.source_rows != 1_000_000
        or plan.query_start != 64
        or plan.query_count != 32
        or plan.global_leaf_limit != 768
    ):
        raise ValueError("V32 global diagnostic shape differs")
    geometric = plan.virtual_geometric_pages or plan.global_geometric_pages
    (
        maximum_routing_leaf_rows,
        maximum_leaves_eligible,
        leaf_beam,
        scan_budget,
    ) = _read_scale_manifest(plan)
    truth = _read_truth(plan, truth_bytes)
    source_to_logical = _read_logical_sources(plan)
    governing_terminal = _read_governing_terminal(plan) if geometric else None
    if geometric:
        _read_diagnostic_batch(plan, truth, source_to_logical)
    commands = _commands(plan, truth, source_to_logical, leaf_beam)
    global_counts = None
    if plan.global_geometric_pages:
        control_command = _commands(
            plan, truth, source_to_logical, leaf_beam, replay_control=True
        )[0]
        control_bytes = invoke(control_command)
        control_resources = _global_envelope(control_bytes, treatment=False)[
            "resources"
        ]
        control_payloads, replay_hashes = _global_control_payloads(control_bytes)
        control_plan = replace(
            plan,
            global_geometric_pages=False,
            virtual_geometric_pages=False,
            diagnostic_batch=None,
        )
        control = run_v32_no_page_containment(
            control_plan,
            truth_bytes,
            invoke=lambda command: control_payloads[
                int(command[command.index("--query-start") + 1]) - 64
            ],
        )
        if governing_terminal is None or control != governing_terminal[0]:
            raise ValueError("V32 governing terminal preflight replay differs")
        controller_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        control_peak = controller_peak + control_resources["peak_rss_bytes"]
        if control_peak > 2147483648:
            return _canonical_bytes(
                {
                    "schema_version": 10,
                    "claim_eligible": False,
                    "page_body_reads": 0,
                    "status": "failed",
                    "failed_gates": ["process-tree-memory"],
                    "treatment_executed": False,
                    "layout_algorithm": "v32-global-balanced-cosine-v1",
                    "governing_terminal_uri": plan.governing_terminal.uri,
                    "governing_terminal_sha256": plan.governing_terminal.sha256,
                    "control": json.loads(control),
                    "resources": {
                        "control": control_resources,
                        "controller_peak_rss_bytes": controller_peak,
                        "conservative_peak_rss_bytes": control_peak,
                        "memory_limit_bytes": 2147483648,
                    },
                }
            )
        treatment_bytes = invoke(commands[0])
        treatment_resources = _global_envelope(treatment_bytes, treatment=True)[
            "resources"
        ]
        payloads, global_counts = _global_treatment_payloads(
            treatment_bytes, replay_hashes
        )
    else:
        payloads = (
            _virtual_batch_payloads(invoke(commands[0]), plan.query_count)
            if geometric
            else tuple(invoke(command) for command in commands)
        )
    samples = []
    queries = []
    reciprocal_hits_by_query = []
    routing_work = []
    truth_microleaf_ranks = []
    losses: Counter[str] = Counter()
    virtual_hits_by_query: list[int] = []
    virtual_truth_microleaf_counts: list[int] = []
    virtual_truth_page_counts: list[int] = []
    virtual_recovered: list[int] = []
    virtual_lost: list[int] = []
    virtual_queries: list[dict[str, object]] = []
    virtual_layout_sha256: str | None = None
    for offset, (payload, source_ordinals) in enumerate(
        zip(payloads, truth, strict=True)
    ):
        query_ordinal = plan.query_start + offset
        logicals = tuple(source_to_logical[source] for source in source_ordinals)
        if geometric:
            if plan.global_leaf_limit is None:
                raise ValueError("V32 virtual diagnostic global limit is missing")
            diagnostics, routing, page_selections, virtual = _virtual_diagnostics(
                payload,
                query_ordinal,
                logicals,
                maximum_leaves_eligible,
                leaf_beam,
                scan_budget,
                plan.global_leaf_limit,
                plan.root_beam,
            )
            virtual_hits_by_query.append(
                sum(target["selected_at_eight"] for target in virtual["targets"])
            )
            virtual_truth_microleaf_counts.append(virtual["truth_microleaf_count"])
            virtual_truth_page_counts.append(virtual["truth_virtual_page_count"])
            virtual_recovered.extend(virtual["recovered_logicals"])
            virtual_lost.extend(virtual["newly_lost_logicals"])
            if virtual_layout_sha256 is None:
                virtual_layout_sha256 = virtual["virtual_layout_sha256"]
            elif virtual_layout_sha256 != virtual["virtual_layout_sha256"]:
                raise ValueError("V32 virtual diagnostic layout identity differs")
            virtual_queries.append({"query_ordinal": query_ordinal, **virtual})
        else:
            diagnostics, routing, page_selections = _diagnostics(
                payload,
                query_ordinal,
                logicals,
                maximum_leaves_eligible,
                leaf_beam,
                scan_budget,
                plan.global_leaf_limit,
                plan.root_beam,
            )
        routing_work.append(routing)
        truth_microleaf_ranks.extend(
            item["routing_leaf_rank"]
            for item in diagnostics
            if item["routing_leaf_rank"] is not None
        )
        hits = sum(item["stage"] == "selected-page" for item in diagnostics)
        losses.update(
            str(item["stage"])
            for item in diagnostics
            if item["stage"] != "selected-page"
        )
        samples.append({"hits": hits, "query_ordinal": query_ordinal})
        targets_by_logical = {item["logical"]: item for item in diagnostics}
        targets = [
            {
                **targets_by_logical[logical],
                "source_ordinal": source_ordinal,
                "truth_position": truth_position,
            }
            for truth_position, (source_ordinal, logical) in enumerate(
                zip(source_ordinals, logicals, strict=True)
            )
        ]
        reciprocal_hits = sum(item["reciprocal_rank_selected"] for item in targets)
        reciprocal_hits_by_query.append(reciprocal_hits)
        queries.append(
            {
                "baseline_hits": hits,
                "lost_logicals": [
                    item["logical"]
                    for item in targets
                    if item["stage"] == "selected-page"
                    and not item["reciprocal_rank_selected"]
                ],
                "query_ordinal": query_ordinal,
                "page_selections": page_selections,
                "reciprocal_rank_hits": reciprocal_hits,
                "recovered_logicals": [
                    item["logical"]
                    for item in targets
                    if item["stage"] != "selected-page"
                    and item["reciprocal_rank_selected"]
                ],
                "routing": routing,
                "targets": targets,
            }
        )
    total_hits = sum(sample["hits"] for sample in samples)
    aggregate = total_hits * 1_000_000 // (QUERY_COUNT * RECALL_K)
    minimum = min(sample["hits"] for sample in samples) * 1_000_000 // RECALL_K
    perfect = sum(sample["hits"] == RECALL_K for sample in samples)
    failed = [] if total_hits == QUERY_COUNT * RECALL_K else ["perfect-containment"]
    maximum_selected_page_bytes = max(
        work["selected_page_bytes"] for work in routing_work
    )
    maximum_codes_scanned = max(work["codes_scanned"] for work in routing_work)
    maximum_leaves_eligible = max(work["leaves_eligible"] for work in routing_work)
    maximum_leaves_scanned = max(work["leaves_scanned"] for work in routing_work)
    maximum_query_table_pairs_built = max(
        work["query_table_pairs_built"] for work in routing_work
    )
    maximum_peak_query_table_pairs_live = max(
        work["peak_query_table_pairs_live"] for work in routing_work
    )
    if maximum_selected_page_bytes > 3_145_728:
        failed.append("selected-page-bytes")
    if maximum_codes_scanned > scan_budget:
        failed.append("maximum-codes-scanned")
    if maximum_routing_leaf_rows > 1_024:
        failed.append("maximum-routing-leaf-rows")
    reciprocal_total_hits = sum(reciprocal_hits_by_query)
    reciprocal_maximum_selected_page_bytes = max(
        query["page_selections"]["reciprocal_rank"]["selected_page_bytes"]
        for query in queries
    )
    reciprocal_failed = (
        []
        if reciprocal_total_hits == QUERY_COUNT * RECALL_K
        else ["perfect-containment"]
    )
    if reciprocal_maximum_selected_page_bytes > 3_145_728:
        reciprocal_failed.append("selected-page-bytes")
    reciprocal_rank = {
        "aggregate_containment_ppm": reciprocal_total_hits
        * 1_000_000
        // (QUERY_COUNT * RECALL_K),
        "failed_gates": reciprocal_failed,
        "minimum_containment_ppm": min(reciprocal_hits_by_query)
        * 1_000_000
        // RECALL_K,
        "maximum_selected_page_bytes": reciprocal_maximum_selected_page_bytes,
        "perfect_queries": sum(hits == RECALL_K for hits in reciprocal_hits_by_query),
        "selected_page_hits": reciprocal_total_hits,
        "status": "passed" if not reciprocal_failed else "failed",
    }
    virtual_geometric = None
    if geometric:
        if total_hits != 308 or minimum != 700_000 or perfect != 23:
            raise ValueError("V32 virtual diagnostic frozen control differs")
        if reciprocal_total_hits != 298:
            raise ValueError("V32 virtual diagnostic reciprocal control differs")
        virtual_total_hits = sum(virtual_hits_by_query)
        virtual_minimum = min(virtual_hits_by_query) * 1_000_000 // RECALL_K
        virtual_failed = []
        if virtual_total_hits != QUERY_COUNT * RECALL_K:
            virtual_failed.append("perfect-containment")
        if virtual_minimum != 1_000_000:
            virtual_failed.append("minimum-containment")
        if not plan.global_geometric_pages and any(
            count > 8 for count in virtual_truth_microleaf_counts
        ):
            virtual_failed.append("microleaf-eight-page-obstruction")
        if any(count > 8 for count in virtual_truth_page_counts):
            virtual_failed.append("virtual-eight-page-obstruction")
        virtual_geometric = {
            "aggregate_containment_ppm": virtual_total_hits
            * 1_000_000
            // (QUERY_COUNT * RECALL_K),
            "eight_page_microleaf_obstruction_queries": sum(
                count > 8 for count in virtual_truth_microleaf_counts
            ),
            "eight_page_virtual_page_obstruction_queries": sum(
                count > 8 for count in virtual_truth_page_counts
            ),
            "failed_gates": virtual_failed,
            "maximum_truth_microleaf_count": max(virtual_truth_microleaf_counts),
            "maximum_truth_virtual_page_count": max(virtual_truth_page_counts),
            "minimum_containment_ppm": virtual_minimum,
            "newly_lost_logicals": virtual_lost,
            "perfect_queries": sum(hits == RECALL_K for hits in virtual_hits_by_query),
            "projected_selected_page_bytes": 1_572_864,
            "queries": virtual_queries,
            "recovered_logicals": virtual_recovered,
            "selected_page_hits": virtual_total_hits,
            "selected_pages": 8,
            "status": "passed" if not virtual_failed else "failed",
            "virtual_layout_sha256": virtual_layout_sha256,
        }
    value = {
        "aggregate_containment_ppm": aggregate,
        "claim_eligible": False,
        "failed_gates": failed,
        "losses_by_stage": dict(sorted(losses.items())),
        "manifest_sha256": plan.manifest.sha256,
        "maximum_codes_scanned": maximum_codes_scanned,
        "maximum_leaves_eligible": maximum_leaves_eligible,
        "maximum_leaves_scanned": maximum_leaves_scanned,
        "maximum_truth_microleaf_rank": max(truth_microleaf_ranks, default=None),
        "maximum_query_table_pairs_built": maximum_query_table_pairs_built,
        "maximum_peak_query_table_pairs_live": maximum_peak_query_table_pairs_live,
        "maximum_routing_leaf_rows": maximum_routing_leaf_rows,
        "maximum_selected_page_bytes": maximum_selected_page_bytes,
        "minimum_containment_ppm": minimum,
        "logical_sources_sha256": plan.logical_sources.sha256,
        "leaf_beam": leaf_beam,
        "global_leaf_limit": plan.global_leaf_limit,
        "page_body_reads": 0,
        "perfect_queries": perfect,
        "query_count": QUERY_COUNT,
        "query_start": plan.query_start,
        "query_sha256": plan.query.sha256,
        "queries": queries,
        "reciprocal_rank": reciprocal_rank,
        "root_beam": plan.root_beam,
        "routing_scope": "global"
        if plan.global_leaf_limit is not None
        else "root-gated",
        "samples": samples,
        "schema_version": 4,
        "selected_page_hits": total_hits,
        "source_rows": plan.source_rows,
        "status": "passed" if not failed else "failed",
        "truth_sha256": plan.truth.sha256,
        "truth_receipt_sha256": plan.truth_receipt.sha256,
    }
    if virtual_geometric is not None:
        assert governing_terminal is not None
        assert plan.governing_terminal is not None
        governing_raw, _governing_value = governing_terminal
        computed_control = (
            json.dumps(
                value,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        if computed_control != governing_raw:
            raise ValueError("V32 governing terminal replay evidence differs")
        control_keys = {
            "aggregate_containment_ppm",
            "failed_gates",
            "losses_by_stage",
            "maximum_codes_scanned",
            "maximum_leaves_eligible",
            "maximum_leaves_scanned",
            "maximum_peak_query_table_pairs_live",
            "maximum_query_table_pairs_built",
            "maximum_selected_page_bytes",
            "maximum_truth_microleaf_rank",
            "minimum_containment_ppm",
            "perfect_queries",
            "queries",
            "reciprocal_rank",
            "samples",
            "selected_page_hits",
            "status",
        }
        control = {key: value.pop(key) for key in sorted(control_keys)}
        value["control"] = control
        value["governing_terminal_sha256"] = plan.governing_terminal.sha256
        value["governing_terminal_uri"] = plan.governing_terminal.uri
        value["aggregate_containment_ppm"] = virtual_geometric[
            "aggregate_containment_ppm"
        ]
        value["failed_gates"] = virtual_geometric["failed_gates"]
        value["minimum_containment_ppm"] = virtual_geometric["minimum_containment_ppm"]
        value["perfect_queries"] = virtual_geometric["perfect_queries"]
        value["selected_page_hits"] = virtual_geometric["selected_page_hits"]
        value["status"] = virtual_geometric["status"]
        value["virtual_geometric"] = virtual_geometric
        value["schema_version"] = 6
        if plan.global_geometric_pages:
            value["schema_version"] = 7
            value["layout_algorithm"] = "v32-global-balanced-cosine-v1"
            value["page_row_counts"] = global_counts
            controller_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            conservative_peak = controller_peak + max(
                control_resources["peak_rss_bytes"],
                treatment_resources["peak_rss_bytes"],
            )
            value["resources"] = {
                "control": control_resources,
                "treatment": treatment_resources,
                "controller_peak_rss_bytes": controller_peak,
                "conservative_peak_rss_bytes": conservative_peak,
                "memory_limit_bytes": 2147483648,
            }
            if conservative_peak > 2147483648:
                value["failed_gates"] = [*value["failed_gates"], "process-tree-memory"]
                value["status"] = "failed"
    return (
        json.dumps(
            value, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


def containment_exit_status(payload: bytes) -> int:
    """Return success only for the canonical passing containment terminal."""
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return 2
    return (
        0
        if type(value) is dict
        and value.get("status") == "passed"
        and value.get("failed_gates") == []
        else 2
    )


def _invoke(command: tuple[str, ...]) -> bytes:
    completed = subprocess.run(command, check=False, capture_output=True)
    if completed.returncode != 0:
        detail = completed.stderr.decode(errors="replace").strip()
        raise RuntimeError(
            f"V32 containment qualifier failed ({completed.returncode}): {detail}"
        )
    return completed.stdout


def main(arguments: list[str] | None = None) -> int:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.add_argument("--qualifier", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--manifest-bytes", type=int, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--query-parquet", type=Path, required=True)
    parser.add_argument("--query-sha256", required=True)
    parser.add_argument("--query-bytes", type=int, required=True)
    parser.add_argument("--logical-sources-arrow", type=Path, required=True)
    parser.add_argument("--logical-sources-sha256", required=True)
    parser.add_argument("--logical-sources-bytes", type=int, required=True)
    parser.add_argument("--truth-parquet", type=Path, required=True)
    parser.add_argument("--truth-sha256", required=True)
    parser.add_argument("--truth-bytes", type=int, required=True)
    parser.add_argument("--truth-receipt", type=Path, required=True)
    parser.add_argument("--truth-receipt-sha256", required=True)
    parser.add_argument("--truth-receipt-bytes", type=int, required=True)
    parser.add_argument("--source-rows", type=int, required=True)
    parser.add_argument("--query-start", type=int, required=True)
    parser.add_argument("--query-count", type=int, required=True)
    parser.add_argument("--root-beam", type=int, required=True)
    parser.add_argument("--leaf-beam", type=int, required=True)
    parser.add_argument("--global-leaf-limit", type=int)
    parser.add_argument("--virtual-geometric-pages", action="store_true")
    parser.add_argument("--global-geometric-pages", action="store_true")
    parser.add_argument("--diagnostic-batch-arrow", type=Path)
    parser.add_argument("--diagnostic-batch-sha256")
    parser.add_argument("--diagnostic-batch-bytes", type=int)
    parser.add_argument("--governing-terminal", type=Path)
    parser.add_argument("--governing-terminal-uri")
    parser.add_argument("--governing-terminal-sha256")
    parser.add_argument("--governing-terminal-bytes", type=int)
    args = parser.parse_args(arguments)
    batch_values = (
        args.diagnostic_batch_arrow,
        args.diagnostic_batch_sha256,
        args.diagnostic_batch_bytes,
    )
    if any(value is not None for value in batch_values) != all(
        value is not None for value in batch_values
    ):
        parser.error("diagnostic batch authority is incomplete")
    terminal_values = (
        args.governing_terminal,
        args.governing_terminal_uri,
        args.governing_terminal_sha256,
        args.governing_terminal_bytes,
    )
    if any(value is not None for value in terminal_values) != all(
        value is not None for value in terminal_values
    ):
        parser.error("governing terminal authority is incomplete")
    if args.virtual_geometric_pages and args.global_geometric_pages:
        parser.error("geometric modes are mutually exclusive")
    if (
        args.virtual_geometric_pages or args.global_geometric_pages
    ) and args.governing_terminal is None:
        parser.error("virtual geometric pages require a governing terminal")
    plan = V32ContainmentPlan(
        qualifier=args.qualifier,
        manifest=LocalArtifact(
            args.manifest, args.manifest_sha256, args.manifest_bytes
        ),
        artifact_dir=args.artifact_dir,
        query=LocalArtifact(args.query_parquet, args.query_sha256, args.query_bytes),
        logical_sources=LocalArtifact(
            args.logical_sources_arrow,
            args.logical_sources_sha256,
            args.logical_sources_bytes,
        ),
        truth=LocalArtifact(args.truth_parquet, args.truth_sha256, args.truth_bytes),
        truth_receipt=LocalArtifact(
            args.truth_receipt,
            args.truth_receipt_sha256,
            args.truth_receipt_bytes,
        ),
        source_rows=args.source_rows,
        query_start=args.query_start,
        query_count=args.query_count,
        root_beam=args.root_beam,
        leaf_beam=args.leaf_beam,
        global_leaf_limit=args.global_leaf_limit,
        virtual_geometric_pages=args.virtual_geometric_pages,
        global_geometric_pages=args.global_geometric_pages,
        diagnostic_batch=(
            LocalArtifact(
                args.diagnostic_batch_arrow,
                args.diagnostic_batch_sha256,
                args.diagnostic_batch_bytes,
            )
            if args.diagnostic_batch_arrow is not None
            else None
        ),
        governing_terminal=(
            RegisteredLocalArtifact(
                args.governing_terminal,
                args.governing_terminal_uri,
                args.governing_terminal_sha256,
                args.governing_terminal_bytes,
            )
            if args.governing_terminal is not None
            else None
        ),
    )
    truth = args.truth_parquet.read_bytes()
    payload = run_v32_no_page_containment(plan, truth, invoke=_invoke)
    sys.stdout.buffer.write(payload)
    return containment_exit_status(payload)


if __name__ == "__main__":
    raise SystemExit(main())
