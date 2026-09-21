#!/usr/bin/env python3
"""Independent semantic reducer for canonical V101 result bytes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal, Mapping

from scripts.v97_row_width_screen import ScreenAuthority
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v100_page_residual_range_rescore import (
    _aggregate,
    _bootstrap_matrix,
    _canonical_json_bytes,
    _paired_interval,
    _valid_sha256,
    _validate_sample,
)
from scripts.v101_two_stage_residual_pq_screen import (
    project_v101_resident_bytes_100m,
)


@dataclass(frozen=True, slots=True)
class V101Rescore:
    """Authenticated independently recomputed V101 decision summary."""

    schema: str
    result_sha256: str
    query_count: int
    classification: str
    control_aggregate: Mapping[str, object]
    challenger_aggregate: Mapping[str, object]
    paired: Mapping[str, object]
    projection_total_bytes: int
    status: Literal["verified"]


def _validate_array_identity(value: object) -> None:
    if (
        type(value) is not dict
        or set(value) != {"bytes", "dtype", "sha256", "shape"}
        or type(value["bytes"]) is not int
        or value["bytes"] <= 0
        or type(value["dtype"]) is not str
        or not value["dtype"]
        or not _valid_sha256(value["sha256"])
        or type(value["shape"]) is not list
        or not value["shape"]
        or any(type(size) is not int or size <= 0 for size in value["shape"])
    ):
        raise ValueError("V101 artifact array identity differs")


def rescore_v101_result(
    body: bytes,
    *,
    expected_authority: ScreenAuthority,
    expected_config: RankedGapConfig,
    expected_result_sha256: str,
) -> V101Rescore:
    """Authenticate canonical bytes and independently recompute V101 gates."""

    if (
        not _valid_sha256(expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        raise ValueError("V101 result identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V101 result JSON differs") from error
    if type(value) is not dict or _canonical_json_bytes(value) != body:
        raise ValueError("V101 result canonical bytes differ")
    identity_names = (
        "first_books_identity",
        "second_books_identity",
        "first_codes_identity",
        "second_codes_identity",
        "cross_terms_identity",
    )
    expected_keys = {
        "schema",
        "authority",
        "config",
        "query_count",
        "bootstrap_seed",
        "bootstrap_resamples",
        "bootstrap_matrix_sha256",
        "classification",
        "artifact_sha256",
        *identity_names,
        "control_aggregate",
        "control_samples",
        "challenger_aggregate",
        "challenger_samples",
        "paired",
        "projection",
    }
    if (
        set(value) != expected_keys
        or value["schema"] != "borsuk-v101-two-stage-residual-pq-v1"
        or value["authority"] != asdict(expected_authority)
        or value["config"] != asdict(expected_config)
        or type(value["query_count"]) is not int
        or value["query_count"] <= 0
        or value["bootstrap_seed"] != expected_authority.seed
        or value["bootstrap_resamples"] != 10_000
        or not _valid_sha256(value["artifact_sha256"])
    ):
        raise ValueError("V101 result authority differs")
    for name in identity_names:
        _validate_array_identity(value[name])
    query_count = value["query_count"]
    control_samples = value["control_samples"]
    challenger_samples = value["challenger_samples"]
    if (
        type(control_samples) is not list
        or type(challenger_samples) is not list
        or len(control_samples) != query_count
        or len(challenger_samples) != query_count
    ):
        raise ValueError("V101 result sample cohort differs")
    neighbors = (
        len(control_samples[0].get("truth_ids", []))
        if type(control_samples[0]) is dict
        else 0
    )
    if neighbors <= 0:
        raise ValueError("V101 result neighbor count differs")
    control_recalls = tuple(
        _validate_sample(
            sample,
            query_ordinal=index,
            neighbors=neighbors,
            config=expected_config,
        )
        for index, sample in enumerate(control_samples)
    )
    challenger_recalls = tuple(
        _validate_sample(
            sample,
            query_ordinal=index,
            neighbors=neighbors,
            config=expected_config,
        )
        for index, sample in enumerate(challenger_samples)
    )
    control_aggregate = _aggregate(
        control_recalls, control_samples, expected_config
    )
    challenger_aggregate = _aggregate(
        challenger_recalls, challenger_samples, expected_config
    )
    if (
        value["control_aggregate"] != control_aggregate
        or value["challenger_aggregate"] != challenger_aggregate
    ):
        raise ValueError("V101 aggregate differs")
    matrix = _bootstrap_matrix(query_count, expected_authority.seed)
    if (
        hashlib.sha256(matrix.tobytes(order="C")).hexdigest()
        != value["bootstrap_matrix_sha256"]
    ):
        raise ValueError("V101 bootstrap authority differs")
    control10 = tuple(item[0] for item in control_recalls)
    control100 = tuple(item[1] for item in control_recalls)
    challenger10 = tuple(item[0] for item in challenger_recalls)
    challenger100 = tuple(item[1] for item in challenger_recalls)
    paired = {
        "name": "two-stage-residual-pq8x8",
        "average_recall10_ppm": list(
            _paired_interval(challenger10, control10, matrix, p05=False)
        ),
        "average_recall100_ppm": list(
            _paired_interval(challenger100, control100, matrix, p05=False)
        ),
        "p05_recall100_ppm": list(
            _paired_interval(challenger100, control100, matrix, p05=True)
        ),
    }
    if value["paired"] != paired:
        raise ValueError("V101 paired evidence differs")
    projection = asdict(
        project_v101_resident_bytes_100m(
            expected_config, dimensions=expected_authority.dimensions
        )
    )
    if value["projection"] != projection:
        raise ValueError("V101 projection differs")
    qualified = (
        challenger_aggregate["quality_gate_passed"]
        and challenger_aggregate["resource_gate_passed"]
        and projection["eligible"]
        and paired["average_recall100_ppm"][0] >= 0
    )
    classification = (
        "two-stage-residual-pq-qualified"
        if qualified
        else "two-stage-residual-pq-rejected"
    )
    if value["classification"] != classification:
        raise ValueError("V101 classification differs")
    return V101Rescore(
        schema="borsuk-v101-two-stage-residual-pq-rescore-v1",
        result_sha256=expected_result_sha256,
        query_count=query_count,
        classification=classification,
        control_aggregate=control_aggregate,
        challenger_aggregate=challenger_aggregate,
        paired=paired,
        projection_total_bytes=projection["total_bytes"],
        status="verified",
    )


def canonical_v101_rescore_bytes(summary: V101Rescore) -> bytes:
    """Serialize one verified reducer receipt canonically."""

    if not isinstance(summary, V101Rescore) or summary.status != "verified":
        raise ValueError("V101 rescore differs")
    return _canonical_json_bytes(asdict(summary))
