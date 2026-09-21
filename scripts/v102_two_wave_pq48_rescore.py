#!/usr/bin/env python3
"""Independent semantic reducer for canonical V102 evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal, Mapping

from scripts.v97_row_width_screen import ScreenAuthority, ScreenInputs
from scripts.v98_hierarchical_row_router import build_hierarchy, route_hierarchy
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v100_page_residual_range_rescore import (
    _aggregate,
    _bootstrap_matrix,
    _canonical_json_bytes,
    _paired_interval,
    _valid_sha256,
    _validate_sample,
)
from scripts.v101_two_stage_residual_pq_rescore import _validate_array_identity
from scripts.v102_two_wave_pq48_refinement import (
    PQ48X8,
    RefinementFetchEvidence,
    build_refinement_page_directory,
    plan_refinement_fetch,
    project_v102_resident_bytes_100m,
)


@dataclass(frozen=True, slots=True)
class V102Rescore:
    """Authenticated independently recomputed V102 decision summary."""

    schema: str
    result_sha256: str
    query_count: int
    classification: str
    refinement_maximum_gets: int
    refinement_maximum_bytes: int
    control_aggregate: Mapping[str, object]
    challenger_aggregate: Mapping[str, object]
    paired: Mapping[str, object]
    total_resident_bytes: int
    status: Literal["verified"]


def _expected_fetches(
    inputs: ScreenInputs, config: RankedGapConfig
) -> tuple[tuple[RefinementFetchEvidence, ...], bool]:
    hierarchy = build_hierarchy(inputs, config)
    directory = build_refinement_page_directory(inputs, row_bytes=PQ48X8.row_bytes)
    evidence: list[RefinementFetchEvidence] = []
    for ordinal, query in enumerate(inputs.queries):
        fence = route_hierarchy(query, hierarchy, config)
        try:
            selection = plan_refinement_fetch(
                fence.retained_pages,
                directory,
                max_gets=config.maximum_gets,
                max_bytes=config.maximum_bytes,
            )
        except ValueError:
            return tuple(evidence), True
        evidence.append(
            RefinementFetchEvidence(
                query_ordinal=ordinal,
                required_pages=fence.retained_pages,
                selected_ranges=selection.ranges,
                selected_pages=selection.pages,
                gets=selection.gets,
                bytes=selection.bytes,
            )
        )
    return tuple(evidence), False


def rescore_v102_result(
    body: bytes,
    *,
    expected_inputs: ScreenInputs,
    expected_authority: ScreenAuthority,
    expected_config: RankedGapConfig,
    expected_result_sha256: str,
) -> V102Rescore:
    """Authenticate bytes and independently recompute both V102 waves."""

    if (
        not _valid_sha256(expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        raise ValueError("V102 result identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V102 result JSON differs") from error
    if type(value) is not dict or _canonical_json_bytes(value) != body:
        raise ValueError("V102 result canonical bytes differ")
    expected_keys = {
        "schema",
        "authority",
        "config",
        "query_count",
        "refinement_row_bytes",
        "bootstrap_seed",
        "bootstrap_resamples",
        "bootstrap_matrix_sha256",
        "classification",
        "refinement_books_identity",
        "refinement_codes_identity",
        "refinement_fetches",
        "refinement_maximum_gets",
        "refinement_maximum_bytes",
        "refinement_resource_gate_passed",
        "control_aggregate",
        "control_samples",
        "challenger_aggregate",
        "challenger_samples",
        "paired",
        "projection",
    }
    query_count = len(expected_inputs.queries)
    if (
        set(value) != expected_keys
        or value["schema"] != "borsuk-v102-two-wave-pq48-v1"
        or value["authority"] != asdict(expected_authority)
        or value["config"] != asdict(expected_config)
        or value["query_count"] != query_count
        or value["refinement_row_bytes"] != PQ48X8.row_bytes
        or value["bootstrap_seed"] != expected_authority.seed
        or value["bootstrap_resamples"] != 10_000
    ):
        raise ValueError("V102 result authority differs")
    _validate_array_identity(value["refinement_books_identity"])
    _validate_array_identity(value["refinement_codes_identity"])
    expected_fetches, io_failed = _expected_fetches(expected_inputs, expected_config)
    serialized_fetches = json.loads(
        json.dumps([asdict(item) for item in expected_fetches], sort_keys=True)
    )
    if value["refinement_fetches"] != serialized_fetches:
        raise ValueError("V102 refinement I/O evidence differs")
    maximum_gets = max((item.gets for item in expected_fetches), default=0)
    maximum_bytes = max((item.bytes for item in expected_fetches), default=0)
    resource = (
        not io_failed
        and maximum_gets <= expected_config.maximum_gets
        and maximum_bytes <= expected_config.maximum_bytes
    )
    if (
        value["refinement_maximum_gets"] != maximum_gets
        or value["refinement_maximum_bytes"] != maximum_bytes
        or value["refinement_resource_gate_passed"] is not resource
    ):
        raise ValueError("V102 refinement resource evidence differs")
    projection = asdict(project_v102_resident_bytes_100m(expected_config))
    if value["projection"] != projection:
        raise ValueError("V102 projection differs")
    if io_failed:
        if (
            value["classification"] != "refinement-io-rejected"
            or value["control_samples"]
            or value["challenger_samples"]
        ):
            raise ValueError("V102 I/O classification differs")
        return V102Rescore(
            schema="borsuk-v102-two-wave-pq48-rescore-v1",
            result_sha256=expected_result_sha256,
            query_count=query_count,
            classification="refinement-io-rejected",
            refinement_maximum_gets=maximum_gets,
            refinement_maximum_bytes=maximum_bytes,
            control_aggregate=value["control_aggregate"],
            challenger_aggregate=value["challenger_aggregate"],
            paired=value["paired"],
            total_resident_bytes=projection["total_resident_bytes"],
            status="verified",
        )

    control_samples = value["control_samples"]
    challenger_samples = value["challenger_samples"]
    if (
        type(control_samples) is not list
        or type(challenger_samples) is not list
        or len(control_samples) != query_count
        or len(challenger_samples) != query_count
    ):
        raise ValueError("V102 result sample cohort differs")
    neighbors = expected_inputs.neighbors
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
    control_aggregate = _aggregate(control_recalls, control_samples, expected_config)
    challenger_aggregate = _aggregate(
        challenger_recalls, challenger_samples, expected_config
    )
    if (
        value["control_aggregate"] != control_aggregate
        or value["challenger_aggregate"] != challenger_aggregate
    ):
        raise ValueError("V102 aggregate differs")
    matrix = _bootstrap_matrix(query_count, expected_authority.seed)
    matrix_sha256 = hashlib.sha256(matrix.tobytes(order="C")).hexdigest()
    if value["bootstrap_matrix_sha256"] != matrix_sha256:
        raise ValueError("V102 bootstrap authority differs")
    control10 = tuple(item[0] for item in control_recalls)
    control100 = tuple(item[1] for item in control_recalls)
    challenger10 = tuple(item[0] for item in challenger_recalls)
    challenger100 = tuple(item[1] for item in challenger_recalls)
    paired = {
        "name": "pq48x8-s3-refinement",
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
        raise ValueError("V102 paired evidence differs")
    qualified = (
        resource
        and challenger_aggregate["quality_gate_passed"]
        and challenger_aggregate["resource_gate_passed"]
        and projection["resident_eligible"]
        and paired["average_recall100_ppm"][0] >= 0
    )
    classification = (
        "two-wave-pq48-diagnostic-qualified"
        if qualified
        else "two-wave-pq48-rejected"
    )
    if value["classification"] != classification:
        raise ValueError("V102 classification differs")
    return V102Rescore(
        schema="borsuk-v102-two-wave-pq48-rescore-v1",
        result_sha256=expected_result_sha256,
        query_count=query_count,
        classification=classification,
        refinement_maximum_gets=maximum_gets,
        refinement_maximum_bytes=maximum_bytes,
        control_aggregate=control_aggregate,
        challenger_aggregate=challenger_aggregate,
        paired=paired,
        total_resident_bytes=projection["total_resident_bytes"],
        status="verified",
    )


def canonical_v102_rescore_bytes(summary: V102Rescore) -> bytes:
    """Serialize one verified reducer receipt canonically."""

    if not isinstance(summary, V102Rescore) or summary.status != "verified":
        raise ValueError("V102 rescore differs")
    return _canonical_json_bytes(asdict(summary))
