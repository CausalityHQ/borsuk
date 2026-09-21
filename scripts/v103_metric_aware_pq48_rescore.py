#!/usr/bin/env python3
"""Independent semantic reducer for canonical V103 evidence."""

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
    RefinementFetchEvidence,
    build_refinement_page_directory,
    plan_refinement_fetch,
)
from scripts.v103_metric_aware_pq48 import (
    project_v103_resident_bytes_100m,
    vector_norm_evidence,
)


@dataclass(frozen=True, slots=True)
class V103Rescore:
    """Authenticated independently reduced V103 decision summary."""

    schema: str
    result_sha256: str
    query_count: int
    classification: str
    refinement_maximum_gets: int
    refinement_maximum_bytes: int
    l2_control_aggregate: Mapping[str, object]
    metric_aware_aggregate: Mapping[str, object]
    paired: Mapping[str, object]
    total_resident_bytes: int
    status: Literal["verified"]


def _expected_fetches(
    inputs: ScreenInputs, config: RankedGapConfig, *, row_bytes: int
) -> tuple[tuple[RefinementFetchEvidence, ...], bool]:
    hierarchy = build_hierarchy(inputs, config)
    directory = build_refinement_page_directory(inputs, row_bytes=row_bytes)
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


def rescore_v103_result(
    body: bytes,
    *,
    expected_inputs: ScreenInputs,
    expected_authority: ScreenAuthority,
    expected_config: RankedGapConfig,
    expected_result_sha256: str,
) -> V103Rescore:
    """Authenticate bytes and independently reduce every V103 claim."""

    if (
        not _valid_sha256(expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        raise ValueError("V103 result identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V103 result JSON differs") from error
    if type(value) is not dict or _canonical_json_bytes(value) != body:
        raise ValueError("V103 result canonical bytes differ")
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
        "source_norms_identity",
        "source_norms",
        "query_norms",
        "refinement_fetches",
        "refinement_maximum_gets",
        "refinement_maximum_bytes",
        "refinement_resource_gate_passed",
        "l2_control_aggregate",
        "l2_control_samples",
        "metric_aware_aggregate",
        "metric_aware_samples",
        "paired",
        "projection",
    }
    projection = project_v103_resident_bytes_100m(expected_config)
    query_count = len(expected_inputs.queries)
    if (
        set(value) != expected_keys
        or value["schema"] != "borsuk-v103-metric-aware-pq48-v1"
        or value["authority"] != asdict(expected_authority)
        or value["config"] != asdict(expected_config)
        or value["query_count"] != query_count
        or value["refinement_row_bytes"] != projection.refinement_row_bytes
        or value["bootstrap_seed"] != expected_authority.seed
        or value["bootstrap_resamples"] != 10_000
    ):
        raise ValueError("V103 result authority differs")
    for name in (
        "refinement_books_identity",
        "refinement_codes_identity",
        "source_norms_identity",
    ):
        _validate_array_identity(value[name])
    if (
        value["source_norms"] != asdict(vector_norm_evidence(expected_inputs.vectors))
        or value["query_norms"] != asdict(vector_norm_evidence(expected_inputs.queries))
        or value["projection"] != asdict(projection)
    ):
        raise ValueError("V103 norm or projection evidence differs")
    expected_fetches, io_failed = _expected_fetches(
        expected_inputs, expected_config, row_bytes=projection.refinement_row_bytes
    )
    serialized_fetches = json.loads(
        json.dumps([asdict(item) for item in expected_fetches], sort_keys=True)
    )
    maximum_gets = max((item.gets for item in expected_fetches), default=0)
    maximum_bytes = max((item.bytes for item in expected_fetches), default=0)
    resource = (
        not io_failed
        and maximum_gets <= expected_config.maximum_gets
        and maximum_bytes <= expected_config.maximum_bytes
    )
    if (
        value["refinement_fetches"] != serialized_fetches
        or value["refinement_maximum_gets"] != maximum_gets
        or value["refinement_maximum_bytes"] != maximum_bytes
        or value["refinement_resource_gate_passed"] is not resource
    ):
        raise ValueError("V103 refinement I/O evidence differs")
    if io_failed:
        if (
            value["classification"] != "refinement-io-rejected"
            or value["l2_control_samples"]
            or value["metric_aware_samples"]
        ):
            raise ValueError("V103 I/O classification differs")
        return V103Rescore(
            schema="borsuk-v103-metric-aware-pq48-rescore-v1",
            result_sha256=expected_result_sha256,
            query_count=query_count,
            classification="refinement-io-rejected",
            refinement_maximum_gets=maximum_gets,
            refinement_maximum_bytes=maximum_bytes,
            l2_control_aggregate=value["l2_control_aggregate"],
            metric_aware_aggregate=value["metric_aware_aggregate"],
            paired=value["paired"],
            total_resident_bytes=projection.total_resident_bytes,
            status="verified",
        )
    l2_samples = value["l2_control_samples"]
    metric_samples = value["metric_aware_samples"]
    if (
        type(l2_samples) is not list
        or type(metric_samples) is not list
        or len(l2_samples) != query_count
        or len(metric_samples) != query_count
    ):
        raise ValueError("V103 result sample cohort differs")
    l2_recalls = tuple(
        _validate_sample(
            sample,
            query_ordinal=index,
            neighbors=expected_inputs.neighbors,
            config=expected_config,
        )
        for index, sample in enumerate(l2_samples)
    )
    metric_recalls = tuple(
        _validate_sample(
            sample,
            query_ordinal=index,
            neighbors=expected_inputs.neighbors,
            config=expected_config,
        )
        for index, sample in enumerate(metric_samples)
    )
    l2_aggregate = _aggregate(l2_recalls, l2_samples, expected_config)
    metric_aggregate = _aggregate(metric_recalls, metric_samples, expected_config)
    if (
        value["l2_control_aggregate"] != l2_aggregate
        or value["metric_aware_aggregate"] != metric_aggregate
    ):
        raise ValueError("V103 aggregate differs")
    matrix = _bootstrap_matrix(query_count, expected_authority.seed)
    if value["bootstrap_matrix_sha256"] != hashlib.sha256(
        matrix.tobytes(order="C")
    ).hexdigest():
        raise ValueError("V103 bootstrap authority differs")
    l2_10 = tuple(item[0] for item in l2_recalls)
    l2_100 = tuple(item[1] for item in l2_recalls)
    metric_10 = tuple(item[0] for item in metric_recalls)
    metric_100 = tuple(item[1] for item in metric_recalls)
    paired = {
        "name": "metric-aware-pq48",
        "average_recall10_ppm": list(
            _paired_interval(metric_10, l2_10, matrix, p05=False)
        ),
        "average_recall100_ppm": list(
            _paired_interval(metric_100, l2_100, matrix, p05=False)
        ),
        "p05_recall100_ppm": list(
            _paired_interval(metric_100, l2_100, matrix, p05=True)
        ),
    }
    if value["paired"] != paired:
        raise ValueError("V103 paired evidence differs")
    qualified = (
        resource
        and metric_aggregate["quality_gate_passed"]
        and metric_aggregate["resource_gate_passed"]
        and projection.resident_eligible
        and paired["average_recall100_ppm"][0] >= 0
    )
    classification = (
        "metric-aware-pq48-diagnostic-qualified"
        if qualified
        else "metric-aware-pq48-rejected"
    )
    if value["classification"] != classification:
        raise ValueError("V103 classification differs")
    return V103Rescore(
        schema="borsuk-v103-metric-aware-pq48-rescore-v1",
        result_sha256=expected_result_sha256,
        query_count=query_count,
        classification=classification,
        refinement_maximum_gets=maximum_gets,
        refinement_maximum_bytes=maximum_bytes,
        l2_control_aggregate=l2_aggregate,
        metric_aware_aggregate=metric_aggregate,
        paired=paired,
        total_resident_bytes=projection.total_resident_bytes,
        status="verified",
    )


def canonical_v103_rescore_bytes(summary: V103Rescore) -> bytes:
    """Serialize one verified reducer receipt canonically."""

    if not isinstance(summary, V103Rescore) or summary.status != "verified":
        raise ValueError("V103 rescore differs")
    return _canonical_json_bytes(asdict(summary))
