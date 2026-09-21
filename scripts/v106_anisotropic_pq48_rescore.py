#!/usr/bin/env python3
"""Independent semantic reducer for canonical V106 evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal, Mapping

import numpy as np

from scripts.v97_row_width_screen import (
    ScreenAuthority,
    ScreenInputs,
    encode_pq,
    fit_pq,
)
from scripts.v98_hierarchical_row_router import (
    _array_identity,
    build_hierarchy,
    route_hierarchy,
)
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
from scripts.v106_anisotropic_pq48 import encode_anisotropic_pq


@dataclass(frozen=True, slots=True)
class V106Rescore:
    """Authenticated independently reduced V106 decision summary."""

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


def _expected_code_identities(inputs: ScreenInputs):
    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(source_ids)
            if inputs.page_by_id[int(row_id)].object_role == "base"
        ],
        dtype=np.int64,
    )
    books = fit_pq(
        np.ascontiguousarray(vectors[base_positions]),
        PQ48X8,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    control = np.ascontiguousarray(encode_pq(vectors, books, PQ48X8))
    challenger = np.ascontiguousarray(
        encode_anisotropic_pq(vectors, books, PQ48X8, threshold=0.2, passes=1)
    )
    return _array_identity(books), _array_identity(control), _array_identity(challenger)


def rescore_v106_result(
    body: bytes,
    *,
    expected_inputs: ScreenInputs,
    expected_authority: ScreenAuthority,
    expected_config: RankedGapConfig,
    expected_result_sha256: str,
) -> V106Rescore:
    """Authenticate bytes and independently reduce every V106 claim."""

    if (
        not _valid_sha256(expected_result_sha256)
        or hashlib.sha256(body).hexdigest() != expected_result_sha256
    ):
        raise ValueError("V106 result identity differs")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V106 result JSON differs") from error
    if type(value) is not dict or _canonical_json_bytes(value) != body:
        raise ValueError("V106 result canonical bytes differ")
    expected_keys = {
        "schema",
        "authority",
        "config",
        "query_count",
        "threshold",
        "coordinate_passes",
        "refinement_row_bytes",
        "bootstrap_seed",
        "bootstrap_resamples",
        "bootstrap_matrix_sha256",
        "classification",
        "books_identity",
        "control_codes_identity",
        "challenger_codes_identity",
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
    projection = project_v102_resident_bytes_100m(expected_config)
    if (
        set(value) != expected_keys
        or value["schema"] != "borsuk-v106-avq-pq48-v1"
        or value["authority"] != asdict(expected_authority)
        or value["config"] != asdict(expected_config)
        or value["query_count"] != query_count
        or value["threshold"] != 0.2
        or value["coordinate_passes"] != 1
        or value["refinement_row_bytes"] != PQ48X8.row_bytes
        or value["bootstrap_seed"] != expected_authority.seed
        or value["bootstrap_resamples"] != 10_000
        or value["projection"] != asdict(projection)
    ):
        raise ValueError("V106 result authority differs")
    expected_identities = _expected_code_identities(expected_inputs)
    for name, expected in zip(
        ("books_identity", "control_codes_identity", "challenger_codes_identity"),
        expected_identities,
        strict=True,
    ):
        _validate_array_identity(value[name])
        serialized_expected = json.loads(json.dumps(asdict(expected), sort_keys=True))
        if value[name] != serialized_expected:
            raise ValueError("V106 code identity differs")

    expected_fetches, io_failed = _expected_fetches(expected_inputs, expected_config)
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
        raise ValueError("V106 refinement I/O evidence differs")
    if io_failed:
        if (
            value["classification"] != "refinement-io-rejected"
            or value["control_samples"]
            or value["challenger_samples"]
        ):
            raise ValueError("V106 I/O classification differs")
        return V106Rescore(
            "borsuk-v106-avq-pq48-rescore-v1",
            expected_result_sha256,
            query_count,
            "refinement-io-rejected",
            maximum_gets,
            maximum_bytes,
            value["control_aggregate"],
            value["challenger_aggregate"],
            value["paired"],
            projection.total_resident_bytes,
            "verified",
        )

    control_samples = value["control_samples"]
    challenger_samples = value["challenger_samples"]
    if (
        type(control_samples) is not list
        or type(challenger_samples) is not list
        or len(control_samples) != query_count
        or len(challenger_samples) != query_count
    ):
        raise ValueError("V106 result sample cohort differs")
    control_recalls = tuple(
        _validate_sample(
            sample,
            query_ordinal=index,
            neighbors=expected_inputs.neighbors,
            config=expected_config,
        )
        for index, sample in enumerate(control_samples)
    )
    challenger_recalls = tuple(
        _validate_sample(
            sample,
            query_ordinal=index,
            neighbors=expected_inputs.neighbors,
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
        raise ValueError("V106 aggregate differs")
    matrix = _bootstrap_matrix(query_count, expected_authority.seed)
    if value["bootstrap_matrix_sha256"] != hashlib.sha256(
        matrix.tobytes(order="C")
    ).hexdigest():
        raise ValueError("V106 bootstrap authority differs")
    control_10 = tuple(item[0] for item in control_recalls)
    control_100 = tuple(item[1] for item in control_recalls)
    challenger_10 = tuple(item[0] for item in challenger_recalls)
    challenger_100 = tuple(item[1] for item in challenger_recalls)
    paired = {
        "name": "anisotropic-pq48",
        "average_recall10_ppm": list(
            _paired_interval(challenger_10, control_10, matrix, p05=False)
        ),
        "average_recall100_ppm": list(
            _paired_interval(challenger_100, control_100, matrix, p05=False)
        ),
        "p05_recall100_ppm": list(
            _paired_interval(challenger_100, control_100, matrix, p05=True)
        ),
    }
    if value["paired"] != paired:
        raise ValueError("V106 paired evidence differs")
    loses = any(interval[1] < 0 for interval in paired.values() if type(interval) is list)
    qualified = (
        resource
        and challenger_aggregate["quality_gate_passed"]
        and challenger_aggregate["resource_gate_passed"]
        and projection.resident_eligible
        and not loses
    )
    classification = (
        "anisotropic-pq48-qualified" if qualified else "anisotropic-pq48-rejected"
    )
    if value["classification"] != classification:
        raise ValueError("V106 classification differs")
    return V106Rescore(
        "borsuk-v106-avq-pq48-rescore-v1",
        expected_result_sha256,
        query_count,
        classification,
        maximum_gets,
        maximum_bytes,
        control_aggregate,
        challenger_aggregate,
        paired,
        projection.total_resident_bytes,
        "verified",
    )


def canonical_v106_rescore_bytes(receipt: V106Rescore) -> bytes:
    """Serialize one verified reducer receipt canonically."""

    if (
        not isinstance(receipt, V106Rescore)
        or receipt.schema != "borsuk-v106-avq-pq48-rescore-v1"
        or receipt.status != "verified"
        or not _valid_sha256(receipt.result_sha256)
        or receipt.query_count <= 0
    ):
        raise ValueError("V106 rescore summary differs")
    return _canonical_json_bytes(asdict(receipt))
