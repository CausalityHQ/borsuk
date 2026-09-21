#!/usr/bin/env python3
"""Full-development confirmation of V104's 768-page exact survivor."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v97_row_width_screen import ScreenAuthority, ScreenInputs
from scripts.v98_hierarchical_row_router import (
    HierarchyEvidence,
    _hierarchy_evidence,
    _retained_row_positions,
    _score_exact_retained_rows,
    build_hierarchy,
    route_hierarchy,
)
from scripts.v99_ranked_gap_range_router import (
    RangeSample,
    _aggregate_range_samples,
    _range_sample,
    _selection_from_rows,
)
from scripts.v104_exact_retention_ladder import (
    RetentionArmEvidence,
    V104Config,
    _arm_fence,
)


@dataclass(frozen=True, slots=True)
class V105Result:
    """Typed evidence for one 768-page full-development confirmation."""

    schema: str
    authority: ScreenAuthority
    config: V104Config
    query_count: int
    hierarchy: HierarchyEvidence
    classification: Literal["exact-retention-confirmed", "exact-retention-rejected"]
    arm: RetentionArmEvidence


def evaluate_v105(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: V104Config,
) -> V105Result:
    """Evaluate only the preregistered 768-page arm over the supplied cohort."""

    queries = np.asarray(inputs.queries)
    truth_ids = np.asarray(inputs.truth_ids)
    baseline = config.baseline()
    if (
        queries.ndim != 2
        or queries.dtype != np.float32
        or not np.isfinite(queries).all()
        or truth_ids.shape != (queries.shape[0], inputs.neighbors)
        or not np.issubdtype(truth_ids.dtype, np.integer)
        or authority.dimensions != inputs.vectors.shape[1]
        or authority.seed != inputs.seed
        or inputs.max_gets != config.maximum_gets
        or inputs.max_bytes != config.maximum_bytes
    ):
        raise ValueError("V105 input authority differs")
    try:
        registered = next(arm for arm in config.arms if arm.retained_pages == 768)
    except StopIteration as error:
        raise ValueError("V105 retained arm differs") from error

    hierarchy = build_hierarchy(inputs, baseline)
    baseline_fences = tuple(
        route_hierarchy(query, hierarchy, baseline) for query in queries
    )
    fences = tuple(
        _arm_fence(fence, inputs, registered) for fence in baseline_fences
    )
    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    samples: list[RangeSample] = []
    for ordinal, query in enumerate(queries):
        row_ids, positions = _retained_row_positions(
            fences[ordinal], inputs, position_by_id
        )
        ranked = _score_exact_retained_rows(
            query,
            row_ids,
            positions,
            vectors,
            min(config.shortlist_rows, row_ids.size),
        )
        selection = _selection_from_rows(ranked, inputs, baseline)
        samples.append(
            _range_sample(
                ordinal, truth_ids[ordinal], selection, fences[ordinal], inputs
            )
        )
    sample_tuple = tuple(samples)
    aggregate = _aggregate_range_samples(sample_tuple, baseline)
    arm = RetentionArmEvidence(
        retained_pages=registered.retained_pages,
        maximum_scanned_rows=registered.maximum_scanned_rows,
        maximum_observed_scanned_rows=max(
            sample.scanned_rows for sample in sample_tuple
        ),
        aggregate=aggregate,
        samples=sample_tuple,
    )
    classification = (
        "exact-retention-confirmed"
        if aggregate.quality_gate_passed and aggregate.resource_gate_passed
        else "exact-retention-rejected"
    )
    return V105Result(
        schema="borsuk-v105-exact-retention-confirmation-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        hierarchy=_hierarchy_evidence(hierarchy),
        classification=classification,
        arm=arm,
    )


def canonical_v105_result_bytes(result: V105Result) -> bytes:
    """Validate and serialize canonical V105 evidence."""

    if (
        not isinstance(result, V105Result)
        or result.schema != "borsuk-v105-exact-retention-confirmation-v1"
        or result.query_count <= 0
        or result.arm.retained_pages != 768
        or result.arm.maximum_scanned_rows != 196_608
        or len(result.arm.samples) != result.query_count
        or tuple(sample.query_ordinal for sample in result.arm.samples)
        != tuple(range(result.query_count))
        or max(sample.scanned_rows for sample in result.arm.samples)
        != result.arm.maximum_observed_scanned_rows
        or result.arm.maximum_observed_scanned_rows
        > result.arm.maximum_scanned_rows
        or _aggregate_range_samples(result.arm.samples, result.config.baseline())
        != result.arm.aggregate
    ):
        raise ValueError("V105 result evidence differs")
    expected_classification = (
        "exact-retention-confirmed"
        if result.arm.aggregate.quality_gate_passed
        and result.arm.aggregate.resource_gate_passed
        else "exact-retention-rejected"
    )
    if result.classification != expected_classification:
        raise ValueError("V105 result evidence differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
