#!/usr/bin/env python3
"""Exact-f32 retained-row capacity screen for the V99 hierarchy."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v97_row_width_screen import ScreenAuthority, ScreenInputs
from scripts.v98_hierarchical_row_router import (
    AggregateEvidence,
    HierarchyEvidence,
    HierarchyFence,
    _hierarchy_evidence,
    _retained_row_positions,
    _score_exact_retained_rows,
    build_hierarchy,
    route_hierarchy,
)
from scripts.v99_ranked_gap_range_router import (
    RangeSample,
    RankedGapConfig,
    _aggregate_range_samples,
    _range_sample,
    _selection_from_rows,
)

REGISTERED_RETENTION_PAGES = (128, 256, 512, 768, 1_024)


@dataclass(frozen=True, slots=True)
class RetentionArmConfig:
    """One preregistered retained-page and scanned-row envelope."""

    retained_pages: int
    maximum_scanned_rows: int

    def __post_init__(self) -> None:
        if (
            type(self.retained_pages) is not int
            or self.retained_pages not in REGISTERED_RETENTION_PAGES
            or type(self.maximum_scanned_rows) is not int
            or self.maximum_scanned_rows != self.retained_pages * 256
        ):
            raise ValueError("retention arm differs")


@dataclass(frozen=True, slots=True)
class V104Config:
    """Complete fixed contract for the exact retention ladder."""

    pages_per_root: int
    maximum_root_groups: int
    maximum_exposed_pages: int
    arms: tuple[RetentionArmConfig, ...]
    shortlist_rows: int
    maximum_gets: int
    maximum_bytes: int

    def __post_init__(self) -> None:
        if (
            self.pages_per_root != 8
            or self.maximum_root_groups != 65_536
            or self.maximum_exposed_pages != 4_096
            or tuple(arm.retained_pages for arm in self.arms)
            != REGISTERED_RETENTION_PAGES
            or self.shortlist_rows != 8_192
            or self.maximum_gets != 32
            or self.maximum_bytes != 16 * 1024**2
        ):
            raise ValueError("V104 configuration differs")

    def baseline(self) -> RankedGapConfig:
        """Return the immutable V99 hierarchy contract used by every arm."""

        return RankedGapConfig(
            pages_per_root=self.pages_per_root,
            maximum_root_groups=self.maximum_root_groups,
            maximum_exposed_pages=self.maximum_exposed_pages,
            retained_pages=1_024,
            maximum_scanned_rows=262_144,
            shortlist_rows=self.shortlist_rows,
            maximum_gets=self.maximum_gets,
            maximum_bytes=self.maximum_bytes,
        )


@dataclass(frozen=True, slots=True)
class RetentionArmEvidence:
    """Complete exact-ranking evidence for one retention envelope."""

    retained_pages: int
    maximum_scanned_rows: int
    maximum_observed_scanned_rows: int
    aggregate: AggregateEvidence
    samples: tuple[RangeSample, ...]


@dataclass(frozen=True, slots=True)
class V104Result:
    """Typed result for the V104 capacity decision."""

    schema: str
    authority: ScreenAuthority
    config: V104Config
    query_count: int
    hierarchy: HierarchyEvidence
    classification: Literal["exact-retention-survivor", "exact-retention-rejected"]
    arms: tuple[RetentionArmEvidence, ...]
    winner_retained_pages: int | None


def _arm_fence(
    baseline: HierarchyFence,
    inputs: ScreenInputs,
    arm: RetentionArmConfig,
) -> HierarchyFence:
    retained = baseline.retained_pages[: arm.retained_pages]
    scanned_rows = sum(len(inputs.row_order_by_page[key]) for key in retained)
    if len(retained) != arm.retained_pages or scanned_rows > arm.maximum_scanned_rows:
        raise ValueError("retention arm row envelope differs")
    return HierarchyFence(
        exposed_pages=baseline.exposed_pages,
        retained_pages=retained,
        root_evaluations=baseline.root_evaluations,
        page_evaluations=baseline.page_evaluations,
        scanned_rows=scanned_rows,
    )


def _decision(
    arms: tuple[RetentionArmEvidence, ...],
) -> tuple[Literal["exact-retention-survivor", "exact-retention-rejected"], int | None]:
    passing = [
        arm.retained_pages
        for arm in arms
        if arm.aggregate.quality_gate_passed and arm.aggregate.resource_gate_passed
    ]
    if not passing:
        return "exact-retention-rejected", None
    return "exact-retention-survivor", min(passing)


def evaluate_v104(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: V104Config,
) -> V104Result:
    """Evaluate exact ranked-gap quality across the registered retention ladder."""

    queries = np.asarray(inputs.queries)
    truth_ids = np.asarray(inputs.truth_ids)
    baseline_config = config.baseline()
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
        raise ValueError("V104 input authority differs")

    artifact = build_hierarchy(inputs, baseline_config)
    baseline_fences = tuple(
        route_hierarchy(query, artifact, baseline_config) for query in queries
    )
    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    evidence: list[RetentionArmEvidence] = []
    for arm in config.arms:
        fences = tuple(_arm_fence(fence, inputs, arm) for fence in baseline_fences)
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
            selection = _selection_from_rows(ranked, inputs, baseline_config)
            samples.append(
                _range_sample(
                    ordinal, truth_ids[ordinal], selection, fences[ordinal], inputs
                )
            )
        sample_tuple = tuple(samples)
        evidence.append(
            RetentionArmEvidence(
                retained_pages=arm.retained_pages,
                maximum_scanned_rows=arm.maximum_scanned_rows,
                maximum_observed_scanned_rows=max(
                    sample.scanned_rows for sample in sample_tuple
                ),
                aggregate=_aggregate_range_samples(sample_tuple, baseline_config),
                samples=sample_tuple,
            )
        )
    arms = tuple(evidence)
    classification, winner = _decision(arms)
    return V104Result(
        schema="borsuk-v104-exact-retention-ladder-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        hierarchy=_hierarchy_evidence(artifact),
        classification=classification,
        arms=arms,
        winner_retained_pages=winner,
    )


def canonical_v104_result_bytes(result: V104Result) -> bytes:
    """Validate and serialize canonical V104 evidence."""

    if (
        not isinstance(result, V104Result)
        or result.schema != "borsuk-v104-exact-retention-ladder-v1"
        or result.query_count <= 0
        or tuple(arm.retained_pages for arm in result.arms)
        != REGISTERED_RETENTION_PAGES
        or len(result.arms) != len(result.config.arms)
    ):
        raise ValueError("V104 result evidence differs")
    baseline = result.config.baseline()
    for arm, registered in zip(result.arms, result.config.arms, strict=True):
        if (
            arm.retained_pages != registered.retained_pages
            or arm.maximum_scanned_rows != registered.maximum_scanned_rows
            or len(arm.samples) != result.query_count
            or tuple(sample.query_ordinal for sample in arm.samples)
            != tuple(range(result.query_count))
            or max(sample.scanned_rows for sample in arm.samples)
            != arm.maximum_observed_scanned_rows
            or arm.maximum_observed_scanned_rows > arm.maximum_scanned_rows
            or _aggregate_range_samples(arm.samples, baseline) != arm.aggregate
        ):
            raise ValueError("V104 result evidence differs")
    classification, winner = _decision(result.arms)
    if (
        result.classification != classification
        or result.winner_retained_pages != winner
    ):
        raise ValueError("V104 result evidence differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
