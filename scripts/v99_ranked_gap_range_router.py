#!/usr/bin/env python3
"""Typed ranked-gap range planning for the V99 G1 screen."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Iterable, Literal, Mapping, Sequence

import numpy as np

from scripts.v97_row_width_screen import (
    PQ16X8,
    PQ24X8,
    PQ32X4,
    PQ32X8,
    SUMMARY_ONLY_PQ16X8,
    PageKey,
    RoutedPage,
    ScreenAuthority,
    ScreenInputs,
    encode_pq,
    evaluate_selected_pages,
    fit_pq,
)
from scripts.v98_hierarchical_row_router import (
    AggregateEvidence,
    ArmEligibilityEvidence,
    ArmEvidence,
    ContainmentSample,
    HierarchyEvidence,
    PairedIntervalEvidence,
    _aggregate_samples,
    _array_identity,
    _containment_sample,
    _hierarchy_evidence,
    _producer_bootstrap_matrix,
    _producer_decisions,
    _retained_row_positions,
    _score_exact_retained_rows,
    build_hierarchy,
    project_v98_resident_bytes_100m,
    route_hierarchy,
    score_retained_rows,
)


@dataclass(frozen=True, slots=True, order=True)
class PageRange:
    """One contiguous byte-range GET within one immutable run object."""

    object_role: str
    first_page: int
    last_page: int
    offset: int
    bytes: int

    def __post_init__(self) -> None:
        if (
            self.object_role not in ("base", "delta")
            or self.first_page < 0
            or self.last_page < self.first_page
            or self.offset < 0
            or self.bytes <= 0
        ):
            raise ValueError("page range differs")


@dataclass(frozen=True, slots=True)
class RangeSelection:
    """Canonical nonoverlapping range selection and its complete page union."""

    ranges: tuple[PageRange, ...]
    pages: tuple[PageKey, ...]
    gets: int
    bytes: int

    def __post_init__(self) -> None:
        if not self.ranges or tuple(sorted(self.ranges)) != self.ranges:
            raise ValueError("range order differs")
        for left, right in zip(self.ranges, self.ranges[1:], strict=False):
            if (
                left.object_role == right.object_role
                and left.last_page >= right.first_page
            ):
                raise ValueError("range overlap differs")
        expected_pages = tuple(
            PageKey(item.object_role, ordinal)
            for item in self.ranges
            for ordinal in range(item.first_page, item.last_page + 1)
        )
        if self.pages != expected_pages:
            raise ValueError("page union differs")
        if self.gets != len(self.ranges) or self.bytes != sum(
            item.bytes for item in self.ranges
        ):
            raise ValueError("range accounting differs")


@dataclass(frozen=True, slots=True)
class RankedGapConfig:
    """The complete fixed-work contract for the V99 range router."""

    pages_per_root: int
    maximum_root_groups: int
    maximum_exposed_pages: int
    retained_pages: int
    maximum_scanned_rows: int
    shortlist_rows: int
    maximum_gets: int
    maximum_bytes: int

    def __post_init__(self) -> None:
        values = (
            self.pages_per_root,
            self.maximum_root_groups,
            self.maximum_exposed_pages,
            self.retained_pages,
            self.maximum_scanned_rows,
            self.shortlist_rows,
            self.maximum_gets,
            self.maximum_bytes,
        )
        if (
            any(type(value) is not int or value <= 0 for value in values)
            or self.pages_per_root != 8
            or self.maximum_root_groups != 65_536
            or self.maximum_exposed_pages != 4_096
            or self.retained_pages != 1_024
            or self.maximum_scanned_rows != 262_144
            or self.shortlist_rows != 8_192
            or self.maximum_gets != 32
            or self.maximum_bytes != 16 * 1024**2
        ):
            raise ValueError("ranked-gap configuration differs")


@dataclass(frozen=True, slots=True)
class RangeSample:
    """One query's range selection, quality, and physical-work evidence."""

    query_ordinal: int
    truth_ids: tuple[int, ...]
    truth_pages: tuple[PageKey, ...]
    selected_ranges: tuple[PageRange, ...]
    selected_pages: tuple[PageKey, ...]
    hit10_ids: tuple[int, ...]
    hit_ids: tuple[int, ...]
    hits10: int
    hits: int
    recall10_ppm: int
    recall100_ppm: int
    gets: int
    bytes: int
    root_evaluations: int
    page_evaluations: int
    scanned_rows: int


@dataclass(frozen=True, slots=True)
class V99Result:
    """Complete typed evidence for the V99 ranked-gap decision."""

    schema: str
    authority: ScreenAuthority
    config: RankedGapConfig
    query_count: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    classification: Literal[
        "hierarchy-containment-rejected",
        "range-exact-ceiling-rejected",
        "widths-evaluated",
    ]
    hierarchy: HierarchyEvidence
    containment_aggregate: AggregateEvidence
    containment_samples: tuple[ContainmentSample, ...]
    exact_aggregate: AggregateEvidence | None
    exact_samples: tuple[RangeSample, ...]
    arms: tuple[ArmEvidence, ...]
    paired_intervals: tuple[PairedIntervalEvidence, ...]
    eligibility: tuple[ArmEligibilityEvidence, ...]
    winner: str | None


def _registered_page(key: PageKey, pages: Mapping[PageKey, RoutedPage]) -> RoutedPage:
    page = pages.get(key)
    if page is None or page.key != key or page.offset < 0 or page.encoded_bytes <= 0:
        raise ValueError("registered page differs")
    return page


def _page_range(
    object_role: str,
    first_page: int,
    last_page: int,
    pages: Mapping[PageKey, RoutedPage],
) -> PageRange:
    registered = tuple(
        _registered_page(PageKey(object_role, ordinal), pages)
        for ordinal in range(first_page, last_page + 1)
    )
    for left, right in zip(registered, registered[1:], strict=False):
        if left.offset + left.encoded_bytes != right.offset:
            raise ValueError("page ranges are not contiguous")
    first, last = registered[0], registered[-1]
    return PageRange(
        object_role=object_role,
        first_page=first_page,
        last_page=last_page,
        offset=first.offset,
        bytes=last.offset + last.encoded_bytes - first.offset,
    )


def _selection(
    intervals: list[tuple[str, int, int]],
    pages: Mapping[PageKey, RoutedPage],
) -> RangeSelection:
    ranges = tuple(
        _page_range(role, first, last, pages) for role, first, last in sorted(intervals)
    )
    selected_pages = tuple(
        PageKey(item.object_role, ordinal)
        for item in ranges
        for ordinal in range(item.first_page, item.last_page + 1)
    )
    return RangeSelection(
        ranges=ranges,
        pages=selected_pages,
        gets=len(ranges),
        bytes=sum(item.bytes for item in ranges),
    )


def select_ranked_gap_ranges(
    ranked_pages: Iterable[PageKey],
    pages: Mapping[PageKey, RoutedPage],
    *,
    max_gets: int,
    max_bytes: int,
) -> RangeSelection:
    """Select ranked pages while greedily coalescing the cheapest byte gaps."""

    if max_gets <= 0 or max_bytes <= 0 or not pages:
        raise ValueError("range budget differs")
    intervals: list[tuple[str, int, int]] = []
    accepted: RangeSelection | None = None
    for key in ranked_pages:
        _registered_page(key, pages)
        if any(
            role == key.object_role and first <= key.ordinal <= last
            for role, first, last in intervals
        ):
            continue
        proposal = sorted((*intervals, (key.object_role, key.ordinal, key.ordinal)))
        valid = True
        while len(proposal) > max_gets:
            candidates: list[tuple[int, str, int, int, int]] = []
            for index, (left, right) in enumerate(
                zip(proposal, proposal[1:], strict=False)
            ):
                left_role, left_first, left_last = left
                right_role, right_first, right_last = right
                if left_role != right_role:
                    continue
                merged = _page_range(left_role, left_first, right_last, pages)
                left_range = _page_range(left_role, left_first, left_last, pages)
                right_range = _page_range(right_role, right_first, right_last, pages)
                candidates.append(
                    (
                        merged.bytes - left_range.bytes - right_range.bytes,
                        left_role,
                        left_first,
                        right_last,
                        index,
                    )
                )
            if not candidates:
                valid = False
                break
            _, role, first, last, index = min(candidates)
            proposal[index : index + 2] = [(role, first, last)]
        if not valid:
            continue
        candidate = _selection(proposal, pages)
        if candidate.bytes > max_bytes:
            continue
        intervals = proposal
        accepted = candidate
    if accepted is None:
        raise ValueError("ranked pages do not fit range budget")
    return accepted


def _range_sample(
    query_ordinal: int,
    truth: np.ndarray,
    selection: RangeSelection,
    fence: object,
    inputs: ScreenInputs,
) -> RangeSample:
    evidence = evaluate_selected_pages(
        selected_pages=selection.pages,
        truth_ids=truth,
        page_by_id=inputs.page_by_id,
        neighbors=inputs.neighbors,
    )
    selected = set(selection.pages)
    cutoff = min(10, inputs.neighbors)
    return RangeSample(
        query_ordinal=query_ordinal,
        truth_ids=tuple(int(row_id) for row_id in truth),
        truth_pages=tuple(inputs.page_by_id[int(row_id)] for row_id in truth),
        selected_ranges=selection.ranges,
        selected_pages=selection.pages,
        hit10_ids=tuple(
            int(row_id)
            for row_id in truth[:cutoff]
            if inputs.page_by_id[int(row_id)] in selected
        ),
        hit_ids=evidence.hit_ids,
        hits10=evidence.hits10,
        hits=evidence.hits,
        recall10_ppm=evidence.recall10_ppm,
        recall100_ppm=evidence.recall100_ppm,
        gets=selection.gets,
        bytes=selection.bytes,
        root_evaluations=fence.root_evaluations,  # type: ignore[attr-defined]
        page_evaluations=fence.page_evaluations,  # type: ignore[attr-defined]
        scanned_rows=fence.scanned_rows,  # type: ignore[attr-defined]
    )


def _aggregate_range_samples(
    samples: Sequence[RangeSample], config: RankedGapConfig
) -> AggregateEvidence:
    if not samples:
        raise ValueError("V99 samples are empty")
    recall10 = [sample.recall10_ppm for sample in samples]
    recall100 = sorted(sample.recall100_ppm for sample in samples)
    p05_index = max(0, (len(samples) * 5 + 99) // 100 - 1)
    average10 = sum(recall10) // len(samples)
    average100 = sum(recall100) // len(samples)
    p05 = recall100[p05_index]
    maximum_gets = max(sample.gets for sample in samples)
    maximum_bytes = max(sample.bytes for sample in samples)
    return AggregateEvidence(
        average_recall10_ppm=average10,
        average_recall100_ppm=average100,
        p05_recall100_ppm=p05,
        maximum_gets=maximum_gets,
        maximum_bytes=maximum_bytes,
        quality_gate_passed=average10 >= 960_000
        and average100 >= 975_000
        and p05 >= 900_000,
        resource_gate_passed=maximum_gets <= config.maximum_gets
        and maximum_bytes <= config.maximum_bytes,
    )


def _selection_from_rows(
    ranked_row_ids: Sequence[int], inputs: ScreenInputs, config: RankedGapConfig
) -> RangeSelection:
    return select_ranked_gap_ranges(
        (inputs.page_by_id[int(row_id)] for row_id in ranked_row_ids),
        inputs.pages,
        max_gets=config.maximum_gets,
        max_bytes=config.maximum_bytes,
    )


def _early_result(
    *,
    authority: ScreenAuthority,
    config: RankedGapConfig,
    artifact: object,
    containment_samples: tuple[ContainmentSample, ...],
    containment_aggregate: AggregateEvidence,
    classification: Literal[
        "hierarchy-containment-rejected", "range-exact-ceiling-rejected"
    ],
    exact_samples: tuple[RangeSample, ...] = (),
    exact_aggregate: AggregateEvidence | None = None,
) -> V99Result:
    matrix = _producer_bootstrap_matrix(
        len(containment_samples), seed=authority.seed, resamples=10_000
    )
    return V99Result(
        schema="borsuk-v99-ranked-gap-range-router-v1",
        authority=authority,
        config=config,
        query_count=len(containment_samples),
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        classification=classification,
        hierarchy=_hierarchy_evidence(artifact),  # type: ignore[arg-type]
        containment_aggregate=containment_aggregate,
        containment_samples=containment_samples,
        exact_aggregate=exact_aggregate,
        exact_samples=exact_samples,
        arms=(),
        paired_intervals=(),
        eligibility=(),
        winner=None,
    )


def evaluate_v99(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: RankedGapConfig,
) -> V99Result:
    """Run containment, exact ranked-gap ceiling, then registered width arms."""

    queries = np.asarray(inputs.queries)
    truth_ids = np.asarray(inputs.truth_ids)
    if (
        queries.ndim != 2
        or queries.dtype != np.float32
        or truth_ids.shape != (queries.shape[0], inputs.neighbors)
        or not np.issubdtype(truth_ids.dtype, np.integer)
        or not np.isfinite(queries).all()
        or authority.dimensions != inputs.vectors.shape[1]
        or authority.seed != inputs.seed
        or inputs.max_gets != config.maximum_gets
        or inputs.max_bytes != config.maximum_bytes
        or config.shortlist_rows != 8_192
    ):
        raise ValueError("V99 input authority differs")
    artifact = build_hierarchy(inputs, config)
    fences = tuple(route_hierarchy(query, artifact, config) for query in queries)
    containment_samples = tuple(
        _containment_sample(ordinal, truth_ids[ordinal], fences[ordinal], inputs)
        for ordinal in range(queries.shape[0])
    )
    containment_aggregate = _aggregate_samples(
        containment_samples, enforce_resources=False, config=config
    )
    if not containment_aggregate.quality_gate_passed:
        return _early_result(
            authority=authority,
            config=config,
            artifact=artifact,
            containment_samples=containment_samples,
            containment_aggregate=containment_aggregate,
            classification="hierarchy-containment-rejected",
        )

    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    retained = tuple(
        _retained_row_positions(fence, inputs, position_by_id) for fence in fences
    )
    exact_samples_list: list[RangeSample] = []
    for ordinal, query in enumerate(queries):
        row_ids, positions = retained[ordinal]
        ranked = _score_exact_retained_rows(
            query, row_ids, positions, vectors, config.shortlist_rows
        )
        selection = _selection_from_rows(ranked, inputs, config)
        exact_samples_list.append(
            _range_sample(
                ordinal, truth_ids[ordinal], selection, fences[ordinal], inputs
            )
        )
    exact_samples = tuple(exact_samples_list)
    exact_aggregate = _aggregate_range_samples(exact_samples, config)
    if not (
        exact_aggregate.quality_gate_passed and exact_aggregate.resource_gate_passed
    ):
        return _early_result(
            authority=authority,
            config=config,
            artifact=artifact,
            containment_samples=containment_samples,
            containment_aggregate=containment_aggregate,
            classification="range-exact-ceiling-rejected",
            exact_samples=exact_samples,
            exact_aggregate=exact_aggregate,
        )

    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(source_ids)
            if inputs.page_by_id[int(row_id)].object_role == "base"
        ],
        dtype=np.int64,
    )
    base_vectors = np.ascontiguousarray(vectors[base_positions])
    arms: list[ArmEvidence] = []
    for spec in (PQ16X8, PQ24X8, PQ32X8, PQ32X4):
        books = fit_pq(
            base_vectors,
            spec,
            seed=inputs.seed,
            sample_rows=inputs.training_rows,
            iterations=inputs.training_iterations,
        )
        codes = encode_pq(vectors, books, spec)
        samples: list[RangeSample] = []
        for ordinal, query in enumerate(queries):
            row_ids, positions = retained[ordinal]
            ranked = score_retained_rows(
                query,
                row_ids,
                np.ascontiguousarray(codes[positions]),
                books,
                spec,
                maximum_rows=config.maximum_scanned_rows,
                shortlist_rows=min(config.shortlist_rows, row_ids.size),
            )
            selection = _selection_from_rows(ranked, inputs, config)
            samples.append(
                _range_sample(
                    ordinal, truth_ids[ordinal], selection, fences[ordinal], inputs
                )
            )
        sample_tuple = tuple(samples)
        arms.append(
            ArmEvidence(
                name=spec.name,
                row_bytes=spec.row_bytes,
                codebook_identity=_array_identity(books),
                codes_identity=_array_identity(codes),
                projection=project_v98_resident_bytes_100m(spec, config),
                aggregate=_aggregate_range_samples(sample_tuple, config),
                samples=sample_tuple,  # type: ignore[arg-type]
            )
        )
    summary_samples = tuple(
        _range_sample(
            ordinal,
            truth_ids[ordinal],
            select_ranked_gap_ranges(
                fences[ordinal].retained_pages,
                inputs.pages,
                max_gets=config.maximum_gets,
                max_bytes=config.maximum_bytes,
            ),
            fences[ordinal],
            inputs,
        )
        for ordinal in range(queries.shape[0])
    )
    arms.append(
        ArmEvidence(
            name=SUMMARY_ONLY_PQ16X8.name,
            row_bytes=0,
            codebook_identity=None,
            codes_identity=None,
            projection=project_v98_resident_bytes_100m(SUMMARY_ONLY_PQ16X8, config),
            aggregate=_aggregate_range_samples(summary_samples, config),
            samples=summary_samples,  # type: ignore[arg-type]
        )
    )
    arm_tuple = tuple(arms)
    matrix = _producer_bootstrap_matrix(
        queries.shape[0], seed=authority.seed, resamples=10_000
    )
    paired_intervals, eligibility, winner = _producer_decisions(
        arm_tuple, exact_aggregate, matrix
    )
    return V99Result(
        schema="borsuk-v99-ranked-gap-range-router-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        classification="widths-evaluated",
        hierarchy=_hierarchy_evidence(artifact),
        containment_aggregate=containment_aggregate,
        containment_samples=containment_samples,
        exact_aggregate=exact_aggregate,
        exact_samples=exact_samples,
        arms=arm_tuple,
        paired_intervals=paired_intervals,
        eligibility=eligibility,
        winner=winner,
    )


def canonical_v99_result_bytes(result: V99Result) -> bytes:
    """Serialize typed V99 evidence as canonical newline-terminated JSON."""

    if (
        not isinstance(result, V99Result)
        or result.schema != "borsuk-v99-ranked-gap-range-router-v1"
        or result.config.shortlist_rows != 8_192
    ):
        raise ValueError("V99 result differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
