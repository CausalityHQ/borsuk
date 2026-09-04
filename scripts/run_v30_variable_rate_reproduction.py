#!/usr/bin/env python3
"""Authority and fail-fast contracts for the V30 variable-rate reproduction."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass

SOURCE_ROWS = 100_000
QUERY_COUNT = 32
RECALL_K = 10
TRUTH_MEMBERSHIPS = QUERY_COUNT * RECALL_K
FIDELITY_FRACTIONS_PPM = (0, 50_000, 100_000, 200_000)
MAX_CANDIDATE_DEPTH = 12_288
PAGE_COUNT = 10
MAX_ENCODED_BYTES = 4_587_520
MAX_SCANNED_CODES = 1_000_000
ARTIFACT_ROLES = (
    "pages-manifest",
    "leaf-postings",
    "leaf-centroids",
    "query-parquet",
)


@dataclass(frozen=True)
class ArtifactAuthority:
    """Exact immutable identity for one reproduction input."""

    role: str
    uri: str
    sha256: str
    encoded_bytes: int


@dataclass(frozen=True)
class V30ConstructionInputs:
    """Construction-only capabilities; evaluation inputs are deliberately absent."""

    pages_manifest: ArtifactAuthority
    leaf_postings: ArtifactAuthority
    leaf_centroids: ArtifactAuthority
    output_uri: str


@dataclass(frozen=True)
class V30ArmObservation:
    """Raw per-query evidence for one fixed fidelity fraction."""

    fidelity_fraction_ppm: int
    hits: tuple[int, ...]
    selected_page_counts: tuple[int, ...]
    maximum_encoded_bytes: int
    maximum_scanned_codes: int


def _exact_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def validate_reproduction_authority(
    artifacts: tuple[ArtifactAuthority, ...],
    *,
    source_rows: int,
    query_count: int,
    truth_memberships: int,
) -> None:
    """Fail closed on input identity or frozen-shape drift."""

    if (
        type(artifacts) is not tuple
        or len(artifacts) != len(ARTIFACT_ROLES)
        or tuple(artifact.role for artifact in artifacts) != ARTIFACT_ROLES
    ):
        raise ValueError("V30 reproduction artifact roles differ")
    for artifact in artifacts:
        if type(artifact) is not ArtifactAuthority:
            raise ValueError("V30 reproduction artifact type differs")
        if not artifact.uri.startswith("s3://") or artifact.uri.endswith("/"):
            raise ValueError("V30 reproduction artifact URI differs")
        if not _exact_digest(artifact.sha256):
            raise ValueError("V30 reproduction artifact digest differs")
        if type(artifact.encoded_bytes) is not int or artifact.encoded_bytes <= 0:
            raise ValueError("V30 reproduction artifact length differs")
    if (
        type(source_rows) is not int
        or source_rows != SOURCE_ROWS
        or type(query_count) is not int
        or query_count != QUERY_COUNT
        or type(truth_memberships) is not int
        or truth_memberships != TRUTH_MEMBERSHIPS
    ):
        raise ValueError("V30 reproduction frozen shape differs")


def pq8_replacement_geometry() -> dict[str, int]:
    """Return the single preregistered replacement interpretation."""

    return {
        "base_centroids": 256,
        "base_dimensions": 4,
        "base_subquantizers": 24,
        "base_width_bytes": 24,
        "high_centroids": 256,
        "high_dimensions": 2,
        "high_subquantizers": 48,
        "high_width_bytes": 48,
    }


def select_high_fidelity(errors: list[float], fraction_ppm: int) -> tuple[int, ...]:
    """Select the exact query-independent reconstruction-error tail."""

    if (
        type(errors) is not list
        or not errors
        or any(type(value) not in {int, float} or not math.isfinite(value) or value < 0 for value in errors)
    ):
        raise ValueError("V30 reconstruction errors differ")
    if type(fraction_ppm) is not int or fraction_ppm not in FIDELITY_FRACTIONS_PPM:
        raise ValueError("V30 fidelity fraction differs")
    count = len(errors) * fraction_ppm // 1_000_000
    ranked = sorted(range(len(errors)), key=lambda ordinal: (-errors[ordinal], ordinal))
    return tuple(sorted(ranked[:count]))


def reduce_page_candidates(
    ranked_rows: list[tuple[float, int]],
    row_pages: tuple[int, ...],
    *,
    candidate_depth: int,
    page_count: int,
) -> tuple[int, ...]:
    """Reduce a bounded row frontier to deterministic unique page ordinals."""

    if (
        type(candidate_depth) is not int
        or candidate_depth <= 0
        or candidate_depth > MAX_CANDIDATE_DEPTH
        or candidate_depth > len(ranked_rows)
    ):
        raise ValueError("V30 candidate depth differs")
    if type(page_count) is not int or page_count != PAGE_COUNT:
        raise ValueError("V30 page count differs")
    if type(row_pages) is not tuple or any(type(page) is not int or page < 0 for page in row_pages):
        raise ValueError("V30 row page authority differs")
    checked: list[tuple[float, int]] = []
    for score, row in ranked_rows:
        if (
            type(score) not in {int, float}
            or not math.isfinite(score)
            or type(row) is not int
            or not 0 <= row < len(row_pages)
        ):
            raise ValueError("V30 ranked row differs")
        checked.append((float(score), row))
    selected: list[int] = []
    seen: set[int] = set()
    for _score, row in sorted(checked, key=lambda item: (item[0], item[1]))[:candidate_depth]:
        page = row_pages[row]
        if page not in seen:
            seen.add(page)
            selected.append(page)
            if len(selected) == page_count:
                return tuple(selected)
    raise ValueError("V30 page count cannot be satisfied")


def _arm_result(observation: V30ArmObservation) -> dict[str, object]:
    if (
        type(observation) is not V30ArmObservation
        or observation.fidelity_fraction_ppm not in FIDELITY_FRACTIONS_PPM
        or type(observation.hits) is not tuple
        or len(observation.hits) != QUERY_COUNT
        or any(type(hit) is not int or not 0 <= hit <= RECALL_K for hit in observation.hits)
        or type(observation.selected_page_counts) is not tuple
        or observation.selected_page_counts != (PAGE_COUNT,) * QUERY_COUNT
        or type(observation.maximum_encoded_bytes) is not int
        or not 0 < observation.maximum_encoded_bytes <= MAX_ENCODED_BYTES
        or type(observation.maximum_scanned_codes) is not int
        or not 0 < observation.maximum_scanned_codes <= MAX_SCANNED_CODES
    ):
        raise ValueError("V30 arm evidence differs")
    hit_sum = sum(observation.hits)
    return {
        "aggregate_recall_ppm": hit_sum * 1_000_000 // TRUTH_MEMBERSHIPS,
        "fidelity_fraction_ppm": observation.fidelity_fraction_ppm,
        "maximum_encoded_bytes": observation.maximum_encoded_bytes,
        "maximum_scanned_codes": observation.maximum_scanned_codes,
        "minimum_recall_ppm": min(observation.hits) * 1_000_000 // RECALL_K,
        "perfect_queries": sum(hit == RECALL_K for hit in observation.hits),
        "query_count": QUERY_COUNT,
        "selected_pages": PAGE_COUNT,
    }


def build_reproduction_result(observations: tuple[V30ArmObservation, ...]) -> bytes:
    """Independently reduce raw arm evidence to one canonical claim-ineligible result."""

    if (
        type(observations) is not tuple
        or tuple(item.fidelity_fraction_ppm for item in observations) != FIDELITY_FRACTIONS_PPM
    ):
        raise ValueError("V30 arm ordering differs")
    arms = [_arm_result(observation) for observation in observations]
    passing = [
        arm
        for arm in arms
        if arm["aggregate_recall_ppm"] >= 996_875
        and arm["minimum_recall_ppm"] >= 900_000
        and arm["perfect_queries"] >= 31
    ]
    selected = min((int(arm["fidelity_fraction_ppm"]) for arm in passing), default=None)
    value = {
        "arms": arms,
        "claim_eligible": False,
        "geometry": pq8_replacement_geometry(),
        "schema": "borsuk-v30-variable-rate-reproduction-v1",
        "selected_fraction_ppm": selected,
        "status": "reproduced" if selected == 50_000 else "rejected",
    }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _nearest_rank(values: list[int], numerator: int, denominator: int) -> int:
    index = max(0, (len(values) * numerator + denominator - 1) // denominator - 1)
    return sorted(values)[index]


def simulate_concurrent_get_latency_ns(waves: tuple[tuple[int, ...], ...]) -> dict[str, object]:
    """Project concurrent-wave latency from injected request observations without sleeping."""

    if (
        type(waves) is not tuple
        or len(waves) != QUERY_COUNT
        or any(
            type(wave) is not tuple
            or len(wave) != PAGE_COUNT
            or any(type(value) is not int or value <= 0 for value in wave)
            for wave in waves
        )
    ):
        raise ValueError("V30 S3 latency samples differ")
    wave_maxima = [max(wave) for wave in waves]
    return {
        "maximum_ns": max(wave_maxima),
        "model": "concurrent-max-no-sleep",
        "p50_ns": _nearest_rank(wave_maxima, 50, 100),
        "p95_ns": _nearest_rank(wave_maxima, 95, 100),
        "p99_ns": _nearest_rank(wave_maxima, 99, 100),
        "request_count": QUERY_COUNT * PAGE_COUNT,
        "wave_count": QUERY_COUNT,
    }
