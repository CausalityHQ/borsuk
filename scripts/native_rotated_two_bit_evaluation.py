#!/usr/bin/env python3
"""Authenticated two-bit group reads and three paired page nominations."""

from __future__ import annotations

import dataclasses
import hashlib
import math
import struct
from collections.abc import Callable, Mapping, Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_page_centered_group_evaluation import plan_groups
from scripts.native_rotated_two_bit_codes import (
    PACKED_BYTES,
    ROW_BYTES,
    GroupRange,
    TwoBitCodes,
    decode_levels,
    rotate_rows,
)
from scripts.native_row_score_nomination import nominate_pages

MAXIMUM_CODE_GETS = 32
MAXIMUM_CODE_BYTES = 16_777_216
MAXIMUM_CANDIDATE_BATCH = 2048


@dataclasses.dataclass(frozen=True, slots=True)
class TwoBitSample:
    query_ordinal: int
    retained_pages: tuple[int, ...]
    group_ranges: tuple[GroupRange, ...]
    grouped_pages: tuple[int, ...]
    code_gets: int
    code_bytes: int
    grouped_hits_at_10: int
    grouped_hits_at_100: int
    exact_pages: tuple[int, ...]
    exact_data_bytes: int
    exact_hits_at_10: int
    exact_hits_at_100: int
    primary_pages: tuple[int, ...]
    primary_data_bytes: int
    primary_hits_at_10: int
    primary_hits_at_100: int
    diagnostic_pages: tuple[int, ...]
    diagnostic_data_bytes: int
    diagnostic_hits_at_10: int
    diagnostic_hits_at_100: int
    prior_pq_hits_at_100: int
    prior_residual_hits_at_100: int


def _read_record_scalars(records: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    scales = np.frombuffer(records[:, PACKED_BYTES : PACKED_BYTES + 4].tobytes(), dtype="<f4")
    norms = np.frombuffer(records[:, PACKED_BYTES + 4 : ROW_BYTES].tobytes(), dtype="<f4")
    if (
        not np.isfinite(scales).all()
        or not np.isfinite(norms).all()
        or np.any(scales < 0)
        or np.any(norms < 0)
    ):
        raise ValueError("two-bit record scalar differs")
    return scales.astype(np.float64), norms.astype(np.float64)


def score_records(
    query: np.ndarray,
    mean: np.ndarray,
    records: np.ndarray,
    *,
    rotation_seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Reconstructed primary and exact-norm diagnostic on the same code bits."""
    if (
        type(query) is not np.ndarray
        or query.dtype != np.float32
        or query.shape != (768,)
        or type(mean) is not np.ndarray
        or mean.dtype != np.float32
        or mean.shape != (768,)
        or type(records) is not np.ndarray
        or records.dtype != np.uint8
        or records.ndim != 2
        or records.shape[1] != ROW_BYTES
        or not np.isfinite(query).all()
        or not np.isfinite(mean).all()
    ):
        raise ValueError("two-bit score authority differs")
    centered_q = query.astype(np.float64) - mean.astype(np.float64)
    rotated_q = rotate_rows(centered_q[None, :], rotation_seed=rotation_seed)[0]
    query_norm = float(np.sum(centered_q * centered_q, dtype=np.float64))
    primary = np.empty(len(records), dtype=np.float32)
    diagnostic = np.empty(len(records), dtype=np.float32)
    for first in range(0, len(records), MAXIMUM_CANDIDATE_BATCH):
        last = min(first + MAXIMUM_CANDIDATE_BATCH, len(records))
        part = records[first:last]
        levels = decode_levels(part[:, :PACKED_BYTES]).astype(np.float64)
        scales, norms = _read_record_scalars(part)
        dot = np.sum(levels * rotated_q, axis=1, dtype=np.float64)
        coded_norm = np.sum(levels * levels, axis=1, dtype=np.float64) * (scales * scales)
        primary[first:last] = (query_norm + coded_norm - 2 * scales * dot).astype(np.float32)
        diagnostic[first:last] = (query_norm + norms - 2 * scales * dot).astype(np.float32)
    if not np.isfinite(primary).all() or not np.isfinite(diagnostic).all():
        raise ValueError("two-bit score is nonfinite")
    return primary, diagnostic


def exact_scores(query: np.ndarray, vectors: np.ndarray, sources: Sequence[int]) -> np.ndarray:
    """Chunk exact source distances so no full float64 candidate matrix exists."""
    scores = np.empty(len(sources), dtype=np.float32)
    q = query.astype(np.float64)
    for first in range(0, len(sources), MAXIMUM_CANDIDATE_BATCH):
        last = min(first + MAXIMUM_CANDIDATE_BATCH, len(sources))
        delta = vectors[list(sources[first:last])].astype(np.float64) - q
        scores[first:last] = np.einsum("ij,ij->i", delta, delta).astype(np.float32)
    return scores


def _decode_group(
    payload: bytes,
    group: GroupRange,
    page_counts: Sequence[int],
) -> np.ndarray:
    first, end, _, length, digest = group
    if (
        type(payload) is not bytes
        or len(payload) != length
        or hashlib.sha256(payload).hexdigest() != digest
    ):
        raise ValueError("two-bit group range identity differs")
    row_count = sum(page_counts[first:end])
    header_bytes = 4 + 4 * (end - first)
    if len(payload) != header_bytes + row_count * ROW_BYTES:
        raise ValueError("two-bit group payload differs")
    if struct.unpack_from("<I", payload)[0] != end - first:
        raise ValueError("two-bit group page count differs")
    for page in range(first, end):
        if struct.unpack_from("<I", payload, 4 + 4 * (page - first))[0] != page_counts[page]:
            raise ValueError("two-bit group row count differs")
    return np.frombuffer(payload, dtype=np.uint8, offset=header_bytes).reshape(row_count, ROW_BYTES)


def evaluate_two_bit_query(
    *,
    query_ordinal: int,
    query: np.ndarray,
    retained_pages: Sequence[int],
    artifacts: TwoBitCodes,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    truth_ids: Sequence[bytes],
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    read_group_range: Callable[[int, int], bytes],
    prior_exact_pages: Sequence[int],
    prior_pq_hits_at_100: int,
    prior_residual_hits_at_100: int,
    prior_exact_hits_at_10: int | None = None,
    prior_exact_hits_at_100: int | None = None,
    owner_by_id: Mapping[bytes, int] | None = None,
) -> TwoBitSample:
    """Fetch every chosen group and bind the unchanged exact control."""
    if (
        len(stable_ids) != len(vectors)
        or len(truth_ids) != 100
        or len(set(truth_ids)) != 100
        or len(page_byte_sizes) != len(artifacts.page_row_counts)
        or query_ordinal < 0
        or not 0 <= prior_pq_hits_at_100 <= 100
        or not 0 <= prior_residual_hits_at_100 <= 100
    ):
        raise ValueError("two-bit query authority differs")
    plan = plan_groups(
        retained_pages, artifacts.group_ranges,
        maximum_groups=MAXIMUM_CODE_GETS, maximum_bytes=MAXIMUM_CODE_BYTES,
    )
    offsets = [0]
    for count in artifacts.page_row_counts:
        offsets.append(offsets[-1] + count)
    before_gets = read_group_range.gets
    before_bytes = read_group_range.bytes
    parts: list[np.ndarray] = []
    sources: list[int] = []
    row_pages: list[int] = []
    for group in plan.ranges:
        first, end, offset, length, _ = group
        records = _decode_group(
            read_group_range(offset, length), group, artifacts.page_row_counts
        )
        if not np.array_equal(records, artifacts.records[offsets[first] : offsets[end]]):
            raise ValueError("two-bit group range identity differs")
        parts.append(records)
        sources.extend(artifacts.source_ordinals[offsets[first] : offsets[end]])
        for page in range(first, end):
            row_pages.extend([page] * artifacts.page_row_counts[page])
    gets = read_group_range.gets - before_gets
    bytes_read = read_group_range.bytes - before_bytes
    if gets != plan.gets or bytes_read != plan.bytes:
        raise ValueError("two-bit actual code wave differs")
    records = np.concatenate(parts)
    primary_scores, diagnostic_scores = score_records(
        query, artifacts.mean, records, rotation_seed=artifacts.rotation_seed
    )
    exact = exact_scores(query, vectors, sources)
    exact_pages = nominate_pages(exact, sources, row_pages, page_byte_sizes, limits)
    if tuple(prior_exact_pages) != exact_pages:
        raise ValueError("two-bit closed exact control differs")
    primary_pages = nominate_pages(primary_scores, sources, row_pages, page_byte_sizes, limits)
    diagnostic_pages = nominate_pages(diagnostic_scores, sources, row_pages, page_byte_sizes, limits)
    if owner_by_id is None:
        owners = {
            stable_ids[artifacts.source_ordinals[position]]: page
            for page in range(len(page_byte_sizes))
            for position in range(offsets[page], offsets[page + 1])
        }
    else:
        owners = owner_by_id
    if any(stable_id not in owners for stable_id in truth_ids):
        raise ValueError("two-bit truth authority differs")
    grouped_set = set(plan.pages)
    exact_set = set(exact_pages)
    primary_set = set(primary_pages)
    diagnostic_set = set(diagnostic_pages)
    exact_hits10 = sum(owners[value] in exact_set for value in truth_ids[:10])
    exact_hits100 = sum(owners[value] in exact_set for value in truth_ids)
    if (
        prior_exact_hits_at_10 is not None and exact_hits10 != prior_exact_hits_at_10
    ) or (
        prior_exact_hits_at_100 is not None and exact_hits100 != prior_exact_hits_at_100
    ):
        raise ValueError("two-bit closed exact control differs")
    sample = TwoBitSample(
        query_ordinal=query_ordinal,
        retained_pages=tuple(retained_pages),
        group_ranges=plan.ranges,
        grouped_pages=plan.pages,
        code_gets=gets,
        code_bytes=bytes_read,
        grouped_hits_at_10=sum(owners[value] in grouped_set for value in truth_ids[:10]),
        grouped_hits_at_100=sum(owners[value] in grouped_set for value in truth_ids),
        exact_pages=exact_pages,
        exact_data_bytes=sum(page_byte_sizes[page] for page in exact_pages),
        exact_hits_at_10=exact_hits10,
        exact_hits_at_100=exact_hits100,
        primary_pages=primary_pages,
        primary_data_bytes=sum(page_byte_sizes[page] for page in primary_pages),
        primary_hits_at_10=sum(owners[value] in primary_set for value in truth_ids[:10]),
        primary_hits_at_100=sum(owners[value] in primary_set for value in truth_ids),
        diagnostic_pages=diagnostic_pages,
        diagnostic_data_bytes=sum(page_byte_sizes[page] for page in diagnostic_pages),
        diagnostic_hits_at_10=sum(owners[value] in diagnostic_set for value in truth_ids[:10]),
        diagnostic_hits_at_100=sum(owners[value] in diagnostic_set for value in truth_ids),
        prior_pq_hits_at_100=prior_pq_hits_at_100,
        prior_residual_hits_at_100=prior_residual_hits_at_100,
    )
    if any(
        size > limits.maximum_bytes
        for size in (sample.exact_data_bytes, sample.primary_data_bytes, sample.diagnostic_data_bytes)
    ):
        raise ValueError("two-bit planned data wave differs")
    return sample


def aggregate_two_bit_samples(samples: Sequence[TwoBitSample]) -> dict[str, int | str]:
    """Reduce paired query containment and classify only the preregistered arm."""
    if (
        not samples
        or any(type(sample) is not TwoBitSample or sample.query_ordinal != index for index, sample in enumerate(samples))
        or any(sample.code_gets > MAXIMUM_CODE_GETS or sample.code_bytes > MAXIMUM_CODE_BYTES for sample in samples)
        or any(max(len(sample.exact_pages), len(sample.primary_pages), len(sample.diagnostic_pages)) > 32 for sample in samples)
        or any(max(sample.exact_data_bytes, sample.primary_data_bytes, sample.diagnostic_data_bytes) > 16_777_216 for sample in samples)
    ):
        raise ValueError("two-bit aggregate authority differs")
    count = len(samples)

    def mean_ppm(field: str, top: int) -> int:
        return sum(getattr(sample, field) for sample in samples) * 1_000_000 // (count * top)

    def p05_ppm(field: str) -> int:
        return sorted(getattr(sample, field) for sample in samples)[math.ceil(count * 0.05) - 1] * 10_000

    exact_mean = mean_ppm("exact_hits_at_100", 100)
    exact_p05 = p05_ppm("exact_hits_at_100")
    exact_r10 = mean_ppm("exact_hits_at_10", 10)
    primary_mean = mean_ppm("primary_hits_at_100", 100)
    primary_p05 = p05_ppm("primary_hits_at_100")
    primary_r10 = mean_ppm("primary_hits_at_10", 10)
    diagnostic_mean = mean_ppm("diagnostic_hits_at_100", 100)
    diagnostic_p05 = p05_ppm("diagnostic_hits_at_100")
    diagnostic_r10 = mean_ppm("diagnostic_hits_at_10", 10)
    exact_pass = exact_mean >= 975_000 and exact_p05 >= 900_000 and exact_r10 >= 960_000
    primary_pass = primary_mean >= 975_000 and primary_p05 >= 900_000 and primary_r10 >= 960_000
    diagnostic_pass = diagnostic_mean >= 975_000 and diagnostic_p05 >= 900_000 and diagnostic_r10 >= 960_000
    decision = (
        "invalid-exact-control"
        if not exact_pass
        else "quality-advance-memory-pending"
        if primary_pass
        else "estimator-failed-diagnostic-pass"
        if diagnostic_pass
        else "representation-killed"
    )
    return {
        "query_count": count,
        "prior_pq_mean_recall_at_100_ppm": mean_ppm("prior_pq_hits_at_100", 100),
        "prior_residual_mean_recall_at_100_ppm": mean_ppm("prior_residual_hits_at_100", 100),
        "grouped_mean_recall_at_100_ppm": mean_ppm("grouped_hits_at_100", 100),
        "exact_mean_recall_at_100_ppm": exact_mean,
        "exact_p05_recall_at_100_ppm": exact_p05,
        "exact_recall_at_10_ppm": exact_r10,
        "primary_mean_recall_at_100_ppm": primary_mean,
        "primary_p05_recall_at_100_ppm": primary_p05,
        "primary_recall_at_10_ppm": primary_r10,
        "primary_worst_recall_at_100_ppm": min(sample.primary_hits_at_100 for sample in samples) * 10_000,
        "diagnostic_mean_recall_at_100_ppm": diagnostic_mean,
        "diagnostic_p05_recall_at_100_ppm": diagnostic_p05,
        "diagnostic_recall_at_10_ppm": diagnostic_r10,
        "max_code_gets": max(sample.code_gets for sample in samples),
        "max_code_bytes": max(sample.code_bytes for sample in samples),
        "max_data_pages": max(max(len(sample.exact_pages), len(sample.primary_pages), len(sample.diagnostic_pages)) for sample in samples),
        "max_data_bytes": max(max(sample.exact_data_bytes, sample.primary_data_bytes, sample.diagnostic_data_bytes) for sample in samples),
        "decision": decision,
    }
