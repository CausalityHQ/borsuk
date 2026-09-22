#!/usr/bin/env python3
"""Bounded page nomination from two-stage residual row scores."""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Callable, Mapping, Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_residual_row_score_codes import ResidualCodes
from scripts.native_row_score_code_artifacts import CodeArtifacts
from scripts.native_row_score_evaluation import RowScoreSample, evaluate_row_score_query
from scripts.native_row_score_nomination import nominate_pages, plan_code_blocks


@dataclasses.dataclass(frozen=True, slots=True)
class ResidualSample:
    query_ordinal: int
    baseline: RowScoreSample
    code_blocks: tuple[tuple[int, int, int, int], ...]
    code_gets: int
    code_bytes: int
    residual_pages: tuple[int, ...]
    residual_data_bytes: int
    residual_hits_at_10: int
    residual_hits_at_100: int


def build_cross_terms(first_books: np.ndarray, residual_books: np.ndarray) -> np.ndarray:
    """Precompute the cross-stage terms required by exact two-code distances."""
    if (
        type(first_books) is not np.ndarray
        or type(residual_books) is not np.ndarray
        or first_books.dtype != np.float32
        or residual_books.dtype != np.float32
        or first_books.ndim != 3
        or residual_books.ndim != 3
        or first_books.shape[:2] != (48, 256)
        or residual_books.shape != (24, 256, 2 * first_books.shape[2])
        or not np.isfinite(first_books).all()
        or not np.isfinite(residual_books).all()
    ):
        raise ValueError("residual score codebook authority differs")
    width = first_books.shape[2]
    tables = np.empty((24, 2, 256, 256), dtype=np.float64)
    for group in range(24):
        for half in range(2):
            first = first_books[2 * group + half].astype(np.float64)
            residual = residual_books[group, :, half * width : (half + 1) * width].astype(
                np.float64
            )
            tables[group, half] = 2.0 * (first @ residual.T)
    return tables


def residual_scores(
    query: np.ndarray,
    first_books: np.ndarray,
    residual_books: np.ndarray,
    codes: np.ndarray,
    *,
    cross_terms: np.ndarray | None = None,
) -> np.ndarray:
    """Return squared distances to exact C1+C2 reconstructions."""
    width = first_books.shape[2]
    if (
        type(query) is not np.ndarray
        or query.dtype != np.float32
        or query.shape != (48 * width,)
        or type(codes) is not np.ndarray
        or codes.dtype != np.uint8
        or codes.ndim != 2
        or codes.shape[1] != 72
        or not np.isfinite(query).all()
    ):
        raise ValueError("residual score query authority differs")
    if cross_terms is None:
        cross_terms = build_cross_terms(first_books, residual_books)
    elif cross_terms.shape != (24, 2, 256, 256) or cross_terms.dtype != np.float64:
        raise ValueError("residual score cross terms differ")
    vector = query.astype(np.float64)
    scores = np.zeros(codes.shape[0], dtype=np.float64)
    for group in range(24):
        left = 2 * group * width
        middle = left + width
        right = middle + width
        first_left = first_books[2 * group].astype(np.float64)
        first_right = first_books[2 * group + 1].astype(np.float64)
        second = residual_books[group].astype(np.float64)
        left_code = codes[:, 2 * group]
        right_code = codes[:, 2 * group + 1]
        second_code = codes[:, 48 + group]
        left_dist = np.sum((first_left - vector[left:middle]) ** 2, axis=1)
        right_dist = np.sum((first_right - vector[middle:right]) ** 2, axis=1)
        second_term = np.sum(second * second, axis=1) - 2.0 * (
            second @ vector[left:right]
        )
        scores += (
            left_dist[left_code]
            + right_dist[right_code]
            + second_term[second_code]
            + cross_terms[group, 0, left_code, second_code]
            + cross_terms[group, 1, right_code, second_code]
        )
    return scores


def evaluate_residual_query(
    *,
    query_ordinal: int,
    query: np.ndarray,
    retained_pages: Sequence[int],
    artifacts: ResidualCodes,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    truth_ids: Sequence[bytes],
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    read_code_range: Callable[[int, int], bytes],
    cross_terms: np.ndarray | None = None,
    owner_by_id: Mapping[bytes, int] | None = None,
) -> ResidualSample:
    """Read sealed 72-byte codes and pair residual pages against a0001 controls."""
    if (
        type(artifacts) is not ResidualCodes
        or artifacts.codes.shape != (len(stable_ids), 72)
        or artifacts.codes.dtype != np.uint8
        or len(artifacts.source_ordinals) != len(stable_ids)
        or sum(artifacts.page_row_counts) != len(stable_ids)
    ):
        raise ValueError("residual evaluation authority differs")
    first_codes = np.ascontiguousarray(artifacts.codes[:, :48])
    first_artifacts = CodeArtifacts(
        artifacts.first_books,
        first_codes,
        artifacts.source_ordinals,
        artifacts.page_row_counts,
        artifacts.seed,
    )
    first_plane = first_codes.tobytes(order="C")
    baseline = evaluate_row_score_query(
        query_ordinal=query_ordinal,
        query=query,
        retained_pages=retained_pages,
        artifacts=first_artifacts,
        stable_ids=stable_ids,
        vectors=vectors,
        truth_ids=truth_ids,
        page_byte_sizes=page_byte_sizes,
        limits=limits,
        read_code_range=lambda offset, length: first_plane[offset : offset + length],
        owner_by_id=owner_by_id,
    )
    plan = plan_code_blocks(
        retained_pages, artifacts.page_row_counts, code_row_bytes=72
    )
    offsets = np.concatenate(([0], np.cumsum(artifacts.page_row_counts, dtype=np.int64)))
    positions = np.concatenate(
        [np.arange(offsets[page], offsets[page + 1]) for page in retained_pages]
    )
    codes = np.empty((len(positions), 72), dtype=np.uint8)
    filled = np.zeros(len(positions), dtype=np.bool_)
    sealed_plane = artifacts.codes.reshape(-1)
    for _, _, offset, length in plan.blocks:
        payload = read_code_range(offset, length)
        expected = sealed_plane[offset : offset + length].tobytes(order="C")
        if type(payload) is not bytes or payload != expected:
            raise ValueError("residual code range identity differs")
        first_row = offset // 72
        last_row = (offset + length) // 72
        within = (positions >= first_row) & (positions < last_row)
        block_codes = np.frombuffer(payload, dtype=np.uint8).reshape(-1, 72)
        codes[within] = block_codes[positions[within] - first_row]
        filled[within] = True
    if not filled.all():
        raise ValueError("residual code range coverage differs")
    scores = residual_scores(
        query, artifacts.first_books, artifacts.residual_books, codes,
        cross_terms=cross_terms,
    )
    source_ordinals = tuple(artifacts.source_ordinals[int(position)] for position in positions)
    page_ordinals = tuple(
        page for page in retained_pages for _ in range(artifacts.page_row_counts[page])
    )
    pages = nominate_pages(
        scores, source_ordinals, page_ordinals, page_byte_sizes, limits
    )
    if owner_by_id is None:
        owners = {
            stable_ids[artifacts.source_ordinals[position]]: page
            for page in range(len(artifacts.page_row_counts))
            for position in range(int(offsets[page]), int(offsets[page + 1]))
        }
    else:
        owners = owner_by_id
    page_set = set(pages)
    return ResidualSample(
        query_ordinal=query_ordinal,
        baseline=baseline,
        code_blocks=plan.blocks,
        code_gets=plan.gets,
        code_bytes=plan.bytes,
        residual_pages=pages,
        residual_data_bytes=sum(page_byte_sizes[page] for page in pages),
        residual_hits_at_10=sum(owners[stable_id] in page_set for stable_id in truth_ids[:10]),
        residual_hits_at_100=sum(owners[stable_id] in page_set for stable_id in truth_ids),
    )


def aggregate_residual_samples(samples: Sequence[ResidualSample]) -> dict[str, int | str]:
    """Apply the frozen 100k quality and two-wave limits to paired samples."""
    if (
        not samples
        or [sample.query_ordinal for sample in samples] != list(range(len(samples)))
        or any(
            sample.baseline.query_ordinal != sample.query_ordinal
            or sample.residual_hits_at_10 < 0
            or sample.residual_hits_at_10 > 10
            or sample.residual_hits_at_100 < 0
            or sample.residual_hits_at_100 > 100
            or not 0 < sample.code_gets <= 32
            or not 0 < sample.code_bytes <= 16_777_216
            or not 0 < len(sample.residual_pages) <= 32
            or not 0 < sample.residual_data_bytes <= 16_777_216
            for sample in samples
        )
    ):
        raise ValueError("residual aggregate evidence differs")
    from scripts.native_row_score_evaluation import aggregate_samples

    baseline = aggregate_samples(tuple(sample.baseline for sample in samples))
    hits = sorted(sample.residual_hits_at_100 for sample in samples)
    result: dict[str, int | str] = {
        "query_count": len(samples),
        "baseline_pq_mean_recall_at_100_ppm": baseline["pq_mean_recall_at_100_ppm"],
        "baseline_pq_p05_recall_at_100_ppm": baseline["pq_p05_recall_at_100_ppm"],
        "baseline_pq_recall_at_10_ppm": baseline["pq_recall_at_10_ppm"],
        "exact_mean_recall_at_100_ppm": baseline["exact_mean_recall_at_100_ppm"],
        "exact_p05_recall_at_100_ppm": baseline["exact_p05_recall_at_100_ppm"],
        "retained_mean_recall_at_100_ppm": baseline["retained_mean_recall_at_100_ppm"],
        "restricted_oracle_mean_recall_at_100_ppm": baseline["restricted_oracle_mean_recall_at_100_ppm"],
        "residual_mean_recall_at_100_ppm": sum(hits) * 10_000 // len(hits),
        "residual_p05_recall_at_100_ppm": hits[math.ceil(0.05 * len(hits)) - 1] * 10_000,
        "residual_worst_recall_at_100_ppm": hits[0] * 10_000,
        "residual_recall_at_10_ppm": sum(sample.residual_hits_at_10 for sample in samples) * 100_000 // len(samples),
        "max_code_gets": max(sample.code_gets for sample in samples),
        "max_code_bytes": max(sample.code_bytes for sample in samples),
        "max_data_pages": max(len(sample.residual_pages) for sample in samples),
        "max_data_bytes": max(sample.residual_data_bytes for sample in samples),
    }
    result["decision"] = (
        "quality-advance-memory-pending"
        if result["residual_mean_recall_at_100_ppm"] >= 975_000
        and result["residual_p05_recall_at_100_ppm"] >= 900_000
        and result["residual_recall_at_10_ppm"] >= 960_000
        else "killed"
    )
    return result
