#!/usr/bin/env python3
"""One-query paired row-score page selection over a fixed tree shortlist."""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Callable, Mapping, Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_row_score_code_artifacts import CodeArtifacts
from scripts.native_row_score_nomination import nominate_pages, plan_code_blocks
from scripts.v97_row_width_screen import adc_scores
from scripts.v102_two_wave_pq48_refinement import PQ48X8


@dataclasses.dataclass(frozen=True, slots=True)
class RowScoreSample:
    query_ordinal: int
    retained_pages: tuple[int, ...]
    code_blocks: tuple[tuple[int, int, int, int], ...]
    code_gets: int
    code_bytes: int
    retained_hits_at_10: int
    retained_hits_at_100: int
    restricted_oracle_pages: tuple[int, ...]
    restricted_oracle_hits_at_10: int
    restricted_oracle_hits_at_100: int
    pq_pages: tuple[int, ...]
    pq_data_bytes: int
    pq_hits_at_10: int
    pq_hits_at_100: int
    exact_pages: tuple[int, ...]
    exact_data_bytes: int
    exact_hits_at_10: int
    exact_hits_at_100: int


def aggregate_samples(samples: Sequence[RowScoreSample]) -> dict[str, int | str]:
    """Derive the fixed quality and two-wave resource decision from samples."""
    if (
        not samples
        or [sample.query_ordinal for sample in samples] != list(range(len(samples)))
        or any(
            any(
                not 0 <= getattr(sample, field) <= maximum
                for field, maximum in (
                    ("retained_hits_at_10", 10),
                    ("retained_hits_at_100", 100),
                    ("restricted_oracle_hits_at_10", 10),
                    ("restricted_oracle_hits_at_100", 100),
                    ("pq_hits_at_10", 10),
                    ("pq_hits_at_100", 100),
                    ("exact_hits_at_10", 10),
                    ("exact_hits_at_100", 100),
                )
            )
            or not 0 < sample.code_gets <= 32
            or not 0 < sample.code_bytes <= 16_777_216
            or not 0 < len(sample.pq_pages) <= 32
            or not 0 < sample.pq_data_bytes <= 16_777_216
            or not 0 < len(sample.exact_pages) <= 32
            or not 0 < sample.exact_data_bytes <= 16_777_216
            for sample in samples
        )
    ):
        raise ValueError("row-score aggregate evidence differs")
    result: dict[str, int | str] = {"query_count": len(samples)}
    for arm, field in (
        ("retained", "retained_hits_at_100"),
        ("restricted_oracle", "restricted_oracle_hits_at_100"),
        ("pq", "pq_hits_at_100"),
        ("exact", "exact_hits_at_100"),
    ):
        hits = sorted(getattr(sample, field) for sample in samples)
        result[f"{arm}_mean_recall_at_100_ppm"] = sum(hits) * 10_000 // len(hits)
        result[f"{arm}_p05_recall_at_100_ppm"] = hits[math.ceil(0.05 * len(hits)) - 1] * 10_000
        result[f"{arm}_worst_recall_at_100_ppm"] = hits[0] * 10_000
    result["pq_recall_at_10_ppm"] = sum(sample.pq_hits_at_10 for sample in samples) * 100_000 // len(samples)
    result["exact_recall_at_10_ppm"] = sum(sample.exact_hits_at_10 for sample in samples) * 100_000 // len(samples)
    result["max_code_gets"] = max(sample.code_gets for sample in samples)
    result["max_code_bytes"] = max(sample.code_bytes for sample in samples)
    result["max_data_pages"] = max(len(sample.pq_pages) for sample in samples)
    result["max_data_bytes"] = max(sample.pq_data_bytes for sample in samples)
    result["decision"] = (
        "quality-advance-memory-pending"
        if result["pq_recall_at_10_ppm"] >= 960_000
        and result["pq_mean_recall_at_100_ppm"] >= 975_000
        and result["pq_p05_recall_at_100_ppm"] >= 900_000
        else "killed"
    )
    return result


def evaluate_row_score_query(
    *,
    query_ordinal: int,
    query: np.ndarray,
    retained_pages: Sequence[int],
    artifacts: CodeArtifacts,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    truth_ids: Sequence[bytes],
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    read_code_range: Callable[[int, int], bytes],
    owner_by_id: Mapping[bytes, int] | None = None,
) -> RowScoreSample:
    """Score retained rows, nominate pages, then count frozen GT containment."""
    page_count = len(artifacts.page_row_counts)
    if (
        type(query_ordinal) is not int
        or query_ordinal < 0
        or type(query) is not np.ndarray
        or query.dtype != np.float32
        or query.ndim != 1
        or type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.shape != (len(stable_ids), query.size)
        or not np.isfinite(query).all()
        or not np.isfinite(vectors).all()
        or artifacts.books.shape != (48, 256, query.size // 48)
        or artifacts.codes.shape != (len(stable_ids), 48)
        or artifacts.codes.dtype != np.uint8
        or len(artifacts.source_ordinals) != len(stable_ids)
        or sum(artifacts.page_row_counts) != len(stable_ids)
        or len(page_byte_sizes) != page_count
        or len(truth_ids) != 100
        or len(set(truth_ids)) != 100
    ):
        raise ValueError("row-score evaluation authority differs")
    code_plan = plan_code_blocks(retained_pages, artifacts.page_row_counts)
    offsets = np.concatenate(([0], np.cumsum(artifacts.page_row_counts, dtype=np.int64)))
    positions = np.concatenate(
        [np.arange(offsets[page], offsets[page + 1]) for page in retained_pages]
    )
    source_ordinals = tuple(artifacts.source_ordinals[int(position)] for position in positions)
    page_ordinals = tuple(
        page
        for page in retained_pages
        for _ in range(artifacts.page_row_counts[page])
    )
    codes = np.empty((len(positions), 48), dtype=np.uint8)
    filled = np.zeros(len(positions), dtype=np.bool_)
    sealed_plane = artifacts.codes.reshape(-1)
    for _, _, offset, length in code_plan.blocks:
        payload = read_code_range(offset, length)
        expected = sealed_plane[offset : offset + length].tobytes(order="C")
        if type(payload) is not bytes or payload != expected:
            raise ValueError("code range identity differs")
        first_row = offset // 48
        last_row = (offset + length) // 48
        in_block = (positions >= first_row) & (positions < last_row)
        block_codes = np.frombuffer(payload, dtype=np.uint8).reshape(-1, 48)
        codes[in_block] = block_codes[positions[in_block] - first_row]
        filled[in_block] = True
    if not filled.all():
        raise ValueError("code range coverage differs")
    pq_scores = adc_scores(query, artifacts.books, codes, PQ48X8)
    selected_vectors = vectors[list(source_ordinals)].astype(np.float64)
    delta = selected_vectors - query.astype(np.float64)
    exact_scores = np.einsum("ij,ij->i", delta, delta)
    pq_pages = nominate_pages(
        pq_scores, source_ordinals, page_ordinals, page_byte_sizes, limits
    )
    exact_pages = nominate_pages(
        exact_scores, source_ordinals, page_ordinals, page_byte_sizes, limits
    )
    if owner_by_id is None:
        owners = {
            stable_ids[artifacts.source_ordinals[position]]: page
            for page in range(page_count)
            for position in range(int(offsets[page]), int(offsets[page + 1]))
        }
    else:
        owners = owner_by_id
    if any(stable_id not in owners for stable_id in truth_ids):
        raise ValueError("row-score truth authority differs")
    retained_set = set(retained_pages)
    if max(page_byte_sizes[page] for page in retained_set) * min(
        limits.maximum_pages, len(retained_set)
    ) > limits.maximum_bytes:
        raise ValueError("restricted oracle byte cap needs exact optimization")
    truth_counts = {page: 0 for page in retained_set}
    for stable_id in truth_ids:
        if owners[stable_id] in truth_counts:
            truth_counts[owners[stable_id]] += 1
    oracle_pages = tuple(
        sorted(retained_set, key=lambda page: (-truth_counts[page], page))[
            : limits.maximum_pages
        ]
    )
    oracle_set = set(oracle_pages)
    pq_set, exact_set = set(pq_pages), set(exact_pages)
    return RowScoreSample(
        query_ordinal=query_ordinal,
        retained_pages=tuple(retained_pages),
        code_blocks=code_plan.blocks,
        code_gets=code_plan.gets,
        code_bytes=code_plan.bytes,
        retained_hits_at_10=sum(owners[stable_id] in retained_set for stable_id in truth_ids[:10]),
        retained_hits_at_100=sum(owners[stable_id] in retained_set for stable_id in truth_ids),
        restricted_oracle_pages=oracle_pages,
        restricted_oracle_hits_at_10=sum(owners[stable_id] in oracle_set for stable_id in truth_ids[:10]),
        restricted_oracle_hits_at_100=sum(owners[stable_id] in oracle_set for stable_id in truth_ids),
        pq_pages=pq_pages,
        pq_data_bytes=sum(page_byte_sizes[page] for page in pq_pages),
        pq_hits_at_10=sum(owners[stable_id] in pq_set for stable_id in truth_ids[:10]),
        pq_hits_at_100=sum(owners[stable_id] in pq_set for stable_id in truth_ids),
        exact_pages=exact_pages,
        exact_data_bytes=sum(page_byte_sizes[page] for page in exact_pages),
        exact_hits_at_10=sum(owners[stable_id] in exact_set for stable_id in truth_ids[:10]),
        exact_hits_at_100=sum(owners[stable_id] in exact_set for stable_id in truth_ids),
    )
