#!/usr/bin/env python3
"""Independent all-query replay of two-stage residual page nomination."""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_residual_row_score_codes import ResidualCodes
from scripts.native_residual_row_score_evaluation import ResidualSample
from scripts.native_row_score_code_artifacts import CodeArtifacts
from scripts.native_row_score_evaluation import aggregate_samples
from scripts.validate_native_row_score_result import _select_pages, validate_samples


def validate_residual_samples(
    samples: Sequence[ResidualSample],
    reported_metrics: dict[str, int | str],
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_by_query: Sequence[Sequence[int]],
    artifacts: ResidualCodes,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
) -> dict[str, int | str]:
    """Replay controls and residual scores without the producer's scorer/planner."""
    if (
        not samples
        or len(samples) != len(queries)
        or len(samples) != len(truth)
        or len(samples) != len(retained_by_query)
        or artifacts.codes.shape != (len(stable_ids), 72)
        or artifacts.codes.dtype != np.uint8
        or artifacts.first_books.shape != (48, 256, queries.shape[1] // 48)
        or artifacts.residual_books.shape != (24, 256, queries.shape[1] // 24)
        or len(page_byte_sizes) != len(artifacts.page_row_counts)
    ):
        raise ValueError("independent residual inputs differ")
    first_artifacts = CodeArtifacts(
        artifacts.first_books,
        np.ascontiguousarray(artifacts.codes[:, :48]),
        artifacts.source_ordinals,
        artifacts.page_row_counts,
        artifacts.seed,
    )
    baseline = tuple(sample.baseline for sample in samples)
    baseline_metrics = validate_samples(
        baseline,
        aggregate_samples(baseline),
        queries,
        truth,
        retained_by_query,
        first_artifacts,
        stable_ids,
        vectors,
        page_byte_sizes,
        limits,
    )
    offsets = [0]
    owners: dict[bytes, int] = {}
    for page, count in enumerate(artifacts.page_row_counts):
        for source in artifacts.source_ordinals[offsets[-1] : offsets[-1] + count]:
            stable_id = stable_ids[source]
            if stable_id in owners:
                raise ValueError("independent residual owner differs")
            owners[stable_id] = page
        offsets.append(offsets[-1] + count)
    if len(owners) != len(stable_ids):
        raise ValueError("independent residual owner differs")
    expected = []
    for ordinal, query in enumerate(queries):
        retained = tuple(retained_by_query[ordinal])
        starts = sorted({page // 16 * 16 for page in retained})
        blocks = tuple(
            (
                first,
                min(first + 16, len(page_byte_sizes)),
                offsets[first] * 72,
                (offsets[min(first + 16, len(page_byte_sizes))] - offsets[first]) * 72,
            )
            for first in starts
        )
        code_bytes = sum(block[3] for block in blocks)
        if len(blocks) > 32 or code_bytes > 16_777_216:
            raise ValueError("independent residual code wave budget differs")
        positions = [
            position
            for page in retained
            for position in range(offsets[page], offsets[page + 1])
        ]
        sources = [artifacts.source_ordinals[position] for position in positions]
        pages = [page for page in retained for _ in range(artifacts.page_row_counts[page])]
        codes = artifacts.codes[positions]
        scores = np.zeros(len(positions), dtype=np.float64)
        width = query.size // 48
        query64 = query.astype(np.float64)
        for group in range(24):
            lo = group * width * 2
            mid = lo + width
            hi = mid + width
            first_left = artifacts.first_books[2 * group, codes[:, 2 * group]].astype(np.float64)
            first_right = artifacts.first_books[2 * group + 1, codes[:, 2 * group + 1]].astype(np.float64)
            second = artifacts.residual_books[group, codes[:, 48 + group]].astype(np.float64)
            left = query64[lo:mid] - first_left - second[:, :width]
            right = query64[mid:hi] - first_right - second[:, width:]
            scores += np.einsum("ij,ij->i", left, left) + np.einsum("ij,ij->i", right, right)
        selected = _select_pages(
            scores.astype(np.float32), sources, pages, page_byte_sizes, limits
        )
        selected_set = set(selected)
        targets = truth[ordinal]
        expected.append(
            ResidualSample(
                query_ordinal=ordinal,
                baseline=baseline[ordinal],
                code_blocks=blocks,
                code_gets=len(blocks),
                code_bytes=code_bytes,
                residual_pages=selected,
                residual_data_bytes=sum(page_byte_sizes[page] for page in selected),
                residual_hits_at_10=sum(owners[target] in selected_set for target in targets[:10]),
                residual_hits_at_100=sum(owners[target] in selected_set for target in targets),
            )
        )
    if tuple(samples) != tuple(expected):
        raise ValueError("independent residual route differs")
    hits = sorted(sample.residual_hits_at_100 for sample in expected)
    result: dict[str, int | str] = {
        "query_count": len(expected),
        "baseline_pq_mean_recall_at_100_ppm": baseline_metrics["pq_mean_recall_at_100_ppm"],
        "baseline_pq_p05_recall_at_100_ppm": baseline_metrics["pq_p05_recall_at_100_ppm"],
        "baseline_pq_recall_at_10_ppm": baseline_metrics["pq_recall_at_10_ppm"],
        "exact_mean_recall_at_100_ppm": baseline_metrics["exact_mean_recall_at_100_ppm"],
        "exact_p05_recall_at_100_ppm": baseline_metrics["exact_p05_recall_at_100_ppm"],
        "retained_mean_recall_at_100_ppm": baseline_metrics["retained_mean_recall_at_100_ppm"],
        "restricted_oracle_mean_recall_at_100_ppm": baseline_metrics["restricted_oracle_mean_recall_at_100_ppm"],
        "residual_mean_recall_at_100_ppm": sum(hits) * 10_000 // len(hits),
        "residual_p05_recall_at_100_ppm": hits[math.ceil(len(hits) / 20) - 1] * 10_000,
        "residual_worst_recall_at_100_ppm": hits[0] * 10_000,
        "residual_recall_at_10_ppm": sum(sample.residual_hits_at_10 for sample in expected) * 100_000 // len(expected),
        "max_code_gets": max(sample.code_gets for sample in expected),
        "max_code_bytes": max(sample.code_bytes for sample in expected),
        "max_data_pages": max(len(sample.residual_pages) for sample in expected),
        "max_data_bytes": max(sample.residual_data_bytes for sample in expected),
    }
    result["decision"] = (
        "quality-advance-memory-pending"
        if result["residual_mean_recall_at_100_ppm"] >= 975_000
        and result["residual_p05_recall_at_100_ppm"] >= 900_000
        and result["residual_recall_at_10_ppm"] >= 960_000
        else "killed"
    )
    if result != reported_metrics:
        raise ValueError("independent residual aggregates differ")
    return result
