#!/usr/bin/env python3
"""Independent all-query replay of PQ48 row-score page nomination."""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_row_score_code_artifacts import CodeArtifacts
from scripts.native_row_score_evaluation import RowScoreSample


def _select_pages(
    scores: np.ndarray,
    source_ordinals: Sequence[int],
    page_ordinals: Sequence[int],
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
) -> tuple[int, ...]:
    """Independent stable row-order and page-count selector."""
    ordered = sorted(
        range(len(scores)), key=lambda index: (float(scores[index]), source_ordinals[index])
    )
    counts: dict[int, int] = {}
    for index in ordered[:100]:
        page = page_ordinals[index]
        counts[page] = counts.get(page, 0) + 1
    best: dict[int, float] = {}
    for index in ordered:
        best.setdefault(page_ordinals[index], float(scores[index]))
    nominated = sorted(counts, key=lambda page: (-counts[page], best[page], page))
    remaining = sorted(
        (page for page in best if page not in counts),
        key=lambda page: (best[page], page),
    )
    pages: list[int] = []
    size = 0
    for page in (*nominated, *remaining):
        if size + page_byte_sizes[page] > limits.maximum_bytes:
            continue
        pages.append(page)
        size += page_byte_sizes[page]
        if len(pages) == limits.maximum_pages:
            break
    if not pages:
        raise ValueError("independent route selected no page")
    return tuple(pages)


def _metrics(samples: Sequence[RowScoreSample]) -> dict[str, int | str]:
    result: dict[str, int | str] = {"query_count": len(samples)}
    for arm, field in (
        ("retained", "retained_hits_at_100"),
        ("restricted_oracle", "restricted_oracle_hits_at_100"),
        ("pq", "pq_hits_at_100"),
        ("exact", "exact_hits_at_100"),
    ):
        values = sorted(getattr(sample, field) for sample in samples)
        result[f"{arm}_mean_recall_at_100_ppm"] = sum(values) * 10_000 // len(values)
        result[f"{arm}_p05_recall_at_100_ppm"] = values[math.ceil(len(values) / 20) - 1] * 10_000
        result[f"{arm}_worst_recall_at_100_ppm"] = values[0] * 10_000
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


def validate_samples(
    samples: Sequence[RowScoreSample],
    reported_metrics: dict[str, int | str],
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    retained_by_query: Sequence[Sequence[int]],
    artifacts: CodeArtifacts,
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
) -> dict[str, int | str]:
    """Replay every route without calling producer planner, scorer, or aggregator."""
    if (
        not samples
        or len(samples) != len(queries)
        or len(samples) != len(truth)
        or len(retained_by_query) != len(samples)
        or queries.ndim != 2
        or queries.dtype != np.float32
        or vectors.shape != (len(stable_ids), queries.shape[1])
        or artifacts.codes.shape != (len(stable_ids), 48)
        or artifacts.books.shape != (48, 256, queries.shape[1] // 48)
        or len(page_byte_sizes) != len(artifacts.page_row_counts)
        or not np.isfinite(queries).all()
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("independent row-score inputs differ")
    offsets = [0]
    owners: dict[bytes, int] = {}
    for page, count in enumerate(artifacts.page_row_counts):
        for source in artifacts.source_ordinals[offsets[-1] : offsets[-1] + count]:
            stable_id = stable_ids[source]
            if stable_id in owners:
                raise ValueError("independent row-score owner differs")
            owners[stable_id] = page
        offsets.append(offsets[-1] + count)
    if len(owners) != len(stable_ids):
        raise ValueError("independent row-score owner differs")
    expected: list[RowScoreSample] = []
    for ordinal, query in enumerate(queries):
        retained = tuple(retained_by_query[ordinal])
        targets = tuple(truth[ordinal])
        if (
            len(set(retained)) != len(retained)
            or not retained
            or any(page < 0 or page >= len(page_byte_sizes) for page in retained)
            or len(targets) != 100
            or len(set(targets)) != 100
            or any(target not in owners for target in targets)
        ):
            raise ValueError("independent row-score route authority differs")
        starts = sorted({page // 16 * 16 for page in retained})
        blocks = tuple(
            (first, min(first + 16, len(page_byte_sizes)),
             offsets[first] * 48,
             (offsets[min(first + 16, len(page_byte_sizes))] - offsets[first]) * 48)
            for first in starts
        )
        code_bytes = sum(block[3] for block in blocks)
        if len(blocks) > 32 or code_bytes > 16_777_216:
            raise ValueError("independent code wave budget differs")
        positions = [position for page in retained for position in range(offsets[page], offsets[page + 1])]
        sources = [artifacts.source_ordinals[position] for position in positions]
        pages = [page for page in retained for _ in range(artifacts.page_row_counts[page])]
        codes = artifacts.codes[positions]
        pq_scores = np.zeros(len(positions), dtype=np.float32)
        width = query.size // 48
        for subspace in range(48):
            delta = artifacts.books[subspace] - query[subspace * width : (subspace + 1) * width]
            table = np.einsum("ij,ij->i", delta, delta, dtype=np.float32)
            pq_scores += table[codes[:, subspace]]
        selected_vectors = vectors[sources].astype(np.float64)
        delta = selected_vectors - query.astype(np.float64)
        exact_scores = np.einsum("ij,ij->i", delta, delta)
        pq_pages = _select_pages(pq_scores, sources, pages, page_byte_sizes, limits)
        exact_pages = _select_pages(exact_scores, sources, pages, page_byte_sizes, limits)
        retained_set = set(retained)
        if max(page_byte_sizes[page] for page in retained_set) * min(limits.maximum_pages, len(retained_set)) > limits.maximum_bytes:
            raise ValueError("independent restricted oracle byte cap differs")
        counts = {page: sum(owners[target] == page for target in targets) for page in retained_set}
        oracle_pages = tuple(sorted(retained_set, key=lambda page: (-counts[page], page))[:limits.maximum_pages])
        pq_set, exact_set, oracle_set = set(pq_pages), set(exact_pages), set(oracle_pages)
        expected.append(RowScoreSample(
            query_ordinal=ordinal, retained_pages=retained, code_blocks=blocks,
            code_gets=len(blocks), code_bytes=code_bytes,
            retained_hits_at_10=sum(owners[target] in retained_set for target in targets[:10]),
            retained_hits_at_100=sum(owners[target] in retained_set for target in targets),
            restricted_oracle_pages=oracle_pages,
            restricted_oracle_hits_at_10=sum(owners[target] in oracle_set for target in targets[:10]),
            restricted_oracle_hits_at_100=sum(owners[target] in oracle_set for target in targets),
            pq_pages=pq_pages, pq_data_bytes=sum(page_byte_sizes[page] for page in pq_pages),
            pq_hits_at_10=sum(owners[target] in pq_set for target in targets[:10]),
            pq_hits_at_100=sum(owners[target] in pq_set for target in targets),
            exact_pages=exact_pages, exact_data_bytes=sum(page_byte_sizes[page] for page in exact_pages),
            exact_hits_at_10=sum(owners[target] in exact_set for target in targets[:10]),
            exact_hits_at_100=sum(owners[target] in exact_set for target in targets),
        ))
    if tuple(expected) != tuple(samples):
        raise ValueError("independent route differs")
    metrics = _metrics(expected)
    if metrics != reported_metrics:
        raise ValueError("independent aggregates differ")
    return metrics
