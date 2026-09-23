"""Stream paired two-bit and exact-source priorities on one sealed page cover."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import numpy as np

from scripts.native_one_million_source_priority import (
    SourcePriorityAccumulator,
    source_distance_batch,
)
from scripts.native_progressive_score import coded_distance_batch


def paired_priorities_from_batches(
    queries: np.ndarray, mean: np.ndarray, records: np.ndarray,
    batches: Iterable[tuple[np.ndarray, np.ndarray, np.ndarray]],
    included_pages_by_query: Sequence[Sequence[int]],
    *, page_count: int, expected_rows: int, rotation_seed: int,
) -> list[dict[str, tuple[int, ...]]]:
    """Reduce scores to page priority without opening truth or full corpus arrays."""
    if (
        type(queries) is not np.ndarray or queries.dtype != np.float32
        or queries.ndim != 2 or queries.shape[1] != 768
        or not 1 <= len(queries) <= 1000 or not np.isfinite(queries).all()
        or type(mean) is not np.ndarray or mean.dtype != np.float32
        or mean.shape != (768,) or not np.isfinite(mean).all()
        or not isinstance(records, np.ndarray) or records.dtype != np.uint8
        or records.shape != (expected_rows, 200)
        or type(page_count) is not int or page_count <= 0
        or type(expected_rows) is not int or expected_rows <= 0
        or len(included_pages_by_query) != len(queries)
    ):
        raise ValueError("progressive paired source population differs")
    accumulators: list[dict[str, SourcePriorityAccumulator]] = []
    for pages in included_pages_by_query:
        selected = tuple(pages)
        if (
            not selected or len(set(selected)) != len(selected)
            or any(type(page) is not int or not 0 <= page < page_count for page in selected)
        ):
            raise ValueError("progressive paired page cover differs")
        accumulators.append({
            arm: SourcePriorityAccumulator(
                selected, page_count=page_count, group_count=page_count,
            )
            for arm in ("two_bit", "source")
        })
    seen = np.zeros(expected_rows, dtype=np.bool_)
    for ordinals, pages, vectors in batches:
        if (
            type(ordinals) is not np.ndarray or ordinals.dtype != np.int64
            or ordinals.ndim != 1 or not 1 <= len(ordinals) <= 4096
            or type(pages) is not np.ndarray or pages.dtype != np.int64
            or pages.shape != ordinals.shape
            or type(vectors) is not np.ndarray or vectors.dtype != np.float32
            or vectors.shape != (len(ordinals), 768)
            or not np.isfinite(vectors).all()
            or np.any(ordinals < 0) or np.any(ordinals >= expected_rows)
            or np.any(pages < 0) or np.any(pages >= page_count)
            or len(np.unique(ordinals)) != len(ordinals)
            or np.any(seen[ordinals])
        ):
            raise ValueError("progressive paired source batch differs")
        seen[ordinals] = True
        source_scores = source_distance_batch(queries, vectors)
        coded_scores = coded_distance_batch(
            queries, mean, records[ordinals], rotation_seed=rotation_seed,
        ).astype(np.float64)
        for query_index, case in enumerate(accumulators):
            case["source"].add(source_scores[query_index], ordinals, pages, pages)
            case["two_bit"].add(coded_scores[query_index], ordinals, pages, pages)
    if not np.all(seen):
        raise ValueError("progressive paired source stream incomplete")
    return [
        {arm: case[arm].finalize() for arm in ("two_bit", "source")}
        for case in accumulators
    ]
