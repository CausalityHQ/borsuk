"""Streaming source-score page priority must match the full-score rule."""

from __future__ import annotations

import numpy as np
import pytest

from scripts.native_one_million_page_priority import rank_selected_pages
from scripts.native_one_million_source_priority import SourcePriorityAccumulator


@pytest.mark.parametrize("selected", [(0, 2), (1, 2)])
def test_streaming_priority_matches_full_score(selected: tuple[int, ...]) -> None:
    scores = np.array([5, 2, 1, 2, 1, 6, 1, 3, 2, 0, 4, 2], dtype=np.float32)
    pages = np.array([0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5], dtype=np.uint32)
    page_groups = np.array([0, 0, 1, 1, 2, 2], dtype=np.uint32)
    groups = page_groups[pages]
    physical = np.arange(len(scores), dtype=np.int64)
    source = SourcePriorityAccumulator(selected, page_count=6, group_count=3, top_rows=5)
    for batch in (np.array([9, 4, 2, 0, 7]), np.array([1, 11, 5, 8]), np.array([3, 6, 10])):
        source.add(scores[batch].astype(np.float64), physical[batch], pages[batch], groups[batch])
    assert source.finalize() == rank_selected_pages(
        scores, pages, groups, selected, 6, top_rows=5
    )


def test_streaming_priority_rejects_invalid_rows_and_nonfinite_scores() -> None:
    source = SourcePriorityAccumulator((0,), page_count=2, group_count=1, top_rows=2)
    with pytest.raises(ValueError, match="invalid"):
        source.add(np.array([1.0]), np.array([-1]), np.array([0]), np.array([0]))
    with pytest.raises(ValueError, match="nonfinite"):
        source.add(np.array([np.inf]), np.array([1]), np.array([1]), np.array([0]))


def test_source_distance_batch_uses_float64_source_coordinates() -> None:
    from scripts.native_one_million_source_priority import source_distance_batch

    queries = np.array([[1.5, -2.0], [0.0, 4.0]], dtype=np.float32)
    rows = np.array([[1.0, 3.0], [2.5, -2.0], [-1.0, 5.0]], dtype=np.float32)
    expected = np.array([
        [np.sum((query.astype(np.float64) - row.astype(np.float64)) ** 2)
         for row in rows]
        for query in queries
    ])
    assert np.allclose(source_distance_batch(queries, rows), expected, rtol=0, atol=1e-12)
    with pytest.raises(ValueError, match="nonfinite"):
        source_distance_batch(queries, np.array([[np.nan, 0]], dtype=np.float32))


def test_streaming_batches_match_full_score_for_two_arms() -> None:
    from scripts.native_one_million_source_priority import priorities_from_batches

    queries = np.array([[0, 0], [2, 1]], dtype=np.float32)
    rows = np.array([[0, 1], [2, 0], [3, 1], [0, 2], [4, 0], [2, 3]], dtype=np.float32)
    pages = np.array([0, 0, 1, 2, 3, 3], dtype=np.uint32)
    page_groups = np.array([0, 0, 1, 1], dtype=np.uint32)
    selected = [
        {"candidate": (0, 1), "control": (1,)},
        {"candidate": (1,), "control": (0, 1)},
    ]
    order = np.array([4, 1, 5, 0, 2, 3], dtype=np.int64)
    batches = (
        (order[:3], pages[order[:3]], page_groups[pages[order[:3]]], rows[order[:3]]),
        (order[3:], pages[order[3:]], page_groups[pages[order[3:]]], rows[order[3:]]),
    )
    actual = priorities_from_batches(
        queries, batches, selected, page_count=4, group_count=2,
        expected_rows=6, top_rows=3,
    )
    for qindex, query in enumerate(queries):
        scores = np.sum((rows.astype(np.float64) - query.astype(np.float64)) ** 2, axis=1)
        groups = page_groups[pages]
        for arm in ("candidate", "control"):
            expected = rank_selected_pages(
                scores.astype(np.float32), pages, groups,
                selected[qindex][arm], 4, top_rows=3,
            )
            assert actual[qindex][arm] == expected


def test_streaming_batches_reject_missing_row() -> None:
    from scripts.native_one_million_source_priority import priorities_from_batches

    queries = np.array([[0, 0]], dtype=np.float32)
    batch = (
        np.array([0], dtype=np.int64), np.array([0], dtype=np.uint32),
        np.array([0], dtype=np.uint32), np.array([[1, 1]], dtype=np.float32),
    )
    with pytest.raises(ValueError, match="population"):
        priorities_from_batches(
            queries, [batch], [{"candidate": (0,), "control": (0,)}],
            page_count=1, group_count=1, expected_rows=2,
        )
