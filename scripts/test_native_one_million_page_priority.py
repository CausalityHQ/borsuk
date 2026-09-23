"""Exact physical-score page priority cases for the 1M range gate."""

import numpy as np
import pytest

from scripts.native_one_million_page_priority import rank_selected_pages


def test_top_rows_rank_owner_pages_then_other_pages_by_minimum_score() -> None:
    scores = np.array([2.0, 1.0, 1.0, 0.5, 0.1, 3.0], dtype=np.float32)
    row_pages = np.array([0, 0, 1, 1, 2, 3], dtype=np.uint32)
    row_groups = np.array([0, 0, 0, 0, 1, 0], dtype=np.uint32)
    assert rank_selected_pages(scores, row_pages, row_groups, (0,), 4, top_rows=3) == (1, 0, 3)


def test_tied_scores_use_physical_row_order_and_page_ordinal() -> None:
    scores = np.ones(5, dtype=np.float32)
    row_pages = np.array([2, 1, 2, 3, 1], dtype=np.uint32)
    row_groups = np.zeros(5, dtype=np.uint32)
    assert rank_selected_pages(scores, row_pages, row_groups, (0,), 4, top_rows=2) == (1, 2, 3)


@pytest.mark.parametrize(
    ("scores", "pages", "groups", "selected", "page_count"),
    [
        ([float("nan")], [0], [0], (0,), 1),
        ([0.0], [1], [0], (0,), 1),
        ([0.0], [0], [0], (1,), 1),
        ([0.0], [0], [0], (), 1),
    ],
)
def test_bad_score_or_membership_fails_closed(scores, pages, groups, selected, page_count) -> None:
    with pytest.raises(ValueError):
        rank_selected_pages(
            np.asarray(scores, dtype=np.float32),
            np.asarray(pages, dtype=np.uint32),
            np.asarray(groups, dtype=np.uint32),
            selected,
            page_count,
        )
