"""Synthetic truth boundary and sealed plan for source-score 1M gate."""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pytest

from scripts.native_one_million_source_range_cell import run_plan


def test_source_range_plan_seals_both_arms_before_truth(tmp_path) -> None:
    (tmp_path / "range-seal.json").write_text('{"page_order_sha256":"physical"}\n')
    queries = np.zeros((1, 2), dtype=np.float32)
    rows = np.array([[0, 1], [3, 0]], dtype=np.float32)
    row_pages = np.array([0, 1], dtype=np.uint32)
    page_groups = np.array([0, 1], dtype=np.uint32)
    prior = {"samples": [{
        "query_ordinal": 0,
        "candidate": {"groups": (0,)},
        "control": {"groups": (1,)},
    }]}
    source = ((None, None), prior, {"base": (10,), "delta": (12,)}, row_pages, page_groups, None)
    batches = [(
        np.array([1, 0], dtype=np.int64), row_pages[[1, 0]],
        page_groups[[1, 0]], rows[[1, 0]],
    )]
    with (
        patch("scripts.native_one_million_source_range_cell._read_source", return_value=source),
        patch("scripts.native_one_million_source_range_cell._read_queries", return_value=queries),
        patch("scripts.native_one_million_source_range_cell._projected", return_value=()),
        patch("scripts.native_one_million_source_range_cell._checked_group_plan", side_effect=lambda _, old: old["groups"]),
        patch("scripts.native_one_million_source_range_cell._physical_ordinals", return_value=({1: 0, 2: 1}, "physical")),
        patch("scripts.native_one_million_source_range_cell.iter_authenticated_source_batches", return_value=iter(batches)),
    ):
        plans = run_plan(tmp_path, tmp_path, query_count=1, expected_rows=2)
    assert plans["samples"][0]["candidate"]["priority_pages"] == [["base", 0]]
    assert plans["samples"][0]["control"]["priority_pages"] == [["delta", 0]]
    assert (tmp_path / "source-range-plan-seal.json").is_file()
    (tmp_path / "truth.parquet").write_bytes(b"forbidden")
    with pytest.raises(ValueError, match="query-only"):
        run_plan(tmp_path, tmp_path, query_count=1, expected_rows=2)
