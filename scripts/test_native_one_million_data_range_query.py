"""Score-to-physical-range integration without development truth."""

import numpy as np

from scripts.native_one_million_data_range_query import plan_query


def test_query_plan_bridges_cheap_gap_and_keeps_role_coordinates() -> None:
    scores = np.array([0.1, 9.0, 0.2, 5.0], dtype=np.float32)
    row_pages = np.array([0, 1, 2, 3], dtype=np.uint32)
    page_groups = np.array([0, 0, 0, 1], dtype=np.uint32)
    result = plan_query(
        scores,
        row_pages,
        page_groups,
        {"base": (10, 2, 10), "delta": (8,)},
        (0,),
        maximum_gets=1,
        maximum_bytes=22,
        top_rows=2,
    )
    assert result["priority_pages"] == [["base", 0], ["base", 2], ["base", 1]]
    assert result["target_pages"] == [["base", 0], ["base", 2], ["base", 1]]
    assert result["ranges"] == [["base", 0, 3]]
    assert result["included_pages"] == [["base", 0], ["base", 1], ["base", 2]]
    assert result["gets"] == 1
    assert result["encoded_bytes"] == 22


def test_query_plan_cannot_bridge_roles() -> None:
    result = plan_query(
        np.array([0.1, 0.2], dtype=np.float32),
        np.array([0, 1], dtype=np.uint32),
        np.array([0, 1], dtype=np.uint32),
        {"base": (3,), "delta": (4,)},
        (0, 1),
        maximum_gets=1,
        maximum_bytes=7,
    )
    assert result["ranges"] == [["base", 0, 1]]
    assert result["target_pages"] == [["base", 0]]
