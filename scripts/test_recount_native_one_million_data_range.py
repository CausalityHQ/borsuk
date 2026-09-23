"""Independent range geometry and hit recount from sealed output."""

import pytest

from scripts.recount_native_one_million_data_range import recount_range_masks


def test_recount_includes_bridged_truth_page_and_rejects_bad_bytes() -> None:
    plan = {
        "priority_pages": [["base", 0], ["base", 2]],
        "target_pages": [["base", 0], ["base", 2]],
        "ranges": [["base", 0, 3]],
        "included_pages": [["base", 0], ["base", 1], ["base", 2]],
        "gets": 1,
        "encoded_bytes": 22,
    }
    samples = [{"query_ordinal": 0, "candidate": plan, "control": plan}]
    evidence, metrics = recount_range_masks(samples, [[1, 2, 3]], {"base": (10, 2, 10), "delta": (8,)})
    assert evidence[0]["candidate"]["hit_mask"] == "110"
    assert metrics["candidate"]["gt100_hits"] == 2
    broken = {**plan, "encoded_bytes": 21}
    with pytest.raises(ValueError, match="geometry"):
        recount_range_masks(
            [{"query_ordinal": 0, "candidate": broken, "control": plan}],
            [[1, 2, 3]], {"base": (10, 2, 10), "delta": (8,)},
        )
