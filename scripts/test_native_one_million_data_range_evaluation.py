"""Truth opens only after a fixed physical data-range plan exists."""

from scripts.native_one_million_data_range_evaluation import evaluate_query_pages


def test_bridged_page_counts_as_truth_hit() -> None:
    plan = {
        "priority_pages": [["base", 0], ["base", 2]],
        "target_pages": [["base", 0], ["base", 2]],
        "ranges": [["base", 0, 3]],
        "included_pages": [["base", 0], ["base", 1], ["base", 2]],
        "gets": 1,
        "encoded_bytes": 22,
    }
    result = evaluate_query_pages(plan, (1, 2, 3), {"base": (10, 2, 10), "delta": (8,)}, maximum_gets=1, maximum_bytes=22)
    assert result == {
        "hit_mask": "110", "hits_at_10": 2, "hits_at_100": 2,
        "priority_owner_ranks": [None, 2, None],
        "hit_kinds": ["bridge", "target", "miss"],
    }
