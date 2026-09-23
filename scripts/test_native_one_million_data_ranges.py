"""Small exact cases for the query-only 1M data-range planner."""

from itertools import combinations

import pytest

from scripts.native_one_million_data_ranges import admit_ranked_pages, minimum_cover


def test_cheapest_gap_and_bridged_bytes() -> None:
    lengths = {"base": (10, 2, 10, 9, 10), "delta": (7,)}
    cover = minimum_cover(lengths, (("base", 0), ("base", 2), ("base", 4)), 2)
    assert cover.intervals == (("base", 0, 3), ("base", 4, 5))
    assert cover.bytes == 32
    assert cover.included_pages == (("base", 0), ("base", 1), ("base", 2), ("base", 4))


def test_equal_gap_cost_breaks_by_physical_ordinal() -> None:
    cover = minimum_cover(
        {"base": (10, 2, 10, 2, 10)},
        (("base", 0), ("base", 2), ("base", 4)),
        2,
    )
    assert cover.intervals == (("base", 0, 3), ("base", 4, 5))


def test_distinct_roles_never_merge() -> None:
    cover = minimum_cover(
        {"base": (5, 5), "delta": (6, 6)},
        (("base", 1), ("delta", 0)),
        1,
    )
    assert cover is None


def test_minimum_cover_matches_exhaustive_four_page_covers() -> None:
    lengths = (3, 9, 2, 7)
    intervals = tuple((start, end) for start in range(4) for end in range(start + 1, 5))
    for target_count in range(1, 5):
        for targets in combinations(range(4), target_count):
            for maximum_gets in range(1, 5):
                actual = minimum_cover(
                    {"base": lengths}, tuple(("base", page) for page in targets), maximum_gets
                )
                assert actual is not None
                expected = min(
                    sum(sum(lengths[start:end]) for start, end in choice)
                    for count in range(1, maximum_gets + 1)
                    for choice in combinations(intervals, count)
                    if all(any(start <= page < end for start, end in choice) for page in targets)
                )
                assert actual.bytes == expected


def test_admission_skips_expensive_page_and_counts_bridge() -> None:
    result = admit_ranked_pages(
        {"base": (10, 2, 10, 100, 10)},
        (("base", 0), ("base", 2), ("base", 4)),
        1,
        25,
    )
    assert result.targets == (("base", 0), ("base", 2))
    assert result.cover.intervals == (("base", 0, 3),)
    assert result.cover.bytes == 22


@pytest.mark.parametrize(
    ("lengths", "targets", "limit"),
    [
        ({"base": (0,)}, (("base", 0),), 1),
        ({"base": (1,)}, (("base", 1),), 1),
        ({"base": (1,)}, (("delta", 0),), 1),
        ({"base": (1,)}, (("base", 0), ("base", 0)), 1),
        ({"base": (1,)}, (("base", 0),), 0),
    ],
)
def test_invalid_cover_input_fails_closed(lengths, targets, limit) -> None:
    with pytest.raises(ValueError):
        minimum_cover(lengths, targets, limit)
