"""Source-only page-local 200-byte code-wave geometry for the fixed 1M cohort."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from scripts.native_one_million_data_range_query import plan_ranked_pages

ROW_BYTES = 200
MAXIMUM_GETS = 32
MAXIMUM_BYTES = 16_777_216


def code_page_lengths(
    page_row_counts: Sequence[int], *, base_pages: int,
) -> dict[str, tuple[int, ...]]:
    """Project exact payload bytes of an incompatible page-local code format."""
    counts = tuple(page_row_counts)
    if (
        not counts or type(base_pages) is not int
        or not 0 < base_pages <= len(counts)
        or any(type(count) is not int or count <= 0 for count in counts)
    ):
        raise ValueError("code page rows differ")
    lengths = tuple(ROW_BYTES * count for count in counts)
    return {
        "base": lengths[:base_pages],
        **({"delta": lengths[base_pages:]} if base_pages < len(counts) else {}),
    }


def plan_code_wave(
    ranked_pages: Sequence[int], page_lengths: Mapping[str, Sequence[int]],
    *, maximum_gets: int = MAXIMUM_GETS,
    maximum_bytes: int = MAXIMUM_BYTES,
) -> dict[str, object]:
    """Admit OPQ8-ranked code pages with the frozen merged-range rule."""
    return plan_ranked_pages(
        ranked_pages, page_lengths,
        maximum_gets=maximum_gets, maximum_bytes=maximum_bytes,
    )
