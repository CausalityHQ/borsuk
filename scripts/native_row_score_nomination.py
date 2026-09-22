#!/usr/bin/env python3
"""Deterministic, bounded code reads and row-score page nomination."""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Sequence

from scripts.native_geometric_layout_screen import EvaluationLimits

CODE_ROW_BYTES = 48


@dataclasses.dataclass(frozen=True, slots=True)
class CodePlan:
    """Complete code blocks to fetch: (first page, end page, offset, bytes)."""

    blocks: tuple[tuple[int, int, int, int], ...]
    gets: int
    bytes: int


def plan_code_blocks(
    retained_pages: Sequence[int],
    page_row_counts: Sequence[int],
    *,
    block_pages: int = 16,
    maximum_gets: int = 32,
    maximum_bytes: int = 16_777_216,
) -> CodePlan:
    """Fetch every block intersecting the retained page set, without pruning."""
    page_count = len(page_row_counts)
    if (
        page_count == 0
        or not retained_pages
        or type(block_pages) is not int
        or block_pages <= 0
        or type(maximum_gets) is not int
        or maximum_gets <= 0
        or type(maximum_bytes) is not int
        or maximum_bytes <= 0
        or any(type(count) is not int or count <= 0 for count in page_row_counts)
        or any(type(page) is not int or not 0 <= page < page_count for page in retained_pages)
        or len(set(retained_pages)) != len(retained_pages)
    ):
        raise ValueError("code wave authority differs")
    offsets = [0]
    for count in page_row_counts:
        offsets.append(offsets[-1] + count * CODE_ROW_BYTES)
    blocks = tuple(
        (
            first,
            min(first + block_pages, page_count),
            offsets[first],
            offsets[min(first + block_pages, page_count)] - offsets[first],
        )
        for first in sorted({(page // block_pages) * block_pages for page in retained_pages})
    )
    total_bytes = sum(block[3] for block in blocks)
    if len(blocks) > maximum_gets or total_bytes > maximum_bytes:
        raise ValueError("code wave budget exceeded")
    return CodePlan(blocks=blocks, gets=len(blocks), bytes=total_bytes)


def nominate_pages(
    row_scores: Sequence[float],
    row_source_ordinals: Sequence[int],
    row_page_ordinals: Sequence[int],
    page_byte_sizes: Sequence[int],
    limits: EvaluationLimits,
    *,
    top_rows: int = 100,
) -> tuple[int, ...]:
    """Nominate pages by top-row counts, then fill by nearest remaining row."""
    row_count = len(row_scores)
    page_count = len(page_byte_sizes)
    if (
        row_count == 0
        or len(row_source_ordinals) != row_count
        or len(row_page_ordinals) != row_count
        or page_count == 0
        or type(limits) is not EvaluationLimits
        or type(top_rows) is not int
        or top_rows <= 0
        or any(not math.isfinite(float(score)) for score in row_scores)
        or any(type(ordinal) is not int or ordinal < 0 for ordinal in row_source_ordinals)
        or len(set(row_source_ordinals)) != row_count
        or any(type(page) is not int or not 0 <= page < page_count for page in row_page_ordinals)
        or any(type(size) is not int or size <= 0 for size in page_byte_sizes)
    ):
        raise ValueError("row-score nomination authority differs")

    ordered = sorted(range(row_count), key=lambda i: (float(row_scores[i]), row_source_ordinals[i]))
    top = ordered[: min(top_rows, row_count)]
    counts = [0] * page_count
    minimum = [math.inf] * page_count
    for index in ordered:
        page = row_page_ordinals[index]
        minimum[page] = min(minimum[page], float(row_scores[index]))
    for index in top:
        counts[row_page_ordinals[index]] += 1
    nominated = sorted(
        (page for page in range(page_count) if counts[page]),
        key=lambda page: (-counts[page], minimum[page], page),
    )
    remaining = sorted(
        (page for page in range(page_count) if not counts[page] and minimum[page] < math.inf),
        key=lambda page: (minimum[page], page),
    )
    selected: list[int] = []
    encoded_bytes = 0
    for page in (*nominated, *remaining):
        size = page_byte_sizes[page]
        if encoded_bytes + size > limits.maximum_bytes:
            continue
        selected.append(page)
        encoded_bytes += size
        if len(selected) == limits.maximum_pages:
            break
    if not selected:
        raise ValueError("data wave budget admits no page")
    return tuple(selected)
