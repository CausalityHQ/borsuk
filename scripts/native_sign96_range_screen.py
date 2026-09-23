"""Truth-free paired source/sign96 page plans over identical selected rows."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence

import numpy as np

from scripts.native_one_million_data_range_query import plan_ranked_pages
from scripts.native_one_million_page_priority import rank_selected_pages
from scripts.native_one_million_source_priority import source_distance_batch
from scripts.native_rotated_sign96 import BATCH_ROWS, RECORD_BYTES, score_records


@dataclasses.dataclass(frozen=True, slots=True)
class ScoredRows:
    positions: np.ndarray
    row_pages: np.ndarray
    row_groups: np.ndarray
    sign_scores: np.ndarray
    source_scores: np.ndarray


def score_selected_rows(
    query: np.ndarray,
    mean: np.ndarray,
    records: np.ndarray,
    physical_vectors: np.ndarray,
    page_row_counts: Sequence[int],
    selected_groups: Sequence[int],
    *,
    rotation_seed: int,
) -> ScoredRows:
    """Bounded 100k screen: independently score identical physical candidate rows."""
    counts = tuple(page_row_counts)
    selected = tuple(selected_groups)
    if (
        type(physical_vectors) is not np.ndarray
        or physical_vectors.dtype != np.float32
        or physical_vectors.ndim != 2
        or physical_vectors.shape[1] != 768
        or not 0 < len(physical_vectors) <= 100_000
        or type(records) is not np.ndarray
        or records.dtype != np.uint8
        or records.shape != (len(physical_vectors), RECORD_BYTES)
        or not counts
        or any(type(count) is not int or count <= 0 for count in counts)
        or sum(counts) != len(physical_vectors)
        or not selected
        or len(set(selected)) != len(selected)
        or any(type(group) is not int or not 0 <= group < (len(counts) + 3) // 4
               for group in selected)
    ):
        raise ValueError("sign96 selected-row authority differs")
    all_pages = np.repeat(np.arange(len(counts), dtype=np.uint32), counts)
    all_groups = (all_pages // 4).astype(np.uint32)
    positions = np.flatnonzero(np.isin(all_groups, selected))
    if len(positions) == 0:
        raise ValueError("sign96 selected groups are empty")
    sign_scores = score_records(query, mean, records[positions], rotation_seed=rotation_seed)
    source_scores = np.empty(len(positions), dtype=np.float64)
    for first in range(0, len(positions), BATCH_ROWS):
        last = min(first + BATCH_ROWS, len(positions))
        source_scores[first:last] = source_distance_batch(
            query[None, :], physical_vectors[positions[first:last]]
        )[0]
    return ScoredRows(positions, all_pages[positions], all_groups[positions],
                      sign_scores, source_scores)


def paired_range_plans(
    sign_scores: np.ndarray,
    source_scores: np.ndarray,
    row_pages: np.ndarray,
    row_groups: np.ndarray,
    selected_groups: Sequence[int],
    page_lengths: Mapping[str, Sequence[int]],
    *,
    maximum_gets: int = 32,
    maximum_bytes: int = 16_777_216,
) -> dict[str, dict[str, object]]:
    """Keep group/row inputs fixed and independently derive both page plans."""
    if (
        type(sign_scores) is not np.ndarray
        or sign_scores.dtype != np.float32
        or type(source_scores) is not np.ndarray
        or source_scores.dtype != np.float64
        or sign_scores.shape != source_scores.shape
        or row_pages.shape != sign_scores.shape
        or row_groups.shape != sign_scores.shape
        or set(page_lengths) not in ({"base"}, {"base", "delta"})
    ):
        raise ValueError("sign96 paired score authority differs")
    page_count = sum(len(lengths) for lengths in page_lengths.values())
    return {
        arm: plan_ranked_pages(
            rank_selected_pages(scores, row_pages, row_groups, selected_groups, page_count),
            page_lengths,
            maximum_gets=maximum_gets,
            maximum_bytes=maximum_bytes,
        )
        for arm, scores in (("sign96", sign_scores), ("source", source_scores))
    }
