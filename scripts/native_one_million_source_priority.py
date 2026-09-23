"""Bounded streaming reduction of source-row scores to page priority."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

import numpy as np


def source_distance_batch(queries: np.ndarray, rows: np.ndarray) -> np.ndarray:
    """Squared L2 on stored float32 coordinates with float64 dot products."""
    if (
        queries.ndim != 2 or rows.ndim != 2
        or queries.shape[1] != rows.shape[1]
        or queries.dtype != np.float32 or rows.dtype != np.float32
        or not np.isfinite(queries).all() or not np.isfinite(rows).all()
    ):
        raise ValueError("source-distance nonfinite or malformed coordinates")
    q = queries.astype(np.float64)
    x = rows.astype(np.float64)
    squared = np.sum(q * q, axis=1, dtype=np.float64)[:, None]
    squared = squared + np.sum(x * x, axis=1, dtype=np.float64)[None, :]
    squared -= 2 * (q @ x.T)
    np.maximum(squared, 0, out=squared)
    if not np.isfinite(squared).all():
        raise ValueError("source-distance nonfinite scores")
    return squared


class SourcePriorityAccumulator:
    """Keep exact page minima and stable top rows for one query and arm."""

    def __init__(
        self, selected_groups: Sequence[int], *, page_count: int, group_count: int,
        top_rows: int = 100,
    ) -> None:
        selected = tuple(selected_groups)
        if (
            type(page_count) is not int or page_count <= 0
            or type(group_count) is not int or group_count <= 0
            or type(top_rows) is not int or top_rows <= 0
            or not selected or len(set(selected)) != len(selected)
            or any(type(group) is not int or not 0 <= group < group_count for group in selected)
        ):
            raise ValueError("source-priority layout differs")
        self.page_count = page_count
        self.top_rows = top_rows
        self.selected_mask = np.zeros(group_count, dtype=np.bool_)
        self.selected_mask[list(selected)] = True
        self.minima = np.full(page_count, np.inf, dtype=np.float64)
        self.top_scores = np.empty(0, dtype=np.float64)
        self.top_ordinals = np.empty(0, dtype=np.int64)
        self.top_pages = np.empty(0, dtype=np.intp)

    def add(
        self, scores: np.ndarray, ordinals: np.ndarray,
        row_pages: np.ndarray, row_groups: np.ndarray,
    ) -> None:
        if (
            scores.ndim != 1 or scores.dtype != np.float64
            or ordinals.shape != scores.shape or row_pages.shape != scores.shape
            or row_groups.shape != scores.shape
            or not np.isfinite(scores).all()
        ):
            raise ValueError("source-priority nonfinite or malformed scores")
        if (
            ordinals.dtype.kind not in "iu" or row_pages.dtype.kind not in "iu"
            or row_groups.dtype.kind not in "iu"
            or np.any(ordinals < 0) or np.any(row_pages < 0) or np.any(row_groups < 0)
            or np.any(row_pages >= self.page_count)
            or np.any(row_groups >= len(self.selected_mask))
        ):
            raise ValueError("source-priority invalid row")
        chosen = self.selected_mask[row_groups.astype(np.intp, copy=False)]
        if not np.any(chosen):
            return
        selected_scores = scores[chosen]
        selected_pages = row_pages[chosen].astype(np.intp, copy=False)
        selected_ordinals = ordinals[chosen]
        np.minimum.at(self.minima, selected_pages, selected_scores)
        combined_scores = np.concatenate((self.top_scores, selected_scores))
        combined_ordinals = np.concatenate((self.top_ordinals, selected_ordinals))
        combined_pages = np.concatenate((self.top_pages, selected_pages))
        take = min(self.top_rows, len(combined_scores))
        threshold = np.partition(combined_scores, take - 1)[take - 1]
        contenders = np.flatnonzero(combined_scores <= threshold)
        best = contenders[np.lexsort((combined_ordinals[contenders], combined_scores[contenders]))[:take]]
        self.top_scores = combined_scores[best]
        self.top_ordinals = combined_ordinals[best]
        self.top_pages = combined_pages[best]

    def finalize(self) -> tuple[int, ...]:
        pages = np.flatnonzero(np.isfinite(self.minima))
        if len(pages) == 0 or len(self.top_pages) == 0:
            raise ValueError("source-priority selected groups are empty")
        counts = np.bincount(self.top_pages, minlength=self.page_count)
        return tuple(sorted(
            (int(page) for page in pages),
            key=lambda page: (
                -int(counts[page] > 0), -int(counts[page]),
                float(self.minima[page]), page,
            ),
        ))


def priorities_from_batches(
    queries: np.ndarray,
    batches: Iterable[tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]],
    selected_by_query: Sequence[Mapping[str, Sequence[int]]],
    *, page_count: int, group_count: int, expected_rows: int, top_rows: int = 100,
) -> list[dict[str, tuple[int, ...]]]:
    """Score each authenticated source row once; retain bounded reductions."""
    if (
        queries.ndim != 2 or queries.dtype != np.float32
        or not np.isfinite(queries).all()
        or len(selected_by_query) != len(queries)
        or type(expected_rows) is not int or expected_rows <= 0
    ):
        raise ValueError("source-priority query population differs")
    accumulators: list[dict[str, SourcePriorityAccumulator]] = []
    for selected in selected_by_query:
        if set(selected) != {"candidate", "control"}:
            raise ValueError("source-priority arm population differs")
        accumulators.append({
            arm: SourcePriorityAccumulator(
                selected[arm], page_count=page_count,
                group_count=group_count, top_rows=top_rows,
            )
            for arm in ("candidate", "control")
        })
    seen = np.zeros(expected_rows, dtype=np.bool_)
    for ordinals, row_pages, row_groups, rows in batches:
        if (
            ordinals.ndim != 1 or ordinals.dtype.kind not in "iu"
            or row_pages.shape != ordinals.shape
            or row_groups.shape != ordinals.shape
            or rows.ndim != 2 or len(rows) != len(ordinals)
            or rows.shape[1] != queries.shape[1]
            or rows.dtype != np.float32
            or np.any(ordinals < 0) or np.any(ordinals >= expected_rows)
            or len(np.unique(ordinals)) != len(ordinals)
            or np.any(seen[ordinals])
        ):
            raise ValueError("source-priority duplicate or invalid source row")
        seen[ordinals] = True
        score_matrix = source_distance_batch(queries, rows)
        for qindex, score_row in enumerate(score_matrix):
            for arm in ("candidate", "control"):
                accumulators[qindex][arm].add(score_row, ordinals, row_pages, row_groups)
    if not np.all(seen):
        raise ValueError("source-priority source population incomplete")
    return [
        {arm: case[arm].finalize() for arm in ("candidate", "control")}
        for case in accumulators
    ]
