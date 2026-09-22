#!/usr/bin/env python3
"""Query-independent page-local microcluster routing screen."""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Sequence

import numpy as np

from scripts.native_geometric_layout_screen import (
    EvaluationLimits,
    LayoutMethod,
    MembershipRow,
    _two_means_order,
)


@dataclasses.dataclass(frozen=True, slots=True)
class PageMicroclusters:
    page_ordinal: int
    encoded_page_bytes: int
    means: tuple[tuple[float, ...], ...]

    def __post_init__(self) -> None:
        if type(self.page_ordinal) is not int or self.page_ordinal < 0:
            raise ValueError("microcluster page ordinal differs")
        if type(self.encoded_page_bytes) is not int or self.encoded_page_bytes <= 0:
            raise ValueError("microcluster page byte size differs")
        if len(self.means) != 8 or not self.means[0]:
            raise ValueError("microcluster representative count differs")
        dimensions = len(self.means[0])
        if any(
            len(mean) != dimensions or any(not math.isfinite(value) for value in mean)
            for mean in self.means
        ):
            raise ValueError("microcluster geometry differs")


def construct_representatives(
    membership: Sequence[MembershipRow],
    stable_ids: Sequence[bytes],
    vectors: np.ndarray,
) -> tuple[PageMicroclusters, ...]:
    """Build eight deterministic balanced two-means child means per sealed page."""
    if (
        not membership
        or len(membership) != len(stable_ids)
        or type(vectors) is not np.ndarray
        or vectors.dtype != np.float32
        or vectors.ndim != 2
        or vectors.shape[0] != len(stable_ids)
        or not np.isfinite(vectors).all()
        or len(set(stable_ids)) != len(stable_ids)
    ):
        raise ValueError("microcluster source differs")
    page_rows: dict[int, list[MembershipRow]] = {}
    source_ordinals: set[int] = set()
    bindings: set[tuple[bytes, int, bytes, LayoutMethod]] = set()
    for row in membership:
        if (
            type(row) is not MembershipRow
            or type(row.source_ordinal) is not int
            or not 0 <= row.source_ordinal < len(stable_ids)
            or row.source_ordinal in source_ordinals
            or row.stable_id != stable_ids[row.source_ordinal]
            or row.method is not LayoutMethod.TWO_MEANS_480K
        ):
            raise ValueError("microcluster membership differs")
        source_ordinals.add(row.source_ordinal)
        page_rows.setdefault(row.page_ordinal, []).append(row)
        bindings.add((row.source_sha256, row.seed, row.construction_sha256, row.method))
    if (
        source_ordinals != set(range(len(stable_ids)))
        or len(bindings) != 1
        or list(sorted(page_rows)) != list(range(len(page_rows)))
    ):
        raise ValueError("microcluster membership authority differs")

    representatives: list[PageMicroclusters] = []
    for page_ordinal in range(len(page_rows)):
        rows = sorted(page_rows[page_ordinal], key=lambda row: row.in_page_ordinal)
        if (
            len(rows) < 8
            or [row.in_page_ordinal for row in rows] != list(range(len(rows)))
            or any(row.page_rows != len(rows) for row in rows)
            or len({row.encoded_page_bytes for row in rows}) != 1
        ):
            raise ValueError("microcluster physical page differs")
        leaves = [np.asarray([row.source_ordinal for row in rows], dtype=np.int64)]
        for _ in range(3):
            next_leaves: list[np.ndarray] = []
            for leaf in leaves:
                left, right = _two_means_order(
                    leaf, vectors, stable_ids, len(leaf) // 2
                )
                next_leaves.extend((left, right))
            leaves = next_leaves
        means = tuple(
            tuple(float(value) for value in vectors[leaf].mean(axis=0, dtype=np.float64).astype(np.float32))
            for leaf in leaves
        )
        representatives.append(
            PageMicroclusters(
                page_ordinal=page_ordinal,
                encoded_page_bytes=rows[0].encoded_page_bytes,
                means=means,
            )
        )
    return tuple(representatives)


def route_query(
    pages: Sequence[PageMicroclusters],
    query: np.ndarray,
    limits: EvaluationLimits,
) -> tuple[int, ...]:
    """Select every eligible page by its nearest local mean under fixed limits."""
    if not pages or [page.page_ordinal for page in pages] != list(range(len(pages))):
        raise ValueError("microcluster page roster differs")
    dimensions = len(pages[0].means[0])
    if (
        type(query) is not np.ndarray
        or query.dtype != np.float32
        or query.shape != (dimensions,)
        or not np.isfinite(query).all()
        or any(len(page.means[0]) != dimensions for page in pages)
    ):
        raise ValueError("microcluster query differs")
    query64 = query.astype(np.float64)
    scores: list[tuple[float, int]] = []
    for page in pages:
        means = np.asarray(page.means, dtype=np.float64)
        residuals = means - query64
        score = float(np.min(np.einsum("ij,ij->i", residuals, residuals)))
        if not math.isfinite(score):
            raise ValueError("microcluster page score differs")
        scores.append((score, page.page_ordinal))
    scores.sort()
    selected: list[int] = []
    encoded_bytes = 0
    for _, ordinal in scores:
        page_bytes = pages[ordinal].encoded_page_bytes
        if encoded_bytes + page_bytes > limits.maximum_bytes:
            continue
        selected.append(ordinal)
        encoded_bytes += page_bytes
        if len(selected) == limits.maximum_pages:
            break
    if not selected:
        raise ValueError("microcluster byte budget admits no page")
    return tuple(sorted(selected))
