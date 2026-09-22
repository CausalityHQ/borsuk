#!/usr/bin/env python3
"""Independent route and containment replay for page microcluster evidence."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    MembershipRow,
)
from scripts.native_page_microcluster_router import PageMicroclusters


def _evidence_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("query_ordinal", pa.uint32(), nullable=False),
            pa.field(
                "selected_page_ordinals",
                pa.list_(pa.field("element", pa.uint32(), nullable=False)),
                nullable=False,
            ),
            pa.field("encoded_bytes", pa.uint32(), nullable=False),
            pa.field("hits_at_10", pa.uint8(), nullable=False),
            pa.field("hits_at_100", pa.uint8(), nullable=False),
            pa.field("routing_nanoseconds", pa.uint64(), nullable=False),
        ]
    )


def validate_evidence(
    path: Path,
    identity: ArtifactIdentity,
    pages: Sequence[PageMicroclusters],
    membership: Sequence[MembershipRow],
    queries: np.ndarray,
    truth: Sequence[Sequence[bytes]],
    limits: EvaluationLimits,
) -> dict[str, int | str]:
    """Recompute every page choice and GT hit independently of the producer route."""
    payload = path.read_bytes()
    if (
        identity.role != "page-microcluster-evidence"
        or identity.encoded_bytes != len(payload)
        or identity.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError("microcluster evidence identity differs")
    if pq.read_schema(path) != _evidence_schema():
        raise ValueError("microcluster evidence physical schema differs")
    if (
        not pages
        or [page.page_ordinal for page in pages] != list(range(len(pages)))
        or type(queries) is not np.ndarray
        or queries.dtype != np.float32
        or queries.ndim != 2
        or queries.shape != (len(truth), len(pages[0].means[0]))
        or not np.isfinite(queries).all()
        or not truth
    ):
        raise ValueError("microcluster replay inputs differ")
    owner_by_id: dict[bytes, int] = {}
    page_bytes: dict[int, int] = {}
    for row in membership:
        if row.stable_id in owner_by_id or not 0 <= row.page_ordinal < len(pages):
            raise ValueError("microcluster replay membership differs")
        owner_by_id[row.stable_id] = row.page_ordinal
        previous = page_bytes.setdefault(row.page_ordinal, row.encoded_page_bytes)
        if previous != row.encoded_page_bytes:
            raise ValueError("microcluster replay page bytes differ")
    if page_bytes != {page.page_ordinal: page.encoded_page_bytes for page in pages}:
        raise ValueError("microcluster replay page roster differs")
    table = pq.read_table(path)
    if table.num_rows != len(queries):
        raise ValueError("microcluster evidence query count differs")
    evidence = table.to_pylist()
    centers = np.asarray([page.means for page in pages], dtype=np.float64)
    hits10: list[int] = []
    hits100: list[int] = []
    max_pages = 0
    max_bytes = 0
    for ordinal, (query, expected, row) in enumerate(
        zip(queries, truth, evidence, strict=True)
    ):
        if (
            row["query_ordinal"] != ordinal
            or len(expected) != 100
            or len(set(expected)) != 100
            or any(stable_id not in owner_by_id for stable_id in expected)
        ):
            raise ValueError("microcluster replay truth or query ordinal differs")
        deltas = centers - query.astype(np.float64)
        scores = np.square(deltas).sum(axis=2).min(axis=1)
        order = sorted(range(len(pages)), key=lambda page: (float(scores[page]), page))
        selected: list[int] = []
        encoded = 0
        for page in order:
            size = pages[page].encoded_page_bytes
            if encoded + size > limits.maximum_bytes:
                continue
            selected.append(page)
            encoded += size
            if len(selected) == limits.maximum_pages:
                break
        selected.sort()
        if (
            not selected
            or row["selected_page_ordinals"] != selected
            or row["encoded_bytes"] != encoded
        ):
            raise ValueError("microcluster evidence route differs")
        selected_set = set(selected)
        count10 = sum(
            owner_by_id[stable_id] in selected_set for stable_id in expected[:10]
        )
        count100 = sum(owner_by_id[stable_id] in selected_set for stable_id in expected)
        if row["hits_at_10"] != count10 or row["hits_at_100"] != count100:
            raise ValueError("microcluster evidence containment differs")
        hits10.append(count10)
        hits100.append(count100)
        max_pages = max(max_pages, len(selected))
        max_bytes = max(max_bytes, encoded)
    mean10 = sum(hits10) * 1_000_000 // (10 * len(hits10))
    mean100 = sum(hits100) * 1_000_000 // (100 * len(hits100))
    ordered_hits = sorted(hits100)
    p05 = ordered_hits[math.ceil(0.05 * len(ordered_hits)) - 1] * 10_000
    return {
        "recall_at_10_ppm": mean10,
        "mean_recall_at_100_ppm": mean100,
        "p05_recall_at_100_ppm": p05,
        "worst_recall_at_100_ppm": ordered_hits[0] * 10_000,
        "max_pages": max_pages,
        "max_bytes": max_bytes,
        "decision": "advance"
        if mean10 >= 960_000 and mean100 >= 975_000 and p05 >= 900_000
        else "killed",
    }
