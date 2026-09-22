#!/usr/bin/env python3
"""Query-independent page-local microcluster routing screen."""

from __future__ import annotations

import dataclasses
import hashlib
import math
import struct
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
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
            tuple(
                float(value)
                for value in vectors[leaf]
                .mean(axis=0, dtype=np.float64)
                .astype(np.float32)
            )
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


def representatives_schema(dimensions: int) -> pa.Schema:
    if type(dimensions) is not int or not 0 < dimensions <= (1 << 16) - 1:
        raise ValueError("microcluster dimensions differ")
    return pa.schema(
        [
            pa.field("page_ordinal", pa.uint32(), nullable=False),
            pa.field("encoded_page_bytes", pa.uint32(), nullable=False),
            pa.field(
                "means",
                pa.list_(
                    pa.field("element", pa.float32(), nullable=False), 8 * dimensions
                ),
                nullable=False,
            ),
            pa.field("source_sha256", pa.binary(32), nullable=False),
            pa.field("membership_sha256", pa.binary(32), nullable=False),
            pa.field("construction_sha256", pa.binary(32), nullable=False),
        ]
    )


def _representatives_digest(
    pages: Sequence[PageMicroclusters], source_sha: bytes, membership_sha: bytes
) -> bytes:
    if (
        not pages
        or [page.page_ordinal for page in pages] != list(range(len(pages)))
        or len(source_sha) != 32
        or len(membership_sha) != 32
        or source_sha == bytes(32)
        or membership_sha == bytes(32)
    ):
        raise ValueError("microcluster representative authority differs")
    dimensions = len(pages[0].means[0])
    if any(len(page.means[0]) != dimensions for page in pages):
        raise ValueError("microcluster representative dimensions differ")
    digest = hashlib.sha256(b"borsuk-page-microclusters-v1\0")
    digest.update(source_sha)
    digest.update(membership_sha)
    digest.update(struct.pack("<II", len(pages), dimensions))
    for page in pages:
        digest.update(struct.pack("<II", page.page_ordinal, page.encoded_page_bytes))
        digest.update(np.asarray(page.means, dtype="<f4").tobytes(order="C"))
    return digest.digest()


def write_representatives(
    path: Path,
    pages: Sequence[PageMicroclusters],
    source_sha: bytes,
    membership_sha: bytes,
) -> ArtifactIdentity:
    """Seal the query-blind representatives in a canonical physical schema."""
    construction_sha = _representatives_digest(pages, source_sha, membership_sha)
    dimensions = len(pages[0].means[0])
    schema = representatives_schema(dimensions)
    flat = np.asarray([page.means for page in pages], dtype=np.float32).reshape(-1)
    table = pa.Table.from_arrays(
        [
            pa.array([page.page_ordinal for page in pages], type=pa.uint32()),
            pa.array([page.encoded_page_bytes for page in pages], type=pa.uint32()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(flat, type=pa.float32()), 8 * dimensions
            ),
            pa.array([source_sha] * len(pages), type=pa.binary(32)),
            pa.array([membership_sha] * len(pages), type=pa.binary(32)),
            pa.array([construction_sha] * len(pages), type=pa.binary(32)),
        ],
        schema=schema,
    )
    pq.write_table(
        table,
        path,
        version="2.6",
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
    )
    payload = path.read_bytes()
    return ArtifactIdentity(
        role="page-microcluster-representatives",
        uri=path.resolve().as_uri(),
        sha256=hashlib.sha256(payload).hexdigest(),
        encoded_bytes=len(payload),
    )


def read_representatives(
    path: Path,
    identity: ArtifactIdentity,
    source_sha: bytes,
    membership_sha: bytes,
) -> tuple[PageMicroclusters, ...]:
    """Authenticate and parse a sealed representative artifact."""
    payload = path.read_bytes()
    if (
        identity.role != "page-microcluster-representatives"
        or identity.encoded_bytes != len(payload)
        or identity.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError("microcluster representative identity differs")
    schema = pq.read_schema(path)
    means_type = schema.field("means").type if "means" in schema.names else None
    if (
        not isinstance(means_type, pa.FixedSizeListType)
        or means_type.list_size % 8 != 0
        or schema != representatives_schema(means_type.list_size // 8)
    ):
        raise ValueError("microcluster representative physical schema differs")
    table = pq.read_table(path)
    columns = {
        name: table[name].combine_chunks().to_pylist() for name in table.column_names
    }
    if table.num_rows == 0 or any(
        value != source_sha for value in columns["source_sha256"]
    ):
        raise ValueError("microcluster source binding differs")
    if any(value != membership_sha for value in columns["membership_sha256"]):
        raise ValueError("microcluster membership binding differs")
    dimensions = means_type.list_size // 8
    pages = tuple(
        PageMicroclusters(
            page_ordinal=columns["page_ordinal"][index],
            encoded_page_bytes=columns["encoded_page_bytes"][index],
            means=tuple(
                tuple(
                    columns["means"][index][part * dimensions : (part + 1) * dimensions]
                )
                for part in range(8)
            ),
        )
        for index in range(table.num_rows)
    )
    construction_sha = _representatives_digest(pages, source_sha, membership_sha)
    if any(value != construction_sha for value in columns["construction_sha256"]):
        raise ValueError("microcluster construction binding differs")
    return pages


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
