#!/usr/bin/env python3
"""Source-only page moments and fixed group-mass routing surrogate."""

from __future__ import annotations

import struct
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.special import ndtr

from scripts.native_one_million_group_selector import DIMENSIONS
from scripts.native_one_million_page_selector import (
    PageSelectorArtifact,
    _page_membership,
)
from scripts.v97_row_width_screen import ObjectIdentity

MAGIC = b"BRSMASS1"
HEADER = struct.Struct("<8sII")


@dataclass(frozen=True, slots=True)
class PageMoments:
    counts: np.ndarray
    means: np.ndarray
    variances: np.ndarray


def build_page_moments(
    root: Path, artifact: PageSelectorArtifact,
    identities: Mapping[str, ObjectIdentity], *, batch_rows: int = 4096,
) -> PageMoments:
    if not 1 <= batch_rows <= 4096:
        raise ValueError("page moment batch differs")
    groups, id_to_page, page_groups, page_order = _page_membership(root, identities)
    if (
        groups != artifact.groups
        or not np.array_equal(page_groups, artifact.page_groups)
        or page_order != artifact.seal["page_order_sha256"]
    ):
        raise ValueError("page moment source membership differs")
    source = pq.ParquetFile(root / "source.parquet")
    count = np.zeros(len(page_groups), dtype=np.uint32)
    sums = np.zeros((len(page_groups), DIMENSIONS), dtype=np.float64)
    seen: set[int] = set()
    for batch in source.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        column = table.column("embedding").combine_chunks()
        vectors = np.asarray(column.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if column.null_count or not np.isfinite(vectors).all():
            raise ValueError("page moment source vectors differ")
        pages = np.empty(len(ids), dtype=np.int64)
        for position, raw in enumerate(ids):
            row_id = int(raw)
            if row_id not in id_to_page or row_id in seen:
                raise ValueError("page moment source IDs differ")
            seen.add(row_id)
            pages[position] = id_to_page[row_id]
        np.add.at(count, pages, 1)
        np.add.at(sums, pages, vectors.astype(np.float64))
    if len(seen) != len(id_to_page) or np.any(count == 0):
        raise ValueError("page moment source count differs")
    means = sums / count[:, None]
    sum_squared = np.zeros_like(sums)
    second_rows = 0
    for batch in source.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        column = table.column("embedding").combine_chunks()
        vectors = np.asarray(column.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if column.null_count or not np.isfinite(vectors).all():
            raise ValueError("page moment second-pass vectors differ")
        pages = np.fromiter((id_to_page[int(i)] for i in ids), dtype=np.int64, count=len(ids))
        delta = vectors.astype(np.float64) - means[pages]
        np.add.at(sum_squared, pages, delta * delta)
        second_rows += len(ids)
    if second_rows != len(id_to_page):
        raise ValueError("page moment second-pass count differs")
    variances = sum_squared / count[:, None]
    if not np.isfinite(means).all() or not np.isfinite(variances).all() or np.any(variances < 0):
        raise ValueError("page moments differ")
    result = PageMoments(count, means, variances)
    write_page_moments(root / "moments.bin", result)
    return result


def write_page_moments(path: Path, moments: PageMoments) -> None:
    pages = len(moments.counts)
    if (
        moments.means.shape != (pages, DIMENSIONS)
        or moments.variances.shape != (pages, DIMENSIONS)
        or np.any(moments.counts <= 0)
        or not np.isfinite(moments.means).all()
        or not np.isfinite(moments.variances).all()
        or np.any(moments.variances < 0)
    ):
        raise ValueError("page moment write authority differs")
    with path.open("wb") as stream:
        stream.write(HEADER.pack(MAGIC, pages, DIMENSIONS))
        stream.write(np.asarray(moments.counts, dtype="<u4").tobytes())
        stream.write(np.asarray(moments.means, dtype="<f8").tobytes())
        stream.write(np.asarray(moments.variances, dtype="<f8").tobytes())


def read_page_moments(path: Path, artifact: PageSelectorArtifact) -> PageMoments:
    body = path.read_bytes()
    pages = len(artifact.page_groups)
    expected_bytes = HEADER.size + pages * 4 + pages * DIMENSIONS * 16
    if len(body) != expected_bytes:
        raise ValueError("page moment binary length differs")
    magic, encoded_pages, dimensions = HEADER.unpack_from(body)
    if magic != MAGIC or encoded_pages != pages or dimensions != DIMENSIONS:
        raise ValueError("page moment header differs")
    count = np.frombuffer(body, dtype="<u4", count=pages, offset=HEADER.size)
    mean_offset = HEADER.size + pages * 4
    means = np.frombuffer(body, dtype="<f8", count=pages * DIMENSIONS, offset=mean_offset).reshape(pages, DIMENSIONS)
    var_offset = mean_offset + pages * DIMENSIONS * 8
    variances = np.frombuffer(body, dtype="<f8", count=pages * DIMENSIONS, offset=var_offset).reshape(pages, DIMENSIONS)
    expected_groups = np.bincount(
        artifact.page_groups.astype(np.int64), weights=count.astype(np.float64),
        minlength=len(artifact.groups),
    )
    if (
        np.any(count <= 0)
        or not np.isfinite(means).all()
        or not np.isfinite(variances).all()
        or np.any(variances < 0)
        or not np.array_equal(expected_groups, [group.row_count for group in artifact.groups])
    ):
        raise ValueError("page moment arrays differ")
    return PageMoments(count, means, variances)


def rank_mass_groups(
    query: np.ndarray, artifact: PageSelectorArtifact, moments: PageMoments,
) -> tuple[int, ...]:
    if query.shape != (DIMENSIONS,) or not np.isfinite(query).all():
        raise ValueError("page mass query differs")
    delta = query.astype(np.float64) - moments.means
    squared = delta * delta
    means = np.sum(squared, axis=1, dtype=np.float64) + np.sum(moments.variances, axis=1, dtype=np.float64)
    variance = 4 * np.sum(moments.variances * squared, axis=1, dtype=np.float64) + 2 * np.sum(moments.variances * moments.variances, axis=1, dtype=np.float64)
    scales = np.sqrt(variance)
    lower = float(np.min(means) - 12 * np.max(scales) - 1)
    upper = float(np.max(means) + 12 * np.max(scales) + 1)
    nonzero = scales > 0

    def page_mass(threshold: float) -> np.ndarray:
        cdf = np.empty(len(means), dtype=np.float64)
        cdf[nonzero] = ndtr((threshold - means[nonzero]) / scales[nonzero])
        cdf[~nonzero] = (threshold >= means[~nonzero]).astype(np.float64)
        return moments.counts.astype(np.float64) * cdf

    if not float(np.sum(page_mass(upper), dtype=np.float64)) >= 100:
        raise ValueError("page mass threshold bracket differs")
    for _ in range(48):
        midpoint = (lower + upper) / 2
        if float(np.sum(page_mass(midpoint), dtype=np.float64)) >= 100:
            upper = midpoint
        else:
            lower = midpoint
    page_weights = page_mass(upper)
    group_weights = np.bincount(
        artifact.page_groups.astype(np.int64), weights=page_weights,
        minlength=len(artifact.groups),
    )
    if not np.isfinite(group_weights).all():
        raise ValueError("page mass group weights differ")
    return tuple(sorted(
        range(len(artifact.groups)),
        key=lambda i: (-float(group_weights[i]), artifact.groups[i].role, artifact.groups[i].ordinal),
    ))
