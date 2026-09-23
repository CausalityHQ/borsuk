"""Authenticate ReLAION source rows and map them to frozen physical order."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_hundred_thousand_opq8_router import DIMENSIONS
from scripts.native_one_million_group_selector import _source_schema
from scripts.v97_row_width_screen import ObjectIdentity, _authenticate_object


def iter_authenticated_source_batches(
    path: Path, identity: ObjectIdentity, physical: Mapping[int, int],
    row_pages: np.ndarray, page_groups: np.ndarray, *, batch_rows: int = 4096,
) -> Iterator[tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
    """Yield physical ordinal, page, group and float32 vector per source row."""
    if (
        not 1 <= batch_rows <= 4096 or row_pages.ndim != 1
        or page_groups.ndim != 1 or len(physical) != len(row_pages)
        or np.any(row_pages >= len(page_groups))
    ):
        raise ValueError("source-scan physical layout differs")
    _authenticate_object("source", path, identity)
    parquet = pq.ParquetFile(path)
    if parquet.schema_arrow != _source_schema():
        raise ValueError("source-scan Parquet schema differs")
    for batch in parquet.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        values = table.column("embedding").combine_chunks()
        rows = np.asarray(values.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if values.null_count or len(ids) != len(rows) or not np.isfinite(rows).all():
            raise ValueError("source-scan vector rows differ")
        ordinals = np.empty(len(ids), dtype=np.int64)
        for index, row_id in enumerate(ids):
            ordinal = physical.get(int(row_id))
            if ordinal is None:
                raise ValueError("source-scan physical ID differs")
            ordinals[index] = ordinal
        pages = row_pages[ordinals]
        yield ordinals, pages, page_groups[pages.astype(np.intp)], rows
