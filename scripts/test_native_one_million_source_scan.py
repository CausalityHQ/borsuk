"""Authenticated source traversal for the exact-score diagnostic."""

from __future__ import annotations

import hashlib

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.native_hundred_thousand_opq8_router import DIMENSIONS
from scripts.native_one_million_group_selector import _source_schema
from scripts.native_one_million_source_scan import iter_authenticated_source_batches
from scripts.v97_row_width_screen import ObjectIdentity


def test_source_scan_maps_parquet_ids_to_physical_pages(tmp_path) -> None:
    vectors = np.zeros((3, DIMENSIONS), dtype=np.float32)
    vectors[:, 0] = [2, 0, 1]
    ids = pa.array([22, 11, 33], type=pa.uint64())
    embeddings = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.ravel(), type=pa.float32()), DIMENSIONS
    )
    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_arrays([ids, embeddings], schema=_source_schema()), source)
    body = source.read_bytes()
    identity = ObjectIdentity("s3://test/source", hashlib.sha256(body).hexdigest(), len(body))
    row_pages = np.array([0, 1, 2], dtype=np.uint32)
    page_groups = np.array([0, 1, 1], dtype=np.uint32)
    batches = list(iter_authenticated_source_batches(
        source, identity, {11: 0, 22: 1, 33: 2}, row_pages, page_groups,
        batch_rows=2,
    ))
    assert [item[0].tolist() for item in batches] == [[1, 0], [2]]
    assert [item[1].tolist() for item in batches] == [[1, 0], [2]]
    assert [item[2].tolist() for item in batches] == [[1, 0], [1]]
    assert [item[3][:, 0].tolist() for item in batches] == [[2, 0], [1]]


def test_source_scan_rejects_unmapped_id(tmp_path) -> None:
    vectors = np.zeros((1, DIMENSIONS), dtype=np.float32)
    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_arrays([
        pa.array([7], type=pa.uint64()),
        pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), DIMENSIONS),
    ], schema=_source_schema()), source)
    body = source.read_bytes()
    identity = ObjectIdentity("s3://test/source", hashlib.sha256(body).hexdigest(), len(body))
    with pytest.raises(ValueError, match="physical ID"):
        list(iter_authenticated_source_batches(
            source, identity, {8: 0}, np.array([0], dtype=np.uint32),
            np.array([0], dtype=np.uint32), batch_rows=1,
        ))
