"""Materialize an authenticated V122 100k Parquet source as ID+F32 rows."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

import numpy as np

ROWS = 100_000
DIMENSIONS = 96


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def materialize(source: Path, output: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    expected_sha = os.environ["V131_SOURCE_SHA256"]
    if len(expected_sha) != 64 or sha256(source) != expected_sha or output.exists():
        raise ValueError("source hash or output path differs")
    table = pq.read_table(source, columns=["feature_row_id", "embedding"])
    field = table.schema.field("embedding")
    if (table.num_rows != ROWS
            or not pa.types.is_fixed_size_list(field.type)
            or field.type.list_size != DIMENSIONS
            or field.type.value_type != pa.float32()):
        raise ValueError("source geometry differs")
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(ids, np.arange(ROWS, dtype=ids.dtype)):
        raise ValueError("source ID order differs")
    coordinates = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        dtype="<f4",
    ).reshape(ROWS, DIMENSIONS)
    if (not np.isfinite(coordinates).all()
            or (np.linalg.norm(coordinates, axis=1) <= 0).any()):
        raise ValueError("source vectors differ")
    raw = np.empty(ROWS, dtype=[("id", "<u8"), ("vector", "<f4", DIMENSIONS)])
    raw["id"] = ids
    raw["vector"] = coordinates
    raw.tofile(output)
    if output.stat().st_size != ROWS * (8 + 4 * DIMENSIONS):
        raise ValueError("raw source byte length differs")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    materialize(arguments.source, arguments.output)
    print(f"{sha256(arguments.output)}  {arguments.output}")


if __name__ == "__main__":
    main()
