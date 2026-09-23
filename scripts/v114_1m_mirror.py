"""Seal an existing source-only SQ8 object for local RAM/file placement."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v114_exact_local_100k import _canonical, _sha256_file


def _valid_hash(value: str) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def seal_existing_sq8(
    source: Path, sq8: Path, mirror: Path, *,
    expected_source_sha256: str, expected_sq8_sha256: str,
    rows: int, dimensions: int, max_nominees: int,
) -> dict[str, object]:
    """Bind source-derived V70 scales and exact SQ8 blocks before queries."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if (
        not _valid_hash(expected_source_sha256)
        or not _valid_hash(expected_sq8_sha256)
        or rows <= 0 or dimensions <= 0
        or not 0 < max_nominees <= rows
        or _sha256_file(source) != expected_source_sha256
        or sq8.stat().st_size != rows * (dimensions + 12)
        or mirror.exists()
    ):
        raise ValueError("V114 source or SQ8 geometry/identity differs")
    source_rows = pq.read_table(source, columns=["feature_row_id", "embedding"])
    identifier_type = source_rows.schema.field("feature_row_id").type
    vector_type = source_rows.schema.field("embedding").type
    if (
        source_rows.num_rows != rows
        or not pa.types.is_integer(identifier_type)
        or not pa.types.is_fixed_size_list(vector_type)
        or vector_type.list_size != dimensions
        or vector_type.value_type != pa.float32()
    ):
        raise ValueError("V114 source schema differs")
    identifiers = source_rows["feature_row_id"].combine_chunks().to_numpy(
        zero_copy_only=False,
    )
    if np.unique(identifiers).size != rows:
        raise ValueError("V114 source IDs differ")
    values = source_rows["embedding"].combine_chunks().values.to_numpy(
        zero_copy_only=False,
    )
    vectors = np.asarray(values, dtype=np.float32).reshape(rows, dimensions)
    if not np.isfinite(vectors).all():
        raise ValueError("V114 source contains a nonfinite vector")
    low = vectors.min(axis=0).astype(np.float32)
    high = vectors.max(axis=0).astype(np.float32)
    span = np.maximum(high - low, np.float32(1e-12)).astype(np.float32)
    step = (span / np.float32(255.0)).astype(np.float32)
    with tempfile.TemporaryDirectory(prefix=mirror.name + ".tmp-", dir=mirror.parent) as tmp:
        temporary = Path(tmp)
        object_digest = hashlib.sha256()
        sidecar_digest = hashlib.sha256()
        with sq8.open("rb") as object_file, (temporary / "blocks.sha256").open("wb") as sidecar:
            while block := object_file.read(4096):
                object_digest.update(block)
                digest = hashlib.sha256(block).digest()
                sidecar.write(digest)
                sidecar_digest.update(digest)
        if object_digest.hexdigest() != expected_sq8_sha256:
            raise ValueError("V114 SQ8 object SHA-256 differs")
        manifest: dict[str, object] = {
            "format_version": 1,
            "generation": 1,
            "max_nominees": max_nominees,
            "geometry": {"rows": rows, "dimensions": dimensions},
            "object_sha256": expected_sq8_sha256,
            "block_digest_sha256": sidecar_digest.hexdigest(),
            "low": [float(value) for value in low],
            "step": [float(value) for value in step],
        }
        (temporary / "manifest.json").write_text(_canonical(manifest))
        (temporary / "source.json").write_text(_canonical({
            "schema": "borsuk-v114-existing-sq8-mirror-v1",
            "source_sha256": expected_source_sha256,
            "sq8_sha256": expected_sq8_sha256,
            "manifest_sha256": _sha256_file(temporary / "manifest.json"),
        }))
        temporary.rename(mirror)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--sq8", required=True, type=Path)
    parser.add_argument("--mirror", required=True, type=Path)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--sq8-sha256", required=True)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--dimensions", required=True, type=int)
    parser.add_argument("--max-nominees", required=True, type=int)
    args = parser.parse_args()
    seal_existing_sq8(
        args.source, args.sq8, args.mirror,
        expected_source_sha256=args.source_sha256,
        expected_sq8_sha256=args.sq8_sha256,
        rows=args.rows, dimensions=args.dimensions,
        max_nominees=args.max_nominees,
    )


if __name__ == "__main__":
    main()
