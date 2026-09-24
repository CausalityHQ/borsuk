"""Construct a corpus-only physical layout and SQ8 body from sealed Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import tempfile
from pathlib import Path

import numpy as np

from scripts.v82_scale_build import centroid_chain, lloyd

CHUNK_ROWS = 16_384
SCHEMA = "borsuk-source-layout-sq8-v1"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(4 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _valid_sha(value: str) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def cluster_count(rows: int) -> int:
    """Smooth corpus-size rule anchored to the frozen 1M 8192-cluster layout."""
    if type(rows) is not int or rows <= 0:
        raise ValueError("source row count differs")
    return min(rows, math.ceil(8192 * (rows / 1_000_000) ** (1 / 3)))


def _load_source(
    source: Path, provenance: Path, *, expected_source_sha256: str,
    expected_provenance_sha256: str,
) -> tuple[np.ndarray, dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if (not _valid_sha(expected_source_sha256)
            or not _valid_sha(expected_provenance_sha256)
            or _sha256_file(provenance) != expected_provenance_sha256):
        raise ValueError("source provenance identity differs")
    declared = json.loads(provenance.read_text())
    if (type(declared) is not dict
            or declared.get("schema") != "borsuk-source-shards-v1"
            or declared.get("metric") != "cosine"
            or type(declared.get("rows")) is not int or declared["rows"] <= 0
            or type(declared.get("dimensions")) is not int
            or declared["dimensions"] < 64
            or declared.get("source_bytes") != source.stat().st_size
            or declared.get("source_sha256") != expected_source_sha256):
        raise ValueError("source provenance geometry or identity differs")
    if declared.get("query_or_truth_used") is not False:
        raise ValueError("query or truth entered source construction")
    if _sha256_file(source) != expected_source_sha256:
        raise ValueError("source Parquet SHA-256 differs")
    table = pq.read_table(source, columns=["feature_row_id", "embedding"])
    identifier_type = table.schema.field("feature_row_id").type
    embedding_type = table.schema.field("embedding").type
    rows, dimensions = declared["rows"], declared["dimensions"]
    if (table.num_rows != rows
            or not pa.types.is_integer(identifier_type)
            or not pa.types.is_fixed_size_list(embedding_type)
            or embedding_type.list_size != dimensions
            or embedding_type.value_type != pa.float32()):
        raise ValueError("source Parquet schema differs")
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if not np.array_equal(ids, np.arange(rows, dtype=ids.dtype)):
        raise ValueError("source IDs are not monotone train ordinals")
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    vectors = np.asarray(values, dtype=np.float32).reshape(rows, dimensions)
    if not np.isfinite(vectors).all():
        raise ValueError("source vector is nonfinite")
    norms = np.linalg.norm(vectors, axis=1)
    if (norms <= 0).any() or not np.allclose(norms, 1.0, atol=1e-4, rtol=0):
        raise ValueError("source cosine unit norm differs")
    return vectors, declared


def _fit_order(vectors: np.ndarray, clusters: int) -> np.ndarray:
    rows = vectors.shape[0]
    generator = np.random.default_rng(8201)
    sample = vectors[generator.choice(rows, min(rows, clusters * 64), replace=False)]
    centroids = lloyd(sample, clusters, 12, 8202)
    centroid_norms = np.einsum("ij,ij->i", centroids, centroids)
    assignment = np.empty(rows, dtype=np.int32)
    radius = np.empty(rows, dtype=np.float32)
    for start in range(0, rows, CHUNK_ROWS):
        stop = min(start + CHUNK_ROWS, rows)
        scores = centroid_norms[None, :] - 2.0 * (vectors[start:stop] @ centroids.T)
        best = np.argmin(scores, axis=1)
        assignment[start:stop] = best
        radius[start:stop] = np.take_along_axis(scores, best[:, None], axis=1)[:, 0]
    chain = centroid_chain(centroids)
    chain_rank = np.empty(clusters, dtype=np.int64)
    chain_rank[chain] = np.arange(clusters, dtype=np.int64)
    scale = float(radius.max()) - float(radius.min()) + 1.0
    key = chain_rank[assignment].astype(np.float64) * scale + radius.astype(np.float64)
    return np.argsort(key, kind="stable").astype(np.int64)


def _write_sq8(path: Path, vectors: np.ndarray, order: np.ndarray) -> str:
    rows, dimensions = vectors.shape
    low = vectors.min(axis=0).astype(np.float32)
    high = vectors.max(axis=0).astype(np.float32)
    span = np.maximum(high - low, np.float32(1e-12)).astype(np.float32)
    step = (span / np.float32(255.0)).astype(np.float32)
    record = np.dtype([("id", "<i8"), ("norm", "<f4"),
                       ("code", "u1", (dimensions,))])
    with path.open("xb") as output:
        for start in range(0, rows, CHUNK_ROWS):
            selected = order[start:min(start + CHUNK_ROWS, rows)]
            block = vectors[selected]
            codes = np.clip(np.rint((block - low) / span * 255.0), 0, 255).astype(np.uint8)
            restored = low + codes.astype(np.float32) * step
            encoded = np.empty(selected.size, dtype=record)
            encoded["id"] = selected
            encoded["norm"] = np.einsum("ij,ij->i", restored, restored)
            encoded["code"] = codes
            output.write(encoded.tobytes())
    return _sha256_file(path)


def build_source_layout(
    source: Path, provenance: Path, output_dir: Path, *,
    expected_source_sha256: str, expected_provenance_sha256: str,
) -> dict:
    """Seal one layout/SQ8 generation; no query or GT input is accepted."""
    if output_dir.exists():
        raise ValueError("source layout output already exists")
    vectors, declared = _load_source(
        source, provenance, expected_source_sha256=expected_source_sha256,
        expected_provenance_sha256=expected_provenance_sha256,
    )
    rows, dimensions = vectors.shape
    clusters = cluster_count(rows)
    order = _fit_order(vectors, clusters)
    if (order.shape != (rows,) or order.min() != 0 or order.max() != rows - 1
            or np.unique(order).size != rows):
        raise ValueError("source layout is not a permutation")
    with tempfile.TemporaryDirectory(prefix=output_dir.name + ".tmp-",
                                     dir=output_dir.parent) as temporary:
        pending = Path(temporary)
        np.save(pending / "layout.npy", order, allow_pickle=False)
        sq8_sha256 = _write_sq8(pending / "sq8.bin", vectors, order)
        manifest = {
            "schema": SCHEMA,
            "source_sha256": expected_source_sha256,
            "source_provenance_sha256": expected_provenance_sha256,
            "rows": rows, "dimensions": dimensions,
            "clusters": clusters,
            "cluster_rule": "ceil(8192*(N/1000000)^(1/3)); cap at N",
            "page_rows": 256,
            "layout_sha256": _sha256_file(pending / "layout.npy"),
            "sq8_sha256": sq8_sha256,
            "sq8_bytes": (dimensions + 12) * rows,
            "query_or_truth_used": False,
        }
        if (pending / "sq8.bin").stat().st_size != manifest["sq8_bytes"]:
            raise ValueError("source SQ8 byte length differs")
        (pending / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(output_dir)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--provenance-sha256", required=True)
    args = parser.parse_args()
    manifest = build_source_layout(
        args.source, args.provenance, args.output,
        expected_source_sha256=args.source_sha256,
        expected_provenance_sha256=args.provenance_sha256,
    )
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
