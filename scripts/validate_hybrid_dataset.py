#!/usr/bin/env python3
"""Validate checksums and structural counts for a prepared hybrid dataset."""

from __future__ import annotations

import json
import struct
import sys
from pathlib import Path

from prepare_hybrid_dataset import RETRIEVAL_MODES, sha256


def fail(message: str) -> None:
    raise SystemExit(message)


def line_count(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for line in handle if line.strip())


def last_u64(path: Path, expected_values: int) -> int:
    expected_bytes = expected_values * 8
    actual_bytes = path.stat().st_size
    if actual_bytes != expected_bytes:
        fail(f"{path.name}: expected {expected_bytes} offset bytes, got {actual_bytes}")
    with path.open("rb") as handle:
        first = struct.unpack("<Q", handle.read(8))[0]
        if first != 0:
            fail(f"{path.name}: first sparse offset must be zero")
        handle.seek(-8, 2)
        return struct.unpack("<Q", handle.read(8))[0]


def validate_sparse(root: Path, prefix: str, rows: int) -> None:
    final_offset = last_u64(
        root / f"{prefix}.sparse.offsets.u64",
        rows + 1,
    )
    for suffix in ("indices.u32", "values.f32"):
        path = root / f"{prefix}.sparse.{suffix}"
        expected = final_offset * 4
        actual = path.stat().st_size
        if actual != expected:
            fail(f"{path.name}: expected {expected} bytes, got {actual}")


def validate(root: Path) -> dict[str, object]:
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        fail(f"missing {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        fail("manifest schema_version must be 1")
    if manifest.get("qrels_semantics") != "shared-across-all-retrieval-modes":
        fail("manifest qrels are not shared across retrieval modes")
    if manifest.get("retrieval_modes") != RETRIEVAL_MODES:
        fail("manifest does not enumerate the canonical seven retrieval modes")

    documents = int(manifest["documents"])
    queries = int(manifest["queries"])
    qrels = int(manifest["qrels"])
    dense_dimensions = int(manifest["dense"]["dimensions"])
    sparse_dimensions = int(manifest["sparse"]["dimensions"])
    if min(documents, queries, qrels, dense_dimensions, sparse_dimensions) <= 0:
        fail("manifest counts and dimensions must be positive")

    artifacts = manifest.get("artifacts_sha256")
    if not isinstance(artifacts, dict) or not artifacts:
        fail("manifest artifacts_sha256 is required")
    for name, expected in sorted(artifacts.items()):
        path = root / name
        if not path.is_file():
            fail(f"missing artifact {name}")
        actual = sha256(path)
        if actual != expected:
            fail(f"{name}: SHA256 mismatch; expected {expected}, got {actual}")

    for prefix, rows in (("corpus", documents), ("queries", queries)):
        jsonl = root / f"{prefix}.jsonl"
        actual_rows = line_count(jsonl)
        if actual_rows != rows:
            fail(f"{jsonl.name}: expected {rows} rows, got {actual_rows}")
        dense = root / f"{prefix}.dense.f32"
        expected_dense = rows * dense_dimensions * 4
        actual_dense = dense.stat().st_size
        if actual_dense != expected_dense:
            fail(f"{dense.name}: expected {expected_dense} bytes, got {actual_dense}")
        validate_sparse(root, prefix, rows)

    qrel_rows = max(0, line_count(root / "qrels.tsv") - 1)
    if qrel_rows != qrels:
        fail(f"qrels.tsv: expected {qrels} judgments, got {qrel_rows}")
    return {
        "status": "valid",
        "dataset": manifest["dataset"],
        "documents": documents,
        "queries": queries,
        "qrels": qrels,
        "dense_dimensions": dense_dimensions,
        "sparse_dimensions": sparse_dimensions,
        "artifacts": len(artifacts),
    }


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: validate_hybrid_dataset.py DATASET_DIRECTORY")
    print(json.dumps(validate(Path(sys.argv[1])), sort_keys=True))


if __name__ == "__main__":
    main()
