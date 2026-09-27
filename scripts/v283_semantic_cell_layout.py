"""Source-only page-sized semantic cells for one V282 physical-layout falsifier."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v120_source_layout import _fit_order, _load_source, _sha256_file, _write_sq8


def build_semantic_cell_layout(
    source: Path, provenance: Path, output_dir: Path, *,
    expected_source_sha256: str, expected_provenance_sha256: str,
) -> dict:
    if output_dir.exists():
        raise ValueError("semantic-cell output already exists")
    vectors, declared = _load_source(
        source, provenance,
        expected_source_sha256=expected_source_sha256,
        expected_provenance_sha256=expected_provenance_sha256,
    )
    rows, dimensions = vectors.shape
    cells = (rows + 255) // 256
    order = _fit_order(vectors, cells)
    if (order.shape != (rows,) or order.min() != 0 or order.max() != rows - 1
            or np.unique(order).size != rows):
        raise ValueError("semantic-cell order is not a permutation")
    with tempfile.TemporaryDirectory(prefix=output_dir.name + ".tmp-",
                                     dir=output_dir.parent) as temporary:
        pending = Path(temporary)
        np.save(pending / "layout.npy", order, allow_pickle=False)
        sq8_sha256 = _write_sq8(pending / "sq8.bin", vectors, order)
        manifest = {
            "schema": "borsuk-semantic-cell-layout-v1",
            "source_sha256": expected_source_sha256,
            "source_provenance_sha256": expected_provenance_sha256,
            "rows": rows, "dimensions": dimensions, "page_rows": 256,
            "cells": cells, "cell_rule": "ceil(N/256) source-only kmeans; stable physical order",
            "layout_sha256": _sha256_file(pending / "layout.npy"),
            "sq8_sha256": sq8_sha256,
            "sq8_bytes": (dimensions + 12) * rows,
            "query_or_truth_used": False,
        }
        (pending / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(output_dir)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--provenance-sha256", required=True)
    args = parser.parse_args()
    print(json.dumps(build_semantic_cell_layout(
        args.source, args.provenance, args.output,
        expected_source_sha256=args.source_sha256,
        expected_provenance_sha256=args.provenance_sha256,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
