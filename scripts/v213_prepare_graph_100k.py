#!/usr/bin/env python3
"""Seal source float32 vectors in the authenticated FP16 plane's row order."""

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import authenticate, sha256
from scripts.v160_geometric_relayout_primary import DTYPE
from scripts.v163_smooth_layout_100k import source_vectors
from scripts.v210_resident_neighborhood_100k import DIMS, ROWS, SOURCE_SHA


def run(args: argparse.Namespace) -> None:
    if args.vectors.exists() or args.summary.exists():
        raise ValueError("V213 output exists")
    prep = json.loads(args.preparation.read_text())
    if (prep.get("schema") != "borsuk-v212-rust-100k-preparation-v1"
            or prep.get("source_sha256") != SOURCE_SHA
            or prep.get("rows") != ROWS or prep.get("dimensions") != DIMS
            or prep.get("new_to_old_sha256") != sha256(args.new_to_old)
            or prep.get("plane_sha256") != sha256(args.plane)):
        raise ValueError("V212 plane/preparation identity differs")
    authenticate(args.old_sq8, "sq8")
    old = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    source = source_vectors(args.source, old)
    order = np.fromfile(args.new_to_old, dtype="<u4")
    if (len(order) != ROWS or len(np.unique(order)) != ROWS
            or order.min() != 0 or order.max() != ROWS - 1):
        raise ValueError("physical mapping differs")
    with args.vectors.open("xb") as target:
        for start in range(0, ROWS, 4096):
            selected = order[start:start + 4096]
            target.write(source[selected].astype("<f4", copy=False).tobytes())
    if args.vectors.stat().st_size != ROWS * DIMS * 4:
        raise ValueError("source vector byte length differs")
    args.summary.write_text(json.dumps({
        "schema": "borsuk-v213-graph-preparation-v1",
        "source_sha256": SOURCE_SHA,
        "plane_sha256": prep["plane_sha256"],
        "vectors_sha256": sha256(args.vectors),
        "rows": ROWS, "dimensions": DIMS, "generation": prep["generation"],
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("source", "old_sq8", "new_to_old", "plane", "preparation", "vectors", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
