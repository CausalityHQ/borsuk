#!/usr/bin/env python3
"""Seal source rows and PQ mapping for the frozen 1M resident graph."""

import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np

from scripts.v164_smooth_layout_1m import source_arrays
from scripts.v155_relaion_returned_quality import sha256

ROWS = 1_000_000
DIMS = 768
GENERATION = 196
SOURCE_SHA = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
OLD_ORDER_SHA = "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"
ORDER_SHA = "5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f"
PLANE_SHA = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"
BOOKS_SHA = "1ca5aa29c32dd155f0309a4d9f5bd8294ccbe75f50d1a2ff213603fba08800ce"
CODES_SHA = "599e359b02ddb85876234f64bac3fcf7bfcb759e121f6a1fbcb4fbd5dfc95460"
REQUESTS_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
PLANE_DTYPE = np.dtype([("id", "<u8"), ("coordinates", "<f2", (DIMS,))])


def run(args: argparse.Namespace) -> None:
    if any(path.exists() for path in (args.vectors, args.map, args.summary)):
        raise ValueError("V217 output already exists")
    for path, expected in ((args.source, SOURCE_SHA), (args.old_layout, OLD_ORDER_SHA),
                           (args.order, ORDER_SHA), (args.plane, PLANE_SHA),
                           (args.books, BOOKS_SHA), (args.codes, CODES_SHA),
                           (args.requests, REQUESTS_SHA)):
        if sha256(path) != expected:
            raise ValueError(f"V217 input digest differs: {path.name}")
    source_ids, source, _ = source_arrays(args.source)
    if source.shape != (ROWS, DIMS) or source.dtype != np.float32:
        raise ValueError("source vector geometry differs")
    old = np.load(args.old_layout, allow_pickle=False)
    order = np.load(args.order, allow_pickle=False)
    if (old.shape != (ROWS,) or order.shape != (ROWS,)
            or old.dtype not in (np.int32, np.int64) or order.dtype != np.int64
            or not np.array_equal(np.sort(old), np.arange(ROWS))
            or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("source physical orders differ")
    old_inverse = np.empty(ROWS, dtype=np.int64)
    old_inverse[old] = np.arange(ROWS)
    old_for_new = old_inverse[order]
    if old_for_new.min() != 0 or old_for_new.max() != ROWS - 1:
        raise ValueError("old/new physical map range differs")
    args.map.write_bytes(old_for_new.astype("<u4").tobytes())
    expected_header = (b"BORSF160" + struct.pack("<IIQQ", 1, DIMS, ROWS, GENERATION)
                       + bytes.fromhex(SOURCE_SHA))
    if (len(expected_header) != 64 or PLANE_DTYPE.itemsize != 1544
            or args.plane.stat().st_size != 64 + ROWS * 1544):
        raise ValueError("FP16 plane geometry differs")
    with args.plane.open("rb") as input_file:
        if input_file.read(64) != expected_header:
            raise ValueError("FP16 plane source/generation differs")
    plane = np.memmap(args.plane, dtype=PLANE_DTYPE, mode="r", offset=64, shape=(ROWS,))
    digest = hashlib.sha256()
    with args.vectors.open("xb") as target:
        for first in range(0, ROWS, 8192):
            selected = order[first:first + 8192]
            block = np.asarray(source[selected], dtype="<f4")
            if (not np.isfinite(block).all() or not (block != 0).any(axis=1).all()
                    or not np.array_equal(plane["id"][first:first + len(selected)],
                                          source_ids[selected])
                    or not np.array_equal(plane["coordinates"][first:first + len(selected)],
                                          block.astype("<f2"))):
                raise ValueError("source/plane row identity differs")
            raw = block.tobytes()
            target.write(raw)
            digest.update(raw)
    if args.vectors.stat().st_size != ROWS * DIMS * 4:
        raise ValueError("source raw vector length differs")
    args.summary.write_text(json.dumps({
        "schema": "borsuk-v217-graph-1m-preparation-v1",
        "source_sha256": SOURCE_SHA, "old_order_sha256": OLD_ORDER_SHA,
        "order_sha256": ORDER_SHA, "plane_sha256": PLANE_SHA,
        "books_sha256": BOOKS_SHA, "codes_sha256": CODES_SHA,
        "requests_sha256": REQUESTS_SHA, "map_sha256": sha256(args.map),
        "vectors_sha256": digest.hexdigest(), "rows": ROWS,
        "dimensions": DIMS, "generation": GENERATION,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("source", "old_layout", "order", "plane", "books", "codes",
                 "requests", "vectors", "map", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
