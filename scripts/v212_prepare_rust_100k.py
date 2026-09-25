#!/usr/bin/env python3
"""Create pinned Rust kernel inputs from completed 100k source artifacts."""

import argparse
import struct
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import authenticate, sha256
from scripts.v160_geometric_relayout_primary import DTYPE
from scripts.v163_smooth_layout_100k import source_vectors
from scripts.v210_resident_neighborhood_100k import (
    DIMS, ORDER_SHA, ROWS, SOURCE_SHA, canonical,
)
from scripts.v211_pq_resident_neighborhood_100k import PQ_HASHES

NEW_SQ8_SHA = "76d325d20dd38063bb050f83cfa693f7748921c75280cb1bf1f988041329dd38"
GENERATION = 212
PLANE_DTYPE = np.dtype([("id", "<u8"), ("coordinates", "<f2", (DIMS,))])


def run(args: argparse.Namespace) -> None:
    outputs = (args.plane, args.old_to_new, args.new_to_old,
               args.books_raw, args.codes_raw, args.summary)
    if any(path.exists() for path in outputs):
        raise ValueError("V212 output exists")
    authenticate(args.old_sq8, "sq8")
    authenticate(args.requests, "requests")
    if (sha256(args.source) != SOURCE_SHA or sha256(args.order) != ORDER_SHA
            or sha256(args.new_sq8) != NEW_SQ8_SHA):
        raise ValueError("frozen source/order/SQ8 differs")
    for role, expected in PQ_HASHES.items():
        if sha256(getattr(args, "pq_" + role)) != expected:
            raise ValueError(f"V113 PQ {role} differs")
    old = np.memmap(args.old_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    new = np.memmap(args.new_sq8, dtype=DTYPE, mode="r", shape=(ROWS,))
    source = source_vectors(args.source, old)
    order = np.load(args.order, allow_pickle=False)
    ids = np.load(args.pq_ids, allow_pickle=False)
    books = np.load(args.pq_books, allow_pickle=False)
    codes = np.load(args.pq_codes, allow_pickle=False)
    if (order.shape != (ROWS,) or order.dtype != np.int64
            or not np.array_equal(np.sort(order), np.arange(ROWS))
            or ids.shape != (ROWS,) or ids.dtype != np.int64
            or books.shape != (64, 256, 12) or books.dtype != np.float32
            or codes.shape != (ROWS, 64) or codes.dtype != np.uint8):
        raise ValueError("V212 order/PQ geometry differs")
    if not np.array_equal(new["id"], old["id"][order]):
        raise ValueError("physical SQ8 IDs differ")
    sorted_pq = np.argsort(ids)
    index = np.searchsorted(ids[sorted_pq], old["id"])
    if (np.any(index >= ROWS)
            or not np.array_equal(ids[sorted_pq[index]], old["id"])):
        raise ValueError("PQ stable IDs differ")
    pq_for_old = sorted_pq[index]
    inverse = np.empty(ROWS, dtype=np.uint32)
    inverse[order] = np.arange(ROWS, dtype=np.uint32)
    args.old_to_new.write_bytes(inverse.astype("<u4").tobytes())
    args.new_to_old.write_bytes(order.astype("<u4").tobytes())
    args.books_raw.write_bytes(books.astype("<f4", copy=False).tobytes())
    args.codes_raw.write_bytes(codes[pq_for_old].tobytes())
    header = (b"BORSF160" + struct.pack("<IIQQ", 1, DIMS, ROWS, GENERATION)
              + bytes.fromhex(SOURCE_SHA))
    if len(header) != 64 or PLANE_DTYPE.itemsize != 1544:
        raise ValueError("FP16 plane format geometry differs")
    with args.plane.open("xb") as target:
        target.write(header)
        for first in range(0, ROWS, 4096):
            selected = order[first:first + 4096]
            payload = source[selected].astype("<f2")
            if not np.isfinite(payload).all() or not (payload != 0).any(axis=1).all():
                raise ValueError("FP16 source row differs")
            rows = np.empty(len(selected), dtype=PLANE_DTYPE)
            rows["id"] = old["id"][selected]
            rows["coordinates"] = payload
            target.write(rows.tobytes())
    if args.plane.stat().st_size != 64 + ROWS * 1544:
        raise ValueError("FP16 plane length differs")
    args.summary.write_text(canonical({
        "schema": "borsuk-v212-rust-100k-preparation-v1", "source_sha256": SOURCE_SHA,
        "generation": GENERATION, "rows": ROWS, "dimensions": DIMS,
        "plane_sha256": sha256(args.plane), "old_to_new_sha256": sha256(args.old_to_new),
        "new_to_old_sha256": sha256(args.new_to_old),
        "books_raw_sha256": sha256(args.books_raw),
        "codes_raw_sha256": sha256(args.codes_raw),
        "requests_sha256": sha256(args.requests), "pq_source_sha256": PQ_HASHES,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("source", "old_sq8", "new_sq8", "order", "requests",
                 "pq_ids", "pq_books", "pq_codes", "plane", "old_to_new",
                 "new_to_old", "books_raw", "codes_raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
