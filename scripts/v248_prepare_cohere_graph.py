#!/usr/bin/env python3
"""Prepare authenticated source-only FP16 and PQ64 planes from staged shards."""

import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v115_source_router import _encode, _train_books

DIMS = 768
PLANE_DTYPE = np.dtype([("id", "<u8"), ("coordinates", "<f2", (DIMS,))])


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def prepare(args):
    receipt = args.receipt.read_bytes()
    if hashlib.sha256(receipt).hexdigest() != args.receipt_sha256:
        raise ValueError("staging receipt differs")
    authority = json.loads(receipt)
    if (authority["dataset_id"] != "cohere-large-10m-768"
            or authority["dataset_content_sha256"] !=
            "fa8ccb38e5c761388e0c2ac211cc219438cd79e802e69debd6197d74f83f11ad"
            or not 0 < args.rows <= 10_000_000 or args.generation <= 0
            or args.output.exists()):
        raise ValueError("source authority or geometry differs")
    if authority["object_count"] != len(authority["objects"]):
        raise ValueError("staging object roster differs")
    source_objects = sorted((row for row in authority["objects"]
                             if row["role"] == "train"), key=lambda row: row["uri"])
    if (len(source_objects) != 458
            or [path.name for path in args.train]
            != [row["uri"].rsplit("/", 1)[-1] for row in source_objects[:len(args.train)]]):
        raise ValueError("train shards are not the canonical prefix")
    vectors = []
    remaining = args.rows
    for path, identity in zip(args.train, source_objects, strict=False):
        if (path.stat().st_size != identity["bytes"]
                or digest(path) != identity["sha256"]):
            raise ValueError(f"train shard differs: {path.name}")
        table = pq.read_table(path, columns=["emb"])
        column = table.schema.field("emb")
        if (not pa.types.is_fixed_size_list(column.type)
                or column.type.list_size != DIMS
                or column.type.value_type != pa.float32()
                or table.num_rows != identity["rows"]
                or table.column("emb").null_count
                or table.column("emb").combine_chunks().values.null_count):
            raise ValueError(f"train shard geometry differs: {path.name}")
        values = table.column("emb").combine_chunks().values.to_numpy(zero_copy_only=False)
        block = np.asarray(values, dtype="<f4").reshape(table.num_rows, DIMS)
        count = min(remaining, len(block))
        vectors.append(np.ascontiguousarray(block[:count]))
        remaining -= count
        if remaining == 0:
            break
    if remaining:
        raise ValueError("train shards do not cover requested rows")
    data = np.concatenate(vectors)
    if not np.isfinite(data).all() or not (data != 0).any(axis=1).all():
        raise ValueError("source contains invalid vectors")
    args.output.mkdir(parents=True)
    raw = args.output / "vectors.raw"
    data.tofile(raw)
    source_sha = digest(raw)
    plane = args.output / "plane.bin"
    header = (b"BORSF160" + struct.pack("<IIQQ", 1, DIMS, args.rows, args.generation)
              + bytes.fromhex(source_sha))
    if len(header) != 64 or PLANE_DTYPE.itemsize != 1544:
        raise ValueError("FP16 plane geometry differs")
    with plane.open("xb") as output:
        output.write(header)
        for first in range(0, args.rows, 8192):
            block = np.empty(min(8192, args.rows - first), dtype=PLANE_DTYPE)
            block["id"] = np.arange(first, first + len(block), dtype=np.uint64)
            block["coordinates"] = data[first:first + len(block)].astype("<f2")
            output.write(block.tobytes())
    books = _train_books(data, 64, seed=7301)
    codes = _encode(data, books)
    books.astype("<f4", copy=False).tofile(args.output / "books.bin")
    codes.tofile(args.output / "codes.bin")
    np.arange(args.rows, dtype="<u4").tofile(args.output / "map.u32")
    summary = {"schema": "borsuk-v248-source-preparation-v1",
               "dataset_id": authority["dataset_id"], "rows": args.rows,
               "dimensions": DIMS, "generation": args.generation,
               "staging_receipt_sha256": args.receipt_sha256,
               "source_sha256": source_sha,
               "artifacts": {name: {"bytes": (args.output / name).stat().st_size,
                                    "sha256": digest(args.output / name)}
                             for name in ("vectors.raw", "plane.bin", "books.bin",
                                          "codes.bin", "map.u32")}}
    (args.output / "prep.json").write_text(
        json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--receipt-sha256", required=True)
    parser.add_argument("--train", nargs="+", required=True, type=Path)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    prepare(parser.parse_args())
