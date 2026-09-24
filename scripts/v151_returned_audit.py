#!/usr/bin/env python3
"""Independently verify V151 ordered returned IDs without opening truth."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from scripts.v151_prepare_fresh import SOURCE_SHA, SQ8_SHA, sha256


def records(path: Path) -> list[dict]:
    result = [json.loads(line) for line in path.read_text().splitlines()]
    if len(result) != 1000:
        raise ValueError("V151 audit record count differs")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "sq8", "low", "step", "queries", "routing", "replay", "output"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args()
    if (args.output.exists() or sha256(args.source) != SOURCE_SHA
            or sha256(args.sq8) != SQ8_SHA):
        raise ValueError("V151 audit source differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (96,))])
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(100000,))
    low = np.fromfile(args.low, dtype="<f4")
    step = np.fromfile(args.step, dtype="<f4")
    if low.shape != (96,) or step.shape != (96,) or np.any(step <= 0):
        raise ValueError("V151 audit SQ8 coefficients differ")
    table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if table.num_rows != 100000 or not np.array_equal(ids, np.arange(100000)):
        raise ValueError("V151 audit source IDs differ")
    vectors = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        np.float32,
    ).reshape(100000, 96)
    queries, routes, replay = (records(path) for path in
                               (args.queries, args.routing, args.replay))
    checked = 0
    for ordinal, (request, route, record) in enumerate(zip(queries, routes, replay)):
        if (request["query_ordinal"] != ordinal or route["query_ordinal"] != ordinal
                or record["query_ordinal"] != ordinal):
            raise ValueError("V151 audit query order differs")
        query = np.asarray(request["query"], dtype=np.float32)
        if query.shape != (96,) or not np.isfinite(query).all():
            raise ValueError("V151 audit query differs")
        nominee_ids = set(map(int, sq8["id"][route["nominees"]]))
        weights = query * step
        shift = float(query @ low - (query @ query) / 2.0)
        for arm in ("flat", "v150", "v151"):
            result = record["arms"][arm]
            physical = np.concatenate([
                np.arange(first // dtype.itemsize, last // dtype.itemsize, dtype=np.int64)
                for first, last in result["ranges"]
            ])
            if physical.size < 512 or physical.size > 100000:
                raise ValueError("V151 audit fetched roster differs")
            fetched = sq8[physical]
            inner = fetched["code"].astype(np.float32) @ weights
            distances = fetched["norm"] - np.float32(2.0) * (inner + shift)
            order = np.lexsort((fetched["id"], distances))[:512]
            top512 = fetched["id"][order].astype(np.int64)
            if result["sq8_top100_ids"] != top512[:100].tolist():
                raise ValueError(f"V151 {arm} independent SQ8 ranking differs")
            union = np.asarray(sorted(nominee_ids | set(map(int, top512))), dtype=np.int64)
            if union.size != result["union_size"]:
                raise ValueError(f"V151 {arm} source union differs")
            selected = vectors[union].astype(np.float64)
            selected /= np.linalg.norm(selected, axis=1)[:, None]
            scores = selected @ query.astype(np.float64)
            ordered = union[np.lexsort((union, -scores))[:100]].tolist()
            if ordered != result["source_top100_ids"]:
                raise ValueError(f"V151 {arm} independent source ranking differs")
            checked += 1
    args.output.write_text(json.dumps({
        "schema": "borsuk-v151-returned-audit-v1",
        "replay_sha256": sha256(args.replay),
        "queries_sha256": sha256(args.queries),
        "routing_sha256": sha256(args.routing),
        "source_sha256": SOURCE_SHA,
        "sq8_sha256": SQ8_SHA,
        "checked_arm_queries": checked,
        "truth_opened": False,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
