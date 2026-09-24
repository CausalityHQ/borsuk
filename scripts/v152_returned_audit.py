#!/usr/bin/env python3
"""Independently verify V152 ordered returned IDs without opening truth."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from scripts.v152_prepare_fresh import SOURCE_SHA, SQ8_SHA, sha256

CENTROID_SHA = "9f8a924ebeacd3c512365934c49c9fc42705a1bde1098ec99c981fb1a7fe3f12"


def records(path: Path) -> list[dict]:
    result = [json.loads(line) for line in path.read_text().splitlines()]
    if len(result) != 1000:
        raise ValueError("V152 audit record count differs")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "sq8", "low", "step", "centroids", "plans",
                 "queries", "routing", "replay", "output"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args()
    if (args.output.exists() or sha256(args.source) != SOURCE_SHA
            or sha256(args.sq8) != SQ8_SHA
            or sha256(args.centroids) != CENTROID_SHA):
        raise ValueError("V152 audit source differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (96,))])
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(100000,))
    low = np.fromfile(args.low, dtype="<f4")
    step = np.fromfile(args.step, dtype="<f4")
    if low.shape != (96,) or step.shape != (96,) or np.any(step <= 0):
        raise ValueError("V152 audit SQ8 coefficients differ")
    table = pq.read_table(args.source, columns=["feature_row_id", "embedding"])
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if table.num_rows != 100000 or not np.array_equal(ids, np.arange(100000)):
        raise ValueError("V152 audit source IDs differ")
    vectors = np.asarray(
        table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False),
        np.float32,
    ).reshape(100000, 96)
    centers = np.frombuffer(args.centroids.read_bytes(), dtype="<f2", offset=32)
    if centers.size != 3125 * 96:
        raise ValueError("V152 centroid geometry differs")
    centers = centers.astype(np.float64).reshape(3125, 96)
    center_norms = np.einsum("ij,ij->i", centers, centers)
    queries, routes, replay, plans = (records(path) for path in
                                      (args.queries, args.routing, args.replay, args.plans))
    checked = 0
    max_flat_diff = 0.0
    for ordinal, (request, route, record, plan) in enumerate(zip(queries, routes, replay, plans)):
        if (request["query_ordinal"] != ordinal or route["query_ordinal"] != ordinal
                or record["query_ordinal"] != ordinal):
            raise ValueError("V152 audit query order differs")
        query = np.asarray(request["query"], dtype=np.float32)
        if query.shape != (96,) or not np.isfinite(query).all():
            raise ValueError("V152 audit query differs")
        query64 = query.astype(np.float64)
        squared = np.maximum(0.0, center_norms + query64 @ query64 - 2.0 * (centers @ query64))
        pages = np.minimum.reduceat(np.sqrt(squared), np.arange(0, 3125, 8))
        reported = np.asarray(plan["flat_scores"], dtype=np.float64)
        if reported.shape != (391,) or not np.isfinite(reported).all():
            raise ValueError("V152 flat score vector differs")
        diff = float(np.max(np.abs(pages - reported)))
        max_flat_diff = max(max_flat_diff, diff)
        if diff > 0.001:
            raise ValueError("V152 independent flat centroid score differs")
        nominee_ids = set(map(int, sq8["id"][route["nominees"]]))
        weights = query * step
        shift = float(query @ low - (query @ query) / 2.0)
        for arm in ("flat", "v150", "v151", "v152"):
            result = record["arms"][arm]
            physical = np.concatenate([
                np.arange(first // dtype.itemsize, last // dtype.itemsize, dtype=np.int64)
                for first, last in result["ranges"]
            ])
            if physical.size < 512 or physical.size > 100000:
                raise ValueError("V152 audit fetched roster differs")
            fetched = sq8[physical]
            inner = fetched["code"].astype(np.float32) @ weights
            distances = fetched["norm"] - np.float32(2.0) * (inner + shift)
            order = np.lexsort((fetched["id"], distances))[:512]
            top512 = fetched["id"][order].astype(np.int64)
            if result["sq8_top100_ids"] != top512[:100].tolist():
                raise ValueError(f"V152 {arm} independent SQ8 ranking differs")
            union = np.asarray(sorted(nominee_ids | set(map(int, top512))), dtype=np.int64)
            if union.size != result["union_size"]:
                raise ValueError(f"V152 {arm} source union differs")
            selected = vectors[union].astype(np.float64)
            selected /= np.linalg.norm(selected, axis=1)[:, None]
            scores = selected @ query.astype(np.float64)
            ordered = union[np.lexsort((union, -scores))[:100]].tolist()
            if ordered != result["source_top100_ids"]:
                raise ValueError(f"V152 {arm} independent source ranking differs")
            checked += 1
    args.output.write_text(json.dumps({
        "schema": "borsuk-v152-returned-audit-v1",
        "replay_sha256": sha256(args.replay),
        "queries_sha256": sha256(args.queries),
        "routing_sha256": sha256(args.routing),
        "source_sha256": SOURCE_SHA,
        "sq8_sha256": SQ8_SHA,
        "centroid_sha256": CENTROID_SHA,
        "plans_sha256": sha256(args.plans),
        "flat_checked_queries": 1000,
        "max_flat_score_difference": max_flat_diff,
        "checked_arm_queries": checked,
        "truth_opened": False,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
