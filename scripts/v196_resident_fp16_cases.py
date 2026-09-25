#!/usr/bin/env python3
"""Rebuild sealed V195 used-panel top-128 cases and a physical FP16 plane."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np

from scripts.check_v194_fresh_1m_optional_source import HASHES as V194_HASHES
from scripts.check_v195_used_1m_rerank_diagnostic import HASHES as V195_HASHES
from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_ranking_run import canonical, records
from scripts.v177_source_candidate_ceiling import _inputs
from scripts.v194_fresh_1m_optional_source import COUNT, FIRST, UNIT_BYTES

DIMS = 768
GENERATION = 196
SOURCE_SHA = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
ROW_DTYPE = np.dtype([("id", "<u8"), ("coordinates", "<f2", (DIMS,))])


def run(args: argparse.Namespace) -> None:
    for role in V195_HASHES:
        if sha256(getattr(args, "v195_" + role)) != V195_HASHES[role]:
            raise ValueError(f"V195 {role} identity differs")
    if sha256(args.v194_plans) != V194_HASHES["plans"]:
        raise ValueError("V194 plan identity differs")
    if sha256(args.source) != SOURCE_SHA:
        raise ValueError("source identity differs")
    old, inverse_new, source_ids, vectors, old_sq8, planes = _inputs(args)
    if vectors.shape != (ROWS, DIMS) or len(set(map(int, source_ids))) != ROWS:
        raise ValueError("source geometry or IDs differ")
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    new_sq8 = np.memmap(args.new_sq8, dtype=old_sq8.dtype, mode="w+", shape=(ROWS,))
    for start in range(0, ROWS, 8192):
        stop = min(start + 8192, ROWS)
        new_sq8[start:stop] = old_sq8[inverse_old[new_order[start:stop]]]
    new_sq8.flush()
    if not np.array_equal(new_sq8["id"], source_ids[new_order]):
        raise ValueError("physical SQ8 IDs differ")
    header = (b"BORSF160" + struct.pack("<IIQQ", 1, DIMS, ROWS, GENERATION)
              + bytes.fromhex(SOURCE_SHA))
    if len(header) != 64 or ROW_DTYPE.itemsize != 8 + 2 * DIMS:
        raise ValueError("resident format geometry differs")
    plane_digest = hashlib.sha256()
    with args.plane.open("xb") as plane:
        plane.write(header)
        plane_digest.update(header)
        for start in range(0, ROWS, 8192):
            stop = min(start + 8192, ROWS)
            block = np.asarray(vectors[new_order[start:stop]], dtype=np.float32)
            if not np.isfinite(block).all() or (block != 0).sum(axis=1).min() == 0:
                raise ValueError("source nonfinite or zero row")
            encoded = block.astype("<f2")
            if not np.isfinite(encoded).all() or not (encoded != 0).any(axis=1).all():
                raise ValueError("FP16 coordinate overflow or zero row")
            rows = np.empty(stop - start, dtype=ROW_DTYPE)
            rows["id"] = source_ids[new_order[start:stop]]
            rows["coordinates"] = encoded
            data = rows.tobytes()
            plane.write(data)
            plane_digest.update(data)
    if args.plane.stat().st_size != 64 + ROWS * ROW_DTYPE.itemsize:
        raise ValueError("resident plane length differs")

    positions = {int(identifier): index for index, identifier in enumerate(source_ids)}
    plans = records(args.v194_plans)
    expected = records(args.v195_raw)
    summary = json.loads(args.v195_summary.read_text())
    if len(plans) != COUNT or len(expected) != COUNT:
        raise ValueError("used-panel count differs")
    low = np.asarray(planes["low"], dtype=np.float32)
    step = np.asarray(planes["step"], dtype=np.float32)
    with args.cases.open("x") as output:
        for index, (plan, record) in enumerate(zip(plans, expected, strict=True)):
            ordinal = FIRST + index
            source_id = record["source_id"]
            if record["ordinal"] != ordinal or plan["source_id"] != source_id:
                raise ValueError("query identity differs")
            intervals = (summary["dynamic_floor"]["intervals"] if ordinal == 3321
                         else plan["arms"]["optional_risk"]["intervals"])
            if not intervals:
                raise ValueError("empty physical plan")
            query = np.asarray(vectors[positions[source_id]], dtype=np.float32)
            ranges = [[start * UNIT_BYTES, (end + 1) * UNIT_BYTES]
                      for start, end in intervals]
            sq8 = score_sq8_ranges(new_sq8, query, low, step, ranges, top_k=129)
            shortlist = [identifier for identifier in sq8 if identifier != source_id][:128]
            if len(shortlist) != 128 or len(set(shortlist)) != 128:
                raise ValueError("SQ8 shortlist geometry differs")
            candidates = [{"ordinal": int(inverse_new[positions[identifier]]),
                           "source_id": int(identifier)} for identifier in shortlist]
            expected_ids = record["k"]["128"]["fp16_returned_ids"]
            if len(expected_ids) != 100 or not set(expected_ids).issubset(shortlist):
                raise ValueError("V195 expected IDs outside shortlist")
            output.write(canonical({"ordinal": ordinal, "query": query.tolist(),
                                    "candidates": candidates,
                                    "expected": expected_ids}))
    args.summary.write_text(canonical({
        "schema": "borsuk-v196-resident-fp16-cases-v1",
        "source_sha256": SOURCE_SHA,
        "v194_plans_sha256": V194_HASHES["plans"],
        "v195_raw_sha256": V195_HASHES["raw"],
        "rows": ROWS, "dimensions": DIMS, "generation": GENERATION,
        "cases": COUNT, "shortlist": 128, "returned": 100,
        "plane_bytes": args.plane.stat().st_size,
        "plane_sha256": plane_digest.hexdigest(),
        "cases_sha256": sha256(args.cases),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    for role in ("source", "old_layout", "old_sq8", "router", "order",
                 "v164_terminal", "v194_plans", "new_sq8", "plane",
                 "cases", "summary"):
        parser.add_argument("--" + role.replace("_", "-"), type=Path, required=True)
    for role in V195_HASHES:
        parser.add_argument("--v195-" + role, type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
