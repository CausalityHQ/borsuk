#!/usr/bin/env python3
"""Post-hoc GT coverage diagnostic for immutable V122 control-range halos.

This reads sealed development truth only to count coverage after the range
schedule has been built. It does not select a serving policy or measure S3.
"""

from __future__ import annotations

import argparse
import array
import ast
import hashlib
import json
import struct
import sys
from pathlib import Path

EVIDENCE_SHA = "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6"
LAYOUT_SHA = "8b23fb6d2f76704394733f5540f25e36db961386b568495b7bfd73fc52d60aea"
ROWS = 100_000
ROW_BYTES = 108
PAGE_ROWS = 256
MAX_GETS = 32
MAX_BYTES = 16_777_216


def authenticated(path: Path, expected_sha: str) -> bytes:
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != expected_sha:
        raise ValueError(f"sealed V122 artifact differs: {path}")
    return data


def inverse_layout(data: bytes) -> list[int]:
    if data[:6] != b"\x93NUMPY" or data[7] != 0:
        raise ValueError("layout NPY header differs")
    major = data[6]
    if major not in (1, 2):
        raise ValueError("layout NPY version differs")
    header_size = struct.unpack_from("<H" if major == 1 else "<I", data, 8)[0]
    start = 10 if major == 1 else 12
    header = ast.literal_eval(data[start : start + header_size].decode())
    if header != {"descr": "<i8", "fortran_order": False, "shape": (ROWS,)}:
        raise ValueError("layout geometry differs")
    values = array.array("q")
    values.frombytes(data[start + header_size :])
    if sys.byteorder != "little" or len(values) != ROWS or set(values) != set(range(ROWS)):
        raise ValueError("layout is not the sealed permutation")
    inverse = [0] * ROWS
    for physical, source in enumerate(values):
        inverse[source] = physical
    return inverse


def merge(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, stop in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(stop, merged[-1][1]))
        else:
            merged.append((start, stop))
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--layout", required=True, type=Path)
    args = parser.parse_args()
    inverse = inverse_layout(authenticated(args.layout, LAYOUT_SHA))
    rows = [json.loads(line) for line in authenticated(args.evidence, EVIDENCE_SHA).splitlines()]
    if len(rows) != 1_000:
        raise ValueError("V122 query count differs")
    outcomes = []
    for halo in (0, 1, 2, 4, 8, 16):
        coverage = 0
        bytes_per_query = []
        gets_per_query = []
        cap_violations = 0
        for ordinal, row in enumerate(rows):
            if (row["query_ordinal"] != ordinal
                or row["source_query_ordinal"] != 9_000 + ordinal
                or len(row["truth_ids"]) != 100):
                raise ValueError("V122 query identity differs")
            spans = merge([
                (max(0, first - halo * PAGE_ROWS * ROW_BYTES),
                 min(ROWS * ROW_BYTES, last + halo * PAGE_ROWS * ROW_BYTES))
                for first, last in row["baseline_ranges"]
            ])
            charged = sum(last - first for first, last in spans)
            gets = len(spans)
            offsets = [inverse[source] * ROW_BYTES for source in row["truth_ids"]]
            covered = sum(any(first <= offset < last for first, last in spans)
                          for offset in offsets)
            if halo == 0 and (
                charged != row["baseline_bytes"]
                or gets != row["baseline_gets"]
                or covered != row["baseline_physical_coverage"]
            ):
                raise ValueError("baseline recount differs")
            coverage += covered
            bytes_per_query.append(charged)
            gets_per_query.append(gets)
            cap_violations += charged > MAX_BYTES or gets > MAX_GETS
        outcomes.append({
            "halo_pages_each_side": halo,
            "gt100_coverage_hits": coverage,
            "mean_bytes_per_query": sum(bytes_per_query) / len(rows),
            "p95_bytes_per_query": sorted(bytes_per_query)[949],
            "max_bytes_per_query": max(bytes_per_query),
            "mean_merged_gets_per_query": sum(gets_per_query) / len(rows),
            "max_merged_gets_per_query": max(gets_per_query),
            "cap_violations": cap_violations,
        })
    print(json.dumps({"schema": "borsuk-v137-v122-halo-ceiling-diagnostic-v1",
                      "dataset": "deep-image-96-angular random100k train subset",
                      "split": "used publication-test ordinals 9000-9999",
                      "outcomes": outcomes}, sort_keys=True))


if __name__ == "__main__":
    main()
