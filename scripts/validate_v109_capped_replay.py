#!/usr/bin/env python3
"""Independent roster, physical-plan and recall reducer for the V109 replay."""

from __future__ import annotations

import argparse
import bisect
import json
import sys
from pathlib import Path

import numpy as np

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.v109_capped_reader_replay import (
    MAX_BYTES, MAX_GETS, ROW_BYTES, ROWS, SQ8_DTYPE, SQ8_SHA256,
    load_manifest, sha256_file,
)
from scripts.v109_range_plan import admit_ranked_pages, plan_page_ranges


def nearest_rank(values: list[int], numerator: int, denominator: int) -> int:
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1,
                              (len(ordered) * numerator - 1) // denominator))]


def reduce_replay(evidence: Path, manifest_path: Path, sq8_path: Path) -> dict[str, object]:
    manifest_sha = sha256_file(manifest_path)
    if sha256_file(sq8_path) != SQ8_SHA256 or sq8_path.stat().st_size != ROWS * ROW_BYTES:
        raise ValueError("SQ8 input identity differs")
    manifest = load_manifest(manifest_path)
    sq8 = np.memmap(sq8_path, dtype=SQ8_DTYPE, mode="r", shape=(ROWS,))
    all_ids = np.asarray(sq8["id"])
    order = np.argsort(all_ids, kind="stable")
    sorted_ids = all_ids[order]
    if np.any(sorted_ids[1:] == sorted_ids[:-1]):
        raise ValueError("SQ8 stable IDs are not unique")

    def verify_returned(ids: list[int], ranges: tuple[tuple[int, int], ...]) -> None:
        if len(ids) != 100 or len(set(ids)) != 100:
            raise ValueError("returned top-100 IDs differ")
        positions = np.searchsorted(sorted_ids, np.asarray(ids, dtype=np.int64))
        if np.any(positions >= ROWS) or not np.array_equal(sorted_ids[positions], ids):
            raise ValueError("returned ID absent from SQ8 object")
        starts = [start // ROW_BYTES for start, _ in ranges]
        ends = [end // ROW_BYTES for _, end in ranges]
        for row in order[positions]:
            slot = bisect.bisect_right(starts, int(row)) - 1
            if slot < 0 or row >= ends[slot]:
                raise ValueError("returned ID was not in a fetched interval")

    counts = {"historical": [], "capped": []}
    gets = {"historical": [], "capped": []}
    bytes_read = {"historical": [], "capped": []}
    violations = {"historical": 0, "capped": 0}
    with evidence.open() as handle:
        header = json.loads(handle.readline())
        if (header.get("schema") != "borsuk-v109-capped-replay-v1"
            or header.get("manifest_sha256") != manifest_sha
            or header.get("sq8_sha256") != SQ8_SHA256
            or header.get("max_gets") != MAX_GETS
            or header.get("max_bytes") != MAX_BYTES):
            raise ValueError("replay header identity differs")
        query_count = header["query_count"]
        if type(query_count) is not int or not 1 <= query_count <= 1000:
            raise ValueError("query count differs")
        for ordinal in range(query_count):
            line = handle.readline()
            if not line:
                raise ValueError("replay ended before query roster")
            record = json.loads(line)
            if record.get("query_ordinal") != ordinal:
                raise ValueError("query roster differs")
            ranked = record["ranked_pages"]
            historical = record["historical_pages"]
            if sorted(set(ranked)) != historical:
                raise ValueError("ranked/historical page set differs")
            old = plan_page_ranges(historical, gap_pages=2, rows=ROWS,
                                   page_rows=256, row_bytes=ROW_BYTES)
            new = admit_ranked_pages(ranked, rows=ROWS, page_rows=256,
                                     row_bytes=ROW_BYTES, max_gets=MAX_GETS,
                                     max_bytes=MAX_BYTES)
            if (record["historical_gets"], record["historical_bytes"]) != (old.gets, old.bytes):
                raise ValueError("historical plan differs")
            if (record["capped_gets"], record["capped_bytes"]) != (new.plan.gets, new.plan.bytes):
                raise ValueError("capped plan differs")
            if (tuple(record["capped_nominated_pages"]) != new.nominated_pages
                or tuple(record["capped_rejected_pages"]) != new.rejected_pages):
                raise ValueError("capped admission differs")
            truth = set(map(int, manifest["truth"][ordinal]))
            for label, plan, ids_key, hits_key in (
                ("historical", old, "historical_returned_ids", "historical_hits"),
                ("capped", new.plan, "capped_returned_ids", "capped_hits"),
            ):
                ids = record[ids_key]
                verify_returned(ids, plan.ranges)
                hits = len(truth.intersection(ids))
                if record[hits_key] != hits:
                    raise ValueError(f"{label} truth hit count differs")
                counts[label].append(hits)
                gets[label].append(plan.gets)
                bytes_read[label].append(plan.bytes)
                violations[label] += not plan.within(gets=MAX_GETS, bytes_limit=MAX_BYTES)
        if handle.readline():
            raise ValueError("replay has extra queries")
    result: dict[str, object] = {
        "schema": "borsuk-v109-capped-reduction-v1",
        "evidence_sha256": sha256_file(evidence), "manifest_sha256": manifest_sha,
        "sq8_sha256": SQ8_SHA256, "query_count": query_count,
        "regions": header["regions"], "shortlist": header["shortlist"],
    }
    for label in ("historical", "capped"):
        result[label] = {
            "hits": sum(counts[label]),
            "recall100_ppm": sum(counts[label]) * 10_000 // query_count,
            "p05_hits": nearest_rank(counts[label], 5, 100),
            "sub90_queries": sum(hit < 90 for hit in counts[label]),
            "gets_p50": nearest_rank(gets[label], 50, 100),
            "gets_p95": nearest_rank(gets[label], 95, 100),
            "gets_max": max(gets[label]), "bytes_max": max(bytes_read[label]),
            "bytes_p50": nearest_rank(bytes_read[label], 50, 100),
            "cap_violations": violations[label],
        }
    if query_count >= 200:
        prefix_hits = sum(counts["historical"][:200])
        prefix_gets_p95 = nearest_rank(gets["historical"][:200], 95, 100)
        prefix_bytes_p50 = nearest_rank(bytes_read["historical"][:200], 50, 100)
        result["historical_prefix_200"] = {
            "hits": prefix_hits, "gets_p95": prefix_gets_p95,
            "bytes_p50": prefix_bytes_p50,
            "matches_v77_v78_control": (
                abs(prefix_hits - 19831) <= 10
                and abs(prefix_gets_p95 - 61) <= 3
                and abs(prefix_bytes_p50 - 10782720) <= 199680
            ),
        }
    result["paired_capped_minus_historical_hits"] = (
        result["capped"]["hits"] - result["historical"]["hits"]
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sq8", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = reduce_replay(args.evidence, args.manifest, args.sq8)
    args.output.write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
