#!/usr/bin/env python3
"""Independently reduce V112's physical intervals and returned GT100 hits."""

from __future__ import annotations

import argparse
import bisect
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v109_capped_reader_replay import (
    MAX_BYTES, MAX_GETS, ROW_BYTES, ROWS, SQ8_DTYPE, SQ8_SHA256,
    load_manifest, nominate, score_ranges, sha256_file,
)
from scripts.v109_range_plan import admit_ranked_pages, plan_page_ranges
from scripts.v110_physical_interval_oracle import optimal_truth_hits
from scripts.v111_weighted_reader_replay import nominate_rows, weighted_plan
from scripts.validate_v109_capped_replay import nearest_rank, reduce_replay


def reduce_precise(evidence: Path, manifest_path: Path, sq8_path: Path) -> dict[str, object]:
    with evidence.open() as handle:
        header = json.loads(handle.readline())
        if (header.get("schema") != "borsuk-v112-precise-nominee-replay-v1"
            or header.get("manifest_sha256") != sha256_file(manifest_path)
            or header.get("sq8_sha256") != SQ8_SHA256
            or header.get("weight") != "local-SQ8-top100-of-PQ64-top512-plus-secondary"
            or header.get("primary_rows") != 100
            or header.get("primary_factor") != 513
            or header.get("serving_eligible") is not False
            or header.get("regions") != 1024
            or header.get("shortlist") != 512
            or header.get("query_count") not in (200, 1000)
            or header.get("max_gets") != MAX_GETS
            or header.get("max_bytes") != MAX_BYTES):
            raise ValueError("V112 evidence header differs")
        # V109's independent baseline reducer checks both paired controls,
        # their returned-ID membership, and all historical identities.
        baseline_header = dict(header, schema="borsuk-v109-capped-replay-v1")
        with tempfile.TemporaryDirectory() as folder:
            baseline_evidence = Path(folder) / "baseline.jsonl"
            with baseline_evidence.open("w") as target:
                target.write(json.dumps(baseline_header, sort_keys=True) + "\n")
                for line in handle:
                    target.write(line)
            baseline = reduce_replay(baseline_evidence, manifest_path, sq8_path)

    manifest = load_manifest(manifest_path)
    sq8 = np.memmap(sq8_path, dtype=SQ8_DTYPE, mode="r", shape=(ROWS,))
    ids = np.asarray(sq8["id"])
    order = np.argsort(ids, kind="stable")
    sorted_ids = ids[order]
    if np.any(sorted_ids[1:] == sorted_ids[:-1]):
        raise ValueError("SQ8 stable IDs are not unique")
    hits: list[int] = []
    gets: list[int] = []
    byte_counts: list[int] = []
    violations = 0
    with evidence.open() as handle:
        handle.readline()
        for ordinal in range(header["query_count"]):
            record = json.loads(handle.readline())
            if record["query_ordinal"] != ordinal:
                raise ValueError("V112 query roster differs")
            query = manifest["queries"][ordinal]
            ranked, historical, _, nominated_rows = nominate_rows(
                query, manifest,
                regions=header["regions"], shortlist=header["shortlist"],
            )
            control_ranked, control_historical = nominate(
                query, manifest, regions=1024, shortlist=512,
            )
            if (ranked != control_ranked or historical != control_historical
                or record["ranked_pages"] != control_ranked
                or record["historical_pages"] != control_historical):
                raise ValueError("V112 paired nomination differs")
            nominated_records = sq8[nominated_rows]
            score_weights = query * manifest["span_step"]
            score_shift = float(query @ manifest["low"] - (query @ query) / 2.0)
            nominee_scores = nominated_records["norm"] - 2.0 * (
                nominated_records["code"].astype(np.float32) @ score_weights
                + score_shift
            )
            if not np.isfinite(nominee_scores).all():
                raise ValueError("V112 nominated SQ8 score differs")
            precise_ordinals = set(map(int, np.lexsort(
                (nominated_records["id"], nominee_scores),
            )[:100]))
            primary: dict[int, int] = {}
            secondary: dict[int, int] = {}
            for index, row in enumerate(nominated_rows):
                page = int(row) // 256
                votes = primary if index in precise_ordinals else secondary
                votes[page] = votes.get(page, 0) + 1
            weights = {page: 513 * primary.get(page, 0) + secondary.get(page, 0)
                       for page in primary.keys() | secondary.keys()}
            if {int(key): int(value) for key, value in record["page_weights"].items()} != weights:
                raise ValueError("V112 precise nominee weights differ")
            objective, plan = weighted_plan(weights)
            independent_objective = optimal_truth_hits(
                weights, page_count=(ROWS + 255) // 256,
                max_gets=MAX_GETS, max_units=MAX_BYTES // (64 * ROW_BYTES),
                full_page_units=4,
                last_page_units=(ROWS - (((ROWS + 255) // 256) - 1) * 256) // 64,
            )
            if (objective != independent_objective
                or record["precise_objective"] != objective
                or tuple(map(tuple, record["precise_ranges"])) != plan.ranges
                or record["precise_gets"] != plan.gets
                or record["precise_bytes"] != plan.bytes):
                raise ValueError("V112 precise physical optimum differs")
            witnessed_weight = sum(
                weight for page, weight in weights.items()
                if any(start <= page * 256 * ROW_BYTES < end
                       for start, end in plan.ranges)
            )
            if witnessed_weight != objective:
                raise ValueError("V112 interval witness weight differs")
            old = plan_page_ranges(control_historical, gap_pages=2, rows=ROWS,
                                   page_rows=256, row_bytes=ROW_BYTES)
            capped = admit_ranked_pages(
                control_ranked, rows=ROWS, page_rows=256, row_bytes=ROW_BYTES,
                max_gets=MAX_GETS, max_bytes=MAX_BYTES,
            ).plan
            for label, ranges in (("historical", old.ranges),
                                  ("capped", capped.ranges),
                                  ("precise", plan.ranges)):
                rescored = score_ranges(sq8, query, manifest, ranges)
                if record[f"{label}_returned_ids"] != rescored:
                    raise ValueError(f"V112 {label} returned SQ8 ranking differs")
            selected = record["precise_returned_ids"]
            if len(selected) != 100 or len(set(selected)) != 100:
                raise ValueError("V112 returned roster differs")
            positions = np.searchsorted(sorted_ids, np.asarray(selected, dtype=np.int64))
            if np.any(positions >= ROWS) or not np.array_equal(sorted_ids[positions], selected):
                raise ValueError("V112 returned ID absent from SQ8 object")
            starts = [start // ROW_BYTES for start, _ in plan.ranges]
            ends = [end // ROW_BYTES for _, end in plan.ranges]
            for row in order[positions]:
                slot = bisect.bisect_right(starts, int(row)) - 1
                if slot < 0 or row >= ends[slot]:
                    raise ValueError("V112 returned ID outside fetched intervals")
            truth = set(map(int, manifest["truth"][ordinal]))
            count = len(truth.intersection(selected))
            if record["precise_hits"] != count:
                raise ValueError("V112 precise truth hit count differs")
            hits.append(count)
            gets.append(plan.gets)
            byte_counts.append(plan.bytes)
            violations += not plan.within(gets=MAX_GETS, bytes_limit=MAX_BYTES)
        if handle.readline():
            raise ValueError("V112 extra query record")
    reduction = {"schema": "borsuk-v112-precise-nominee-reduction-v1",
                 "evidence_sha256": sha256_file(evidence),
                 "query_count": header["query_count"],
                 "manifest_sha256": header["manifest_sha256"],
                 "sq8_sha256": SQ8_SHA256,
                 "historical": baseline["historical"],
                 "capped": baseline["capped"],
                 "historical_prefix_200": baseline.get("historical_prefix_200"),
                 "precise": {
                     "hits": sum(hits), "recall100_ppm": sum(hits) * 10_000 // len(hits),
                     "p05_hits": nearest_rank(hits, 5, 100),
                     "sub90_queries": sum(hit < 90 for hit in hits),
                     "gets_p50": nearest_rank(gets, 50, 100),
                     "gets_p95": nearest_rank(gets, 95, 100),
                     "gets_max": max(gets),
                     "bytes_p50": nearest_rank(byte_counts, 50, 100),
                     "bytes_max": max(byte_counts),
                     "cap_violations": violations},
                 "paired_precise_minus_capped_hits": sum(hits) - baseline["capped"]["hits"]}
    return reduction


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sq8", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reduction = reduce_precise(args.evidence, args.manifest, args.sq8)
    args.output.write_text(json.dumps(reduction, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
