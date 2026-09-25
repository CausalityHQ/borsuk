#!/usr/bin/env python3
"""Score screened mutation delta against a same-run exact full scan."""

import argparse
import json
import math
from pathlib import Path

from scripts.v232_score_decoded_delta_100k import (
    BASE_SHA, BASE_SPLITS, jsonl, load_truth, percentiles, sha256,
)


def run(args):
    control, candidate = (json.loads(path.read_text()) for path in
                          (args.control_serving, args.candidate_serving))
    for mode, serving, raw, loaded_raw, schema in (
        ("decoded-10k", control, args.control_raw, args.control_loaded_raw,
         "borsuk-v232-decoded-delta-100k-serving-v1"),
        ("screened-10k", candidate, args.candidate_raw, args.candidate_loaded_raw,
         "borsuk-v235-screened-delta-100k-serving-v1"),
    ):
        if (serving["schema"] != schema or serving["mode"] != mode
                or serving["upsert_rows"] != 10_000 or serving["upsert_stride"] != 10
                or serving["vector_body_gets"] != 0
                or serving["delta_rows_scanned_total"] != 10_000_000
                or serving["raw_sha256"] != sha256(raw)
                or serving["loaded_raw_sha256"] != sha256(loaded_raw)):
            raise ValueError(f"{mode} identity differs")
        measured = percentiles(list(jsonl(loaded_raw)))
        if (any(serving["loaded"][name] != value for name, value in measured.items())
                or not math.isclose(serving["loaded"]["qps"],
                                    1000 * 1e9 / serving["loaded_wall_ns"], rel_tol=1e-12)):
            raise ValueError(f"{mode} loaded raw differs")
    if sha256(args.baseline) != BASE_SHA:
        raise ValueError("V218 baseline identity differs")
    base, control_rows, candidate_rows = (list(jsonl(path)) for path in
                                        (args.baseline, args.control_raw, args.candidate_raw))
    truth = load_truth(args.truth)
    if any(len(rows) != 1000 for rows in (base, control_rows, candidate_rows)) or len(truth) < 1000:
        raise ValueError("query panel length differs")
    hits, control_hits, base_hits, exact_rows, matching_lists = [], [], [], [], 0
    for ordinal, (old, row, screened) in enumerate(
            zip(base, control_rows, candidate_rows, strict=True)):
        ids, other = row["returned_ids"], screened["returned_ids"]
        if (old["ordinal"] != ordinal or row["ordinal"] != ordinal
                or screened["ordinal"] != ordinal
                or any(len(values) != 100 or len(set(values)) != 100
                       for values in (ids, other))
                or row["delta_rows_scanned"] != 10_000
                or screened["delta_rows_scanned"] != 10_000
                or row["delta_rows_scored"] != 10_000
                or not 0 <= screened["delta_rows_scored"] <= 10_000
                or row["vector_body_gets"] != 0 or screened["vector_body_gets"] != 0):
            raise ValueError(f"query geometry differs at {ordinal}")
        matching_lists += ids == other
        exact_rows.append(screened["delta_rows_scored"])
        gold = set(truth[ordinal])
        base_hits.append(len(set(old["arms"]["2048-2048"]["returned_ids"]) & gold))
        control_hits.append(len(set(ids) & gold))
        hits.append(len(set(other) & gold))
    if ((sum(base_hits[:256]), sum(base_hits[256:])) != BASE_SPLITS
            or candidate["delta_rows_scored_total"] != sum(exact_rows)
            or control["delta_rows_scored_total"] != 10_000_000):
        raise ValueError("baseline or rescore count differs")
    quality = (matching_lists == 1000 and sum(hits[:256]) >= BASE_SPLITS[0]
               and sum(hits[256:]) >= BASE_SPLITS[1] and sorted(hits)[49] >= 99)
    performance = (candidate["loaded"]["p95_ns"] * 4 <= control["loaded"]["p95_ns"] * 3
                   and candidate["loaded"]["qps"] * 2 >= control["loaded"]["qps"] * 3
                   and candidate["decode_ns"] <= 1_000_000_000
                   and candidate["process_peak_rss_bytes"] <= 512 * 1024 * 1024
                   and candidate["overlay_resident_bytes"] <= 64 * 1024 * 1024)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v235-screened-delta-100k-quality-v1",
        "dataset": "ReLAION-100k D768",
        "split": "development-256-plus-method-heldout-744-prior-used", "k": 100,
        "development": {"v218_hits": BASE_SPLITS[0], "control_hits": sum(control_hits[:256]),
                        "candidate_hits": sum(hits[:256])},
        "method_heldout": {"v218_hits": BASE_SPLITS[1], "control_hits": sum(control_hits[256:]),
                           "candidate_hits": sum(hits[256:])},
        "control_hits": sum(control_hits), "candidate_hits": sum(hits),
        "candidate_p05_hits": sorted(hits)[49], "exact_control_id_lists": matching_lists,
        "loaded": {"control": control["loaded"], "candidate": candidate["loaded"]},
        "exact_delta_rows_scored_total": sum(exact_rows),
        "exact_delta_rows_scored_p95": sorted(exact_rows)[949],
        "exact_delta_rows_scored_max": max(exact_rows),
        "quality_pass": quality, "performance_pass": performance,
        "pass": quality and performance,
        "control_raw_sha256": sha256(args.control_raw),
        "candidate_raw_sha256": sha256(args.candidate_raw),
        "control_loaded_raw_sha256": sha256(args.control_loaded_raw),
        "candidate_loaded_raw_sha256": sha256(args.candidate_loaded_raw),
        "truth_sha256": sha256(args.truth),
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("control_serving", "control_raw", "control_loaded_raw",
                 "candidate_serving", "candidate_raw", "candidate_loaded_raw",
                 "baseline", "truth", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
