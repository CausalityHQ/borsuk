#!/usr/bin/env python3
"""Score blocked exact mutation scan against same-run decoded control."""

import argparse
import json
import math
from pathlib import Path

from scripts.v232_score_decoded_delta_100k import (
    BASE_SHA, BASE_SPLITS, jsonl, load_truth, percentiles, sha256,
)


def run(args):
    control = json.loads(args.control_serving.read_text())
    blocked = json.loads(args.blocked_serving.read_text())
    for mode, serving, raw, loaded_raw, schema in (
        ("decoded-10k", control, args.control_raw, args.control_loaded_raw,
         "borsuk-v232-decoded-delta-100k-serving-v1"),
        ("blocked-10k", blocked, args.blocked_raw, args.blocked_loaded_raw,
         "borsuk-v234-blocked-delta-100k-serving-v1"),
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
    base, control_rows, blocked_rows = (list(jsonl(path)) for path in
                                        (args.baseline, args.control_raw, args.blocked_raw))
    truth = load_truth(args.truth)
    if any(len(rows) != 1000 for rows in (base, control_rows, blocked_rows)) or len(truth) < 1000:
        raise ValueError("query panel length differs")
    hits, exact, control_hits, base_hits = [], 0, [], []
    for ordinal, (old, row, candidate) in enumerate(
            zip(base, control_rows, blocked_rows, strict=True)):
        ids, other = row["returned_ids"], candidate["returned_ids"]
        if (old["ordinal"] != ordinal or row["ordinal"] != ordinal
                or candidate["ordinal"] != ordinal
                or any(len(values) != 100 or len(set(values)) != 100
                       for values in (ids, other))
                or row["delta_rows_scanned"] != 10_000
                or candidate["delta_rows_scanned"] != 10_000
                or row["vector_body_gets"] != 0 or candidate["vector_body_gets"] != 0):
            raise ValueError(f"query geometry differs at {ordinal}")
        exact += ids == other
        gold = set(truth[ordinal])
        base_hits.append(len(set(old["arms"]["2048-2048"]["returned_ids"]) & gold))
        control_hits.append(len(set(ids) & gold))
        hits.append(len(set(other) & gold))
    if (sum(base_hits[:256]), sum(base_hits[256:])) != BASE_SPLITS:
        raise ValueError("V218 baseline GT100 differs")
    quality = (exact == 1000 and sum(hits[:256]) >= BASE_SPLITS[0]
               and sum(hits[256:]) >= BASE_SPLITS[1] and sorted(hits)[49] >= 99)
    performance = (blocked["loaded"]["p95_ns"] * 2 <= control["loaded"]["p95_ns"]
                   and blocked["loaded"]["qps"] * 2 >= control["loaded"]["qps"] * 3
                   and blocked["decode_ns"] <= 1_000_000_000
                   and blocked["process_peak_rss_bytes"] <= 512 * 1024 * 1024
                   and blocked["overlay_resident_bytes"] <= 64 * 1024 * 1024)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v234-blocked-delta-100k-quality-v1",
        "dataset": "ReLAION-100k D768",
        "split": "development-256-plus-method-heldout-744-prior-used", "k": 100,
        "development": {"v218_hits": BASE_SPLITS[0], "control_hits": sum(control_hits[:256]),
                        "blocked_hits": sum(hits[:256])},
        "method_heldout": {"v218_hits": BASE_SPLITS[1], "control_hits": sum(control_hits[256:]),
                           "blocked_hits": sum(hits[256:])},
        "control_hits": sum(control_hits), "blocked_hits": sum(hits),
        "blocked_p05_hits": sorted(hits)[49], "exact_control_id_lists": exact,
        "loaded": {"control": control["loaded"], "blocked": blocked["loaded"]},
        "quality_pass": quality, "performance_pass": performance,
        "pass": quality and performance,
        "control_raw_sha256": sha256(args.control_raw),
        "blocked_raw_sha256": sha256(args.blocked_raw),
        "control_loaded_raw_sha256": sha256(args.control_loaded_raw),
        "blocked_loaded_raw_sha256": sha256(args.blocked_loaded_raw),
        "truth_sha256": sha256(args.truth),
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("control_serving", "control_raw", "control_loaded_raw",
                 "blocked_serving", "blocked_raw", "blocked_loaded_raw",
                 "baseline", "truth", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
