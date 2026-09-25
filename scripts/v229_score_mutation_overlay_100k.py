#!/usr/bin/env python3
"""Score sealed same-vector upsert IDs against frozen V218 and GT100."""

import argparse
import json
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256
from scripts.v210_resident_neighborhood_100k import canonical
from scripts.v218_score_reachable_graph_100k import hits

BASE_SHA = "900872f9572bff1a195c4e3ed595d3ee28a8aa6f6555f5cc082c1ada52882fa0"
BASE_SPLITS = (25537, 74234)
BASE_P95_NS = 6_915_250


def run(args: argparse.Namespace) -> None:
    serving = json.loads(args.serving.read_text())
    current = list(jsonl(args.raw))
    baseline = list(jsonl(args.baseline))
    truth = load_truth(args.truth)
    if (serving.get("schema") != "borsuk-v229-mutation-overlay-100k-serving-v1"
            or serving.get("raw_sha256") != sha256(args.raw)
            or serving.get("upsert_rows") != 1000
            or serving.get("upsert_stride") != 100
            or serving.get("unchanged_vectors") is not True
            or serving.get("vector_body_gets") != 0
            or serving.get("delta_rows_scanned_total") != 1_000_000
            or sha256(args.baseline) != BASE_SHA
            or len(current) != 1000 or len(baseline) != 1000 or len(truth) < 1000):
        raise ValueError("overlay or paired input identity differs")
    candidate_hits, baseline_hits, exact_id_ties = [], [], []
    for ordinal, (new, old) in enumerate(zip(current, baseline, strict=True)):
        if (new["ordinal"] != ordinal or old["ordinal"] != ordinal
                or new["delta_rows_scanned"] != 1000
                or new["vector_body_gets"] != 0):
            raise ValueError("query identity or delta count differs")
        new_ids = new["returned_ids"]
        old_ids = old["arms"]["2048-2048"]["returned_ids"]
        candidate_hits.append(hits(new_ids, truth[ordinal]))
        baseline_hits.append(hits(old_ids, truth[ordinal]))
        exact_id_ties.append(new_ids == old_ids)
    if (sum(baseline_hits[:256]), sum(baseline_hits[256:])) != BASE_SPLITS:
        raise ValueError("V218 baseline hits differ")

    def split(lo: int, hi: int) -> dict:
        new = candidate_hits[lo:hi]
        return {"queries": hi - lo, "candidate_hits": sum(new),
                "v218_hits": sum(baseline_hits[lo:hi]),
                "exact_id_ties": sum(exact_id_ties[lo:hi]),
                "p05_hits_per_query": sorted(new)[(hi - lo + 19) // 20 - 1]}

    development, heldout, combined = split(0, 256), split(256, 1000), split(0, 1000)
    loaded = serving["loaded"]
    passed = (development["candidate_hits"] >= BASE_SPLITS[0]
              and heldout["candidate_hits"] >= BASE_SPLITS[1]
              and combined["p05_hits_per_query"] >= 99
              and loaded["p95_ns"] <= 2 * BASE_P95_NS
              and serving["process_peak_rss_bytes"] <= 256 * 1024 * 1024)
    args.output.write_text(canonical({
        "schema": "borsuk-v229-mutation-overlay-100k-quality-v1",
        "dataset": "ReLAION-100k D768", "k": 100,
        "split": "development-256-plus-method-heldout-744-prior-used",
        "development": development, "method_heldout": heldout, "combined": combined,
        "pass": passed, "loaded": loaded,
        "v218_loaded_p95_ns": BASE_P95_NS,
        "peak_rss_bytes": serving["process_peak_rss_bytes"],
        "overlay_resident_bytes": serving["overlay_resident_bytes"],
        "masked_shortlist_rows_total": serving["masked_shortlist_rows_total"],
        "candidate_raw_sha256": sha256(args.raw),
        "baseline_raw_sha256": BASE_SHA,
        "truth_sha256": sha256(args.truth),
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("serving", "raw", "baseline", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    run(parser.parse_args())
