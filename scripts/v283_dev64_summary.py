"""Apply the frozen V283 CoHere development-only kill gate."""

import argparse
import hashlib
import json
import math
from pathlib import Path


def _rows(path, *, full=False):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    if full:
        if len(records) != 2000 or {(r["ordinal"], r["arm"]) for r in records} != {
            (ordinal, arm) for ordinal in range(1000) for arm in ("graph", "flat")
        }:
            raise ValueError("V282 baseline roster differs")
        records = [r for r in records if r["ordinal"] < 64]
    if len(records) != 128 or {(r["ordinal"], r["arm"]) for r in records} != {
        (ordinal, arm) for ordinal in range(64) for arm in ("graph", "flat")
    }:
        raise ValueError("development query roster differs")
    for row in records:
        hits = [row[key] for key in ("router_shortlist_gt_hits", "fetched_gt_hits",
                                    "sq8_returned_gt_hits")]
        if (any(type(hit) is not int or not 0 <= hit <= 100 for hit in hits)
                or hits[2] > hits[1] or type(row["gets"]) is not int
                or not 1 <= row["gets"] <= 32 or type(row["bytes"]) is not int
                or not 1 <= row["bytes"] <= 16_777_216):
            raise ValueError("development quality or physical cap differs")
    return records


def _metrics(records, arm):
    selected = [row for row in records if row["arm"] == arm]
    hits = sorted(row["sq8_returned_gt_hits"] for row in selected)
    return {
        "mean_router_shortlist_gt_hits": sum(r["router_shortlist_gt_hits"] for r in selected) / 64,
        "mean_fetched_gt_hits": sum(r["fetched_gt_hits"] for r in selected) / 64,
        "mean_returned_gt_hits": sum(hits) / 64,
        "p05_returned_gt_hits": hits[math.ceil(64 * 0.05) - 1],
        "max_gets": max(r["gets"] for r in selected),
        "max_bytes": max(r["bytes"] for r in selected),
        "max_rss_bytes": max(r["rss_bytes"] for r in selected),
    }


def summarize(candidate: Path, baseline: Path, baseline_sha: str) -> dict:
    baseline_raw = baseline.read_bytes()
    if hashlib.sha256(baseline_raw).hexdigest() != baseline_sha:
        raise ValueError("V282 baseline digest differs")
    old = _metrics(_rows(baseline, full=True), "graph")
    if old["mean_returned_gt_hits"] != 96.25 or old["p05_returned_gt_hits"] != 92:
        raise ValueError("V282 development control differs")
    records = _rows(candidate)
    graph, flat = _metrics(records, "graph"), _metrics(records, "flat")
    return {
        "schema": "borsuk-v283-dev64-summary-v1",
        "dataset": "CoHere first100k D768 cosine k100",
        "split": "development0-63", "queries": 64,
        "baseline_sha256": baseline_sha,
        "candidate_sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
        "baseline_graph": old, "candidate_graph": graph, "candidate_flat": flat,
        "advance_dev256": graph["mean_returned_gt_hits"] >= 97.25
        and graph["p05_returned_gt_hits"] >= 92,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--baseline-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.candidate, args.baseline, args.baseline_sha)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
