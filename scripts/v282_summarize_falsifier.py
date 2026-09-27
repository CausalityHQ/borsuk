"""Summarize paired V282 local quality and planned-I/O evidence."""

import argparse
import json
import math
from pathlib import Path


def percentile(values, percent):
    values = sorted(values)
    return values[math.ceil(len(values) * percent / 100) - 1]


def summarize(path: Path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    if len(records) != 2000:
        raise ValueError("expected 1,000 paired queries")
    by_key = {(row["ordinal"], row["arm"]): row for row in records}
    if len(by_key) != 2000 or set(by_key) != {
        (ordinal, arm) for ordinal in range(1000) for arm in ("graph", "flat")
    }:
        raise ValueError("paired query roster differs")
    result = {}
    for split, start, stop in (("development", 0, 256), ("validation", 256, 1000)):
        arms = {}
        for arm in ("graph", "flat"):
            rows = [by_key[ordinal, arm] for ordinal in range(start, stop)]
            for row in rows:
                hits = [row[key] for key in ("router_shortlist_gt_hits", "fetched_gt_hits",
                                            "sq8_returned_gt_hits")]
                if (any(type(value) is not int or not 0 <= value <= 100 for value in hits)
                        or hits[2] > hits[1] or not 1 <= row["gets"] <= 32
                        or not 1 <= row["bytes"] <= 16_777_216):
                    raise ValueError("query quality or physical cap differs")
            arms[arm] = {
                "queries": len(rows),
                "mean_router_shortlist_gt_hits": sum(r["router_shortlist_gt_hits"] for r in rows) / len(rows),
                "mean_fetched_gt_hits": sum(r["fetched_gt_hits"] for r in rows) / len(rows),
                "mean_returned_gt_hits": sum(r["sq8_returned_gt_hits"] for r in rows) / len(rows),
                "p05_returned_gt_hits": percentile([r["sq8_returned_gt_hits"] for r in rows], 5),
                "p95_gets": percentile([r["gets"] for r in rows], 95),
                "p95_bytes": percentile([r["bytes"] for r in rows], 95),
                "p95_plan_us": percentile([r["plan_us"] for r in rows], 95),
                "p95_rank_us": percentile([r["rank_us"] for r in rows], 95),
                "max_rss_bytes": max(r["rss_bytes"] for r in rows),
            }
        graph, flat = arms["graph"], arms["flat"]
        result[split] = {
            "arms": arms,
            "v282_pass": (graph["mean_returned_gt_hits"] >= 98
                          and graph["p05_returned_gt_hits"] >= 95
                          and flat["mean_returned_gt_hits"] - graph["mean_returned_gt_hits"] <= 0.5),
        }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--relaion", type=Path, required=True)
    parser.add_argument("--cohere", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = {
        "schema": "borsuk-v282-paired-falsifier-v1",
        "dataset_rows": 100_000, "dimensions": 768, "metric": "cosine", "k": 100,
        "relaion_100k": summarize(args.relaion),
        "cohere_first100k": summarize(args.cohere),
    }
    output["advance"] = all(
        output[dataset][split]["v282_pass"]
        for dataset in ("relaion_100k", "cohere_first100k")
        for split in ("development", "validation")
    )
    args.output.write_text(json.dumps(output, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(output, sort_keys=True))
