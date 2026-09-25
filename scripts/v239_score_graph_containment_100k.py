#!/usr/bin/env python3
"""Score sealed PQ candidate prefixes against the fixed GT100 witness."""

import argparse
import json
from math import ceil
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256

SIZES = (512, 1024, 2048, 4096)
PAGE_ROWS = 256
PAGE_BYTES = PAGE_ROWS * (768 + 12)
BYTE_CAP = 16 * 1024 * 1024


def score(raw: Path, summary: Path, truth_path: Path) -> dict:
    meta = json.loads(summary.read_text())
    if (meta.get("schema") != "borsuk-v239-graph-containment-100k-v1"
            or meta.get("raw_sha256") != sha256(raw)
            or meta.get("queries") != 1000 or meta.get("max_shortlist") != 4096):
        raise ValueError("sealed candidate identity differs")
    truth = load_truth(truth_path)
    if len(truth) < 1000:
        raise ValueError("GT100 panel length differs")
    counts = {kind: {size: [] for size in SIZES} for kind in ("flat", "graph")}
    pages = {kind: {size: [] for size in SIZES} for kind in counts}
    seen = 0
    for ordinal, row in enumerate(jsonl(raw)):
        if ordinal >= 1000:
            raise ValueError("candidate panel length differs")
        if row["ordinal"] != ordinal:
            raise ValueError("candidate ordinal differs")
        witness = set(truth[ordinal])
        if len(witness) != 100:
            raise ValueError("GT100 witness differs")
        for kind in counts:
            ids = row[kind + "_ids"]
            physical = row[kind + "_rows"]
            if (len(ids) != 4096 or len(set(ids)) != 4096
                    or len(physical) != 4096 or len(set(physical)) != 4096
                    or any(not isinstance(index, int) or index < 0 or index >= 100000
                           for index in physical)):
                raise ValueError("candidate roster differs")
            for size in SIZES:
                counts[kind][size].append(len(witness.intersection(ids[:size])))
                pages[kind][size].append(len({index // PAGE_ROWS for index in physical[:size]}))
        seen += 1
    if seen != 1000:
        raise ValueError("candidate panel length differs")

    def split(start: int, end: int) -> dict:
        return {kind: {str(size): {
            "hits": sum(values[start:end]),
            "p05_hits_per_query": sorted(values[start:end])[ceil((end-start)*.05)-1],
            "p95_distinct_pages": sorted(pages[kind][size][start:end])[ceil((end-start)*.95)-1],
            "p95_minimum_page_bytes":
                sorted(pages[kind][size][start:end])[ceil((end-start)*.95)-1] * PAGE_BYTES,
        } for size, values in by_size.items()} for kind, by_size in counts.items()}

    development, heldout, combined = split(0, 256), split(256, 1000), split(0, 1000)
    passing = [size for size in SIZES if all(
        part["graph"][str(size)]["hits"] >= (queries * 100 * 996 + 999) // 1000
        and part["graph"][str(size)]["p05_hits_per_query"] >= 98
        for part, queries in ((development, 256), (heldout, 744)))]
    smallest = min(passing) if passing else None
    return {
        "schema": "borsuk-v239-graph-containment-100k-quality-v1",
        "dataset": "ReLAION-100k D768", "k": 100,
        "development_first_256_prior_used": development,
        "method_heldout_744_prior_used": heldout,
        "combined_1000_prior_used": combined,
        "smallest_passing_graph_prefix": smallest,
        "gate_pass": smallest is not None
            and combined["graph"][str(smallest)]["p95_minimum_page_bytes"] <= BYTE_CAP,
        "raw_sha256": sha256(raw), "summary_sha256": sha256(summary),
        "truth_sha256": sha256(truth_path),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "summary", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(score(args.raw, args.summary, args.truth), sort_keys=True))
