#!/usr/bin/env python3
"""Recount score-rank and physical-neighbor diagnostics from sealed V140 plans."""

from __future__ import annotations

import argparse
import json
import struct
from pathlib import Path


def records(path: Path) -> list[dict]:
    values = [json.loads(line) for line in path.read_text().splitlines()]
    if len(values) != 1000 or any(row["query_ordinal"] != i for i, row in enumerate(values)):
        raise ValueError(f"frozen query inventory differs: {path}")
    return values


def quantiles(values: list[int]) -> dict[str, int]:
    sorted_values = sorted(values)
    return {"p50": sorted_values[499], "p95": sorted_values[949], "max": sorted_values[-1]}


def run(raw_path: Path, scores_path: Path, primary_path: Path, rows: int) -> dict:
    raw = records(raw_path)
    primary = records(primary_path)
    page_count = (rows + 255) // 256
    scores = scores_path.read_bytes()
    if len(scores) != 1000 * page_count * 4:
        raise ValueError("score matrix byte count differs")
    selected_counts: list[int] = []
    worst_selected_ranks: list[int] = []
    top256_complete = 0
    distance_counts = {8: 0, 32: 0}
    total_selected = 0
    for ordinal, (plan, roster) in enumerate(zip(raw, primary)):
        values = struct.unpack_from(f"<{page_count}f", scores, ordinal * page_count * 4)
        rank = {
            page: position + 1
            for position, page in enumerate(
                sorted(range(page_count), key=lambda page: (values[page], page))
            )
        }
        selected = plan["variants"]["4"]["selected_pages"]
        primary_pages = set(physical // 256 for physical in roster["primary"])
        if not selected or not primary_pages:
            raise ValueError(f"empty page roster: {ordinal}")
        selected_counts.append(len(selected))
        worst_rank = max(rank[page] for page in selected)
        worst_selected_ranks.append(worst_rank)
        top256_complete += worst_rank <= 256
        total_selected += len(selected)
        for page in selected:
            distance = min(abs(page - other) for other in primary_pages)
            for radius in distance_counts:
                distance_counts[radius] += distance <= radius
    return {
        "schema": "borsuk-v147-route-diagnostics-v1",
        "rows": rows,
        "query_count": 1000,
        "selected_pages": total_selected,
        "selected_count": quantiles(selected_counts),
        "worst_selected_score_rank": quantiles(worst_selected_ranks),
        "queries_all_selected_in_top256": top256_complete,
        "within_primary_radius": {
            str(radius): {"count": count, "fraction": count / total_selected}
            for radius, count in distance_counts.items()
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--scores", type=Path, required=True)
    parser.add_argument("--primary", type=Path, required=True)
    parser.add_argument("--rows", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.raw, args.scores, args.primary, args.rows), sort_keys=True))


if __name__ == "__main__":
    main()
