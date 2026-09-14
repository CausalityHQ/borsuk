#!/usr/bin/env python3
"""Render the V63 layout-oracle result as a readable screen table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

MIB = 1024 * 1024


def percent(ppm: int) -> str:
    return f"{ppm / 10_000:.3f}"


def entry_at(cell: dict, pages: int) -> dict | None:
    for entry in cell["curve"]:
        if entry["pages"] == pages:
            return entry
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("--budget", type=int, default=32)
    parser.add_argument("--cost-model", default="sq8_plain")
    args = parser.parse_args()
    result = json.loads(args.result.read_text())

    print(f"schema              {result['schema']}")
    print(f"evidence            {result['evidence_kind']}")
    print(
        f"corpus              {result['source_rows']} rows x "
        f"{result['dimensions']}d, {result['queries']} queries, "
        f"top-{result['neighbors']}"
    )
    gate = result["gate"]
    print(
        f"gate                <= {gate['pages']} pages and "
        f"{gate['bytes'] / MIB:.0f} MiB ({gate['cost_model']}), "
        f">= {percent(gate['aggregate_ppm'])}% aggregate, "
        f">= {percent(gate['worst_query_ppm'])}% worst query"
    )
    for partition in result.get("partitions", []):
        print(
            f"partition           {partition['layout']}: "
            f"{partition['clusters']} clusters, "
            f"{partition['empty_clusters']} empty, rows/cluster "
            f"{partition['minimum_primary_cluster_rows']}/"
            f"{partition['median_primary_cluster_rows']}/"
            f"{partition['maximum_primary_cluster_rows']}, "
            f"{partition['train_and_assign_seconds']}s"
        )
    print()

    header = (
        f"{'family':<11}{'layout':<14}{'rep':>4}{'rows/pg':>8}"
        f"{'agg%':>9}{'worst%':>8}{'MiB':>8}{'store x':>9}{'gate pages':>11}"
    )
    print(header)
    print("-" * len(header))
    rows = sorted(
        result["cells"],
        key=lambda cell: -(entry_at(cell, args.budget) or {"aggregate_oracle_recall_ppm": -1})[
            "aggregate_oracle_recall_ppm"
        ],
    )
    for cell in rows:
        entry = entry_at(cell, args.budget)
        if entry is None:
            continue
        print(
            f"{cell['family']:<11}{cell['layout']:<14}{cell['replication']:>4}"
            f"{cell['page_rows']:>8}"
            f"{percent(entry['aggregate_oracle_recall_ppm']):>9}"
            f"{percent(entry['worst_query_oracle_recall_ppm']):>8}"
            f"{entry['bytes'][args.cost_model] / MIB:>8.2f}"
            f"{cell['storage_multiplier_x1000'] / 1000:>9.2f}"
            f"{str(cell['gate_passing_pages'] or '-'):>11}"
        )

    best = result["best_cell_at_gate_budget"]
    print()
    print(f"best at {args.budget} pages   {json.dumps(best, sort_keys=True)}")
    print(f"promoted cells      {len(result['promoted_cells'])}")
    print(f"next action         {result['next_action']}")

    print()
    print("page-budget curve for the best routed cell")
    routed = [cell for cell in result["cells"] if cell["family"] == "routed"]
    if routed:
        top = max(
            routed,
            key=lambda cell: (entry_at(cell, args.budget) or {"aggregate_oracle_recall_ppm": -1})[
                "aggregate_oracle_recall_ppm"
            ],
        )
        print(
            f"  {top['layout']} replication={top['replication']} "
            f"rows/page={top['page_rows']} storage x"
            f"{top['storage_multiplier_x1000'] / 1000:.2f}"
        )
        print(f"  {'pages':>6}{'rows':>9}{'agg%':>9}{'worst%':>8}{'MiB':>8}")
        for entry in top["curve"]:
            print(
                f"  {entry['pages']:>6}{entry['rows_scanned']:>9}"
                f"{percent(entry['aggregate_oracle_recall_ppm']):>9}"
                f"{percent(entry['worst_query_oracle_recall_ppm']):>8}"
                f"{entry['bytes'][args.cost_model] / MIB:>8.2f}"
            )


if __name__ == "__main__":
    main()
