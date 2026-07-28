#!/usr/bin/env python3
"""Evaluate graph-default promotion gates from consolidated AWS evidence."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

EXPECTED_REPETITIONS = {1, 2, 3}
RECALL_TOLERANCE = 0.001
RSS_RELATIVE_LIMIT = 1.20
MULTI_SECOND_MS = 2_000.0


@dataclass(frozen=True)
class PromotionDecision:
    dataset: str
    passed: bool
    reasons: tuple[str, ...]


def _as_int(row: dict[str, Any], key: str) -> int:
    return int(row[key])


def _as_float(row: dict[str, Any], key: str) -> float:
    return float(row[key])


def evaluate_dataset(rows: Iterable[dict[str, Any]]) -> PromotionDecision:
    rows = list(rows)
    dataset = str(rows[0]["dataset"]) if rows else "<missing>"
    reasons: list[str] = []
    sources = {str(row["source_sha"]) for row in rows}
    if len(sources) != 1:
        reasons.append(f"source_sha mismatch: {sorted(sources)}")

    controls = {
        _as_int(row, "repetition"): row
        for row in rows
        if row["method"] == "pq-scan"
        and row["index_capability"] == "pq-scan-only"
        and row["profile"] == "production"
        and row["cache_state"] == "disk_cached"
    }
    graphs = {
        _as_int(row, "repetition"): row
        for row in rows
        if row["method"] == "graph"
        and row["index_capability"] == "graph-enabled"
        and row["profile"] == "production"
        and row["cache_state"] == "disk_cached"
    }
    if set(controls) != EXPECTED_REPETITIONS or set(graphs) != EXPECTED_REPETITIONS:
        reasons.append(
            "missing production repetitions: "
            f"pq={sorted(controls)} graph={sorted(graphs)} expected={sorted(EXPECTED_REPETITIONS)}"
        )

    for repetition in sorted(EXPECTED_REPETITIONS & set(controls) & set(graphs)):
        control = controls[repetition]
        graph = graphs[repetition]
        if _as_int(control, "queries") < 100 or _as_int(graph, "queries") < 100:
            reasons.append(f"repetition {repetition}: fewer than 100 queries")
        recall_loss = _as_float(control, "recall_at_10") - _as_float(
            graph, "recall_at_10"
        )
        if recall_loss > RECALL_TOLERANCE + 1.0e-12:
            reasons.append(
                f"repetition {repetition}: recall loss {recall_loss:.6f} exceeds 0.001"
            )
        if str(graph.get("meets_target", "false")).lower() != "true":
            reasons.append(
                f"repetition {repetition}: graph misses the corpus recall target"
            )
        if _as_int(graph, "max_candidates") >= _as_int(graph, "segment_max_vectors"):
            reasons.append(
                f"repetition {repetition}: selected graph row is a full-cell scan, not graph traversal"
            )
        for percentile in ("p95_ms", "p99_ms"):
            if _as_float(graph, percentile) > _as_float(control, percentile):
                reasons.append(
                    f"repetition {repetition}: graph {percentile} "
                    f"{_as_float(graph, percentile):.3f} exceeds pq-scan "
                    f"{_as_float(control, percentile):.3f}"
                )
        if _as_float(graph, "qps") < _as_float(control, "qps"):
            reasons.append(f"repetition {repetition}: graph capped throughput is lower")
        graph_rss = _as_int(graph, "peak_rss_bytes")
        control_rss = _as_int(control, "peak_rss_bytes")
        ram_budget = _as_int(graph, "ram_budget_bytes")
        if graph_rss > ram_budget:
            reasons.append(
                f"repetition {repetition}: graph RSS exceeds configured RAM budget"
            )
        if graph_rss > control_rss * RSS_RELATIVE_LIMIT:
            reasons.append(
                f"repetition {repetition}: graph RSS exceeds 1.20x pq-scan RSS"
            )
        if _as_int(graph, "max_concurrent_searches") != 4:
            reasons.append(f"repetition {repetition}: production query cap is not 4")
        if _as_int(graph, "max_concurrent_cell_decodes") != 24:
            reasons.append(f"repetition {repetition}: production decode cap is not 24")
        if (
            _as_float(graph, "network_gets") != 0
            or _as_float(graph, "network_bytes") != 0
        ):
            reasons.append(
                f"repetition {repetition}: disk-cached graph row performed network I/O"
            )
        if _as_float(graph, "max_ms") >= MULTI_SECOND_MS:
            reasons.append(
                f"repetition {repetition}: graph has a multi-second query outlier"
            )

    return PromotionDecision(
        dataset=dataset, passed=not reasons, reasons=tuple(reasons)
    )


def overall_decision(decisions: Iterable[PromotionDecision]) -> str:
    decisions = list(decisions)
    if decisions and all(decision.passed for decision in decisions):
        return "universal-graph"
    if any(decision.passed for decision in decisions):
        return "adaptive"
    return "keep-pq-scan"


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def evaluate_file(path: Path) -> tuple[str, list[PromotionDecision]]:
    rows = load_rows(path)
    datasets = sorted({row["dataset"] for row in rows})
    decisions = [
        evaluate_dataset(row for row in rows if row["dataset"] == dataset)
        for dataset in datasets
    ]
    return overall_decision(decisions), decisions


def write_decisions(
    overall: str, decisions: list[PromotionDecision], output_dir: Path
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "promotion-decision.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["dataset", "passed", "reasons"])
        for decision in decisions:
            writer.writerow(
                [
                    decision.dataset,
                    str(decision.passed).lower(),
                    " | ".join(decision.reasons),
                ]
            )
    (output_dir / "promotion-decision.json").write_text(
        json.dumps(
            {
                "overall": overall,
                "datasets": [asdict(decision) for decision in decisions],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    overall, decisions = evaluate_file(args.input)
    write_decisions(overall, decisions, args.output_dir)
    print(f"graph promotion decision: {overall}; datasets={len(decisions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
