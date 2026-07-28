#!/usr/bin/env python3
"""Render hybrid effectiveness/latency and mixed-cache SVGs from raw evidence."""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from render_cache_coverage_charts import render as render_cache
from render_recall_latency_charts import render as render_effectiveness


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9.-]+", "-", value.lower()).strip("-")


def artifact_path(experiment_root: Path, value: str) -> Path:
    path = Path(value)
    if path.exists() or path.is_absolute():
        return path
    candidate = experiment_root / path
    if candidate.exists():
        return candidate
    return path


def load_coverage(experiment_root: Path) -> list[dict[str, str]]:
    with (experiment_root / "coverage.csv").open(newline="") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if row["stage"] == "query" and row["status"] == "measured"
        ]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def effectiveness_groups(
    experiment_root: Path,
    coverage: list[dict[str, str]],
) -> dict[tuple[str, str, float], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, float], list[dict[str, Any]]] = defaultdict(list)
    for item in coverage:
        directory = artifact_path(experiment_root, item["artifact_dir"])
        for row in read_csv(directory / "hybrid_summary.csv"):
            hot_fraction = float(row["target_hot_query_fraction"])
            grouped[(item["dataset"], item["profile"], hot_fraction)].append(
                {
                    "candidate_depth": float(row["candidate_depth"]),
                    "max_segments": float(row["max_segments"]),
                    "ndcg_at_10": float(row["ndcg_at_10"]),
                    "recall_at_10": float(row["recall_at_10"]),
                    "precision_at_10": float(row["precision_at_10"]),
                    "mrr_at_10": float(row["mrr_at_10"]),
                    "p95_ms": float(row["p95_ms"]),
                    "mean_ms": float(row["mean_ms"]),
                    "stddev_ms": float(row["stddev_ms"]),
                    "campaign_repetition": item.get("campaign_repetition", ""),
                    "method": f"{row['mode']} · {row['fusion']}",
                    "label": (
                        f"c={int(float(row['candidate_depth']))}, "
                        f"p={int(float(row['max_segments']))}, "
                        f"{row['fusion']}"
                    ),
                }
            )
    return grouped


def cache_groups(
    experiment_root: Path,
    coverage: list[dict[str, str]],
) -> dict[tuple[str, str, str, int, int], list[dict[str, Any]]]:
    grouped: dict[
        tuple[str, str, str, int, int],
        list[dict[str, Any]],
    ] = defaultdict(list)
    for item in coverage:
        directory = artifact_path(experiment_root, item["artifact_dir"])
        key = (
            item["dataset"],
            item["profile"],
            item["mode"],
            int(item["candidate_depth"]),
            int(item["max_segments"]),
        )
        for row in read_csv(directory / "hybrid_queries.csv"):
            decoded = float(row["decoded_cache_bytes_read"])
            disk = float(row["disk_cache_bytes_read"])
            backing = float(row["backing_bytes_read"])
            total = decoded + disk + backing
            query_class = {
                "target-hot": "hot",
                "target-outside": "outside_hot_set",
            }.get(row["query_class"], row["query_class"])
            grouped[key].append(
                {
                    "target_hot_query_fraction": float(
                        row["target_hot_query_fraction"]
                    ),
                    "query_class": query_class,
                    "latency_ms": float(row["latency_ms"]),
                    "decoded_access_fraction": decoded / total if total else 0.0,
                    "disk_access_fraction": disk / total if total else 0.0,
                    "backing_access_fraction": backing / total if total else 0.0,
                }
            )
    return grouped


def render_all(experiment_root: Path, output_dir: Path) -> list[Path]:
    coverage = load_coverage(experiment_root)
    if not coverage:
        raise ValueError("hybrid coverage contains no measured query rows")
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []
    metrics = (
        ("ndcg_at_10", "nDCG@10", "nDCG", "effectiveness"),
        ("recall_at_10", "Recall@10", "recall", "recall"),
        ("precision_at_10", "Precision@10", "precision", "precision"),
        ("mrr_at_10", "MRR@10", "MRR", "mrr"),
    )
    for (dataset, profile, hot_fraction), rows in effectiveness_groups(
        experiment_root,
        coverage,
    ).items():
        for metric, axis_label, title_metric, prefix in metrics:
            chart_rows = [
                {
                    **row,
                    "recall_at_10": row[metric],
                }
                for row in rows
            ]
            name = f"{prefix}-{slug(dataset)}-{slug(profile)}-hot-{hot_fraction:g}.svg"
            path = output_dir / name
            path.write_text(
                render_effectiveness(
                    f"{dataset} · {profile} · hot={hot_fraction:g}",
                    chart_rows,
                    (
                        "shared human/oracle qrels · independent process/cache "
                        "repetitions · p95 curve"
                    ),
                    effectiveness_label=axis_label,
                    title_metric=title_metric,
                ),
                encoding="utf-8",
            )
            outputs.append(path)

    for (dataset, profile, mode, candidates, probes), rows in cache_groups(
        experiment_root,
        coverage,
    ).items():
        name = (
            f"cache-{slug(dataset)}-{slug(profile)}-{slug(mode)}-"
            f"c{candidates}-p{probes}.svg"
        )
        path = output_dir / name
        path.write_text(
            render_cache(
                rows,
                (
                    f"{dataset} · {profile} · {mode} · "
                    f"candidates={candidates} · probes={probes}"
                ),
            ),
            encoding="utf-8",
        )
        outputs.append(path)
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    for path in render_all(args.experiment_root, args.output_dir):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
