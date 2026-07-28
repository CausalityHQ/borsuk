#!/usr/bin/env python3
"""Paired promotion analysis for the adaptive cell-WAL layout candidate."""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import statistics
from pathlib import Path
from typing import Iterable

RATIO_METRICS = (
    "wal_bytes",
    "ingest_ms",
    "first_query_ms",
    "warm_query_p95_ms",
    "warm_query_p99_ms",
    "flush_ms",
    "peak_rss_bytes",
    "cpu_core_ms",
)


def finite_nonnegative(row: dict[str, str], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid {field}={row.get(field)!r}") from error
    if value < 0 or value != value:
        raise ValueError(f"{field} must be finite and non-negative")
    return value


def exact_bootstrap_median_interval(
    values: list[float], lower: float = 0.025, upper: float = 0.975
) -> tuple[float, float]:
    if not values:
        raise ValueError("cannot bootstrap an empty sample")
    medians = sorted(
        statistics.median(sample)
        for sample in itertools.product(values, repeat=len(values))
    )

    def quantile(probability: float) -> float:
        position = (len(medians) - 1) * probability
        low = int(position)
        high = min(low + 1, len(medians) - 1)
        fraction = position - low
        return medians[low] * (1.0 - fraction) + medians[high] * fraction

    return quantile(lower), quantile(upper)


def paired_rows(
    rows: Iterable[dict[str, str]], repetitions: int
) -> dict[tuple[str, str], list[tuple[dict[str, str], dict[str, str]]]]:
    cases: dict[tuple[str, str, str], dict[str, dict[str, str]]] = {}
    for row in rows:
        key = (row["repetition_id"], row["workload"], row["backend"])
        arm = row["arm"]
        if arm in cases.setdefault(key, {}):
            raise ValueError(f"duplicate arm {arm} for pair {key}")
        cases[key][arm] = row

    grouped: dict[tuple[str, str], list[tuple[dict[str, str], dict[str, str]]]] = {}
    for key, arms in cases.items():
        if set(arms) != {"fixed-parquet", "adaptive-candidate"}:
            raise ValueError(f"incomplete paired arms for {key}: {sorted(arms)}")
        grouped.setdefault((key[1], key[2]), []).append(
            (arms["fixed-parquet"], arms["adaptive-candidate"])
        )
    for group, pairs in grouped.items():
        if len(pairs) != repetitions:
            raise ValueError(
                f"{group} has {len(pairs)} paired repetitions; expected {repetitions}"
            )
        pairs.sort(key=lambda pair: pair[0]["repetition_id"])
    return grouped


def analyze(cases_path: Path, protocol_path: Path) -> tuple[list[dict[str, str]], bool]:
    with cases_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    repetitions = int(protocol["repetitions"])
    gates = protocol["promotion_gates"]
    grouped = paired_rows(rows, repetitions)
    expected_groups = len(protocol["workloads"]) * len(protocol["backends"])
    if len(grouped) != expected_groups:
        raise ValueError(
            f"qualification has {len(grouped)} groups; expected {expected_groups}"
        )

    decisions: list[dict[str, str]] = []
    for (workload, backend), pairs in sorted(grouped.items()):
        expected_formats = {
            candidate["expected_candidate_format"] for _, candidate in pairs
        }
        if len(expected_formats) != 1:
            raise ValueError(f"{workload}/{backend}: inconsistent expected formats")
        expected_format = expected_formats.pop()
        ratios: dict[str, list[float]] = {}
        for metric in RATIO_METRICS:
            metric_ratios = []
            for baseline, candidate in pairs:
                denominator = finite_nonnegative(baseline, metric)
                numerator = finite_nonnegative(candidate, metric)
                if denominator <= 0:
                    raise ValueError(f"{workload}/{backend}: zero baseline {metric}")
                metric_ratios.append(numerator / denominator)
            ratios[metric] = metric_ratios

        medians = {
            metric: statistics.median(metric_ratios)
            for metric, metric_ratios in ratios.items()
        }
        intervals = {
            metric: exact_bootstrap_median_interval(metric_ratios)
            for metric, metric_ratios in ratios.items()
        }
        ingest_pass = medians["ingest_ms"] <= float(
            gates["maximum_candidate_to_baseline_ingest_median_ratio"]
        )
        warm_pass = medians["warm_query_p95_ms"] <= float(
            gates["maximum_candidate_to_baseline_warm_query_p95_median_ratio"]
        )
        warm_p99_pass = medians["warm_query_p99_ms"] <= float(
            gates["maximum_candidate_to_baseline_warm_query_p99_median_ratio"]
        )
        flush_pass = medians["flush_ms"] <= float(
            gates["maximum_candidate_to_baseline_flush_median_ratio"]
        )
        peak_rss_pass = medians["peak_rss_bytes"] <= float(
            gates["maximum_candidate_to_baseline_peak_rss_median_ratio"]
        )
        cpu_pass = medians["cpu_core_ms"] <= float(
            gates["maximum_candidate_to_baseline_cpu_core_ms_median_ratio"]
        )
        if expected_format == "vortex":
            bytes_pass = medians["wal_bytes"] <= float(
                gates["maximum_vortex_candidate_to_baseline_wal_bytes_median_ratio"]
            )
            first_query_pass = medians["first_query_ms"] <= float(
                gates["maximum_vortex_candidate_to_baseline_first_query_median_ratio"]
            )
            first_query_confidence_pass = intervals["first_query_ms"][1] <= float(
                gates[
                    "maximum_vortex_candidate_to_baseline_first_query_bootstrap_high_95"
                ]
            )
        elif expected_format == "parquet":
            maximum_difference = int(
                gates["maximum_parquet_control_wal_bytes_difference"]
            )
            bytes_pass = all(
                abs(int(candidate["wal_bytes"]) - int(baseline["wal_bytes"]))
                <= maximum_difference
                for baseline, candidate in pairs
            )
            first_query_pass = True
            first_query_confidence_pass = True
        else:
            raise ValueError(
                f"{workload}/{backend}: unexpected format {expected_format!r}"
            )
        group_pass = all(
            (
                ingest_pass,
                warm_pass,
                warm_p99_pass,
                flush_pass,
                peak_rss_pass,
                cpu_pass,
                bytes_pass,
                first_query_pass,
                first_query_confidence_pass,
            )
        )
        decision: dict[str, str] = {
            "scope": "workload-backend",
            "workload": workload,
            "backend": backend,
            "expected_candidate_format": expected_format,
            "paired_repetitions": str(len(pairs)),
            "ingest_gate_pass": str(ingest_pass).lower(),
            "warm_query_gate_pass": str(warm_pass).lower(),
            "warm_query_p99_gate_pass": str(warm_p99_pass).lower(),
            "flush_gate_pass": str(flush_pass).lower(),
            "peak_rss_gate_pass": str(peak_rss_pass).lower(),
            "cpu_gate_pass": str(cpu_pass).lower(),
            "wal_bytes_gate_pass": str(bytes_pass).lower(),
            "first_query_gate_pass": str(first_query_pass).lower(),
            "first_query_confidence_gate_pass": str(
                first_query_confidence_pass
            ).lower(),
            "promotion_gate_pass": str(group_pass).lower(),
        }
        for metric in RATIO_METRICS:
            low, high = intervals[metric]
            decision[f"{metric}_median_ratio"] = f"{medians[metric]:.9f}"
            decision[f"{metric}_bootstrap_low_95"] = f"{low:.9f}"
            decision[f"{metric}_bootstrap_high_95"] = f"{high:.9f}"
        decisions.append(decision)

    promotion = all(row["promotion_gate_pass"] == "true" for row in decisions)
    decisions.append(
        {
            "scope": "global",
            "workload": "all",
            "backend": "all",
            "expected_candidate_format": "mixed",
            "paired_repetitions": str(
                sum(int(row["paired_repetitions"]) for row in decisions)
            ),
            "ingest_gate_pass": str(
                all(row["ingest_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "warm_query_gate_pass": str(
                all(row["warm_query_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "warm_query_p99_gate_pass": str(
                all(row["warm_query_p99_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "flush_gate_pass": str(
                all(row["flush_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "peak_rss_gate_pass": str(
                all(row["peak_rss_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "cpu_gate_pass": str(
                all(row["cpu_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "wal_bytes_gate_pass": str(
                all(row["wal_bytes_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "first_query_gate_pass": str(
                all(row["first_query_gate_pass"] == "true" for row in decisions)
            ).lower(),
            "first_query_confidence_gate_pass": str(
                all(
                    row["first_query_confidence_gate_pass"] == "true"
                    for row in decisions
                )
            ).lower(),
            "promotion_gate_pass": str(promotion).lower(),
            **{
                f"{metric}_{suffix}": ""
                for metric in RATIO_METRICS
                for suffix in (
                    "median_ratio",
                    "bootstrap_low_95",
                    "bootstrap_high_95",
                )
            },
        }
    )
    return decisions, promotion


def write_decisions(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows, promotion = analyze(args.cases, args.protocol)
    write_decisions(args.output, rows)
    print(
        "WAL layout promotion decision: "
        + ("promote adaptive candidate" if promotion else "retain Parquet baseline")
    )


if __name__ == "__main__":
    main()
