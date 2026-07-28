#!/usr/bin/env python3
"""Paired hierarchical-bootstrap decisions for direct publication evidence."""

from __future__ import annotations

import argparse
import csv
import random
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class DirectDecision:
    repetitions: int
    queries_per_repetition: int
    borsuk_p95_ms: float
    s3_p95_ms: float
    latency_ratio: float
    latency_ratio_ci_low: float
    latency_ratio_ci_high: float
    borsuk_p99_ms: float
    s3_p99_ms: float
    p99_latency_ratio: float
    p99_latency_ratio_ci_low: float
    p99_latency_ratio_ci_high: float
    recall_difference: float
    recall_difference_ci_low: float
    recall_difference_ci_high: float
    claim: str


def _nearest_rank(values: Iterable[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot summarize empty evidence")
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


def _normalize(rows: list[dict[str, object]], label: str):
    normalized = {}
    for row in rows:
        if str(row.get("status", "ok")) != "ok":
            raise ValueError(f"{label} contains failed requests")
        query_id = row.get(
            "query_position",
            row.get("sample_index", row.get("query_source_index")),
        )
        if query_id is None:
            raise ValueError(f"{label} is missing a paired query identifier")
        source_id = row.get("query_source_index")
        if source_id is None:
            raise ValueError(f"{label} is missing a query source index")
        key = (str(row["repetition_id"]), int(query_id))
        if key in normalized:
            raise ValueError(f"{label} contains duplicate paired query rows")
        normalized[key] = (
            float(row["latency_ms"]),
            float(row["recall_at_10"]),
            int(source_id),
        )
    return normalized


def compare_direct(
    borsuk_rows: list[dict[str, object]],
    s3_rows: list[dict[str, object]],
    *,
    seed: int = 17,
    bootstrap_samples: int = 2000,
    expected_repetitions: int | None = None,
    expected_queries_per_repetition: int | None = None,
) -> DirectDecision:
    if bootstrap_samples < 100:
        raise ValueError("bootstrap_samples must be at least 100")
    borsuk = _normalize(borsuk_rows, "borsuk")
    s3 = _normalize(s3_rows, "amazon-s3-vectors")
    if set(borsuk) != set(s3):
        raise ValueError("direct evidence must contain paired query identifiers")
    if any(borsuk[key][2] != s3[key][2] for key in borsuk):
        raise ValueError(
            "direct evidence paired positions select different source queries"
        )
    repetitions = sorted({key[0] for key in borsuk})
    if len(repetitions) < 3:
        raise ValueError("direct evidence requires at least three repetitions")
    if expected_repetitions is not None and len(repetitions) != expected_repetitions:
        raise ValueError(
            f"direct evidence requires exactly {expected_repetitions} repetitions"
        )
    queries_by_repetition = {
        repetition: sorted(key[1] for key in borsuk if key[0] == repetition)
        for repetition in repetitions
    }
    sizes = {len(queries) for queries in queries_by_repetition.values()}
    if len(sizes) != 1 or not sizes or next(iter(sizes)) == 0:
        raise ValueError(
            "every repetition must contain the same non-empty query cohort"
        )
    queries_per_repetition = next(iter(sizes))
    if (
        expected_queries_per_repetition is not None
        and queries_per_repetition != expected_queries_per_repetition
    ):
        raise ValueError(
            "direct evidence has the wrong number of queries per repetition: "
            f"{queries_per_repetition} != {expected_queries_per_repetition}"
        )

    borsuk_latencies = [value[0] for value in borsuk.values()]
    s3_latencies = [value[0] for value in s3.values()]
    borsuk_p95 = _nearest_rank(borsuk_latencies, 0.95)
    s3_p95 = _nearest_rank(s3_latencies, 0.95)
    borsuk_p99 = _nearest_rank(borsuk_latencies, 0.99)
    s3_p99 = _nearest_rank(s3_latencies, 0.99)
    if s3_p95 <= 0:
        raise ValueError("S3 Vectors p95 must be positive")
    if s3_p99 <= 0:
        raise ValueError("S3 Vectors p99 must be positive")
    recall_difference = statistics.fmean(
        borsuk[key][1] - s3[key][1] for key in sorted(borsuk)
    )

    rng = random.Random(seed)
    latency_ratios = []
    p99_latency_ratios = []
    recall_differences = []
    for _ in range(bootstrap_samples):
        selected_repetitions = rng.choices(repetitions, k=len(repetitions))
        sampled_borsuk_latency = []
        sampled_s3_latency = []
        sampled_recall_difference = []
        for repetition in selected_repetitions:
            queries = queries_by_repetition[repetition]
            for query_id in rng.choices(queries, k=len(queries)):
                key = (repetition, query_id)
                sampled_borsuk_latency.append(borsuk[key][0])
                sampled_s3_latency.append(s3[key][0])
                sampled_recall_difference.append(borsuk[key][1] - s3[key][1])
        sampled_s3_p95 = _nearest_rank(sampled_s3_latency, 0.95)
        latency_ratios.append(
            _nearest_rank(sampled_borsuk_latency, 0.95) / sampled_s3_p95
        )
        sampled_s3_p99 = _nearest_rank(sampled_s3_latency, 0.99)
        p99_latency_ratios.append(
            _nearest_rank(sampled_borsuk_latency, 0.99) / sampled_s3_p99
        )
        recall_differences.append(statistics.fmean(sampled_recall_difference))

    latency_low = _nearest_rank(latency_ratios, 0.025)
    latency_high = _nearest_rank(latency_ratios, 0.975)
    p99_latency_low = _nearest_rank(p99_latency_ratios, 0.025)
    p99_latency_high = _nearest_rank(p99_latency_ratios, 0.975)
    recall_low = _nearest_rank(recall_differences, 0.025)
    recall_high = _nearest_rank(recall_differences, 0.975)
    claim = (
        "lower-latency-at-matched-recall"
        if latency_high < 1.0 and recall_low >= 0.0
        else "no-superiority-claim"
    )
    return DirectDecision(
        repetitions=len(repetitions),
        queries_per_repetition=queries_per_repetition,
        borsuk_p95_ms=borsuk_p95,
        s3_p95_ms=s3_p95,
        latency_ratio=borsuk_p95 / s3_p95,
        latency_ratio_ci_low=latency_low,
        latency_ratio_ci_high=latency_high,
        borsuk_p99_ms=borsuk_p99,
        s3_p99_ms=s3_p99,
        p99_latency_ratio=borsuk_p99 / s3_p99,
        p99_latency_ratio_ci_low=p99_latency_low,
        p99_latency_ratio_ci_high=p99_latency_high,
        recall_difference=recall_difference,
        recall_difference_ci_low=recall_low,
        recall_difference_ci_high=recall_high,
        claim=claim,
    )


def _read(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--borsuk", type=Path, required=True)
    parser.add_argument("--s3-vectors", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset", default="fashion-mnist-784")
    parser.add_argument("--cache-pair", default="first-pass")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    parser.add_argument("--expected-repetitions", type=int, default=5)
    parser.add_argument("--expected-queries", type=int, default=1000)
    args = parser.parse_args()
    decision = compare_direct(
        _read(args.borsuk),
        _read(args.s3_vectors),
        seed=args.seed,
        bootstrap_samples=args.bootstrap_samples,
        expected_repetitions=args.expected_repetitions,
        expected_queries_per_repetition=args.expected_queries,
    )
    row = {"dataset": args.dataset, "cache_pair": args.cache_pair, **asdict(decision)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
