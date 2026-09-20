#!/usr/bin/env python3
"""Derive paired V85 centroid/PQ16 quality from immutable raw samples."""

from __future__ import annotations

from typing import Any

import numpy as np


def _digest(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} digest differs")
    return value


def _nearest_rank(values: list[int], numerator: int, denominator: int) -> int:
    ordered = sorted(values)
    index = max(0, (len(ordered) * numerator + denominator - 1) // denominator - 1)
    return ordered[index]


def _paired_ci(delta: list[int], denominator: int) -> list[int]:
    values = np.asarray(delta, dtype=np.int16)
    generator = np.random.default_rng(85_100_000)
    draws = generator.choice(values, size=(10_000, len(values)), replace=True).mean(
        axis=1
    )
    lower, upper = np.quantile(draws, [0.025, 0.975], method="nearest")
    return [
        int(lower * 1_000_000 // denominator),
        int(upper * 1_000_000 // denominator),
    ]


def summarize_paired_rescore(
    centroid: Any,
    pq16: Any,
    *,
    centroid_sha256: str,
    pq16_sha256: str,
) -> dict[str, Any]:
    """Recompute paired quality with PQ16's authenticated truth as authority."""

    _digest(centroid_sha256, "centroid")
    _digest(pq16_sha256, "PQ16")
    if not isinstance(centroid, dict) or not isinstance(pq16, dict):
        raise ValueError("paired rescore result differs")
    centroid_samples = centroid.get("samples")
    pq16_samples = pq16.get("samples")
    if (
        not isinstance(centroid_samples, list)
        or not centroid_samples
        or not isinstance(pq16_samples, list)
        or len(centroid_samples) != len(pq16_samples)
    ):
        raise ValueError("paired rescore query count differs")

    centroid10 = []
    centroid100 = []
    pq10 = []
    pq100 = []
    centroid_latencies = []
    for query, (left, right) in enumerate(
        zip(centroid_samples, pq16_samples, strict=True)
    ):
        if left.get("query") != query or right.get("query") != query:
            raise ValueError("paired rescore query ordinal differs")
        truth = right.get("truth_ids")
        result_ids = left.get("result_ids")
        hit_ids = right.get("hit_ids")
        hit10_ids = right.get("hit10_ids")
        if (
            not isinstance(truth, list)
            or len(truth) != 100
            or len(set(truth)) != 100
            or not isinstance(result_ids, list)
            or len(result_ids) != 100
            or len(set(result_ids)) != 100
            or not isinstance(hit_ids, list)
            or not isinstance(hit10_ids, list)
        ):
            raise ValueError("paired rescore sample differs")
        left100 = len(set(result_ids).intersection(truth))
        left10 = len(set(result_ids[:10]).intersection(truth[:10]))
        right100 = len(set(hit_ids).intersection(truth))
        right10 = len(set(hit10_ids).intersection(truth[:10]))
        if (
            left.get("hits") != left100
            or right.get("hits") != right100
            or right.get("hits10") != right10
            or type(left.get("bytes")) is not int
            or left["bytes"] <= 0
            or type(left.get("requests")) is not int
            or left["requests"] <= 0
            or type(left.get("latency_ns")) is not int
            or left["latency_ns"] <= 0
        ):
            raise ValueError("paired rescore derived evidence differs")
        centroid10.append(left10)
        centroid100.append(left100)
        pq10.append(right10)
        pq100.append(right100)
        centroid_latencies.append(left["latency_ns"])

    queries = len(centroid_samples)
    pq_average10 = sum(pq10) * 1_000_000 // (queries * 10)
    pq_average100 = sum(pq100) * 1_000_000 // (queries * 100)
    pq_p05 = _nearest_rank([hits * 10_000 for hits in pq100], 5, 100)
    if (
        pq16.get("average_recall10_ppm") != pq_average10
        or pq16.get("average_recall100_ppm") != pq_average100
        or pq16.get("p05_recall100_ppm") != pq_p05
    ):
        raise ValueError("paired rescore PQ16 aggregate differs")

    return {
        "centroid_average_recall10_ppm": sum(centroid10) * 1_000_000 // (queries * 10),
        "centroid_average_recall100_ppm": sum(centroid100)
        * 1_000_000
        // (queries * 100),
        "centroid_max_bytes_per_query": max(
            sample["bytes"] for sample in centroid_samples
        ),
        "centroid_max_gets_per_query": max(
            sample["requests"] for sample in centroid_samples
        ),
        "centroid_p50_latency_ns": _nearest_rank(centroid_latencies, 50, 100),
        "centroid_p95_latency_ns": _nearest_rank(centroid_latencies, 95, 100),
        "centroid_p99_latency_ns": _nearest_rank(centroid_latencies, 99, 100),
        "centroid_result_sha256": centroid_sha256,
        "paired_recall10_delta_ci95_ppm": _paired_ci(
            np.subtract(pq10, centroid10).tolist(), 10
        ),
        "paired_recall100_delta_ci95_ppm": _paired_ci(
            np.subtract(pq100, centroid100).tolist(), 100
        ),
        "pq16_average_recall10_ppm": pq_average10,
        "pq16_average_recall100_ppm": pq_average100,
        "pq16_gate_passed": pq16.get("gate_passed") is True,
        "pq16_p05_recall100_ppm": pq_p05,
        "pq16_result_sha256": pq16_sha256,
        "queries": queries,
        "schema": "borsuk-v85-pq16-100k-rescore-summary-v1",
    }


def _pq_evidence(
    result: Any, rotation: str, *, allow_legacy_identity: bool = False
) -> tuple[list[int], list[int]]:
    if not isinstance(result, dict) or (
        result.get("rotation") != rotation
        and not (
            allow_legacy_identity
            and rotation == "identity"
            and "rotation" not in result
            and result.get("schema") == "borsuk-v85-pq16-page-nomination-result-v2"
        )
    ):
        raise ValueError(f"{rotation} PQ16 result differs")
    samples = result.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError(f"{rotation} PQ16 samples differ")
    hits10: list[int] = []
    hits100: list[int] = []
    for query, sample in enumerate(samples):
        truth = sample.get("truth_ids")
        hit10_ids = sample.get("hit10_ids")
        hit_ids = sample.get("hit_ids")
        if (
            sample.get("query") != query
            or not isinstance(truth, list)
            or len(truth) != 100
            or len(set(truth)) != 100
            or not isinstance(hit10_ids, list)
            or not isinstance(hit_ids, list)
            or len(set(hit10_ids)) != len(hit10_ids)
            or len(set(hit_ids)) != len(hit_ids)
            or not set(hit10_ids).issubset(truth[:10])
            or not set(hit_ids).issubset(truth)
            or sample.get("hits10") != len(hit10_ids)
            or sample.get("hits") != len(hit_ids)
            or type(sample.get("gets")) is not int
            or sample["gets"] <= 0
            or type(sample.get("bytes")) is not int
            or sample["bytes"] <= 0
        ):
            raise ValueError(f"{rotation} PQ16 sample differs")
        hits10.append(len(hit10_ids))
        hits100.append(len(hit_ids))
    queries = len(samples)
    average10 = sum(hits10) * 1_000_000 // (queries * 10)
    average100 = sum(hits100) * 1_000_000 // (queries * 100)
    p05 = _nearest_rank([hits * 10_000 for hits in hits100], 5, 100)
    max_gets = max(sample["gets"] for sample in samples)
    max_bytes = max(sample["bytes"] for sample in samples)
    gate = (
        average10 >= 960_000
        and average100 >= 975_000
        and p05 >= 900_000
        and max_gets <= 32
        and max_bytes <= 16 * 1024 * 1024
    )
    if (
        result.get("average_recall10_ppm") != average10
        or result.get("average_recall100_ppm") != average100
        or result.get("p05_recall100_ppm") != p05
        or result.get("max_gets_per_query") != max_gets
        or result.get("max_bytes_per_query") != max_bytes
        or result.get("gate_passed") is not gate
    ):
        raise ValueError(f"{rotation} PQ16 aggregate differs")
    return hits10, hits100


def summarize_paired_pq16_rotation(
    baseline: Any,
    challenger: Any,
    *,
    baseline_sha256: str,
    challenger_sha256: str,
) -> dict[str, Any]:
    """Recompute the one fixed SRHT arm against immutable identity evidence."""

    _digest(baseline_sha256, "identity PQ16")
    _digest(challenger_sha256, "SRHT PQ16")
    baseline10, baseline100 = _pq_evidence(
        baseline, "identity", allow_legacy_identity=True
    )
    challenger10, challenger100 = _pq_evidence(challenger, "srht")
    baseline_samples = baseline["samples"]
    challenger_samples = challenger["samples"]
    if len(baseline_samples) != len(challenger_samples):
        raise ValueError("paired PQ16 rotation query count differs")
    for query, (left, right) in enumerate(
        zip(baseline_samples, challenger_samples, strict=True)
    ):
        if left["query"] != query or right["query"] != query:
            raise ValueError("paired PQ16 rotation query ordinal differs")
        if left["truth_ids"] != right["truth_ids"]:
            raise ValueError("paired PQ16 rotation truth differs")
    delta10 = np.subtract(challenger10, baseline10).tolist()
    delta100 = np.subtract(challenger100, baseline100).tolist()
    ci10 = _paired_ci(delta10, 10)
    ci100 = _paired_ci(delta100, 100)
    return {
        "baseline_average_recall10_ppm": baseline["average_recall10_ppm"],
        "baseline_average_recall100_ppm": baseline["average_recall100_ppm"],
        "baseline_p05_recall100_ppm": baseline["p05_recall100_ppm"],
        "baseline_result_sha256": baseline_sha256,
        "challenger_average_recall10_ppm": challenger["average_recall10_ppm"],
        "challenger_average_recall100_ppm": challenger["average_recall100_ppm"],
        "challenger_p05_recall100_ppm": challenger["p05_recall100_ppm"],
        "challenger_promoted": challenger["gate_passed"] is True
        and ci10[0] >= 0
        and ci100[0] >= 0,
        "challenger_result_sha256": challenger_sha256,
        "paired_recall10_delta_ci95_ppm": ci10,
        "paired_recall100_delta_ci95_ppm": ci100,
        "queries": len(baseline_samples),
        "schema": "borsuk-v85-pq16-srht-summary-v1",
    }
